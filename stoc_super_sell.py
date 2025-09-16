import datetime
import logging
import os
import csv
import time
import json
import re
from collections import defaultdict, deque

from dotenv import load_dotenv
from fyers_apiv3 import fyersModel
from fyers_apiv3.FyersWebsocket import order_ws, data_ws

# ---------------- CONFIG ----------------
load_dotenv()

CLIENT_ID = os.getenv("FYERS_CLIENT_ID")
ACCESS_TOKEN = os.getenv("FYERS_ACCESS_TOKEN")
LOT_SIZE = int(os.getenv("LOT_SIZE", "75"))  # lot size for nifty50
TRADED_QTY = int(os.getenv("TRADED_QTY", "75"))  # quantity to trade
TICK_SIZE = 0.05  # NSE options tick size

TRADING_START = datetime.time(9,15)
TRADING_END = datetime.time(15, 15)

# --- Strategy config ---
MAX_RISK_POINTS = 20          # skip if entry - SL > 20

# Exit throttle (milliseconds) to avoid spamming cancel/market orders on rapid ticks
EXIT_THROTTLE_MS = int(os.getenv("EXIT_THROTTLE_MS", "500"))

# Logging verbosity can be controlled via env var, e.g., EXIT_LOG_LEVEL=DEBUG
EXIT_LOG_LEVEL = os.getenv("EXIT_LOG_LEVEL", "INFO").upper()

# Safety buffer for SL placement: leave at least one tick (or a few points) below LTP
MIN_SL_BUFFER = float(os.getenv("MIN_SL_BUFFER", "0.05"))  # smallest unit (tick)

# small pause after cancel before placing new SL to reduce race (seconds)
CANCEL_SETTLE_SEC = float(os.getenv("CANCEL_SETTLE_SEC", "0.06"))

# Reset logging handlers
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

logging.basicConfig(
    filename="nifty_suptrend_sto_strategy.log",
    level=getattr(logging, EXIT_LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# convenience logger
logger = logging.getLogger(__name__)

# ---------------- UTILS ----------------
def round_to_tick(price, tick_size=TICK_SIZE):
    """Round a price to the nearest tick size (default = 0.05)."""
    return round(round(price / tick_size) * tick_size, 2)


def _safe(v, fmt="{:.2f}"):
    try:
        return fmt.format(v)
    except Exception:
        return "None"

# ---------------- JOURNAL LOGGING ----------------
def log_trade(symbol, side, entry, sl, target, exit_price, pnl, file="trades.csv"):
    file_exists = os.path.isfile(file)
    with open(file, "a", newline="") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["date","symbol","side","entry","sl","target","exit","pnl"])
        writer.writerow([
            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            symbol, side, entry, sl, target, exit_price, pnl
        ])
# ---------------- EXPIRY & SYMBOL UTILS ----------------
def get_next_expiry():
    """Return expiry date (Thursday) for NIFTY options."""
    today = datetime.date.today()
    weekday = today.weekday()  # Monday=0 ... Sunday=6

    if weekday == 1:  # Thursday
        expiry = today
    else:
        days_to_thu = (1 - weekday) % 7
        expiry = today + datetime.timedelta(days=days_to_thu)
    return expiry

def format_expiry(expiry_date):
    """Format expiry date based on month rule (YYMDD if month<10, YYMMDD otherwise)."""
    yy = expiry_date.strftime("%y")
    m = expiry_date.month
    d = expiry_date.day
    if m < 10:
        return f"{yy}{m}{d:02d}"   # YYMDD
    else:
        return f"{yy}{m:02d}{d:02d}"  # YYMMDD

def get_atm_symbols(fyers_client):
    """Fetch NIFTY spot, round to ATM strike, and build CE/PE option symbols."""
    data = {"symbols": "NSE:NIFTY50-INDEX"}
    resp = fyers_client.client.quotes(data)
    if not resp.get("d"):
        raise Exception(f"Failed to fetch NIFTY spot: {resp}")

    ltp = float(resp["d"][0]["v"]["lp"])  # last traded price
    atm_strike = round(ltp / 100) * 100   # nearest 100

    expiry = get_next_expiry()
    expiry_str = format_expiry(expiry)
    ce_strike = atm_strike - 100
    pe_strike = atm_strike + 100
    ce_symbol = f"NSE:NIFTY{expiry_str}{ce_strike}CE"
    pe_symbol = f"NSE:NIFTY{expiry_str}{pe_strike}PE"

    print(f"[ATM SYMBOLS] CE={ce_symbol}, PE={pe_symbol}")
    logging.info(f"[ATM SYMBOLS] CE={ce_symbol}, PE={pe_symbol}")
    return [ce_symbol, pe_symbol]


# ---------------- FYERS CLIENT ----------------
class FyersClient:
    def __init__(self, client_id: str, access_token: str):
        self.client_id = client_id
        self.access_token = access_token
        self.auth_token = f"{client_id}:{access_token}"
        self.client = fyersModel.FyersModel(
            client_id=client_id,
            token=access_token,
            is_async=False,
            log_path=""
        )
        self.order_callbacks = []
        self.trade_callbacks = []

    # tag sanitizer placed inside client so logger is available
    def _sanitize_tag(self, tag: str, max_len: int = 20):
        """Remove any non-alphanumeric characters. Truncate to max_len.
        FYERS appears to accept only letters + digits in orderTag.
        """
        if not tag:
            return ""
        s = re.sub(r'[^A-Za-z0-9]', '', str(tag))
        if len(s) > max_len:
            s = s[:max_len]
        if s != tag:
            logger.warning(f"Order tag sanitized: original='{tag}' -> sanitized='{s}'")
        return s

    def _log_order_resp(self, action, resp):
        # Try to stringify resp for logs (safe): extract id/status/message if present
        try:
            if isinstance(resp, dict):
                # Try to surface common id/message keys into top-level for caller convenience
                resp_copy = dict(resp)  # shallow copy
                # nested common places
                if not resp_copy.get('id'):
                    if isinstance(resp_copy.get('raw'), dict) and resp_copy['raw'].get('id'):
                        resp_copy['id'] = resp_copy['raw']['id']
                    elif isinstance(resp_copy.get('orders'), dict) and resp_copy['orders'].get('id'):
                        resp_copy['id'] = resp_copy['orders']['id']
                if not resp_copy.get('message') and isinstance(resp_copy.get('raw'), dict):
                    resp_copy['message'] = resp_copy['raw'].get('message')
                info = {'id': resp_copy.get('id'), 'status': resp_copy.get('status'), 'message': resp_copy.get('message'), 'raw': resp_copy.get('raw') if 'raw' in resp_copy else resp_copy}
            else:
                info = {'raw': str(resp)}
            logger.info(f"{action} Response: {json.dumps(info)}")
            print(f"{action} Response: {info}")
        except Exception:
            logger.info(f"{action} Response: {resp}")
            print(f"{action} Response: {resp}")
        return resp

    def place_market_buy(self, symbol: str, qty: int, tag: str = ""):
        tag_clean = self._sanitize_tag(tag)
        data = {
            "symbol": symbol,
            "qty": qty,
            "type": 2,      # MARKET
            "side": 1,      # BUY
            "productType": "INTRADAY",
            "limitPrice": 0,
            "stopPrice": 0,
            "validity": "DAY",
            "disclosedQty": 0,
            "offlineOrder": False,
            "orderTag": tag_clean
        }
        resp = self.client.place_order(data)
        return self._log_order_resp("Market Buy", resp)

    def place_stoploss_buy(self, symbol: str, qty: int, stop_price: float, tag: str = ""):
        stop_price = round_to_tick(stop_price)
        tag_clean = self._sanitize_tag(tag)
        data = {
            "symbol": symbol,
            "qty": qty,
            "type": 3,  # SL-M
            "side": 1,  # BUY
            "productType": "INTRADAY",
            "limitPrice": 0,
            "stopPrice": stop_price,
            "validity": "DAY",
            "disclosedQty": 0,
            "offlineOrder": False,
            "orderTag": tag_clean
        }
        resp = self.client.place_order(data)
        return self._log_order_resp("SL Buy", resp)

    def place_market_sell(self, symbol: str, qty: int, tag: str = ""):
        tag_clean = self._sanitize_tag(tag)
        data = {
            "symbol": symbol,
            "qty": qty,
            "type": 2,  # MARKET
            "side": -1,  # SELL
            "productType": "INTRADAY",
            "limitPrice": 0,
            "stopPrice": 0,
            "validity": "DAY",
            "disclosedQty": 0,
            "offlineOrder": False,
            "orderTag": tag_clean
        }
        resp = self.client.place_order(data)
        return self._log_order_resp("Market Sell", resp)

    def cancel_order(self, order_id: str):
        try:
            resp = self.client.cancel_order({"id": order_id})
            return self._log_order_resp("Cancel Order", resp)
        except Exception as e:
            logger.error(f"Cancel order exception: {e}")
            raise

    def start_order_socket(self):
        def on_order(msg):
            logger.info(f"Order update: {msg}")
            for cb in self.order_callbacks:
                cb(msg)

        def on_trade(msg):
            logger.info(f"Trade update: {msg}")
            for cb in self.trade_callbacks:
                cb(msg)

        def on_open():
            fyers.subscribe(data_type="OnOrders,OnTrades")
            fyers.keep_running()

        fyers = order_ws.FyersOrderSocket(
            access_token=self.auth_token,
            write_to_file=False,
            log_path="",
            on_connect=on_open,
            on_close=lambda m: logger.info(f"Order socket closed: {m}"),
            on_error=lambda m: logger.error(f"Order socket error: {m}"),
            on_orders=on_order,
            on_trades=on_trade
        )
        fyers.connect()

    def register_order_callback(self, cb):
        self.order_callbacks.append(cb)

    def register_trade_callback(self, cb):
        self.trade_callbacks.append(cb)

    def subscribe_market_data(self, instrument_ids, on_candle_callback, on_tick_callback=None):
        """
        instrument_ids: list of symbols
        on_candle_callback(symbol, candle) -> called when a 3-min candle completes
        on_tick_callback(symbol, ltp, ts) -> called for every tick (immediate exit logic can live here)
        """
        candle_buffers = defaultdict(lambda: None)

        def on_message(tick):
            try:
                if "symbol" not in tick or "ltp" not in tick:
                    return
                symbol = tick["symbol"]
                ltp = float(tick["ltp"])
                ts = int(tick.get("last_traded_time", datetime.datetime.now().timestamp()))
                cum_vol = int(tick.get("vol_traded_today", 0))  # cumulative volume
            except Exception as e:
                logger.error(f"Bad tick: {tick}, {e}")
                return

            # First: call tick-level callback so strategy can react immediately (e.g., target touched)
            if on_tick_callback:
                try:
                    on_tick_callback(symbol, ltp, ts)
                except Exception as e:
                    logger.error(f"on_tick_callback error for {symbol}: {e}")

            # Then update the 3-min candle aggregation and possibly emit a completed candle
            dt = datetime.datetime.fromtimestamp(ts)
            bucket_minute = (dt.minute // 3) * 3
            candle_time = dt.replace(second=0, microsecond=0, minute=bucket_minute)

            c = candle_buffers[symbol]
            if c is None or c["time"] != candle_time:
                if c is not None:
                    # previous candle completed -> notify
                    try:
                        on_candle_callback(symbol, c)
                    except Exception as e:
                        logger.error(f"on_candle_callback error for {symbol}: {e}")
                candle_buffers[symbol] = {
                    "time": candle_time,
                    "open": ltp,
                    "high": ltp,
                    "low": ltp,
                    "close": ltp,
                    "volume": 0,
                    "start_cum_vol": cum_vol
                }
            else:
                c["high"] = max(c["high"], ltp)
                c["low"] = min(c["low"], ltp)
                c["close"] = ltp
                c["volume"] = max(0, cum_vol - c["start_cum_vol"])

        def on_open():
            fyers.subscribe(symbols=instrument_ids, data_type="SymbolUpdate")
            fyers.keep_running()

        fyers = data_ws.FyersDataSocket(
            access_token=self.auth_token,
            log_path="",
            on_connect=on_open,
            on_close=lambda m: logger.info(f"Data socket closed: {m}"),
            on_error=lambda m: logger.error(f"Data socket error: {m}"),
            on_message=on_message
        )
        fyers.connect()

# ---------------- STRATEGY ENGINE ----------------
class NiftyStrategy:
    def __init__(self, fyers_client: FyersClient, traded_qty: int = 75):
        self.fyers = fyers_client
        self.traded_qty = traded_qty
        self.candles = defaultdict(lambda: deque(maxlen=400))
        self.positions = {}       # Active positions per symbol

    # --- history prefill ---
    def prefill_history(self, symbols, days_back=1):
        for symbol in symbols:
            try:
                to_date = datetime.datetime.now().strftime("%Y-%m-%d")
                from_date = (datetime.datetime.now() - datetime.timedelta(days=days_back)).strftime("%Y-%m-%d")

                params = {
                    "symbol": symbol,
                    "resolution": "3",
                    "date_format": "1",
                    "range_from": from_date,
                    "range_to": to_date,
                    "cont_flag": "1"
                }
                hist = self.fyers.client.history(params)

                if hist.get("candles"):
                    for c in hist["candles"]:
                        ts = datetime.datetime.fromtimestamp(c[0])
                        candle = {
                            "time": ts,
                            "open": c[1],
                            "high": c[2],
                            "low": c[3],
                            "close": c[4],
                            "volume": c[5],
                        }
                        self.candles[symbol].append(candle)

                    closes = [c["close"] for c in self.candles[symbol]]
                    highs = [c["high"] for c in self.candles[symbol]]
                    lows = [c["low"] for c in self.candles[symbol]]
                    supertrend, trend = self.supertrend_series(highs, lows, closes)
                    sto_k, sto_d = self.stochastic_series(highs, lows, closes)
                    logger.info(f"[Init] {symbol} Supertrend={_safe(supertrend[-1])}, Trend={trend[-1]}, StoK={_safe(sto_k[-1])}, StoD={_safe(sto_d[-1])}")
            except Exception as e:
                logger.error(f"History fetch fail {symbol}: {e}")

    # --- indicator series ---
    def sma_series(self, series, period):
        n = len(series)
        sma = [None] * n
        for i in range(period - 1, n):
            valid_vals = [x for x in series[i - period + 1 : i + 1] if x is not None]
            if len(valid_vals) == period:
                sma[i] = sum(valid_vals) / period
        return sma

    def atr_series(self, highs, lows, closes, period=7):
        n = len(closes)
        tr = [None] * n
        for i in range(1, n):
            tr[i] = max(highs[i] - lows[i], abs(highs[i] - closes[i-1]), abs(lows[i] - closes[i-1]))
        atr = [None] * n
        if n < period + 1:
            return atr
        atr_sum = sum([t for t in tr[1:period+1] if t is not None])
        atr[period] = atr_sum / period
        for i in range(period + 1, n):
            atr[i] = (atr[i-1] * (period - 1) + tr[i]) / period
        return atr

    def supertrend_series(self, highs, lows, closes, atr_period=7, multiplier=3):
        n = len(closes)
        atr = self.atr_series(highs, lows, closes, atr_period)
        upper_band = [None] * n
        lower_band = [None] * n
        supertrend = [None] * n
        trend = [None] * n  # 1 for up (green), -1 for down (red)
        for i in range(atr_period, n):
            upper_band[i] = (highs[i] + lows[i]) / 2 + (multiplier * atr[i]) if atr[i] is not None else None
            lower_band[i] = (highs[i] + lows[i]) / 2 - (multiplier * atr[i]) if atr[i] is not None else None
            if supertrend[i-1] is None:
                supertrend[i] = upper_band[i]
                trend[i] = -1
            else:
                curr_trend = trend[i-1]
                if closes[i] < supertrend[i-1]:
                    curr_trend = -1
                elif closes[i] > supertrend[i-1]:
                    curr_trend = 1
                if curr_trend == 1:
                    if lower_band[i] is not None and lower_band[i-1] is not None:
                        lower_band[i] = max(lower_band[i], lower_band[i-1])
                    supertrend[i] = lower_band[i]
                else:
                    if upper_band[i] is not None and upper_band[i-1] is not None:
                        upper_band[i] = min(upper_band[i], upper_band[i-1])
                    supertrend[i] = upper_band[i]
                trend[i] = curr_trend
        return supertrend, trend

    def stochastic_series(self, highs, lows, closes, k_period=14, smooth_k=3, smooth_d=3):
        n = len(closes)
        fast_k = [None] * n
        for i in range(k_period - 1, n):
            h_max = max(highs[j] for j in range(i - k_period + 1, i + 1))
            l_min = min(lows[j] for j in range(i - k_period + 1, i + 1))
            denom = h_max - l_min
            if denom > 0:
                fast_k[i] = 100 * (closes[i] - l_min) / denom
            else:
                fast_k[i] = fast_k[i-1] if i > 0 and fast_k[i-1] is not None else 0
        sto_k = self.sma_series(fast_k, smooth_k)
        sto_d = self.sma_series(sto_k, smooth_d)
        return sto_k, sto_d

    # --- ORDER helpers for robust id extraction and safe SL update ---
    def _extract_order_id(self, order_resp):
        """Attempt to extract order id from broker response (which may be dict/string)."""
        try:
            if not order_resp:
                return None
            if isinstance(order_resp, dict):
                if order_resp.get("id"):
                    return str(order_resp.get("id"))
                if order_resp.get("orders") and isinstance(order_resp["orders"], dict) and order_resp["orders"].get("id"):
                    return str(order_resp["orders"].get("id"))
                raw = order_resp.get("raw")
                if isinstance(raw, dict) and raw.get("id"):
                    return str(raw.get("id"))
            if isinstance(order_resp, str):
                try:
                    j = json.loads(order_resp)
                    return j.get("id") or (j.get("raw") and j["raw"].get("id"))
                except Exception:
                    return None
        except Exception:
            return None
        return None

    # --- main candle handler ---
    def on_candle(self, symbol, candle):
        try:
            self.candles[symbol].append(candle)
            candles = list(self.candles[symbol])
            idx = len(candles) - 1

            closes = [c["close"] for c in candles]
            highs = [c["high"] for c in candles]
            lows = [c["low"] for c in candles]

            supertrend_arr, trend_arr = self.supertrend_series(highs, lows, closes)
            sto_k_arr, sto_d_arr = self.stochastic_series(highs, lows, closes)

            supertrend = supertrend_arr[idx]
            trend = trend_arr[idx]
            sto_k = sto_k_arr[idx]
            sto_d = sto_d_arr[idx]

            # --- SNAPSHOT: log indicators for all tracked symbols at this 3-min close ---
            try:
                snapshot = {s: {
                    'time': str(self.candles[s][-1]['time']) if len(self.candles[s])>0 else None,
                    'Supertrend': _safe(self.supertrend_series([c['high'] for c in self.candles[s]], [c['low'] for c in self.candles[s]], [c['close'] for c in self.candles[s]])[0][-1]) if len(self.candles[s])>0 else None,
                    'Trend': self.supertrend_series([c['high'] for c in self.candles[s]], [c['low'] for c in self.candles[s]], [c['close'] for c in self.candles[s]])[1][-1] if len(self.candles[s])>0 else None,
                    'StoK': _safe(self.stochastic_series([c['high'] for c in self.candles[s]], [c['low'] for c in self.candles[s]], [c['close'] for c in self.candles[s]])[0][-1]) if len(self.candles[s])>0 else None,
                    'StoD': _safe(self.stochastic_series([c['high'] for c in self.candles[s]], [c['low'] for c in self.candles[s]], [c['close'] for c in self.candles[s]])[1][-1]) if len(self.candles[s])>0 else None
                } for s in list(self.candles.keys())}
                logger.info(f"Indicators snapshot at {candle['time']}: {json.dumps(snapshot)}")
            except Exception as e:
                logger.debug(f"Failed to log indicators snapshot: {e}")

            if supertrend is None or trend is None or sto_k is None or sto_d is None:
                logger.debug(f"Not enough data for indicators for {symbol} at idx={idx}")
                return

            now = datetime.datetime.now().time()
            if not (TRADING_START <= now <= TRADING_END):
                logger.debug(f"Outside trading hours: now={now}")
                return

            pos = self.positions.get(symbol)

            # Log candle summary
            logger.info(json.dumps({
                'event': 'candle_close',
                'symbol': symbol,
                'time': str(candle['time']),
                'idx': idx,
                'open': candle['open'],
                'high': candle['high'],
                'low': candle['low'],
                'close': candle['close'],
                'volume': candle['volume'],
                'Supertrend': _safe(supertrend),
                'Trend': trend,
                'StoK': _safe(sto_k),
                'StoD': _safe(sto_d),
                'in_position': bool(pos)
            }))

            # Log specific values as required
            logger.info(f"After 3min close {candle['time']} for {symbol}: Supertrend(7,3)={_safe(supertrend)}, Stochastic K={_safe(sto_k)}, D={_safe(sto_d)}")

            # 1) If in a position: candle-close fallback
            if pos:
                target = pos["target"]
                if candle["low"] <= target:
                    if not pos.get("exit_initiated"):
                        self._attempt_target_close(symbol, pos, reason="candle_fallback")
                return  # don't seek new entries while in position

            # 2) Not in a position: check entry conditions
            is_red = trend == -1

            entry = round_to_tick(candle["close"])
            st_value = supertrend_arr[idx]

            if is_red and sto_k < sto_d and sto_k > 60:
                # Selling scenario (short)
                sl = round_to_tick(st_value + TICK_SIZE)
                risk = sl - entry
                if risk <= 0:
                    logger.info(json.dumps({'event': 'skip', 'symbol': symbol, 'reason': 'non_positive_risk', 'entry': entry, 'sl': sl}))
                    return
                if risk > MAX_RISK_POINTS:
                    logger.info(json.dumps({'event': 'skip', 'symbol': symbol, 'reason': 'risk_too_large', 'risk': risk, 'max_allowed': MAX_RISK_POINTS}))
                    return
                target = round_to_tick(entry - 1 * risk)

                # Place market sell then SL-M buy
                sell_resp = self.fyers.place_market_sell(symbol, self.traded_qty, tag="ENTRY_SHORT")
                sl_resp = self.fyers.place_stoploss_buy(symbol, self.traded_qty, sl, tag="SL_SHORT")

                sl_id = self._extract_order_id(sl_resp)

                self.positions[symbol] = {
                    "side": -1,
                    "entry": entry,
                    "sl": sl,
                    "target": target,
                    "sl_order": sl_resp,
                    "sl_order_id": sl_id,
                    "exit_initiated": False,
                    "last_exit_attempt_ms": 0,
                    "last_ltp": None
                }
                logger.info(json.dumps({
                    'event': 'entry_short_assumed',
                    'symbol': symbol,
                    'entry': entry,
                    'sl': sl,
                    'target': target,
                    'sell_resp': str(sell_resp),
                    'sl_resp': str(sl_resp)
                }))

        except Exception as e:
            logger.error(f"on_candle unexpected error for {symbol}: {e}")

    # --- helper to attempt a throttled target close (idempotent) ---
    def _attempt_target_close(self, symbol, pos, reason="tick_target"):
        """
        Attempt to cancel SL and place market close order in a throttled, idempotent way.
        We DO NOT log the final exit here; we wait for on_trade to report the actual fill price from the broker.
        """
        now_ms = int(time.time() * 1000)
        last_ms = pos.get("last_exit_attempt_ms", 0)
        if pos.get("exit_initiated"):
            logger.debug(f"Close already initiated for {symbol}; skipping duplicate attempt.")
            return
        if (now_ms - last_ms) < EXIT_THROTTLE_MS:
            logger.debug(f"Throttle active for {symbol} close attempts (wait {(EXIT_THROTTLE_MS - (now_ms-last_ms))} ms).")
            return

        pos["last_exit_attempt_ms"] = now_ms
        pos["exit_initiated"] = True
        logger.info(f"Attempting target close for {symbol} (reason={reason}, side={pos['side']}) at {now_ms}ms")

        # Attempt to cancel SL order if it exists
        try:
            sl_order = pos.get("sl_order")
            sl_order_id = None
            if sl_order and isinstance(sl_order, dict):
                sl_order_id = sl_order.get("id") or (sl_order.get("raw") and sl_order.get("raw").get("id"))

            if sl_order_id:
                try:
                    cancel_resp = self.fyers.cancel_order(sl_order_id)
                    logger.debug(f"Cancel response for {symbol}: {cancel_resp}")
                except Exception as e:
                    logger.error(f"Cancel attempt failed for {symbol}: {e}")
                time.sleep(CANCEL_SETTLE_SEC)

            # Place market close: buy for short
            tag = "TARGET_EXIT"
            self.fyers.place_market_buy(symbol, self.traded_qty, tag=tag)
        except Exception as e:
            logger.error(f"_attempt_target_close error for {symbol}: {e}")

    # --- tick-level handler for immediate exits ---
    def on_tick(self, symbol, ltp, ts):
        """
        Called for every incoming tick. We use this to check target-hit intra-candle
        and request an immediate market close (throttled & idempotent). Actual exit logging
        will be done when on_trade reports the fill price from the broker.
        """
        try:
            pos = self.positions.get(symbol)
            if not pos:
                return

            pos["last_ltp"] = ltp  # keep latest seen LTP for safety checks

            if ltp <= pos["target"]:
                if not pos.get("exit_initiated"):
                    logger.debug(f"Tick-level target detected for {symbol} ltp={ltp} target={pos['target']}")
                    self._attempt_target_close(symbol, pos, reason="tick_target")
        except Exception as e:
            logger.error(f"on_tick error: {e}")

    # --- trade handler ---
    def on_trade(self, msg):
        """
        Handle trade fills from broker. Interpret SL fills and target exit fills
        and compute exact PnL using the trade fill price from the broker.
        """
        if not msg.get("trades"):
            return
        try:
            t = msg["trades"]
            side = t.get("side") or t.get("s")  # -1 sell, 1 buy
            symbol = t.get("symbol") or t.get("d") or t.get("instrument")
            tag = t.get("orderTag") or t.get("order_tag") or t.get("orderTagName")
            fill_price = None
            if "price" in t:
                try:
                    fill_price = float(t.get("price"))
                except:
                    pass
            if not fill_price and "filled_price" in t:
                try:
                    fill_price = float(t.get("filled_price"))
                except:
                    pass
            if not fill_price and "avg_price" in t:
                try:
                    fill_price = float(t.get("avg_price"))
                except:
                    pass
            if isinstance(t, list):
                for item in t:
                    self.on_trade({"trades": item})
                return

            tag = tag if tag is not None else ""

            logger.info(json.dumps({'event': 'trade_msg', 'symbol': symbol, 'side': side, 'tag': tag, 'fill_price': fill_price, 'raw': t}))

            pos = self.positions.get(symbol)
            if not pos:
                return

            entry = pos["entry"]
            sl = pos["sl"]
            target = pos["target"]

            # SL hit for short (buy trigger)
            if int(side) == 1 and "SL_SHORT" in tag:
                exit_price = round_to_tick(fill_price) if fill_price else round_to_tick(sl)
                pnl = (entry - exit_price) * self.traded_qty
                log_trade(symbol, "SHORT", entry, sl, target, exit_price, pnl)
                logger.info(f"SL HIT SHORT {symbol} @ {exit_price} PnL={pnl}")
                del self.positions[symbol]
                return

            # Target exit for short (buy)
            if int(side) == 1 and "TARGET_EXIT" in tag:
                exit_price = round_to_tick(fill_price) if fill_price else round_to_tick(target)
                pnl = (entry - exit_price) * self.traded_qty
                log_trade(symbol, "SHORT", entry, sl, target, exit_price, pnl)
                logger.info(f"TARGET EXIT SHORT {symbol} @ {exit_price} PnL={pnl} (tag={tag})")
                del self.positions[symbol]
                return

            # Handle entry fills and update entry if fill_price available
            if int(side) == -1 and "ENTRY_SHORT" in tag and fill_price:
                old_entry = entry
                new_entry = round_to_tick(fill_price)
                pos["entry"] = new_entry
                logger.info(f"Updated short entry price for {symbol} from {old_entry} to actual fill {new_entry}")
                return

        except Exception as e:
            logger.error(f"on_trade error: {e}")

# ---------------- MAIN ----------------
if __name__ == "__main__":
    fyers_client = FyersClient(CLIENT_ID, ACCESS_TOKEN)
    engine = NiftyStrategy(fyers_client, TRADED_QTY)

    # Replace with actual NIFTY option symbols as needed
    option_symbols = get_atm_symbols(fyers_client)

    engine.prefill_history(option_symbols, days_back=2)

    fyers_client.register_trade_callback(engine.on_trade)
    # pass the tick callback so we can exit immediately when target touched intra-candle
    fyers_client.subscribe_market_data(option_symbols, engine.on_candle, on_tick_callback=engine.on_tick)
    fyers_client.start_order_socket()