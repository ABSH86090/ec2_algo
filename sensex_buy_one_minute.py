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
LOT_SIZE = int(os.getenv("LOT_SIZE", "20"))  # tweak via env or edit constant
TICK_SIZE = 0.05  # NSE options tick size

TRADING_START = datetime.time(9, 15)
TRADING_END = datetime.time(15, 0)

# --- Indicator config ---
EMA_PERIOD = 5
VWMA_PERIOD = 20

# Exit throttle (milliseconds) to avoid spamming cancel/market orders on rapid ticks
EXIT_THROTTLE_MS = int(os.getenv("EXIT_THROTTLE_MS", "500"))

# Logging verbosity can be controlled via env var, e.g., EXIT_LOG_LEVEL=DEBUG
EXIT_LOG_LEVEL = os.getenv("EXIT_LOG_LEVEL", "INFO").upper()

# Safety buffer for SL placement: leave at least one tick (or a few points) below LTP
MIN_SL_BUFFER = float(os.getenv("MIN_SL_BUFFER", "0.05"))  # smallest unit (tick)

# small pause after cancel before placing new SL to reduce race (seconds)
CANCEL_SETTLE_SEC = float(os.getenv("CANCEL_SETTLE_SEC", "0.06"))

# Risk cap for SL distance (points) — SPEC: skip if SL > 40 points
MAX_SL_POINTS = float(os.getenv("MAX_SL_POINTS", "40.0"))

# minimum minutes between two consecutive trades (global cooldown) — SPEC: 10 minutes
TRADE_MIN_GAP_MIN = float(os.getenv("TRADE_MIN_GAP_MIN", "10.0"))

# Reset logging handlers
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

logging.basicConfig(
    filename="sensex_wpattern_strategy.log",
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

def session_start_dt():
    """Return today's session start datetime (today @ TRADING_START)."""
    today = datetime.date.today()
    return datetime.datetime.combine(today, TRADING_START)

# ---------------- JOURNAL LOGGING ----------------
def log_trade(symbol, side, entry, sl, target, exit_price, pnl, file="trades.csv"):
    file_exists = os.path.isfile(file)
    with open(file, "a", newline="") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["date", "symbol", "side", "entry", "sl", "target", "exit", "pnl"])
        writer.writerow([
            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            symbol, side, entry, sl, target, exit_price, pnl
        ])

# ---------------- EXPIRY & SYMBOL UTILS ----------------
SPECIAL_MARKET_HOLIDAYS = {
    datetime.date(2025, 10, 2),
    datetime.date(2025, 12, 25),
}

def is_last_thursday(date_obj: datetime.date) -> bool:
    """Return True if date_obj is the last Thursday of its month."""
    if date_obj.weekday() != 3:
        return False
    next_week = date_obj + datetime.timedelta(days=7)
    return next_week.month != date_obj.month

def get_next_expiry():
    """Return expiry date for SENSEX options with Wednesday move when Thursday is a special holiday."""
    today = datetime.date.today()
    weekday = today.weekday()  # Monday=0 ... Sunday=6
    if weekday == 3:  # Thursday
        candidate_thu = today
    else:
        days_to_thu = (3 - weekday) % 7
        candidate_thu = today + datetime.timedelta(days=days_to_thu)
    if candidate_thu in SPECIAL_MARKET_HOLIDAYS:
        expiry = candidate_thu - datetime.timedelta(days=1)  # Wednesday
    else:
        expiry = candidate_thu
    return expiry

def format_expiry_for_symbol(expiry_date: datetime.date) -> str:
    yy = expiry_date.strftime("%y")
    treat_as_monthly = False
    if expiry_date.weekday() == 3 and is_last_thursday(expiry_date):
        treat_as_monthly = True
    elif expiry_date.weekday() == 2:
        thursday = expiry_date + datetime.timedelta(days=1)
        if is_last_thursday(thursday):
            treat_as_monthly = True

    if treat_as_monthly:
        mon = (expiry_date + datetime.timedelta(days=(3 - expiry_date.weekday()))) \
            .strftime("%b").upper() if expiry_date.weekday() != 3 else expiry_date.strftime("%b").upper()
        return f"{yy}{mon}"

    m = expiry_date.month
    d = expiry_date.day
    if m == 10:
        m_token = "O"
    elif m == 11:
        m_token = "N"
    elif m == 12:
        m_token = "D"
    else:
        m_token = f"{m:02d}"
    return f"{yy}{m_token}{d:02d}"

def get_atm_symbols(fyers_client):
    """Fetch SENSEX spot, round to ATM strike, and build CE/PE option symbols."""
    data = {"symbols": "BSE:SENSEX-INDEX"}
    resp = fyers_client.client.quotes(data)
    if not resp.get("d"):
        raise Exception(f"Failed to fetch SENSEX spot: {resp}")

    ltp = float(resp["d"][0]["v"]["lp"])  # last traded price
    atm_strike = round(ltp / 100) * 100   # nearest 100

    expiry = get_next_expiry()
    expiry_str = format_expiry_for_symbol(expiry)
    ce_strike = atm_strike
    pe_strike = atm_strike
    ce_symbol = f"BSE:SENSEX{expiry_str}{ce_strike}CE"
    pe_symbol = f"BSE:SENSEX{expiry_str}{pe_strike}PE"

    print(f"[ATM SYMBOLS] CE={ce_symbol}, PE={pe_symbol}")
    logging.info(f"[ATM SYMBOLS] CE={ce_symbol}, PE={pe_symbol}")
    return [ce_symbol, pe_symbol]

# ---------------- FYERS CLIENT ----------------
class FyersClient:
    def __init__(self, client_id: str, access_token: str, lot_size: int = 20):
        self.client_id = client_id
        self.access_token = access_token
        self.auth_token = f"{client_id}:{access_token}"
        self.lot_size = lot_size
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
        if not tag:
            return ""
        s = re.sub(r'[^A-Za-z0-9]', '', str(tag))
        if len(s) > max_len:
            s = s[:max_len]
        if s != tag:
            logger.warning(f"Order tag sanitized: original='{tag}' -> sanitized='{s}'")
        return s

    def _log_order_resp(self, action, resp):
        try:
            if isinstance(resp, dict):
                resp_copy = dict(resp)
                if not resp_copy.get('id'):
                    if isinstance(resp_copy.get('raw'), dict) and resp_copy['raw'].get('id'):
                        resp_copy['id'] = resp_copy['raw']['id']
                    elif isinstance(resp_copy.get('orders'), dict) and resp_copy['orders'].get('id'):
                        resp_copy['id'] = resp_copy['orders']['id']
                if not resp_copy.get('message') and isinstance(resp_copy.get('raw'), dict):
                    resp_copy['message'] = resp_copy['raw'].get('message')
                info = {'id': resp_copy.get('id'), 'status': resp_copy.get('status'),
                        'message': resp_copy.get('message'),
                        'raw': resp_copy.get('raw') if 'raw' in resp_copy else resp_copy}
            else:
                info = {'raw': str(resp)}
            logger.info(f"{action} Response: {json.dumps(info)}")
            print(f"{action} Response: {info}")
        except Exception:
            logger.info(f"{action} Response: {resp}")
            print(f"{action} Response: {resp}")
        return resp

    def place_limit_buy(self, symbol: str, qty: int, price: float, tag: str = ""):
        price = round_to_tick(price)
        tag_clean = self._sanitize_tag(tag)
        data = {
            "symbol": symbol,
            "qty": qty,
            "type": 1,
            "side": 1,  # BUY
            "productType": "INTRADAY",
            "limitPrice": price,
            "stopPrice": 0,
            "validity": "DAY",
            "disclosedQty": 0,
            "offlineOrder": False,
            "orderTag": tag_clean
        }
        resp = self.client.place_order(data)
        return self._log_order_resp("Limit Buy", resp)

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

    def place_stoploss_sell(self, symbol: str, qty: int, stop_price: float, tag: str = ""):
        stop_price = round_to_tick(stop_price)
        tag_clean = self._sanitize_tag(tag)
        data = {
            "symbol": symbol,
            "qty": qty,
            "type": 3,  # SL-M
            "side": -1,  # SELL
            "productType": "INTRADAY",
            "limitPrice": 0,
            "stopPrice": stop_price,
            "validity": "DAY",
            "disclosedQty": 0,
            "offlineOrder": False,
            "orderTag": tag_clean
        }
        resp = self.client.place_order(data)
        return self._log_order_resp("SL Sell", resp)

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
        Subscribe and aggregate to 1-minute candles.
        on_candle_callback(symbol, candle) called when a 1-min candle completes.
        on_tick_callback(symbol, ltp, ts) called for every tick.
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

            if on_tick_callback:
                try:
                    on_tick_callback(symbol, ltp, ts)
                except Exception as e:
                    logger.error(f"on_tick_callback error for {symbol}: {e}")

            dt = datetime.datetime.fromtimestamp(ts)
            candle_time = dt.replace(second=0, microsecond=0)  # 1-minute bucket

            c = candle_buffers[symbol]
            if c is None or c["time"] != candle_time:
                if c is not None:
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
class NiftyBuyStrategy:
    def __init__(self, fyers_client: FyersClient, lot_size: int = 20):
        self.fyers = fyers_client
        self.lot_size = lot_size
        self.candles = defaultdict(lambda: deque(maxlen=800))  # plenty for 1-min
        self.positions = {}       # Active positions per symbol
        self.last_trade_time = None  # global cooldown

    # --- history prefill (1-minute) ---
    def prefill_history(self, symbols, days_back=1):
        for symbol in symbols:
            try:
                to_date = datetime.datetime.now().strftime("%Y-%m-%d")
                from_date = (datetime.datetime.now() - datetime.timedelta(days=days_back)).strftime("%Y-%m-%d")

                params = {
                    "symbol": symbol,
                    "resolution": "1",  # SPEC: 1-minute candles
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
                    volumes = [c["volume"] for c in self.candles[symbol]]
                    ema5 = self.ema_series(closes, EMA_PERIOD)[-1]
                    vwma20 = self.vwma_series(closes, volumes, VWMA_PERIOD)[-1]
                    logger.info(f"[Init] {symbol} EMA5={_safe(ema5)}, VWMA20={_safe(vwma20)}")
            except Exception as e:
                logger.error(f"History fetch fail {symbol}: {e}")

    # --- indicator series ---
    def ema_series(self, prices, period):
        out = [None]*len(prices)
        if len(prices) < 1:
            return out
        k = 2/(period+1)
        ema = prices[0]
        for i,p in enumerate(prices):
            if i == 0:
                ema = p
            else:
                ema = p*k + ema*(1-k)
            out[i] = ema
        return out

    def vwma_series(self, closes, volumes, period):
        out = [None]*len(closes)
        if len(closes) < period:
            return out
        for i in range(len(closes)):
            if i+1 >= period:
                c = closes[i-period+1:i+1]
                v = volumes[i-period+1:i+1]
                denom = sum(v)
                out[i] = (sum([ci*vi for ci,vi in zip(c,v)]) / denom) if denom > 0 else None
        return out

    # --- helpers ---
    @staticmethod
    def is_green(c): return c["close"] > c["open"]

    def is_strong_green_close_near_high(self, c, pct=0.01):
        if c["close"] <= c["open"]:
            return False
        return (c["high"] - c["close"]) <= pct * c["high"]

    # --- ORDER helpers (same as original) ---
    def _extract_order_id(self, order_resp):
        try:
            if not order_resp:
                return None
            if isinstance(order_resp, dict):
                if order_resp.get("id"):
                    return str(order_resp.get("id"))
                if order_resp.get("orders") and isinstance(order_resp["orders"], dict) and order_resp["orders"].get("id"):
                    return str(order_resp["orders"]["id"])
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

    def _safe_cancel_and_place_sl(self, symbol, pos, new_sl, tag="NIFTYSLTRAIL"):
        try:
            old_order = pos.get("sl_order")
            old_order_id = pos.get("sl_order_id") or self._extract_order_id(old_order)
            if old_order_id:
                try:
                    cancel_resp = self.fyers.cancel_order(old_order_id)
                    logger.debug(f"Cancel response while trailing SL for {symbol}: {cancel_resp}")
                except Exception as e:
                    logger.warning(f"Cancel attempt raised while trailing SL for {symbol}: {e}")
                time.sleep(CANCEL_SETTLE_SEC)

            curr_ltp = pos.get("last_ltp") or None
            if curr_ltp is not None:
                max_allowed_sl = round_to_tick(curr_ltp - MIN_SL_BUFFER)
                if new_sl >= max_allowed_sl:
                    logger.info(f"Trailing SL not safe to place for {symbol}: desired {new_sl} >= max_allowed {max_allowed_sl} (ltp={curr_ltp})")
                    return None, None

            sl_resp = self.fyers.place_stoploss_sell(symbol, self.lot_size, new_sl, tag=tag)
            sl_id = self._extract_order_id(sl_resp)
            pos["sl_order"] = sl_resp
            if sl_id:
                pos["sl_order_id"] = sl_id
            logger.info(f"Placed trailing SL {new_sl} for {symbol}, sl_id={sl_id}")
            return sl_resp, sl_id
        except Exception as e:
            logger.error(f"_safe_cancel_and_place_sl failed for {symbol}: {e}")
            return None, None

    def _select_strike_200_from_atm(self, symbol):
        try:
            m = re.match(r'^(BSE:SENSEX)([A-Z0-9]+?)(\d+)(CE|PE)$', symbol)
            if not m:
                raise Exception(f"Unrecognized symbol format: {symbol}")
            prefix, expiry_token, old_strike, opt_type = m.groups()

            resp = self.fyers.client.quotes({"symbols": "BSE:SENSEX-INDEX"})
            if not resp.get("d"):
                raise Exception(f"Failed to fetch SENSEX spot: {resp}")
            ltp = float(resp["d"][0]["v"]["lp"])
            atm = int(round(ltp / 100.0) * 100)

            if opt_type == "CE":
                new_strike = int(atm - 200)
            else:
                new_strike = int(atm + 200)

            new_symbol = f"{prefix}{expiry_token}{new_strike}{opt_type}"
            logger.info(f"Adjusted symbol from {symbol} -> {new_symbol} (atm={atm})")
            return new_symbol
        except Exception as e:
            logger.error(f"_select_strike_200_from_atm failed for {symbol}: {e}")
            raise

    # --- MAIN 1-MIN CANDLE HANDLER (NEW STRATEGY) ---
    def on_candle(self, symbol, candle):
        try:
            self.candles[symbol].append(candle)
            candles = list(self.candles[symbol])
            idx = len(candles) - 1

            closes = [c["close"] for c in candles]
            volumes = [c["volume"] for c in candles]

            ema5_arr = self.ema_series(closes, EMA_PERIOD)
            vwma20_arr = self.vwma_series(closes, volumes, VWMA_PERIOD)

            ema5 = ema5_arr[idx]
            vwma20 = vwma20_arr[idx]

            # --- SNAPSHOT LOG ---
            try:
                snapshot = {s: {
                    'time': str(self.candles[s][-1]['time']) if len(self.candles[s]) > 0 else None,
                    'close': _safe(self.candles[s][-1]['close']),
                    'EMA5': _safe(self.ema_series([c['close'] for c in self.candles[s]], EMA_PERIOD)[-1]) if len(self.candles[s]) > 0 else None,
                    'VWMA20': _safe(self.vwma_series([c['close'] for c in self.candles[s]], [c['volume'] for c in self.candles[s]], VWMA_PERIOD)[-1]) if len(self.candles[s]) > 0 else None
                } for s in list(self.candles.keys())}
                logger.info(f"Indicators snapshot at {candle['time']}: {json.dumps(snapshot)}")
            except Exception as e:
                logger.debug(f"Failed to log indicators snapshot: {e}")

            if ema5 is None or vwma20 is None:
                logger.debug(f"Not enough data for indicators for {symbol} at idx={idx} (EMA5={ema5}, VWMA20={vwma20})")
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
                'EMA5': _safe(ema5),
                'VWMA20': _safe(vwma20),
                'in_position': bool(pos)
            }))

            # If in a position: candle-close fallback for target
            if pos:
                if candle["high"] >= pos["target"] and not pos.get("exit_initiated"):
                    self._attempt_target_exit(symbol, pos, reason="candle_fallback")
                return

            # --- Global cooldown (10 minutes) ---
            try:
                if self.last_trade_time is not None:
                    minutes_since_last = (candle["time"] - self.last_trade_time).total_seconds() / 60.0
                else:
                    minutes_since_last = float("inf")
                if minutes_since_last < TRADE_MIN_GAP_MIN:
                    logger.info(json.dumps({
                        'event': 'entry_skipped_due_to_trade_cooldown',
                        'symbol': symbol,
                        'candidate_time': str(candle['time']),
                        'minutes_since_last_trade': minutes_since_last,
                        'required_gap_min': TRADE_MIN_GAP_MIN
                    }))
                    return
            except Exception as e:
                logger.debug(f"trade cooldown check failed: {e}")

            # --- New Strategy Entry Logic (Scenario 2 prioritized over Scenario 1) ---
            is_green = self.is_green(candle)
            is_strong = self.is_strong_green_close_near_high(candle, pct=0.01)

            cond_gap_5 = (vwma20 <= 0.95 * ema5)  # Scenario 1 gap: VWMA20 <= EMA5 * 0.95
            cond_gap_1 = (vwma20 <= 0.99 * ema5)  # Scenario 2 gap: VWMA20 <= EMA5 * 0.99

            # Scenario 2 (priority): green & strong, low <= VWMA, close > VWMA, and EMA5 > VWMA at close
            scenario2 = (cond_gap_1 and is_green and is_strong and
                         (candle["low"] <= vwma20) and (candle["close"] > vwma20) and (ema5 > vwma20))

            # Scenario 1 (fallback): green & strong, low < EMA5 but closes above EMA5
            scenario1 = (cond_gap_5 and is_green and is_strong and
                         (candle["low"] < ema5) and (candle["close"] > ema5))

            should_enter = False
            scenario = None
            if scenario2:
                should_enter = True
                scenario = "Scenario2"
            elif scenario1:
                should_enter = True
                scenario = "Scenario1"

            if not should_enter:
                return

            # ENTRY on this candle's close
            entry = round_to_tick(candle["close"])

            # SL = low of the green candle (SPEC: no extra buffer)
            sl_price = round_to_tick(candle["low"])
            risk_points = round(entry - sl_price, 2)

            if risk_points <= 0:
                logger.info(json.dumps({'event': 'skip', 'symbol': symbol, 'reason': 'non_positive_risk', 'entry': entry, 'sl': sl_price, 'scenario': scenario}))
                return

            # SPEC: if SL distance > 40 points, skip
            if risk_points > MAX_SL_POINTS:
                logger.info(json.dumps({'event': 'skip', 'symbol': symbol, 'reason': 'risk_exceeds_max', 'risk_points': risk_points, 'max_allowed': MAX_SL_POINTS, 'scenario': scenario}))
                return

            # Target = 1R (equal to SL distance)
            target_price = round_to_tick(entry + risk_points)

            # Strike selection identical; if premium > 700, adjust to ATM±200
            selected_symbol = symbol
            try:
                if candle["close"] > 700:
                    try:
                        selected_symbol = self._select_strike_200_from_atm(symbol)
                        logger.info(json.dumps({
                            'event': 'strike_adjusted_due_to_high_premium',
                            'orig_symbol': symbol,
                            'selected_symbol': selected_symbol,
                            'premium': candle["close"]
                        }))
                    except Exception as e:
                        logger.warning(f"Failed to adjust strike for {symbol}: {e}. Proceeding with original symbol.")
            except Exception:
                pass

            # Place market buy and SL-M
            buy_resp = self.fyers.place_market_buy(selected_symbol, self.lot_size, tag="NIFTYBUYENTRY")
            sl_resp = self.fyers.place_stoploss_sell(selected_symbol, self.lot_size, sl_price, tag="NIFTYSL")

            sl_id = self._extract_order_id(sl_resp)

            # Store position for trailing SL (unchanged logic) and exits
            self.positions[selected_symbol] = {
                "entry": entry,
                "sl": sl_price,
                "orig_sl": sl_price,
                "last_trail_step": -1,
                "target": target_price,
                "sl_order": sl_resp,
                "sl_order_id": sl_id,
                "exit_initiated": False,
                "last_exit_attempt_ms": 0,
                "exit_requested_at": None,
                "max_ltp": None,
                "last_ltp": None,
                "scenario": scenario
            }

            try:
                self.last_trade_time = candle["time"]
            except Exception:
                pass

            logger.info(json.dumps({
                'event': 'entry_assumed_immediate',
                'scenario': scenario,
                'orig_symbol': symbol,
                'selected_symbol': selected_symbol,
                'entry': entry,
                'sl': sl_price,
                'risk_points': risk_points,
                'target': target_price,
                'multiplier': 1.0,  # SPEC: 1R
                'buy_resp': str(buy_resp),
                'sl_resp': str(sl_resp)
            }))

        except Exception as e:
            logger.error(f"on_candle unexpected error for {symbol}: {e}")

    # --- helper to attempt a throttled target exit (idempotent) ---
    def _attempt_target_exit(self, symbol, pos, reason="tick_target"):
        now_ms = int(time.time() * 1000)
        last_ms = pos.get("last_exit_attempt_ms", 0)
        if pos.get("exit_initiated"):
            logger.debug(f"Exit already initiated for {symbol}; skipping duplicate attempt.")
            return
        if (now_ms - last_ms) < EXIT_THROTTLE_MS:
            logger.debug(f"Throttle active for {symbol} exit attempts (wait {(EXIT_THROTTLE_MS - (now_ms-last_ms))} ms).")
            return

        pos["last_exit_attempt_ms"] = now_ms
        pos["exit_initiated"] = True
        pos["exit_requested_at"] = now_ms
        logger.info(f"Attempting target exit for {symbol} (reason={reason}) at {now_ms}ms")

        # Cancel SL then sell at market (SPEC: once target achieved, cancel SL order and exit)
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

            try:
                self.fyers.place_market_sell(symbol, self.lot_size, tag="TARGETEXITTICK")
            except Exception as e:
                logger.error(f"Market sell attempt failed for {symbol}: {e}")
        except Exception as e:
            logger.error(f"_attempt_target_exit error for {symbol}: {e}")

    # --- tick-level handler for immediate exits & robust trailing SL (UNCHANGED LOGIC) ---
    def on_tick(self, symbol, ltp, ts):
        try:
            pos = self.positions.get(symbol)
            if not pos:
                return
            entry = pos["entry"]
            orig_sl = pos.get("orig_sl", pos.get("sl"))

            if pos.get("max_ltp") is None:
                pos["max_ltp"] = ltp
            else:
                if ltp > pos["max_ltp"]:
                    pos["max_ltp"] = ltp

            pos["last_ltp"] = ltp

            # Trailing SL logic (unchanged behavior): trail every 10 pts up by 5 pts, with safety clamp
            if pos.get("entry") is not None and ltp > pos["entry"]:
                move_up = pos["max_ltp"] - pos["entry"]
                trail_step = int(move_up // 10)

                if trail_step > pos.get("last_trail_step", -1):
                    desired_sl = round_to_tick(orig_sl + trail_step * 5)

                    max_allowed_sl = round_to_tick(ltp - MIN_SL_BUFFER)
                    if desired_sl >= max_allowed_sl:
                        clamped = round_to_tick(max_allowed_sl - TICK_SIZE)
                        if clamped > pos["sl"] and clamped < ltp:
                            desired_sl = clamped
                        else:
                            logger.debug(f"Trailing SL for {symbol}: desired {desired_sl} unsafe (>= {max_allowed_sl}); skipping.")
                            desired_sl = None

                    if desired_sl and desired_sl > pos["sl"]:
                        try:
                            sl_resp, sl_id = self._safe_cancel_and_place_sl(symbol, pos, desired_sl, tag="NIFTYSLTRAIL")
                            if sl_resp:
                                pos["sl"] = desired_sl
                                if sl_id:
                                    pos["sl_order_id"] = sl_id
                                pos["last_trail_step"] = trail_step
                                logger.info(f"Trailing SL applied for {symbol}: step={trail_step}, sl={desired_sl}, max_ltp={pos['max_ltp']}")
                            else:
                                logger.debug(f"No new SL placed for {symbol} (desired_sl={desired_sl})")
                        except Exception as e:
                            logger.error(f"Failed to update SL for {symbol}: {e}")

            # Immediate target exit if touched on tick
            if ltp >= pos["target"] and not pos.get("exit_initiated"):
                logger.debug(f"Tick-level target detected for {symbol} ltp={ltp} target={pos['target']}")
                self._attempt_target_exit(symbol, pos, reason="tick_target")
        except Exception as e:
            logger.error(f"on_tick error: {e}")

    # --- trade handler (unchanged) ---
    def on_trade(self, msg):
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

            # SL hit
            if int(side) == -1 and "NIFTYSL" in tag:
                pos = self.positions.get(symbol)
                if pos:
                    exit_price = round_to_tick(fill_price) if fill_price else round_to_tick(pos["sl"])
                    pnl = (exit_price - pos["entry"]) * self.lot_size
                    log_trade(symbol, "BUY", pos["entry"], pos["sl"], pos["target"], exit_price, pnl)
                    logger.info(f"SL HIT {symbol} @ {exit_price} PnL={pnl}")
                    self.positions[symbol] = None
                return

            # Our target exit fills (market sell)
            if int(side) == -1 and ("TARGETEXIT" in tag or "TARGETEXITTICK" in tag):
                pos = self.positions.get(symbol)
                if pos:
                    if fill_price:
                        exit_price = round_to_tick(fill_price)
                    else:
                        exit_price = round_to_tick(pos["target"])
                    pnl = (exit_price - pos["entry"]) * self.lot_size
                    log_trade(symbol, "BUY", pos["entry"], pos["sl"], pos["target"], exit_price, pnl)
                    logger.info(f"TARGET EXIT FILL {symbol} @ {exit_price} PnL={pnl} (tag={tag})")
                    self.positions[symbol] = None
                return

            # Handle fills for initial market buy (entry)
            if int(side) == 1 and ("NIFTYBUYENTRY" in tag):
                pos = self.positions.get(symbol)
                if pos and fill_price:
                    old_entry = pos["entry"]
                    new_entry = round_to_tick(fill_price)
                    pos["entry"] = new_entry
                    logger.info(f"Updated entry price for {symbol} from {old_entry} to actual fill {new_entry}")
                return

        except Exception as e:
            logger.error(f"on_trade error: {e}")

# ---------------- MAIN ----------------
if __name__ == "__main__":
    fyers_client = FyersClient(CLIENT_ID, ACCESS_TOKEN, LOT_SIZE)
    engine = NiftyBuyStrategy(fyers_client, LOT_SIZE)

    option_symbols = get_atm_symbols(fyers_client)

    # Backfill for indicators on 1-minute resolution
    engine.prefill_history(option_symbols, days_back=2)

    fyers_client.register_trade_callback(engine.on_trade)
    # subscribe with 1-minute candle aggregation + tick exits
    fyers_client.subscribe_market_data(option_symbols, engine.on_candle, on_tick_callback=engine.on_tick)
    fyers_client.start_order_socket()
