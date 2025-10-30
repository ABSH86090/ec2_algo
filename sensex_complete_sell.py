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

TRADING_START = datetime.time(9,15)
TRADING_END = datetime.time(15, 0)

# --- M-pattern config (double-top) ---
PIVOT_K = 2
MIN_BARS_BETWEEN_HIGHS = 5
EMA_PERIOD = 5
EMA20_PERIOD = 20

# Trigger conditions (kept for reference but NOT used in immediate-entry now)
EMA_OVER_EMA20_MIN_GAP = 0.02  # 2% relative gap at trigger candle (reference)
EMA_OVER_EMA20_MAX_GAP = 0.06

# ADX filter
ADX_PERIOD = 14
ADX_MIN = 10  # only take trade if ADX > 10

# Exit throttle (milliseconds) to avoid spamming cancel/market orders on rapid ticks
EXIT_THROTTLE_MS = int(os.getenv("EXIT_THROTTLE_MS", "500"))

# Logging verbosity can be controlled via env var, e.g., EXIT_LOG_LEVEL=DEBUG
EXIT_LOG_LEVEL = os.getenv("EXIT_LOG_LEVEL", "INFO").upper()

# Safety buffer for SL placement: leave at least one tick (or a few points) below/above LTP
MIN_SL_BUFFER = float(os.getenv("MIN_SL_BUFFER", "0.05"))  # smallest unit (tick)

# small pause after cancel before placing new SL to reduce race (seconds)
CANCEL_SETTLE_SEC = float(os.getenv("CANCEL_SETTLE_SEC", "0.06"))

# PATCH: risk cap for SL distance (points)
MAX_SL_POINTS = float(os.getenv("MAX_SL_POINTS", "60.0"))

# minimum minutes between two consecutive trades (global cooldown)
TRADE_MIN_GAP_MIN = float(os.getenv("TRADE_MIN_GAP_MIN", "15.0"))

# Reset logging handlers
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

logging.basicConfig(
    filename="sensex_mpattern_strategy.log",
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
            writer.writerow(["date","symbol","side","entry","sl","target","exit","pnl"])
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
    # Thursday weekday() == 3
    if date_obj.weekday() != 3:
        return False
    # Add 7 days; if month changes, current date is last Thursday
    next_week = date_obj + datetime.timedelta(days=7)
    return next_week.month != date_obj.month

def get_next_expiry():
    """Return expiry date for SENSEX options.
    By default expiry is the next Thursday. If that Thursday is a special market holiday
    (e.g., exchange moved expiry to Wednesday), return the previous day (Wednesday).
    """
    today = datetime.date.today()
    weekday = today.weekday()  # Monday=0 ... Sunday=6

    if weekday == 3:  # Thursday
        candidate_thu = today
    else:
        days_to_thu = (3 - weekday) % 7
        candidate_thu = today + datetime.timedelta(days=days_to_thu)

    # If the exchange is closed on the Thursday (special holiday), expiry moves to Wednesday
    if candidate_thu in SPECIAL_MARKET_HOLIDAYS:
        expiry = candidate_thu - datetime.timedelta(days=1)  # Wednesday
    else:
        expiry = candidate_thu
    return expiry


def format_expiry_for_symbol(expiry_date: datetime.date) -> str:
    yy = expiry_date.strftime("%y")  # e.g. '25'

    treat_as_monthly = False
    if expiry_date.weekday() == 3 and is_last_thursday(expiry_date):
        treat_as_monthly = True
    elif expiry_date.weekday() == 2:  # Wednesday
        thursday = expiry_date + datetime.timedelta(days=1)
        if is_last_thursday(thursday):
            treat_as_monthly = True

    if treat_as_monthly:
        mon = (expiry_date + datetime.timedelta(days=(3 - expiry_date.weekday())))\
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
    # <-- flipped as requested: CE = ATM + 200 ; PE = ATM - 200
    ce_strike = atm_strike + 200
    pe_strike = atm_strike - 200
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
                    elif resp_copy.get('orders') and isinstance(resp_copy['orders'], dict) and resp_copy['orders'].get('id'):
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

    def place_stoploss_buy(self, symbol: str, qty: int, stop_price: float, tag: str = ""):
        """SL buy for covering short positions (SL-M buy)."""
        stop_price = round_to_tick(stop_price)
        tag_clean = self._sanitize_tag(tag)
        data = {
            "symbol": symbol,
            "qty": qty,
            "type": 3,  # SL-M
            "side": 1,  # BUY (to cover short)
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
            bucket_minute = (dt.minute // 3) * 3
            candle_time = dt.replace(second=0, microsecond=0, minute=bucket_minute)

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
        self.candles = defaultdict(lambda: deque(maxlen=400))
        self.positions = {}       # Active positions per symbol
        # simplified state: track detected m_span, last_trade_idx, and breakdown_time
        self.state = defaultdict(lambda: {
            "m_span": None,
            "last_trade_idx": -1,
            "breakdown_time": None
        })
        # global last trade time (datetime) to enforce cooldown between trades
        self.last_trade_time = None

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
                    volumes = [c["volume"] for c in self.candles[symbol]]
                    highs = [c["high"] for c in self.candles[symbol]]
                    lows = [c["low"] for c in self.candles[symbol]]
                    ema5 = self.ema_series(closes, EMA_PERIOD)[-1]
                    ema20 = self.ema_series(closes, EMA20_PERIOD)[-1]
                    adx = self.adx_series(highs, lows, closes, ADX_PERIOD)[-1]
                    logger.info(f"[Init] {symbol} EMA5={_safe(ema5)}, EMA20={_safe(ema20)}, ADX={_safe(adx)}")
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
        # kept for compatibility but we don't use VWMA now
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

    def adx_series(self, highs, lows, closes, period=14):
        n = len(closes)
        if n < period + 1:
            return [None] * n

        tr = [None] * n
        plus_dm = [0.0] * n
        minus_dm = [0.0] * n

        for i in range(1, n):
            high = highs[i]
            low = lows[i]
            prev_high = highs[i-1]
            prev_low = lows[i-1]
            prev_close = closes[i-1]

            tr_val = max(high - low, abs(high - prev_close), abs(low - prev_close))
            tr[i] = tr_val

            up_move = high - prev_high
            down_move = prev_low - low
            if up_move > down_move and up_move > 0:
                plus_dm[i] = up_move
            else:
                plus_dm[i] = 0.0
            if down_move > up_move and down_move > 0:
                minus_dm[i] = down_move
            else:
                minus_dm[i] = 0.0

        tr_smooth = [None] * n
        plus_smooth = [None] * n
        minus_smooth = [None] * n

        tr_sum = sum([tr[i] for i in range(1, period+1) if tr[i] is not None])
        plus_sum = sum([plus_dm[i] for i in range(1, period+1)])
        minus_sum = sum([minus_dm[i] for i in range(1, period+1)])

        tr_smooth[period] = tr_sum
        plus_smooth[period] = plus_sum
        minus_smooth[period] = minus_sum

        for i in range(period+1, n):
            tr_smooth[i] = tr_smooth[i-1] - (tr_smooth[i-1] / period) + tr[i]
            plus_smooth[i] = plus_smooth[i-1] - (plus_smooth[i-1] / period) + plus_dm[i]
            minus_smooth[i] = minus_smooth[i-1] - (minus_smooth[i-1] / period) + minus_dm[i]

        plus_di = [None] * n
        minus_di = [None] * n
        dx = [None] * n
        adx = [None] * n

        for i in range(period, n):
            if tr_smooth[i] and tr_smooth[i] > 0:
                plus_di[i] = 100.0 * (plus_smooth[i] / tr_smooth[i])
                minus_di[i] = 100.0 * (minus_smooth[i] / tr_smooth[i])
                denom = plus_di[i] + minus_di[i]
                if denom != 0:
                    dx[i] = 100.0 * (abs(plus_di[i] - minus_di[i]) / denom)
                else:
                    dx[i] = 0.0

        first_adx_index = 2 * period
        if first_adx_index < n:
            dx_sum = sum([dx[i] for i in range(period, first_adx_index) if dx[i] is not None])
            count = len([i for i in range(period, first_adx_index) if dx[i] is not None])
            if count > 0:
                adx[first_adx_index] = dx_sum / count
                for i in range(first_adx_index + 1, n):
                    if dx[i] is None:
                        adx[i] = adx[i-1]
                    else:
                        adx[i] = ((adx[i-1] * (period - 1)) + dx[i]) / period

        return adx

    # --- helpers ---
    @staticmethod
    def is_green(c): return c["close"] > c["open"]
    @staticmethod
    def is_red(c): return c["close"] < c["open"]

    def is_pivot_high(self, highs, idx, k=PIVOT_K):
        if idx - k < 0 or idx + k >= len(highs):
            return False
        center = highs[idx]
        for i in range(idx-k, idx+k+1):
            if i == idx: continue
            if center < highs[i]:
                return False
        return True

    def find_recent_m(self, candles, ema5_arr, ema20_arr, start_dt=None):
        """
        Find an M (double-top) only within today's session window (>= start_dt).
        New conditions: both highs inside session and EMA5 > EMA20 between top1..top2.
        """
        n = len(candles)
        if n < (2*PIVOT_K + 1) + MIN_BARS_BETWEEN_HIGHS:
            return None

        if start_dt is None:
            start_dt = session_start_dt()

        # find first index within today's session
        start_idx = None
        for i, c in enumerate(candles):
            if c["time"] >= start_dt:
                start_idx = i
                break
        if start_idx is None:
            return None  # no candles yet in session

        highs = [c["high"] for c in candles]

        # scan for double-top pattern (i1 = first top, i2 = second top)
        i2_min_bound = max(2*PIVOT_K, start_idx + PIVOT_K)
        for i2 in range(n - 1 - PIVOT_K, i2_min_bound - 1, -1):
            if not self.is_pivot_high(highs, i2, PIVOT_K):
                continue

            jmax = i2 - MIN_BARS_BETWEEN_HIGHS
            i1_min_bound = max(2*PIVOT_K - 1, start_idx + PIVOT_K - 1)
            for i1 in range(jmax, i1_min_bound - 1, -1):
                if not self.is_pivot_high(highs, i1, PIVOT_K):
                    continue
                # second high should not be higher than first high (double-top style)
                if highs[i2] - 1e-9 > highs[i1]:
                    continue

                # between i1..i2, EMA5 > EMA20 and indices within session
                ok = True
                for k in range(i1, i2 + 1):
                    if (k < start_idx or
                        ema5_arr[k] is None or ema20_arr[k] is None or
                        not (ema5_arr[k] > ema20_arr[k])):
                        ok = False
                        break
                if ok:
                    return (i1, i2)
        return None

    def is_bearish_pinbar(self, c):
        body = abs(c["close"] - c["open"])
        lower = min(c["close"], c["open"]) - c["low"]
        upper = c["high"] - max(c["close"], c["open"])
        return (c["close"] < c["open"]) and (upper >= 2*body) and (lower <= body)

    def is_bearish_engulfing(self, prev, curr):
        return (prev["close"] > prev["open"] and
                curr["close"] < curr["open"] and
                curr["open"] >= prev["close"] and
                curr["close"] <= prev["open"])

    def is_strong_red_close_near_low(self, c, pct=0.01):
        if c["close"] >= c["open"]:
            return False
        return (c["close"] - c["low"]) <= pct * c["low"]

    # --- NEW: upper-wick helpers retained for symmetry ---
    def upper_wick_ratio(self, c):
        body = abs(c["close"] - c["open"])
        upper = c["high"] - max(c["close"], c["open"])
        if body <= 1e-9:
            return float("inf")
        return upper / body

    def has_short_upper_wick(self, c, max_ratio=0.5):
        return self.upper_wick_ratio(c) <= max_ratio

    # --- ORDER helpers & SL update adapted for shorts ---
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
        """
        Cancel existing SL order (if any), wait briefly, then place a new SL-M (buy for shorts, sell for longs)
        Returns (sl_resp, sl_id) or (None, None)
        """
        try:
            # Extract and cancel existing SL order if possible
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

            # Safety clamp differs for LONG vs SHORT:
            side = pos.get("side", "LONG")
            if curr_ltp is not None:
                if side == "LONG":
                    # new_sl must be strictly below current LTP by at least MIN_SL_BUFFER
                    max_allowed_sl = round_to_tick(curr_ltp - MIN_SL_BUFFER)
                    if new_sl >= max_allowed_sl:
                        logger.info(f"Trailing SL not safe to place for {symbol}: desired {new_sl} >= max_allowed {max_allowed_sl} (ltp={curr_ltp})")
                        return None, None
                else:  # SHORT
                    # For short, stop (buy) must be strictly above LTP by at least MIN_SL_BUFFER
                    min_allowed_sl = round_to_tick(curr_ltp + MIN_SL_BUFFER)
                    if new_sl <= min_allowed_sl:
                        logger.info(f"Trailing SL not safe to place for {symbol} (short): desired {new_sl} <= min_allowed {min_allowed_sl} (ltp={curr_ltp})")
                        return None, None

            # Place appropriate SL
            if pos.get("side", "LONG") == "LONG":
                sl_resp = self.fyers.place_stoploss_sell(symbol, self.lot_size, new_sl, tag=tag)
            else:
                sl_resp = self.fyers.place_stoploss_buy(symbol, self.lot_size, new_sl, tag=tag)

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
            ltp = float(resp["d"][0]["v"]["lp"])  # last traded price of index
            atm = int(round(ltp / 100.0) * 100)

            # <-- flipped logic: CE -> ATM + 200 ; PE -> ATM - 200
            if opt_type == "CE":
                new_strike = int(atm + 200)
            else:  # "PE"
                new_strike = int(atm - 200)

            new_symbol = f"{prefix}{expiry_token}{new_strike}{opt_type}"
            logger.info(f"Adjusted symbol from {symbol} -> {new_symbol} (atm={atm})")
            return new_symbol
        except Exception as e:
            logger.error(f"_select_strike_200_from_atm failed for {symbol}: {e}")
            raise

    # --- main candle handler ---
    def on_candle(self, symbol, candle):
        try:
            self.candles[symbol].append(candle)
            candles = list(self.candles[symbol])
            idx = len(candles) - 1

            closes = [c["close"] for c in candles]
            volumes = [c["volume"] for c in candles]
            highs = [c["high"] for c in candles]
            lows = [c["low"] for c in candles]

            ema5_arr = self.ema_series(closes, EMA_PERIOD)
            ema20_arr = self.ema_series(closes, EMA20_PERIOD)
            adx_arr = self.adx_series(highs, lows, closes, ADX_PERIOD)

            ema5 = ema5_arr[idx]
            ema20 = ema20_arr[idx]
            adx = adx_arr[idx] if idx < len(adx_arr) else None

            # --- SNAPSHOT: log indicators for all tracked symbols at this 3-min close ---
            try:
                snapshot = {s: {
                    'time': str(self.candles[s][-1]['time']) if len(self.candles[s])>0 else None,
                    'close': _safe(self.candles[s][-1]['close']),
                    'EMA5': _safe(self.ema_series([c['close'] for c in self.candles[s]], EMA_PERIOD)[-1]) if len(self.candles[s])>0 else None,
                    'EMA20': _safe(self.ema_series([c['close'] for c in self.candles[s]], EMA20_PERIOD)[-1]) if len(self.candles[s])>0 else None,
                    'ADX': _safe(self.adx_series([c['high'] for c in self.candles[s]], [c['low'] for c in self.candles[s]], [c['close'] for c in self.candles[s]], ADX_PERIOD)[-1]) if len(self.candles[s])>0 else None
                } for s in list(self.candles.keys())}
                logger.info(f"Indicators snapshot at {candle['time']}: {json.dumps(snapshot)}")
            except Exception as e:
                logger.debug(f"Failed to log indicators snapshot: {e}")

            if ema5 is None or ema20 is None:
                logger.debug(f"Not enough data for indicators for {symbol} at idx={idx} (EMA5={ema5}, EMA20={ema20}, ADX={adx})")
                return

            now = datetime.datetime.now().time()
            if not (TRADING_START <= now <= TRADING_END):
                logger.debug(f"Outside trading hours: now={now}")
                return

            st = self.state[symbol]
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
                'EMA20': _safe(ema20),
                'ADX': _safe(adx),
                'm_span': st.get('m_span'),
                'in_position': bool(pos)
            }))

            # 1) If in a position: candle-close fallback
            if pos:
                # Candle-close fallback: if high >= target for long or low <= target for short, attempt exit
                if pos.get("side", "LONG") == "LONG":
                    if candle["high"] >= pos["target"] and not pos.get("exit_initiated"):
                        self._attempt_target_exit(symbol, pos, reason="candle_fallback")
                else:  # SHORT
                    if candle["low"] <= pos["target"] and not pos.get("exit_initiated"):
                        self._attempt_target_exit(symbol, pos, reason="candle_fallback")
                return  # don't seek new entries while in position

            # 2) Not in a position: build M context and hunt setup
            m_span = self.find_recent_m(candles, ema5_arr, ema20_arr, start_dt=session_start_dt())
            if m_span:
                if st["m_span"] != m_span:
                    st["m_span"] = m_span
                    st["last_trade_idx"] = -1
                    st["breakdown_time"] = None
                    i1, i2 = m_span
                    logger.info(json.dumps({
                        'event': 'm_detected',
                        'symbol': symbol,
                        'm_span': m_span,
                        'high1_time': str(candles[i1]['time']),
                        'high1_price': candles[i1]['high'],
                        'high2_time': str(candles[i2]['time']),
                        'high2_price': candles[i2]['high'],
                        'ema5_high1': _safe(ema5_arr[i1]),
                        'ema20_high1': _safe(ema20_arr[i1]),
                        'ema5_high2': _safe(ema5_arr[i2]),
                        'ema20_high2': _safe(ema20_arr[i2])
                    }))

            if not st["m_span"]:
                return

            high1_idx, high2_idx = st["m_span"]
            if idx <= high2_idx:
                return  # still within the M

            # --- SHORT breakdown entry logic ---
            # Looking for red candle that gives breakdown: red and close < EMA5 and close < EMA20
            if self.is_red(candle) and candle["close"] < ema5 and candle["close"] < ema20:
                # record breakdown_time the first time for this M
                try:
                    if st.get("breakdown_time") is None:
                        st["breakdown_time"] = candle["time"]
                        logger.info(json.dumps({
                            'event': 'breakdown_time_recorded',
                            'symbol': symbol,
                            'breakdown_time': str(st["breakdown_time"])
                        }))
                except Exception:
                    st["breakdown_time"] = None

                # require ema5 < ema20 at breakdown candle (as per your instruction)
                ema5_below_ema20_now = (ema5 < ema20)

                # trigger patterns: bearish pin/bar or engulfing or strong red near low
                prev = candles[idx-1] if idx-1 >= 0 else None
                is_trigger_pattern = self.is_strong_red_close_near_low(candle, pct=0.01)
                if prev:
                    is_trigger_pattern = (
                        is_trigger_pattern
                        or self.is_bearish_pinbar(candle)
                        or self.is_bearish_engulfing(prev, candle)
                    )

                # ADX and wick checks (keep upper wick short check to avoid long upper wicks on breakdown)
                adx_ok = (adx is not None and adx > ADX_MIN)
                wick_ok = self.has_short_upper_wick(candle, max_ratio=0.5)

                # enforce 25-min cutoff similar logic (if desired you can keep or change)
                breakdown_allowed = True
                try:
                    bt = st.get("breakdown_time")
                    if bt is not None:
                        diff_min = (candle["time"] - bt).total_seconds() / 60.0
                        if diff_min > 25.0:
                            breakdown_allowed = False
                            logger.info(json.dumps({
                                'event': 'late_candidate_skipped',
                                'symbol': symbol,
                                'breakdown_time': str(bt),
                                'candidate_time': str(candle['time']),
                                'minutes_since_breakdown': diff_min,
                                'reason': 'exceeds_25_min_cutoff'
                            }))
                            st["m_span"] = None
                            st["breakdown_time"] = None
                except Exception as e:
                    logger.debug(f"breakdown cutoff check failed: {e}")
                    breakdown_allowed = True

                if is_trigger_pattern and adx_ok and wick_ok and ema5_below_ema20_now and breakdown_allowed:
                    entry = round_to_tick(candle["close"])

                    # Enforce global cooldown between trades
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
                            st["m_span"] = None
                            st["breakdown_time"] = None
                            return
                    except Exception as e:
                        logger.debug(f"trade cooldown check failed: {e}")

                    # SL = breakdown candle's HIGH + 10 points
                    sl_price = round_to_tick(candle["high"] + 10.0)
                    risk_points = round(sl_price - entry, 2)  # positive for short

                    if risk_points <= 0:
                        logger.info(json.dumps({'event': 'skip', 'symbol': symbol, 'reason': 'non_positive_risk', 'entry': entry, 'sl': sl_price}))
                        st["m_span"] = None
                        st["breakdown_time"] = None
                        return

                    if risk_points > MAX_SL_POINTS:
                        logger.info(json.dumps({'event': 'skip', 'symbol': symbol, 'reason': 'risk_exceeds_max', 'risk_points': risk_points, 'max_allowed': MAX_SL_POINTS}))
                        st["m_span"] = None
                        st["breakdown_time"] = None
                        return

                    # PATCH: target = entry - 1 * risk_points (1R). You can change multiplier here.
                    target_price = round_to_tick(entry - 1.0 * risk_points)

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

                    # Place market sell then SL-M (SL buy to cover)
                    sell_resp = self.fyers.place_market_sell(selected_symbol, self.lot_size, tag="NIFTYSELLENTRY")
                    sl_resp = self.fyers.place_stoploss_buy(selected_symbol, self.lot_size, sl_price, tag="NIFTYSL")

                    sl_id = self._extract_order_id(sl_resp)

                    # store short position under selected instrument symbol
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
                        "side": "SHORT"  # mark as short
                    }

                    # set last_trade_time only after we have placed the sell (start cooldown)
                    try:
                        self.last_trade_time = candle["time"]
                    except Exception:
                        pass

                    logger.info(json.dumps({
                        'event': 'short_entry_assumed_immediate',
                        'orig_symbol': symbol,
                        'selected_symbol': selected_symbol,
                        'entry': entry,
                        'sl': sl_price,
                        'risk_points': risk_points,
                        'target': target_price,
                        'multiplier': 1.0,
                        'ADX': _safe(adx),
                        'ema5_below_ema20_now': ema5_below_ema20_now,
                        'sell_resp': str(sell_resp),
                        'sl_resp': str(sl_resp)
                    }))

                    # reset M so we don't re-enter immediately on subsequent candles
                    st["m_span"] = None
                    st["last_trade_idx"] = idx
                    st["breakdown_time"] = None
                    return
                else:
                    reasons = []
                    if not is_trigger_pattern:
                        reasons.append('no_trigger_pattern')
                    if not adx_ok:
                        reasons.append(f'adx_too_low(adx={_safe(adx)})')
                    if not wick_ok:
                        reasons.append(f'upper_wick_too_long(ratio={_safe(self.upper_wick_ratio(candle))})')
                    if not ema5_below_ema20_now:
                        reasons.append('ema5_not_below_ema20')
                    if not breakdown_allowed:
                        reasons.append('breakdown_too_old')

                    logger.debug(json.dumps({
                        'event': 'immediate_candidate_rejected',
                        'symbol': symbol,
                        'idx': idx,
                        'reasons': reasons,
                        'EMA5': _safe(ema5),
                        'EMA20': _safe(ema20),
                        'ADX': _safe(adx)
                    }))

                    st["m_span"] = None
                    st["breakdown_time"] = None
                    return

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

            # Place appropriate market exit: buy to cover for short; sell for long
            try:
                if pos.get("side", "LONG") == "SHORT":
                    self.fyers.place_market_buy(symbol, self.lot_size, tag="TARGETEXITTICK")
                else:
                    self.fyers.place_market_sell(symbol, self.lot_size, tag="TARGETEXITTICK")
            except Exception as e:
                logger.error(f"Market exit attempt failed for {symbol}: {e}")
        except Exception as e:
            logger.error(f"_attempt_target_exit error for {symbol}: {e}")

    # --- tick-level handler for immediate exits & robust trailing SL ---
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

            # trailing SL (adapted to use _safe_cancel_and_place_sl which respects side)
            if pos.get("entry") is not None:
                # LONG trailing (unchanged)
                if pos.get("side", "LONG") == "LONG" and ltp > pos["entry"]:
                    move_up = pos["max_ltp"] - pos["entry"]
                    trail_step = int(move_up // 10)
                # SHORT trailing: compute how much price has moved down from entry
                elif pos.get("side", "SHORT") == "SHORT" and ltp < pos["entry"]:
                    move_up = pos["entry"] - ltp
                    trail_step = int(move_up // 10)
                else:
                    trail_step = -1

                if trail_step <= pos.get("last_trail_step", -1):
                    pass
                else:
                    if pos.get("side", "LONG") == "LONG":
                        desired_sl = round_to_tick(orig_sl + trail_step * 5)
                    else:
                        # For short: move SL DOWN by 5 points for every 10 points down move
                        desired_sl = round_to_tick(orig_sl - trail_step * 5)

                    # For LONG: we only place if desired_sl > current sl (improvement)
                    # For SHORT: we only place if desired_sl < current sl (improvement for short)
                    try_place = False
                    if pos.get("side", "LONG") == "LONG":
                        if desired_sl and desired_sl > pos["sl"]:
                            try_place = True
                    else:
                        if desired_sl and desired_sl < pos["sl"]:
                            try_place = True

                    if try_place:
                        try:
                            sl_resp, sl_id = self._safe_cancel_and_place_sl(symbol, pos, desired_sl, tag="NIFTYSLTRAIL")
                            if sl_resp:
                                pos["sl"] = desired_sl
                                if sl_id:
                                    pos["sl_order_id"] = sl_id
                                pos["last_trail_step"] = trail_step
                                logger.info(f"Trailing SL applied for {symbol}: step={trail_step}, sl={desired_sl}, max_ltp={pos.get('max_ltp')}")
                            else:
                                logger.debug(f"No new SL placed for {symbol} (desired_sl={desired_sl})")
                        except Exception as e:
                            logger.error(f"Failed to update SL for {symbol}: {e}")

            # --- Target exit logic (existing) ---
            if pos.get("side", "LONG") == "LONG":
                if ltp >= pos["target"] and not pos.get("exit_initiated"):
                    logger.debug(f"Tick-level target detected for {symbol} ltp={ltp} target={pos['target']}")
                    self._attempt_target_exit(symbol, pos, reason="tick_target")
            else:
                if ltp <= pos["target"] and not pos.get("exit_initiated"):
                    logger.debug(f"Tick-level target detected for SHORT {symbol} ltp={ltp} target={pos['target']}")
                    self._attempt_target_exit(symbol, pos, reason="tick_target")

        except Exception as e:
            logger.error(f"on_tick error: {e}")

    # --- trade handler ---
    def on_trade(self, msg):
        if not msg.get("trades"):
            return
        try:
            t = msg["trades"]
            # handle list case
            if isinstance(t, list):
                for item in t:
                    self.on_trade({"trades": item})
                return

            side = t.get("side") or t.get("s")
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

            tag = tag if tag is not None else ""

            logger.info(json.dumps({'event': 'trade_msg', 'symbol': symbol, 'side': side, 'tag': tag, 'fill_price': fill_price, 'raw': t}))

            # SL hit for LONG positions (SL sell fills)
            if int(side) == -1 and "NIFTYSL" in tag:
                pos = self.positions.get(symbol)
                if pos and pos.get("side", "LONG") == "LONG":
                    exit_price = round_to_tick(fill_price) if fill_price else round_to_tick(pos["sl"])
                    pnl = (exit_price - pos["entry"]) * self.lot_size
                    log_trade(symbol, "BUY", pos["entry"], pos["sl"], pos["target"], exit_price, pnl)
                    logger.info(f"SL HIT LONG {symbol} @ {exit_price} PnL={pnl}")
                    self.positions[symbol] = None
                return

            # SL hit for SHORT positions (SL buy fills)
            if int(side) == 1 and "NIFTYSL" in tag:
                pos = self.positions.get(symbol)
                if pos and pos.get("side") == "SHORT":
                    exit_price = round_to_tick(fill_price) if fill_price else round_to_tick(pos["sl"])
                    # For short, profit = entry - exit_price
                    pnl = (pos["entry"] - exit_price) * self.lot_size
                    log_trade(symbol, "SELL", pos["entry"], pos["sl"], pos["target"], exit_price, pnl)
                    logger.info(f"SL HIT SHORT {symbol} @ {exit_price} PnL={pnl}")
                    self.positions[symbol] = None
                return

            # Our target exit fills (market buy/sell)
            if int(side) == -1 and ("TARGETEXIT" in tag or "TARGETEXITTICK" in tag):
                # market sell fill (closing long)
                pos = self.positions.get(symbol)
                if pos and pos.get("side", "LONG") == "LONG":
                    if fill_price:
                        exit_price = round_to_tick(fill_price)
                    else:
                        exit_price = round_to_tick(pos["target"])
                    pnl = (exit_price - pos["entry"]) * self.lot_size
                    log_trade(symbol, "BUY", pos["entry"], pos["sl"], pos["target"], exit_price, pnl)
                    logger.info(f"TARGET EXIT FILL LONG {symbol} @ {exit_price} PnL={pnl} (tag={tag})")
                    self.positions[symbol] = None
                return

            if int(side) == 1 and ("TARGETEXIT" in tag or "TARGETEXITTICK" in tag):
                # market buy fill (closing short)
                pos = self.positions.get(symbol)
                if pos and pos.get("side") == "SHORT":
                    if fill_price:
                        exit_price = round_to_tick(fill_price)
                    else:
                        exit_price = round_to_tick(pos["target"])
                    pnl = (pos["entry"] - exit_price) * self.lot_size
                    log_trade(symbol, "SELL", pos["entry"], pos["sl"], pos["target"], exit_price, pnl)
                    logger.info(f"TARGET EXIT FILL SHORT {symbol} @ {exit_price} PnL={pnl} (tag={tag})")
                    self.positions[symbol] = None
                return

            # Entry fills: update stored entry if broker returned fill
            if int(side) == -1 and ("NIFTYSELLENTRY" in tag):
                pos = self.positions.get(symbol)
                if pos and pos.get("side") == "SHORT" and fill_price:
                    old_entry = pos["entry"]
                    new_entry = round_to_tick(fill_price)
                    pos["entry"] = new_entry
                    logger.info(f"Updated short entry price for {symbol} from {old_entry} to actual fill {new_entry}")
                return

            if int(side) == 1 and ("NIFTYBUYENTRY" in tag):
                pos = self.positions.get(symbol)
                if pos and pos.get("side") == "LONG" and fill_price:
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

    engine.prefill_history(option_symbols, days_back=2)

    fyers_client.register_trade_callback(engine.on_trade)
    fyers_client.subscribe_market_data(option_symbols, engine.on_candle, on_tick_callback=engine.on_tick)
    fyers_client.start_order_socket()
