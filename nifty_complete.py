#!/usr/bin/env python3
"""
NiftySTRAT15 - full script (ready to run) - PATCHED
Changes:
 - log_to_journal accepts exit alias
 - tag sanitization (sanitize_tag) to satisfy broker orderTag alpha-num rule
 - added place_market_sell in FyersClient
 - hedge_qty stored in position
 - improved on_trade_update lookup for hedge fills
 - SL stop price rounded to exchange tick (round up/ceil) before placing SL order
 - ONE-TIME trailing SL: when price moves 1:1 in our favor, SL moves to entry+2 (rounded to nearest tick 0.05). Happens only once.
"""

import os
import time
import csv
import datetime
import logging
import re
from collections import defaultdict, deque
from decimal import Decimal, ROUND_UP, ROUND_HALF_UP, ROUND_FLOOR, getcontext

from dotenv import load_dotenv

# Set decimal precision for tick rounding
getcontext().prec = 12

# ---------------- CONFIG ----------------
load_dotenv()
CLIENT_ID = os.getenv("FYERS_CLIENT_ID")
ACCESS_TOKEN = os.getenv("FYERS_ACCESS_TOKEN")  # Format often CLIENTID:token for sockets
LOT_SIZE = int(os.getenv("LOT_SIZE", 75))
STRATEGY_NAME = "NiftySTRAT15"
JOURNAL_FILE = "nifty_15_buy_trades.csv"
LOG_FILE = "nifty_15_buy_strategy.log"

TRADING_START = datetime.time(9, 15)
FORCED_EXIT_TIME = datetime.time(15, 8)  # 3:08 PM IST candle-time used for forced exit
TRADING_END = FORCED_EXIT_TIME

# HEDGE config
HEDGE_OFFSET = int(os.getenv("HEDGE_OFFSET", 1000))    # distance in points from main strike for hedge
HEDGE_MANDATORY = os.getenv("HEDGE_MANDATORY", "False").lower() in ("1", "true", "yes")
HEDGE_LOT_MULTIPLIER = float(os.getenv("HEDGE_LOT_MULTIPLIER", 1.0))  # hedge quantity = LOT_SIZE * multiplier

# SL / Risk config
MAX_SL_PER_TRADE = float(os.getenv("MAX_SL_PER_TRADE", 25))  # maximum SL width allowed per trade (points)
MAX_DAILY_SL = float(os.getenv("MAX_DAILY_SL", 100))  # maximum cumulative SL width allowed per day (points)

# Tick size for instruments (default 0.05). You can override via .env: TICK_SIZE=0.05
TICK_SIZE = Decimal(os.getenv("TICK_SIZE", "0.05"))

# ---------------- Utilities ----------------
def sanitize_tag(tag: str) -> str:
    """Return only alphanumeric characters for orderTag (broker requires that)."""
    if tag is None:
        return ""
    out = re.sub(r'[^A-Za-z0-9]', '', str(tag))
    return out[:40]

def round_to_tick(price, tick=TICK_SIZE, method="nearest"):
    """
    Round `price` (Decimal or numeric) to multiple of `tick` (Decimal).
    method: "nearest", "ceil", "floor"
    Returns Decimal.
    """
    try:
        p = Decimal(str(price))
        t = Decimal(str(tick))
    except Exception:
        return Decimal(str(price))
    if t == 0:
        return p

    quotient = p / t

    if method == "nearest":
        rounded_q = quotient.quantize(Decimal('1'), rounding=ROUND_HALF_UP)
    elif method == "ceil":
        # if quotient already integer, keep it, else integer+1
        if quotient == quotient.to_integral_value():
            rounded_q = quotient.to_integral_value()
        else:
            rounded_q = quotient.to_integral_value(rounding=ROUND_UP)
    elif method == "floor":
        rounded_q = quotient.to_integral_value(rounding=ROUND_FLOOR)
    else:
        rounded_q = quotient.quantize(Decimal('1'), rounding=ROUND_HALF_UP)

    result = (rounded_q * t).normalize()
    return result

# Ensure journal exists with headers
if not os.path.exists(JOURNAL_FILE):
    with open(JOURNAL_FILE, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "timestamp", "symbol", "strategy", "action",
            "entry_price", "sl_price", "sl_width", "exit_price", "remarks"
        ])

# Configure logging
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
# Also log DEBUG to console for debugging during development
console = logging.StreamHandler()
console.setLevel(logging.DEBUG)
console.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logging.getLogger().addHandler(console)

# ---------------- JOURNAL ----------------
def log_to_journal(symbol, action, strategy, entry=None, sl=None, exit_price=None, exit=None, remarks=""):
    """
    Write a line to the CSV journal.
    Accepts both exit_price and exit (alias) to be backwards-compatible with older calls.
    """
    # allow either exit or exit_price as the exit value (backwards compat)
    if exit_price is None and exit is not None:
        exit_price = exit

    sl_width = None
    if entry is not None and sl is not None:
        try:
            sl_width = float(sl) - float(entry)
        except Exception:
            sl_width = None
    with open(JOURNAL_FILE, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            symbol, strategy, action,
            entry, sl, sl_width, exit_price, remarks
        ])

# ---------------- INDICATORS ----------------
def compute_ema(prices, period):
    if prices is None or len(prices) < period:
        return None
    k = 2 / (period + 1)
    ema = prices[0]
    for p in prices[1:]:
        ema = p * k + ema * (1 - k)
    return ema


def compute_vwma(closes, volumes, period):
    if len(closes) < period or len(volumes) < period:
        return None
    num = sum(c * v for c, v in zip(closes[-period:], volumes[-period:]))
    den = sum(volumes[-period:])
    if den == 0:
        return None
    return num / den

# ---------------- FYERS CLIENT WRAPPER ----------------
try:
    from fyers_apiv3 import fyersModel
    from fyers_apiv3.FyersWebsocket import order_ws, data_ws
except Exception:
    fyersModel = None
    order_ws = None
    data_ws = None

class FyersClient:
    """
    Minimal wrapper around fyersModel + sockets to provide:
      - quotes()
      - history()
      - place_limit_sell(), place_stoploss_buy(), place_market_buy(), place_market_sell(), cancel_order()
      - subscribe_market_data(instrument_ids, on_candle_callback)
      - start_order_socket()
      - register_trade_callback(cb)
    """
    def __init__(self, client_id, access_token, lot_size=LOT_SIZE):
        self.client_id = client_id
        self.access_token = access_token
        # Some SDKs want clientid:token for socket auth — we'll build both
        self.auth_token = f"{client_id}:{access_token}" if client_id and access_token else access_token
        self.lot_size = lot_size
        self.client = None
        if fyersModel:
            try:
                self.client = fyersModel.FyersModel(client_id=client_id, token=access_token, is_async=False, log_path="")
                logging.info("[FYERS] FyersModel initialized.")
            except Exception as e:
                logging.exception("[FYERS] Failed to init FyersModel client - running in mock mode")
                self.client = None
        else:
            logging.info("[FYERS] fyers_apiv3 not available; running in mock mode")

        # callbacks to forward order/trade messages to engine
        self.order_callbacks = []
        self.trade_callbacks = []

        # internal sockets
        self._data_socket = None
        self._order_socket = None

    # ---------------- Brokerage order helpers ----------------
    def _log_order_resp(self, action, resp):
        logging.info(f"{action} Response: {resp}")
        print(f"{action} Response: {resp}")
        return resp

    def place_limit_sell(self, symbol: str, qty: int, price: float, tag: str = ""):
        tag = sanitize_tag(tag)
        logging.info(f"[BROKER] Place limit SELL {symbol} qty={qty} price={price} tag={tag}")
        if not self.client:
            return {"s": "mock", "msg": "limit_sell_mock"}
        try:
            data = {
                "symbol": symbol, "qty": qty, "type": 1, "side": -1,
                "productType": "INTRADAY", "limitPrice": price, "stopPrice": 0,
                "validity": "DAY", "disclosedQty": 0, "offlineOrder": False, "orderTag": tag
            }
            return self._log_order_resp("Limit Sell", self.client.place_order(data))
        except Exception as e:
            logging.exception("[BROKER] place_limit_sell failed")
            return {"s": "error", "msg": str(e)}

    def place_stoploss_buy(self, symbol: str, qty: int, stop_price: float, tag: str = "", limit_price: float = None):
        """
        Place a trigger order (stop). For options SL-M may be blocked; use SL-L by passing limit_price.
        If limit_price is None, we set limit_price = stop_price (or stop_price + tick) to force SL-L.
        """
        tag = sanitize_tag(tag)
        logging.info(f"[BROKER] Place SL BUY {symbol} qty={qty} stop_price={stop_price} limit_price={limit_price} tag={tag}")
        if not self.client:
            return {"s": "mock", "msg": "sl_buy_mock", "id": f"mock-sl-{datetime.datetime.now().timestamp()}"}
        try:
            # If caller didn't provide a limit, set it to the stop_price (or add a tick buffer)
            if limit_price is None:
                # ensure limit >= trigger for a BUY trigger (exchange rule)
                limit_price = float(Decimal(str(stop_price)) + TICK_SIZE)

            data = {
                "symbol": symbol,
                "qty": qty,
                "type": 3,         # trigger order
                "side": 1,         # buy
                "productType": "INTRADAY",
                "limitPrice": float(limit_price),  # non-zero => SL-L (Trigger-Limit)
                "stopPrice": float(stop_price),
                "validity": "DAY",
                "disclosedQty": 0,
                "offlineOrder": False,
                "orderTag": tag
            }
            return self._log_order_resp("SL Buy", self.client.place_order(data))
        except Exception as e:
            logging.exception("[BROKER] place_stoploss_buy failed")
            return {"s": "error", "msg": str(e)}

    def place_market_buy(self, symbol: str, qty: int, tag: str = ""):
        tag = sanitize_tag(tag)
        logging.info(f"[BROKER] Place MARKET BUY {symbol} qty={qty} tag={tag}")
        if not self.client:
            return {"s": "mock", "msg": "market_buy_mock"}
        try:
            data = {
                "symbol": symbol, "qty": qty, "type": 2, "side": 1,
                "productType": "INTRADAY", "limitPrice": 0, "stopPrice": 0,
                "validity": "DAY", "disclosedQty": 0, "offlineOrder": False, "orderTag": tag
            }
            return self._log_order_resp("Market Buy", self.client.place_order(data))
        except Exception as e:
            logging.exception("[BROKER] place_market_buy failed")
            return {"s": "error", "msg": str(e)}

    def place_market_sell(self, symbol: str, qty: int, tag: str = ""):
        tag = sanitize_tag(tag)
        logging.info(f"[BROKER] Place MARKET SELL {symbol} qty={qty} tag={tag}")
        if not self.client:
            return {"s": "mock", "msg": "market_sell_mock"}
        try:
            data = {
                "symbol": symbol, "qty": qty, "type": 2, "side": -1,
                "productType": "INTRADAY", "limitPrice": 0, "stopPrice": 0,
                "validity": "DAY", "disclosedQty": 0, "offlineOrder": False, "orderTag": tag
            }
            return self._log_order_resp("Market Sell", self.client.place_order(data))
        except Exception as e:
            logging.exception("[BROKER] place_market_sell failed")
            return {"s": "error", "msg": str(e)}

    def cancel_order(self, order_id: str):
        logging.info(f"[BROKER] Cancel order {order_id}")
        if not self.client:
            return {"s": "mock", "msg": "cancel_mock", "id": order_id}
        try:
            return self._log_order_resp("Cancel Order", self.client.cancel_order({"id": order_id}))
        except Exception as e:
            logging.exception("[BROKER] cancel_order failed")
            return {"s": "error", "msg": str(e)}

    # ---------------- Market data & history helpers ----------------
    def quotes(self, symbols_payload):
        """Wrapper for quotes API. symbols_payload example: {'symbols':'NSE:NIFTY50-INDEX'}"""
        if not self.client:
            # return a mock structure similar to fyers v2 (d -> list of data objects)
            return {"s": "mock", "d": [{"v": {"lp": 0}}]}
        try:
            return self.client.quotes(symbols_payload)
        except Exception as e:
            logging.exception("[FYERS] quotes() failed")
            return {"s": "error", "msg": str(e)}

    def history(self, params):
        """Wrapper for history API expected by prefill_history"""
        if not self.client:
            return {"s": "mock", "candles": []}
        try:
            return self.client.history(params)
        except Exception as e:
            logging.exception("[FYERS] history() failed")
            return {"s": "error", "msg": str(e)}

    # ---------------- Socket registration ----------------
    def register_order_callback(self, cb):
        self.order_callbacks.append(cb)

    def register_trade_callback(self, cb):
        self.trade_callbacks.append(cb)

    # ---------------- Market data subscription (15-min bucketer) ----------------
    def subscribe_market_data(self, instrument_ids, on_candle_callback):
        """
        Subscribe to a set of instrument_ids and convert incoming ticks to 15-min candles.
        `on_candle_callback(symbol, candle_dict)` will be called for each *completed* 15-min candle.
        """
        logging.info(f"[FYERS] subscribe_market_data called for {instrument_ids}")

        # We'll create an internal tick -> 15-min bucketer per symbol
        candle_buffers = defaultdict(lambda: None)

        def on_message(tick):
            # tick expected to have: 'symbol', 'ltp' (or 'last_price'), 'last_traded_time', 'vol_traded_today' (cum vol)
            try:
                if "symbol" not in tick or ("ltp" not in tick and "last_price" not in tick):
                    logging.debug(f"[DATA] Ignoring unexpected tick format: {tick}")
                    return
                symbol = tick["symbol"]
                ltp = float(tick.get("ltp") or tick.get("last_price"))
                ts_raw = tick.get("last_traded_time") or tick.get("timestamp") or tick.get("ltp_time")
                if ts_raw is None:
                    ts = int(time.time())
                else:
                    try:
                        ts = int(ts_raw)
                    except Exception:
                        # sometimes the SDK already provides a datetime string - try float
                        try:
                            ts = int(float(ts_raw))
                        except Exception:
                            ts = int(time.time())
                cum_vol = int(tick.get("vol_traded_today", 0) or tick.get("volume", 0))
            except Exception as e:
                logging.exception("[DATA] Bad tick format")
                return

            # Convert ts to datetime; fyers history may give ms or s
            try:
                if ts > 10**12:  # milliseconds
                    dt = datetime.datetime.fromtimestamp(ts / 1000)
                else:
                    dt = datetime.datetime.fromtimestamp(ts)
            except Exception:
                dt = datetime.datetime.now()

            # Determine 15-min bucket start time: floor to nearest 15 min
            minute_bucket = (dt.minute // 15) * 15
            candle_time = dt.replace(second=0, microsecond=0, minute=minute_bucket)

            c = candle_buffers[symbol]
            if c is None or c["time"] != candle_time:
                # finalize existing candle if present
                if c is not None:
                    try:
                        on_candle_callback(symbol, c)
                    except Exception:
                        logging.exception("[DATA] on_candle_callback failed for finished candle")
                # start new candle
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
                # update ongoing bucket
                c["high"] = max(c["high"], ltp)
                c["low"] = min(c["low"], ltp)
                c["close"] = ltp
                # volume: difference of cumulative volume
                try:
                    c["volume"] = max(0, cum_vol - c.get("start_cum_vol", 0))
                except Exception:
                    c["volume"] = c.get("volume", 0)

        # Build a data socket and hook callbacks like in your original code
        def on_open():
            try:
                # subscribe to provided instrument ids; many SDKs expect comma-separated string or list
                if isinstance(instrument_ids, (list, tuple)):
                    subs = ",".join(instrument_ids)
                else:
                    subs = instrument_ids
                logging.info(f"[DATA] Subscribing to: {subs}")
                # SDK may vary; attempt two variants for safety
                try:
                    # prefer the SDK method used in your reference
                    self._data_socket.subscribe(symbols=instrument_ids, data_type="SymbolUpdate")
                except Exception:
                    try:
                        self._data_socket.subscribe(symbols=subs, data_type="SymbolUpdate")
                    except Exception:
                        logging.exception("[DATA] subscribe call threw")
                self._data_socket.keep_running()
            except Exception:
                logging.exception("[DATA] Error in on_open (subscribe/keep_running)")

        try:
            # create data socket using SDK if available
            if data_ws and self.auth_token:
                self._data_socket = data_ws.FyersDataSocket(
                    access_token=self.auth_token,
                    log_path="",
                    on_connect=on_open,
                    on_close=lambda m: logging.info(f"[DATA] Data socket closed: {m}"),
                    on_error=lambda m: logging.error(f"[DATA] Data socket error: {m}"),
                    on_message=on_message
                )
                # connect in background thread/loop - this will block inside connect() depending on SDK
                logging.info("[DATA] Connecting data socket...")
                try:
                    self._data_socket.connect()
                except Exception:
                    logging.exception("[DATA] data_socket.connect() failed")
            else:
                logging.warning("[DATA] data_ws not available or no auth_token — cannot open live data socket. Use mock data or integrate your own bucketer.")
        except Exception:
            logging.exception("[DATA] subscribe_market_data setup failed")

    # ---------------- Order socket (orders/trades) ----------------
    def start_order_socket(self):
        logging.info("[FYERS] start_order_socket called")

        def on_order(msg):
            logging.info(f"[ORDER] Order update: {msg}")
            for cb in self.order_callbacks:
                try:
                    cb(msg)
                except Exception:
                    logging.exception("[ORDER] order callback failed")

        def on_trade(msg):
            logging.info(f"[TRADE] Trade update: {msg}")
            for cb in self.trade_callbacks:
                try:
                    cb(msg)
                except Exception:
                    logging.exception("[TRADE] trade callback failed")

        def on_open():
            try:
                # subscribe to orders & trades
                if self._order_socket:
                    try:
                        self._order_socket.subscribe(data_type="OnOrders,OnTrades")
                    except Exception:
                        try:
                            self._order_socket.subscribe(data_type="orders,trade")
                        except Exception:
                            logging.exception("[ORDER] subscribe failed")
                    self._order_socket.keep_running()
            except Exception:
                logging.exception("[ORDER] Error in on_open (subscribe/keep_running)")

        try:
            if order_ws and self.auth_token:
                self._order_socket = order_ws.FyersOrderSocket(
                    access_token=self.auth_token,
                    write_to_file=False, log_path="",
                    on_connect=on_open,
                    on_close=lambda m: logging.info(f"[ORDER] Order socket closed: {m}"),
                    on_error=lambda m: logging.error(f"[ORDER] order socket error: {m}"),
                    on_orders=on_order,
                    on_trades=on_trade
                )
                logging.info("[ORDER] Connecting order socket...")
                try:
                    self._order_socket.connect()
                except Exception:
                    logging.exception("[ORDER] order_socket.connect() failed")
            else:
                logging.warning("[ORDER] order_ws not available or no auth_token — cannot open order socket.")
        except Exception:
            logging.exception("[ORDER] start_order_socket failed")


# ---------------- ATM SYMBOL & EXPIRY UTIL ----------------
def get_next_tuesday(from_date=None):
    today = from_date or datetime.date.today()
    weekday = today.weekday()  # Monday=0 .. Sunday=6
    days_to_tue = (1 - weekday) % 7
    expiry = today + datetime.timedelta(days=days_to_tue)
    return expiry


def format_expiry(expiry_date):
    yy = expiry_date.strftime("%y")
    m = expiry_date.month
    d = expiry_date.day
    if m < 10:
        return f"{yy}{m}{d:02d}"
    else:
        return f"{yy}{m:02d}{d:02d}"


def get_atm_symbols(fyers_client: FyersClient):
    """
    Fetch NIFTY spot and compute ATM strike (nearest 50), return CE/PE strings.
    Uses fyers_client.quotes({'symbols':'NSE:NIFTY50-INDEX'})
    """
    try:
        data = {"symbols": "NSE:NIFTY50-INDEX"}
        resp = fyers_client.quotes(data)
        if not resp or not resp.get("d"):
            logging.error(f"[ATM] Failed to fetch NIFTY spot: {resp}")
            raise Exception(f"Failed to fetch NIFTY spot: {resp}")

        # Fyres returns d -> list of objects; last price often at ['d'][0]['v']['lp']
        ltp = float(resp["d"][0]["v"]["lp"])
        atm_strike = int(round(ltp / 50.0) * 50)
        expiry = get_next_tuesday()
        expiry_str = format_expiry(expiry)

        ce_symbol = f"NSE:NIFTY{expiry_str}{atm_strike}CE"
        pe_symbol = f"NSE:NIFTY{expiry_str}{atm_strike}PE"

        logging.info(f"[ATM SYMBOLS] CE={ce_symbol}, PE={pe_symbol}")
        print(f"[ATM SYMBOLS] CE={ce_symbol}, PE={pe_symbol}")
        return [ce_symbol, pe_symbol]
    except Exception as e:
        logging.exception("[ATM] Exception computing ATM")
        raise

# ----------------- HEDGING UTIL (assume 7-char tail: 24500PE) -----------------
def parse_option_symbol(symbol: str):
    """
    Parse option symbol assuming last 7 characters are <5-digit-strike><CE|PE>,
    e.g. "NSE:NIFTY24091624500PE" -> strike=24500, opt_type='PE', expiry='240916'.
    Returns dict { 'prefix','expiry','strike','opt_type' } or None if parse fails.
    """
    try:
        # Remove exchange prefix if present
        if ":" in symbol:
            _, rest = symbol.split(":", 1)
        else:
            rest = symbol

        # remove leading 'NIFTY' if present
        rest2 = rest
        if rest2.upper().startswith("NIFTY"):
            rest2 = rest2[5:]

        # Expect last 7 chars = 5-digit strike + 2-char opt type
        if len(rest2) < 7:
            logging.warning(f"[HEDGE] Symbol too short to parse: {symbol}")
            return None

        tail = rest2[-7:]           # e.g., '24500PE'
        opt_type = tail[-2:].upper()  # 'PE' or 'CE'
        strike_str = tail[:-2]        # '24500'
        expiry_str = rest2[:-7]       # what's left is expiry (may be variable length)

        if opt_type not in ("CE", "PE"):
            logging.warning(f"[HEDGE] Unexpected opt type in symbol tail: {tail}")
            return None

        strike = int(strike_str)
        return {"prefix": "NSE:NIFTY", "expiry": expiry_str, "strike": strike, "opt_type": opt_type}
    except Exception:
        logging.exception("[HEDGE] parse_option_symbol failed")
        return None


def build_option_symbol_from_parts(parts: dict, strike: int, opt_type: str = None):
    """
    Rebuild a symbol given parsed parts + new strike.
    Returns form: 'NSE:NIFTY{expiry}{strike}{opt_type}' (matches get_atm_symbols format).
    """
    if not parts:
        return None
    opt_type = opt_type or parts.get("opt_type")
    expiry = parts.get("expiry", "")
    return f"NSE:NIFTY{expiry}{strike}{opt_type}"

# ---------------- STRATEGY ENGINE ----------------
class NiftySTRAT15Engine:
    def __init__(self, fyers_client: FyersClient, lot_size=LOT_SIZE):
        self.fyers = fyers_client
        self.lot_size = lot_size
        self.candles = defaultdict(lambda: deque(maxlen=500))
        self.positions = {}  # symbol -> pos (main symbol keyed)
        self.sl_hits = []    # list of sl widths hit today
        self.today_date = datetime.date.today()

    def prefill_history(self, symbols, days_back=1):
        for symbol in symbols:
            try:
                to_date = datetime.datetime.now().strftime("%Y-%m-%d")
                from_date = (datetime.datetime.now() - datetime.timedelta(days=days_back)).strftime("%Y-%m-%d")
                params = {
                    "symbol": symbol,
                    "resolution": "15",
                    "date_format": "1",
                    "range_from": from_date,
                    "range_to": to_date,
                    "cont_flag": "1"
                }
                hist = self.fyers.history(params)
                if hist.get("candles"):
                    for c in hist["candles"]:
                        # some APIs return ms timestamps; handle both
                        ts_raw = c[0]
                        ts = int(ts_raw)
                        if ts > 10**12:
                            ts_dt = datetime.datetime.fromtimestamp(ts / 1000)
                        else:
                            ts_dt = datetime.datetime.fromtimestamp(ts)
                        candle = {
                            "time": ts_dt,
                            "open": c[1],
                            "high": c[2],
                            "low": c[3],
                            "close": c[4],
                            "volume": c[5] if len(c) > 5 else 0
                        }
                        self.candles[symbol].append(candle)
                    logging.info(f"[PREFILL] {symbol}: loaded {len(self.candles[symbol])} candles")
                else:
                    logging.info(f"[PREFILL] No candles for {symbol}: {hist}")
            except Exception:
                logging.exception(f"[PREFILL] Failed for {symbol}")

    def add_candle(self, symbol, candle):
        # Reset per-day when day changes
        if candle["time"].date() != self.today_date:
            self.today_date = candle["time"].date()
            self.sl_hits = []
            logging.info("[DAY RESET] New trading day - cleared SL history.")
        # Append and process
        self.candles[symbol].append(candle)
        logging.debug(f"[CANDLE] {symbol} {candle['time']} O:{candle['open']} H:{candle['high']} L:{candle['low']} C:{candle['close']} V:{candle.get('volume',0)}")
        try:
            self.on_new_candle(symbol, candle)
        except Exception:
            logging.exception("[ENGINE] on_new_candle failed")

    def check_entry_condition(self, symbol):
        candles = list(self.candles[symbol])
        if len(candles) < 20:
            return None
        last = candles[-1]
        closes = [c["close"] for c in candles]
        volumes = [c["volume"] for c in candles]
        ema5 = compute_ema(closes, 5)
        vwma20 = compute_vwma(closes, volumes, 20)
        if ema5 is None or vwma20 is None:
            return None

        is_red = last["close"] < last["open"]
        if not is_red:
            return None
        if not (last["high"] > ema5 and last["close"] < ema5):
            return None
        if not (ema5 < vwma20):
            return None

        entry = last["close"]
        sl_price = last["high"] + (0.01 * last["high"])  # 1% of high
        sl_width = sl_price - entry

        # Per-trade SL cap
        if sl_width > MAX_SL_PER_TRADE:
            logging.info(f"[{STRATEGY_NAME}] Skip entry: SL width {sl_width:.2f} > {MAX_SL_PER_TRADE}")
            return None

        # Daily cumulative SL cap
        total_sl_today = sum(self.sl_hits) if self.sl_hits else 0.0
        if (total_sl_today + sl_width) > MAX_DAILY_SL:
            logging.info(f"[{STRATEGY_NAME}] Skip entry: Daily SL limit: existing sum={total_sl_today:.2f}, prospective={sl_width:.2f}, max_daily={MAX_DAILY_SL}")
            return None

        logging.info(f"[{STRATEGY_NAME}] EntryOK: entry={entry:.2f}, sl={sl_price:.2f}, sl_width={sl_width:.2f}")
        return {"entry": entry, "sl": sl_price, "sl_width": sl_width, "candle": last, "ema5": ema5, "vwma20": vwma20}

    def place_entry(self, symbol, entry_price, sl_price):
        """
        Buy hedge first (market buy), then place the limit SELL (to open short) and SL (stop-buy).
        Hedge is same option type but HEDGE_OFFSET away (± depending on CE/PE).
        """
        entry_tag = sanitize_tag(f"{STRATEGY_NAME}_ENTRY")
        sl_tag = sanitize_tag(f"{STRATEGY_NAME}_SL")
        hedge_tag = sanitize_tag(f"{STRATEGY_NAME}_HEDGE")

        hedge_resp = None
        hedge_symbol = None
        hedge_qty = max(1, int(self.lot_size * HEDGE_LOT_MULTIPLIER))

        # Attempt to compute hedge symbol
        parsed = parse_option_symbol(symbol)
        if parsed:
            if parsed["opt_type"] == "PE":
                hedge_strike = parsed["strike"] - HEDGE_OFFSET
            else:  # CE
                hedge_strike = parsed["strike"] + HEDGE_OFFSET

            # round hedge strike to nearest 50 for safety
            try:
                hedge_strike = int(round(hedge_strike / 50.0) * 50)
            except Exception:
                pass

            # basic sanity: ensure hedge strike positive
            if hedge_strike > 0:
                hedge_symbol = build_option_symbol_from_parts(parsed, hedge_strike, parsed["opt_type"])
                try:
                    logging.info(f"[{STRATEGY_NAME}] Placing hedge MARKET BUY for {hedge_symbol} qty={hedge_qty}")
                    hedge_resp = self.fyers.place_market_buy(hedge_symbol, hedge_qty, tag=hedge_tag)
                    log_to_journal(hedge_symbol, "HEDGE_BUY", STRATEGY_NAME, entry=None, sl=None, exit=None,
                                   remarks=f"Hedge buy placed before entry. resp={hedge_resp}")
                except Exception:
                    logging.exception(f"[{STRATEGY_NAME}] Hedge buy failed for {hedge_symbol}")
                    hedge_resp = {"s": "error", "msg": "hedge_failed"}
            else:
                logging.warning(f"[{STRATEGY_NAME}] Computed invalid hedge strike {hedge_strike} for {symbol}")
        else:
            logging.warning(f"[{STRATEGY_NAME}] Could not parse symbol to compute hedge: {symbol}")

        # If hedge is mandatory and hedge_resp indicates failure, abort main entry
        hedge_ok = True
        if HEDGE_MANDATORY:
            if not hedge_symbol or not hedge_resp or (isinstance(hedge_resp, dict) and hedge_resp.get("s") == "error"):
                logging.warning(f"[{STRATEGY_NAME}] Hedge mandatory but hedge failed - aborting main entry for {symbol}")
                hedge_ok = False

        if not hedge_ok:
            log_to_journal(symbol, "ENTRY_ABORTED", STRATEGY_NAME, entry=entry_price, sl=sl_price, remarks=f"Hedge failed and HEDGE_MANDATORY=True. hedge_resp={hedge_resp}")
            return None

        # Proceed to place main entry (limit SELL) and SL (stop-buy)
        logging.info(f"[{STRATEGY_NAME}] Placing main SELL {symbol} qty={self.lot_size} at {entry_price}")
        sell_resp = self.fyers.place_limit_sell(symbol, self.lot_size, entry_price, entry_tag)

        # --- TICK ROUNDING: adjust SL to instrument tick multiple (use ceil so stop >= requested)
        try:
            sl_decimal = Decimal(str(sl_price))
            sl_adj = round_to_tick(sl_decimal, tick=TICK_SIZE, method="ceil")
            logging.info(f"[{STRATEGY_NAME}] Requested SL={sl_price} adjusted to SL_adj={sl_adj} (tick={TICK_SIZE})")
        except Exception:
            sl_adj = Decimal(str(sl_price))
            logging.exception(f"[{STRATEGY_NAME}] SL tick-rounding failed for {sl_price}, using original")

        logging.info(f"[{STRATEGY_NAME}] Placing SL buy for {symbol} qty={self.lot_size} at {sl_adj}")
        # Use SL-L by providing a limit slightly worse/safer than trigger to satisfy "limit >= trigger" (BUY)
        limit_for_sl = float(Decimal(str(sl_adj)) + TICK_SIZE)
        sl_resp = self.fyers.place_stoploss_buy(symbol, self.lot_size, float(sl_adj), sl_tag, limit_price=limit_for_sl)


        # compute initial SL width and store sl_moved flag so we only trail once
        try:
            initial_sl_width = float(Decimal(str(sl_price)) - Decimal(str(entry_price)))
        except Exception:
            try:
                initial_sl_width = float(sl_price) - float(entry_price)
            except Exception:
                initial_sl_width = None

        position = {
            "symbol": symbol,
            "strategy": STRATEGY_NAME,
            "entry_price": entry_price,
            "sl_price": float(sl_price),        # original computed SL (float)
            "sl_price_adj": float(sl_adj),     # adjusted SL actually sent (float)
            "initial_sl_width": initial_sl_width,  # NEW: used to detect 1:1 move
            "sl_moved": False,                 # NEW: one-time trail flag
            "sl_order_resp": sl_resp,
            "entry_order_resp": sell_resp,
            "hedge_symbol": hedge_symbol,
            "hedge_order_resp": hedge_resp,
            "hedge_qty": hedge_qty,
            "entered_at": datetime.datetime.now()
        }
        self.positions[symbol] = position

        # Add hedge info to journal row (entry and sl remain for main leg)
        remarks = f"Placed limit sell + SL buy (orig_sl={sl_price}, adj_sl={sl_adj}). hedge={hedge_symbol} resp={hedge_resp}"
        log_to_journal(symbol, "ENTRY", STRATEGY_NAME, entry=entry_price, sl=float(sl_adj), remarks=remarks)

        logging.info(f"[{STRATEGY_NAME}] Placed entry for {symbol} with hedge {hedge_symbol}")
        return position

    def on_new_candle(self, symbol, candle):
        now = datetime.datetime.now().time()
        if not (TRADING_START <= now <= TRADING_END):
            logging.debug(f"[{STRATEGY_NAME}] outside trading window: {now}")
            return

        pos = self.positions.get(symbol)
        candles = list(self.candles[symbol])
        closes = [c["close"] for c in candles]
        volumes = [c["volume"] for c in candles]
        ema5 = compute_ema(closes, 5)
        vwma20 = compute_vwma(closes, volumes, 20)
        is_green = candle["close"] > candle["open"]

        # --------- ONE-TIME TRAIL SL: move SL to entry+2 (rounded to nearest TICK_SIZE) when price moves 1:1 ----------
        # This triggers only once per position. We use candle['low'] to detect intrabar touches.
        if pos and not pos.get("sl_moved", False):
            try:
                entry = float(pos.get("entry_price"))
                sl_original = float(pos.get("sl_price")) if pos.get("sl_price") is not None else None
                initial_width = pos.get("initial_sl_width")
                if initial_width is None:
                    if sl_original is not None:
                        initial_width = sl_original - entry
                    else:
                        initial_width = None

                if initial_width is not None:
                    # target when price has moved in our favour 1:1 (short => price down by initial_width)
                    target_price = entry - float(initial_width)
                    # if the candle touched or went below the target (intrabar low), move SL to entry+2 (only once)
                    if candle.get("low") is not None and candle["low"] <= target_price:
                        desired_raw = entry + 2.0
                        # nearest tick rounding (method="nearest")
                        new_sl_decimal = round_to_tick(Decimal(str(desired_raw)), tick=TICK_SIZE, method="nearest")
                        new_sl = float(new_sl_decimal)

                        # decide whether to move (only if tighter than current adjusted SL to avoid widening)
                        current_sl_for_orders = float(pos.get("sl_price_adj", pos.get("sl_price", new_sl)))
                        if new_sl < current_sl_for_orders:
                            logging.info(f"[{STRATEGY_NAME}] Trailing SL: price touched target ({target_price}). Moving SL {symbol} -> {new_sl}")
                            # cancel existing SL order if we have an id
                            sl_order = pos.get("sl_order_resp", {})
                            sl_order_id = None
                            if isinstance(sl_order, dict):
                                sl_order_id = sl_order.get("id") or sl_order.get("orderId")
                            if sl_order_id:
                                try:
                                    cancel_resp = self.fyers.cancel_order(sl_order_id)
                                    logging.info(f"[{STRATEGY_NAME}] Cancel old SL response: {cancel_resp}")
                                except Exception:
                                    logging.exception("[ENGINE] cancel_order failed while trailing SL")

                            # place new SL buy at entry+2 rounded to nearest tick
                            try:
                                sl_trail_tag = sanitize_tag(f"{STRATEGY_NAME}_SL_TRAIL")
                                limit_for_trail_sl = float(Decimal(str(new_sl_decimal)) + TICK_SIZE)
                                sl_resp = self.fyers.place_stoploss_buy(symbol, self.lot_size, float(new_sl_decimal), tag=sl_trail_tag, limit_price=limit_for_trail_sl)

                                # update position record
                                pos["sl_price_adj"] = float(new_sl_decimal)
                                pos["sl_order_resp"] = sl_resp
                                pos["sl_moved"] = True
                                log_to_journal(symbol, "SL_TRAILED", STRATEGY_NAME, entry=entry, sl=float(new_sl_decimal), remarks=f"SL trailed to entry+2 (nearest tick) after price hit {target_price}. old_sl_adj={current_sl_for_orders}")
                                logging.info(f"[{STRATEGY_NAME}] SL_TRAILED for {symbol}: new_sl={new_sl_decimal} resp={sl_resp}")
                            except Exception:
                                logging.exception("[ENGINE] Failed to place trailed SL")
                        else:
                            # new_sl not tighter than current; still mark sl_moved True to avoid repeated attempts
                            pos["sl_moved"] = True
                            logging.info(f"[{STRATEGY_NAME}] Computed trailed SL {new_sl} not tighter than current {current_sl_for_orders}; no change but marking sl_moved=True")
            except Exception:
                logging.exception("[ENGINE] Error in one-time SL trail check")
        # --------------------------------------------------------------------------------------------

        # Exit condition: green candle closes above EMA5 and VWMA20
        if pos and ema5 is not None and vwma20 is not None and is_green and candle["close"] > ema5 and candle["close"] > vwma20:
            sl_order = pos.get("sl_order_resp", {})
            sl_order_id = None
            if isinstance(sl_order, dict):
                sl_order_id = sl_order.get("id") or sl_order.get("orderId")
            if sl_order_id:
                try:
                    cancel_resp = self.fyers.cancel_order(sl_order_id)
                    logging.info(f"[{STRATEGY_NAME}] Cancel SL response: {cancel_resp}")
                except Exception:
                    logging.exception("[ENGINE] cancel_order failed")
            self.fyers.place_market_buy(symbol, self.lot_size, tag=sanitize_tag(f"{STRATEGY_NAME}_EXIT"))
            exit_price = candle["close"]
            # use adjusted SL for journaling (what we actually sent)
            log_to_journal(symbol, "EXIT", STRATEGY_NAME, entry=pos.get("entry_price"), sl=pos.get("sl_price_adj", pos.get("sl_price")), exit_price=exit_price, remarks="Exit cond (green close > EMA & VWMA)")
            logging.info(f"[{STRATEGY_NAME}] EXIT for {symbol} at {exit_price}")
            self.positions[symbol] = None
            return

        # Forced exit at FORCED_EXIT_TIME using candle time
        current_time = candle["time"].time()
        if pos and current_time >= FORCED_EXIT_TIME:
            self.fyers.place_market_buy(symbol, self.lot_size, tag=sanitize_tag(f"{STRATEGY_NAME}_FORCED_EXIT"))
            exit_price = candle["close"]
            log_to_journal(symbol, "EXIT_FORCED", STRATEGY_NAME, entry=pos.get("entry_price"), sl=pos.get("sl_price_adj", pos.get("sl_price")), exit_price=exit_price, remarks="Forced exit at FORCED_EXIT_TIME")
            logging.info(f"[{STRATEGY_NAME}] Forced EXIT for {symbol} at {exit_price}")
            self.positions[symbol] = None
            return

        # No position -> check entry
        if not pos:
            entry_info = self.check_entry_condition(symbol)
            if entry_info:
                self.place_entry(symbol, entry_info["entry"], entry_info["sl"])

    def on_trade_update(self, msg):
        logging.debug(f"[{STRATEGY_NAME}] trade update: {msg}")
        try:
            trades = msg.get("trades") or msg.get("trade") or []
            if isinstance(trades, dict):
                trades = [trades]
            for t in trades:
                side = t.get("side") or t.get("ordSide")
                symbol = t.get("symbol") or msg.get("symbol")
                price = t.get("price") or t.get("ltp") or t.get("tradePrice")
                # side==1 => buy (SL buy or hedge buy)
                if side == 1 or str(side) == "1":
                    # Try direct lookup by symbol (main leg)
                    pos = self.positions.get(symbol)
                    # if not found, maybe it's a hedge symbol - find position which has this hedge symbol
                    if pos is None:
                        for main_sym, p in list(self.positions.items()):
                            if p and p.get("hedge_symbol") == symbol:
                                pos = p
                                break

                    if pos:
                        entry = pos.get("entry_price")
                        # prefer adjusted SL used for broker orders
                        sl_used_for_orders = pos.get("sl_price_adj", pos.get("sl_price"))
                        try:
                            # only append when this buy corresponds to SL hit (i.e., main symbol SL)
                            # if this trade symbol equals the main symbol, treat as SL hit
                            trade_is_sl_hit = (symbol == pos.get("symbol"))
                            if trade_is_sl_hit:
                                self.sl_hits.append(float(sl_used_for_orders) - float(entry) if entry is not None and sl_used_for_orders is not None else 0.0)
                                log_to_journal(symbol, "SL_HIT", STRATEGY_NAME, entry=entry, sl=sl_used_for_orders, exit=price, remarks=f"SL filled: {t}")
                                logging.info(f"[{STRATEGY_NAME}] SL_HIT for {symbol}: entry={entry} sl={sl_used_for_orders} fill={price}. SLs so far: {self.sl_hits}")

                                # exit hedge if present
                                hedge_sym = pos.get("hedge_symbol")
                                hedge_qty = pos.get("hedge_qty", self.lot_size)
                                if hedge_sym:
                                    try:
                                        sell_hedge_resp = self.fyers.place_market_sell(hedge_sym, hedge_qty, tag=sanitize_tag(f"{STRATEGY_NAME}_HEDGE_EXIT_ON_SL"))
                                        log_to_journal(hedge_sym, "HEDGE_EXIT", STRATEGY_NAME, entry=None, sl=None, exit=None, remarks=f"Hedge exited on SL hit. resp={sell_hedge_resp}")
                                        logging.info(f"[{STRATEGY_NAME}] Hedge EXIT on SL for {hedge_sym}: resp={sell_hedge_resp}")
                                    except Exception:
                                        logging.exception("[ENGINE] Hedge exit failed on SL hit")

                                # clear main position
                                self.positions[pos.get("symbol")] = None
                            else:
                                # If not main SL, probably hedge fill or other buy - record as HEDGE_FILL
                                log_to_journal(symbol, "HEDGE_FILL", STRATEGY_NAME, entry=None, sl=None, exit=price, remarks=f"Hedge or other buy filled: {t}")
                                logging.info(f"[{STRATEGY_NAME}] HEDGE_FILL or other BUY for {symbol}, details: {t}")
                        except Exception:
                            logging.exception("[ENGINE] on_trade_update handling trade")
        except Exception:
            logging.exception("[ENGINE] on_trade_update failed")

# ---------------- MAIN ----------------
if __name__ == "__main__":
    if not CLIENT_ID or not ACCESS_TOKEN:
        logging.error("CLIENT_ID or ACCESS_TOKEN missing. Set FYERS_CLIENT_ID and FYERS_ACCESS_TOKEN in env.")
        raise SystemExit("Missing credentials")

    # instantiate client and engine
    fyers_client = FyersClient(CLIENT_ID, ACCESS_TOKEN, lot_size=LOT_SIZE)
    engine = NiftySTRAT15Engine(fyers_client, lot_size=LOT_SIZE)

    # Build ATM option symbols (CE, PE)
    try:
        option_symbols = get_atm_symbols(fyers_client)  # returns [CE_symbol, PE_symbol]
    except Exception as e:
        logging.exception("[MAIN] Could not compute ATM symbols")
        raise

    # Prefill history (1 day back) for warm-start
    engine.prefill_history(option_symbols, days_back=1)

    # Register trade callback so engine can detect SL fills
    try:
        if hasattr(fyers_client, "register_trade_callback"):
            fyers_client.register_trade_callback(engine.on_trade_update)
        else:
            logging.info("[MAIN] FyersClient.register_trade_callback not available")
    except Exception:
        logging.exception("[MAIN] Error registering trade callback")

    # Subscribe to market data and start order socket
    try:
        logging.info(f"[MAIN] Subscribing to market data for: {option_symbols}")
        if hasattr(fyers_client, "subscribe_market_data"):
            # pass engine.add_candle as the candle-completed callback
            fyers_client.subscribe_market_data(option_symbols, engine.add_candle)
        else:
            logging.info("[MAIN] subscribe_market_data not available - ensure you wire a bucketer to call engine.add_candle")

        logging.info("[MAIN] Starting order socket...")
        if hasattr(fyers_client, "start_order_socket"):
            fyers_client.start_order_socket()
        else:
            logging.info("[MAIN] start_order_socket not available - ensure you wire order socket to forward trades")
    except Exception:
        logging.exception("[MAIN] Error starting sockets")

    logging.info(f"[{STRATEGY_NAME}] Ready. Symbols: {option_symbols}. Prefilled candles: " +
                 ", ".join([f"{s}:{len(engine.candles[s])}" for s in option_symbols]))
    print(f"{STRATEGY_NAME} running. Journal: {JOURNAL_FILE}, Log: {LOG_FILE}")

    # Keep main thread alive (like your original reference's keep_running)
    try:
        while True:
            time.sleep(5)
    except KeyboardInterrupt:
        logging.info("[MAIN] KeyboardInterrupt - exiting")
        print("Shutting down.")
