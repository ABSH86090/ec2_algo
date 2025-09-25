import datetime
import logging
import csv
import os
import time
import uuid
from collections import defaultdict, deque
from dotenv import load_dotenv
from fyers_apiv3 import fyersModel
from fyers_apiv3.FyersWebsocket import order_ws, data_ws

# ---------------- CONFIG ----------------
load_dotenv()

CLIENT_ID = os.getenv("FYERS_CLIENT_ID")
ACCESS_TOKEN = os.getenv("FYERS_ACCESS_TOKEN")  # Format: APPID-XXXXX:token
LOT_SIZE = 20
MAX_SLS_PER_DAY = 3
TRADING_START = datetime.time(9, 15)
TRADING_END = datetime.time(15, 0)
JOURNAL_FILE = "sensex_trades.csv"

# Ensure journal file exists with headers (added trade_id and pnl)
if not os.path.exists(JOURNAL_FILE):
    with open(JOURNAL_FILE, mode="w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "timestamp", "trade_id", "symbol", "action", "strategy",
            "entry_price", "sl_price", "exit_price", "pnl", "remarks"
        ])

# Reset logging handlers
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

logging.basicConfig(
    filename="strategy.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# ---------------- Helpers ----------------
def _safe_float(v):
    try:
        return float(v)
    except Exception:
        return None

def compute_pnl_for_short(entry_price, exit_price, lot_size):
    """For short trades: profit per unit = entry - exit. Multiply by lot_size (qty)."""
    if entry_price is None or exit_price is None:
        return None
    try:
        pnl_points = float(entry_price) - float(exit_price)
        pnl_value = pnl_points * float(lot_size)
        return round(pnl_value, 2)
    except Exception:
        return None

def log_to_journal(symbol, action, strategy=None, entry=None, sl=None, exit=None, remarks="", trade_id=None, lot_size=LOT_SIZE):
    """
    If action == 'ENTRY', append a new row and return trade_id.
    If action in ('EXIT','SL_HIT'), update the matching entry row (by trade_id if provided,
    otherwise by symbol & empty exit_price) and write exit/pnl/remarks into same row.
    """
    # Ensure file exists and header correct (in case script restarted)
    if not os.path.exists(JOURNAL_FILE):
        with open(JOURNAL_FILE, mode="w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                "timestamp", "trade_id", "symbol", "action", "strategy",
                "entry_price", "sl_price", "exit_price", "pnl", "remarks"
            ])

    if action == "ENTRY":
        # create trade_id if not provided
        tid = trade_id or str(uuid.uuid4())
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(JOURNAL_FILE, mode="a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                timestamp, tid, symbol, action, strategy,
                entry, sl, "", "", remarks
            ])
        return tid

    # For EXIT or SL_HIT, update the existing row
    # Read all rows
    updated = False
    rows = []
    with open(JOURNAL_FILE, mode="r", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        for r in reader:
            rows.append(r)

    # Try to find row by trade_id first (most reliable)
    target_idx = None
    if trade_id:
        for idx in range(len(rows)-1, -1, -1):
            r = rows[idx]
            if r.get("trade_id") == trade_id:
                target_idx = idx
                break

    # If not found by trade_id, fallback: find latest row for same symbol with empty exit_price
    if target_idx is None:
        for idx in range(len(rows)-1, -1, -1):
            r = rows[idx]
            if r.get("symbol") == symbol and (not r.get("exit_price")):
                # optionally match strategy/entry if provided
                if strategy and r.get("strategy") and r.get("strategy") != strategy:
                    continue
                if entry is not None and r.get("entry_price"):
                    try:
                        if float(r.get("entry_price")) != float(entry):
                            continue
                    except Exception:
                        pass
                target_idx = idx
                break

    if target_idx is not None:
        r = rows[target_idx]
        # Update fields
        r["action"] = action  # final action on that row (SL_HIT or EXIT)
        if exit is not None:
            r["exit_price"] = str(exit)
        if sl is not None:
            r["sl_price"] = str(sl)
        # compute pnl for short trades
        entry_val = _safe_float(r.get("entry_price"))
        exit_val = _safe_float(r.get("exit_price"))
        pnl = compute_pnl_for_short(entry_val, exit_val, lot_size)
        r["pnl"] = "" if pnl is None else str(pnl)
        # Merge remarks, preserving previous remarks
        prev_remarks = r.get("remarks") or ""
        combined = (prev_remarks + " | " + remarks).strip(" | ")
        r["remarks"] = combined
        rows[target_idx] = r
        updated = True
    else:
        # If no matching entry row, append as fallback (should not usually happen)
        tid = trade_id or str(uuid.uuid4())
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        pnl = compute_pnl_for_short(entry, exit, lot_size) if entry is not None and exit is not None else ""
        with open(JOURNAL_FILE, mode="a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                timestamp, tid, symbol, action, strategy,
                entry, sl, exit, pnl, remarks
            ])
        return

    # Write back all rows
    with open(JOURNAL_FILE, mode="w", newline="") as f:
        if not fieldnames:
            fieldnames = ["timestamp", "trade_id", "symbol", "action", "strategy",
                          "entry_price", "sl_price", "exit_price", "pnl", "remarks"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)

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
    """
    Return expiry token used inside option symbols:
    - If expiry is (or should be treated as) the 'monthly' expiry -> 'YYMON' (e.g. '25SEP')
      We consider it monthly if:
        * expiry is the last Thursday of month (normal case), OR
        * expiry is a Wednesday which was moved from the last-Thursday because Thursday was holiday
          (i.e., expiry.weekday()==2 and expiry+1 is last Thursday)
    - Else -> numeric scheme similar to old logic (YYMDD or YYMMDD).
    """
    yy = expiry_date.strftime("%y")  # e.g. '25'

    # If expiry is Wednesday but the next day (Thursday) is the month's last Thursday,
    # treat it as the monthly expiry token (this handles Thurs->Wed holiday moves).
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

    # fallback to previous numeric behavior for non-monthly expiries
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

    ce_symbol = f"BSE:SENSEX{expiry_str}{atm_strike}CE"
    pe_symbol = f"BSE:SENSEX{expiry_str}{atm_strike}PE"

    print(f"[ATM SYMBOLS] CE={ce_symbol}, PE={pe_symbol}")
    logging.info(f"[ATM SYMBOLS] CE={ce_symbol}, PE={pe_symbol}")
    return [ce_symbol, pe_symbol]

def _extract_order_id(resp):
    """Try common places where fyers place_order/cancel may return an id."""
    if not isinstance(resp, dict):
        return None
    # direct id
    if "id" in resp and resp.get("id"):
        return str(resp.get("id"))
    # nested orders dict
    orders = resp.get("orders")
    if isinstance(orders, dict):
        if "id" in orders and orders.get("id"):
            return str(orders.get("id"))
        # sometimes 'orders' is a list
        if "order" in orders and isinstance(orders.get("order"), dict) and orders["order"].get("id"):
            return str(orders["order"].get("id"))
    # orderNumStatus like "25090900232324:1"
    ons = resp.get("orderNumStatus") or resp.get("orderNumStatusMessage")
    if isinstance(ons, str):
        if ":" in ons:
            return ons.split(":")[0]
        return ons
    # Sometimes inside 'data' / 'd' structures:
    if "d" in resp:
        try:
            d = resp["d"]
            if isinstance(d, list) and len(d) > 0 and isinstance(d[0], dict) and d[0].get("id"):
                return str(d[0].get("id"))
        except Exception:
            pass
    return None

def _is_cancel_success(resp):
    """Return True if response indicates cancel succeeded (including broker quirks)."""
    if not resp:
        return False
    if isinstance(resp, dict):
        # canonical success
        if resp.get("s") == "ok":
            return True
        # broker-specific returned message/code that still means cancelled
        msg = str(resp.get("message") or "").lower()
        code = resp.get("code")
        if "order cancelled" in msg or "already cancelled" in msg or code == -99:
            return True
    return False

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

    def _log_order_resp(self, action, resp):
        logging.info(f"{action} Response: {resp}")
        print(f"{action} Response: {resp}")
        return resp

    def place_limit_sell(self, symbol: str, qty: int, price: float, tag: str = ""):
        data = {
            "symbol": symbol, "qty": qty, "type": 1, "side": -1,
            "productType": "INTRADAY", "limitPrice": price, "stopPrice": 0,
            "validity": "DAY", "disclosedQty": 0, "offlineOrder": False, "orderTag": tag
        }
        return self._log_order_resp("Limit Sell", self.client.place_order(data))

    def place_stoploss_buy(self, symbol: str, qty: int, stop_price: float, tag: str = ""):
        data = {
            "symbol": symbol, "qty": qty, "type": 3, "side": 1,
            "productType": "INTRADAY", "limitPrice": 0, "stopPrice": stop_price,
            "validity": "DAY", "disclosedQty": 0, "offlineOrder": False, "orderTag": tag
        }
        return self._log_order_resp("SL Buy", self.client.place_order(data))

    def place_market_buy(self, symbol: str, qty: int, tag: str = ""):
        data = {
            "symbol": symbol, "qty": qty, "type": 2, "side": 1,
            "productType": "INTRADAY", "limitPrice": 0, "stopPrice": 0,
            "validity": "DAY", "disclosedQty": 0, "offlineOrder": False, "orderTag": tag
        }
        return self._log_order_resp("Market Buy", self.client.place_order(data))

    def cancel_order(self, order_id: str):
        resp = self.client.cancel_order({"id": order_id})
        logging.info(f"Cancel Order Response: {resp}")
        print(f"Cancel Order Response: {resp}")
        return resp

    def start_order_socket(self):
        def on_order(msg):
            logging.info(f"Order update: {msg}")
            for cb in self.order_callbacks:
                try:
                    cb(msg)
                except Exception as e:
                    logging.exception(f"order callback exception: {e}")

        def on_trade(msg):
            logging.info(f"Trade update: {msg}")
            for cb in self.trade_callbacks:
                try:
                    cb(msg)
                except Exception as e:
                    logging.exception(f"trade callback exception: {e}")

        def on_open():
            fyers.subscribe(data_type="OnOrders,OnTrades")
            fyers.keep_running()

        fyers = order_ws.FyersOrderSocket(
            access_token=self.auth_token,
            write_to_file=False, log_path="",
            on_connect=on_open,
            on_close=lambda m: logging.info(f"Order socket closed: {m}"),
            on_error=lambda m: logging.error(f"Order socket error: {m}"),
            on_orders=on_order,
            on_trades=on_trade
        )
        fyers.connect()

    def subscribe_market_data(self, instrument_ids, on_candle_callback):
        candle_buffers = defaultdict(lambda: None)

        def on_message(tick):
            try:
                if "symbol" not in tick or "ltp" not in tick:
                    return
                symbol = tick["symbol"]
                ltp = float(tick["ltp"])
                ts = int(tick["last_traded_time"])
                cum_vol = int(tick.get("vol_traded_today", 0))
            except Exception as e:
                logging.error(f"Bad tick format: {tick}, {e}")
                return

            dt = datetime.datetime.fromtimestamp(ts)
            bucket_minute = (dt.minute // 3) * 3
            candle_open_time = dt.replace(second=0, microsecond=0, minute=bucket_minute)

            c = candle_buffers[symbol]
            if c is None or c["time"] != candle_open_time:
                if c is not None:
                    on_candle_callback(symbol, c)
                candle_buffers[symbol] = {
                    "time": candle_open_time,
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
            on_close=lambda m: logging.info(f"Data socket closed: {m}"),
            on_error=lambda m: logging.error(f"Data socket error: {m}"),
            on_message=on_message
        )
        fyers.connect()

    def register_order_callback(self, cb): self.order_callbacks.append(cb)
    def register_trade_callback(self, cb): self.trade_callbacks.append(cb)

# ---------------- STRATEGY ENGINE ----------------
class StrategyEngine:
    def __init__(self, fyers_client: FyersClient, lot_size: int = 20):
        self.fyers = fyers_client
        self.lot_size = lot_size
        # positions: { symbol: { strategy, sl_order: {id, resp}, entry_order: {...}, entry_price, sl_price, trade_id } }
        self.positions = {}
        self.sl_count = 0
        self.candles = defaultdict(lambda: deque(maxlen=200))
        self.s3_fired_date = {}  # { symbol: date }
        self.s4_pending = {}
        # NEW for Strategy 1
        self.s1_marked_low = {}   # { symbol: (date, low_value) }
        self.s1_fired_date = {}   # { symbol: date }  -> to avoid firing more than once per day per symbol
        # GLOBAL flag: ensure Strategy 1 entry happens only once per day across ALL symbols
        self.s1_global_fired_date = None  # date when S1 was actually taken (global once-per-day)

    def prefill_history(self, symbols, days_back=1):
        """Prefill candles with historical data for warm start."""
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
                logging.info(f"Fetched history for {symbol}")
            except Exception as e:
                logging.error(f"Failed to fetch history for {symbol}: {e}")

    def compute_ema(self, prices, period):
        if len(prices) < period: return None
        ema = prices[0]; k = 2/(period+1)
        for p in prices[1:]:
            ema = p*k + ema*(1-k)
        return ema

    def compute_vwma(self, closes, volumes, period):
        if len(closes) < period: return None
        denom = sum(volumes[-period:])
        if denom == 0:
            return None
        return sum([c*v for c, v in zip(closes[-period:], volumes[-period:])]) / denom

    def detect_pattern(self, candles):
        n = len(candles)
        if n < 6:
            return False

        last6 = list(candles)[-6:]
        def is_red(c):   return c["close"] <= c["open"]
        def is_green(c): return c["close"] > c["open"]

        c0, c1, c2, c3, c4, c5 = last6
        if not (is_red(c0) and is_red(c1) and is_green(c2) and is_red(c3) and is_green(c4) and is_red(c5)):
            return False

        if not (c1["close"] < c0["close"]):
            return False

        closes = [c["close"] for c in candles]
        volumes = [c["volume"] for c in candles]

        start_idx = n - 6
        for i in range(start_idx, n):
            ema5  = self.compute_ema(closes[:i+1], 5)
            vwma20 = self.compute_vwma(closes[:i+1], volumes[:i+1], 20)
            if ema5 is None or vwma20 is None:
                return False
            if not (ema5 < vwma20):
                return False

        return True

    def detect_new_short_strategy(self, candles, ema5, vwma20):
        GAP_GREEN2 = 0.01  # 1% gap for 2nd green candle
        GAP_RED = 0.02     # 2% gap for red candle

        if len(candles) < 3:
            return None
        if ema5 is None or vwma20 is None:
            return None

        c1, c2, c3 = candles[-3], candles[-2], candles[-1]
        is_green = lambda c: c["close"] > c["open"]
        is_red   = lambda c: c["close"] < c["open"]

        if not (is_green(c1) and is_green(c2) and is_red(c3)):
            logging.info(f"[STRAT2] blocked: pattern mismatch -> c1({c1['open']},{c1['close']}), c2({c2['open']},{c2['close']}), c3({c3['open']},{c3['close']})")
            return None

        closes = [c["close"] for c in candles]
        vols   = [c.get("volume", 0) for c in candles]

        idx1, idx2, idx3 = len(closes) - 3, len(closes) - 2, len(closes) - 1

        ema_c1 = self.compute_ema(closes[:idx1+1], 5)
        vw_c1  = self.compute_vwma(closes[:idx1+1], vols[:idx1+1], 20)
        ema_c2 = self.compute_ema(closes[:idx2+1], 5)
        vw_c2  = self.compute_vwma(closes[:idx2+1], vols[:idx2+1], 20)
        ema_c3 = self.compute_ema(closes[:idx3+1], 5)
        vw_c3  = self.compute_vwma(closes[:idx3+1], vols[:idx3+1], 20)

        logging.info(f"[STRAT2] c1: ema={ema_c1}, vwma={vw_c1}; c2: ema={ema_c2}, vwma={vw_c2}; c3: ema={ema_c3}, vwma={vw_c3}")

        if None in (ema_c1, vw_c1, ema_c2, vw_c2, ema_c3, vw_c3):
            logging.info("[STRAT2] blocked: missing indicator values")
            return None

        if not (ema_c1 < vw_c1):
            logging.info(f"[STRAT2] blocked: c1 ema>=vwma (ema={ema_c1}, vwma={vw_c1})")
            return None

        if not (vw_c2 >= ema_c2 * (1 + GAP_GREEN2)):
            logging.info(f"[STRAT2] blocked: c2 gap failed (ema={ema_c2}, vwma={vw_c2})")
            return None

        if not (vw_c2 > vw_c3):
            logging.info(f"[STRAT2] blocked: vwma(c2)={vw_c2} <= vwma(c3)={vw_c3}")
            return None

        if not (vw_c3 >= ema_c3 * (1 + GAP_RED)):
            logging.info(f"[STRAT2] blocked: red candle gap failed (ema={ema_c3}, vwma={vw_c3})")
            return None

        if not (c3["high"] >= vwma20):
            logging.info(f"[STRAT2] blocked: c3.high {c3['high']} < vwma20 {vwma20}")
            return None
        if not (c3["close"] < ema5 and c3["close"] < vwma20):
            logging.info(f"[STRAT2] blocked: c3.close {c3['close']} not below ema5 {ema5}, vwma20 {vwma20}")
            return None

        entry = c3["close"]
        sl    = c3["high"] + 5

        if (sl - entry) > 60:
            logging.info(f"[STRAT2] Skipped due to wide SL: {sl-entry:.2f} pts")
            return None

        logging.info(f"[STRAT2] Triggered: entry={entry}, sl={sl}")
        return {"entry": entry, "sl": sl}

    def detect_strategy3(self, symbol, candles):
        today = datetime.date.today()
        if self.s3_fired_date.get(symbol) == today:
            return None

        todays = [c for c in candles if c["time"].date() == today and c["time"].time() >= TRADING_START]
        if len(todays) < 2:
            return None

        c1 = todays[0]
        c2 = todays[1]

        if not (c1["time"].time() == datetime.time(9, 15) and c2["time"].time() == datetime.time(9, 18)):
            return None

        if not (c1["close"] > c1["open"]):
            return None
        if not (c2["close"] < c2["open"]):
            return None

        closes = [c["close"] for c in candles]
        volumes = [c["volume"] for c in candles]

        try:
            idx1 = candles.index(c1)
            idx2 = candles.index(c2)
        except ValueError:
            return None

        ema1 = self.compute_ema(closes[:idx1+1], 5)
        vw1  = self.compute_vwma(closes[:idx1+1], volumes[:idx1+1], 20)
        if ema1 is None or vw1 is None or not (ema1 < vw1):
            return None

        ema2 = self.compute_ema(closes[:idx2+1], 5)
        vw2  = self.compute_vwma(closes[:idx2+1], volumes[:idx2+1], 20)
        if ema2 is None or vw2 is None:
            return None

        if not (c2["close"] < ema2 and c2["close"] < vw2):
            return None

        entry = c2["low"]
        sl    = c2["high"] + 5

        if (sl - entry) > 60:
            logging.info(f"[STRAT3] Skipped due to wide SL: {sl-entry:.2f} pts")
            return None

        return {"entry": entry, "sl": sl, "date": today}

    def reconcile_position(self, symbol):
        """Check broker positions to ensure internal state is correct."""
        try:
            resp = self.fyers.client.positions()
            if resp.get("s") == "ok":
                net_positions = resp.get("netPositions", [])
                open_qty = 0
                for pos in net_positions:
                    if pos.get("symbol") == symbol or pos.get("symbol", "").startswith(symbol):
                        open_qty = pos.get("netQty", 0)
                        break
                if open_qty == 0:
                    self.positions[symbol] = None
                    logging.warning(f"[RECONCILE] {symbol} - No broker position, resetting local state.")
        except Exception as e:
            logging.error(f"[RECONCILE FAILED] {symbol} - {e}")

    def on_candle(self, symbol, candle):
        self.candles[symbol].append(candle)
        candles = list(self.candles[symbol])
        closes = [c["close"] for c in candles]
        volumes = [c["volume"] for c in candles]
        ema5 = self.compute_ema(closes, 5)
        vwma20 = self.compute_vwma(closes, volumes, 20)
        if ema5 is None or vwma20 is None:
            return

        logging.info(
            f"[Candle] {symbol} {candle['time']} EMA5={ema5:.2f}, "
            f"VWMA20={vwma20:.2f}, Close={candle['close']}"
        )

        now = datetime.datetime.now().time()
        if not (TRADING_START <= now <= TRADING_END):
            return
        if self.sl_count >= MAX_SLS_PER_DAY:
            return

        position = self.positions.get(symbol)

        # -------- STRATEGY 3 (runs as soon as we have the 9:18 candle) --------
        if position is None:
            s3 = self.detect_strategy3(symbol, candles)
            if s3:
                raw_entry_resp = self.fyers.place_limit_sell(symbol, self.lot_size, s3["entry"], "STRAT3ENTRY")
                raw_sl_resp = self.fyers.place_stoploss_buy(symbol, self.lot_size, s3["sl"], "STRAT3SL")
                sl_order_id = _extract_order_id(raw_sl_resp)
                entry_order_id = _extract_order_id(raw_entry_resp)
                trade_id = log_to_journal(symbol, "ENTRY", "strat3", entry=s3["entry"], sl=s3["sl"], remarks="", trade_id=None, lot_size=self.lot_size)
                self.positions[symbol] = {
                    "sl_order": {"id": sl_order_id, "resp": raw_sl_resp},
                    "entry_order": {"id": entry_order_id, "resp": raw_entry_resp},
                    "strategy": "strat3",
                    "entry_price": s3["entry"],
                    "sl_price": s3["sl"],
                    "trade_id": trade_id
                }
                self.s3_fired_date[symbol] = s3["date"]
                logging.info(f"[STRAT3] ENTRY @{s3['entry']} SL @{s3['sl']} for {symbol}")

        # -------- STRATEGY 1 (UPDATED: per-symbol mark + GLOBAL once-per-day limit) --------
        # Mark low of the first 3-min candle of the day (9:15). We set per symbol per day.
        now_date = datetime.date.today()
        first_mark = self.s1_marked_low.get(symbol)
        # if not marked for today, check whether this candle is the first session candle
        if (not first_mark) or (first_mark[0] != now_date):
            # identify today's candle that is exactly TRADING_START time
            # candle["time"] is datetime; if it equals TRADING_START, mark it
            try:
                c_time = candle["time"].time()
                if c_time == TRADING_START:
                    # record low for today
                    self.s1_marked_low[symbol] = (now_date, candle["low"])
                    logging.info(f"[STRAT1] Marked first-candle low for {symbol} = {candle['low']}")
            except Exception as e:
                logging.exception(f"[STRAT1] error marking first candle low for {symbol}: {e}")

        # Now check breakdown trigger only if we haven't taken STRAT1 today (global) and position is empty
        position = self.positions.get(symbol)
        fired_today = self.s1_fired_date.get(symbol)
        # Check global once-per-day flag: if S1 has already been taken today globally, skip
        if self.s1_global_fired_date == now_date:
            # global s1 already taken today; skip checking for further entries
            logging.debug(f"[STRAT1] Global S1 already taken today; skipping checks for {symbol}")
        else:
            if position is None and fired_today != now_date:
                marked = self.s1_marked_low.get(symbol)
                if marked and marked[0] == now_date:
                    marked_low = marked[1]
                    # breakdown condition: any 3-min candle closes below marked_low AND is red (close < open)
                    is_red = lambda c: c["close"] < c["open"]
                    if candle["close"] < marked_low and is_red(candle):
                        entry_price = candle["close"]
                        sl_price = candle["high"] + 10  # SL = high of breakdown candle + 10
                        if (sl_price - entry_price) > 65:
                            logging.info(f"[STRAT1] Skipped wide SL: {sl_price-entry_price:.2f} pts for {symbol}")
                            # mark per-symbol as checked for today to avoid repeated checks for same symbol
                            self.s1_fired_date[symbol] = now_date
                        else:
                            # place orders: limit sell entry + stoploss buy
                            raw_entry = self.fyers.place_limit_sell(symbol, self.lot_size, entry_price, "STRAT1ENTRY")
                            raw_sl = self.fyers.place_stoploss_buy(symbol, self.lot_size, sl_price, "STRAT1SL")
                            sl_order_id = _extract_order_id(raw_sl)
                            entry_order_id = _extract_order_id(raw_entry)
                            trade_id = log_to_journal(symbol, "ENTRY", "strat1", entry=entry_price, sl=sl_price, remarks="breakdown entry", trade_id=None, lot_size=self.lot_size)
                            self.positions[symbol] = {
                                "sl_order": {"id": sl_order_id, "resp": raw_sl},
                                "entry_order": {"id": entry_order_id, "resp": raw_entry},
                                "strategy": "strat1",
                                "entry_price": entry_price,
                                "sl_price": sl_price,
                                "trade_id": trade_id
                            }
                            # mark both per-symbol and GLOBAL that S1 has been taken today
                            self.s1_fired_date[symbol] = now_date
                            self.s1_global_fired_date = now_date
                            logging.info(f"[STRAT1] ENTRY @{entry_price} SL @{sl_price} for {symbol} (marked_low={marked_low})")

        # -------- STRATEGY 2 --------
        position = self.positions.get(symbol)
        if position is None:
            strat2 = self.detect_new_short_strategy(candles, ema5, vwma20)
            if strat2:
                raw_entry = self.fyers.place_limit_sell(symbol, self.lot_size, strat2["entry"], "STRAT2ENTRY")
                raw_sl = self.fyers.place_stoploss_buy(symbol, self.lot_size, strat2["sl"], "STRAT2SL")
                sl_order_id = _extract_order_id(raw_sl)
                entry_order_id = _extract_order_id(raw_entry)
                trade_id = log_to_journal(symbol, "ENTRY", "strat2", entry=strat2["entry"], sl=strat2["sl"], remarks="", trade_id=None, lot_size=self.lot_size)
                self.positions[symbol] = {
                    "sl_order": {"id": sl_order_id, "resp": raw_sl},
                    "entry_order": {"id": entry_order_id, "resp": raw_entry},
                    "strategy": "strat2",
                    "entry_price": strat2["entry"],
                    "sl_price": strat2["sl"],
                    "trade_id": trade_id
                }
                logging.info(f"[STRAT2] ENTRY @{strat2['entry']} SL @{strat2['sl']} for {symbol}")

        # -------- STRATEGY 4 (NEW) --------
        try:
            if self.positions.get(symbol) is None:
                n = len(candles)
                if n >= 2:
                    idx_prev = n - 2
                    idx_curr = n - 1
                    closes_prefix_prev = closes[:idx_prev+1]
                    vols_prefix_prev = volumes[:idx_prev+1]
                    closes_prefix_curr = closes[:idx_curr+1]
                    vols_prefix_curr = volumes[:idx_curr+1]

                    ema_prev = self.compute_ema(closes_prefix_prev, 5)
                    vw_prev = self.compute_vwma(closes_prefix_prev, vols_prefix_prev, 20)
                    ema_curr = self.compute_ema(closes_prefix_curr, 5)
                    vw_curr = self.compute_vwma(closes_prefix_curr, vols_prefix_curr, 20)

                    c_prev = candles[idx_prev]
                    c_curr = candles[idx_curr]
                    is_red = lambda c: c["close"] < c["open"]

                    if None not in (ema_prev, vw_prev, ema_curr, vw_curr):
                        pending = self.s4_pending.get(symbol)

                        if (ema_prev > vw_prev) and is_red(c_curr) and (ema_curr < vw_curr):
                            if not pending:
                                self.s4_pending[symbol] = {"first_idx": idx_curr, "first_time": c_curr["time"]}
                                logging.info(f"[STRAT4] Pending set for {symbol} at idx={idx_curr} time={c_curr['time']}")
                        elif pending:
                            if ema_curr >= vw_curr:
                                logging.info(f"[STRAT4] Pending cleared for {symbol} because ema >= vwma now (ema={ema_curr:.2f}, vwma={vw_curr:.2f})")
                                self.s4_pending.pop(symbol, None)
                            else:
                                GAP = 0.02
                                GAP_MIN_STRAT_4 = 0.01
                                if is_red(c_curr) and (ema_curr <= vw_curr) and (ema_curr >= vw_curr * (1 - GAP)) and (ema_curr <= vw_curr * (1 - GAP_MIN_STRAT_4)):
                                    entry = c_curr["close"]
                                    sl = c_curr["high"] + 5
                                    if (sl - entry) > 60:
                                        logging.info(f"[STRAT4] Skipped due to wide SL: {sl-entry:.2f} pts")
                                        self.s4_pending.pop(symbol, None)
                                    else:
                                        raw_entry = self.fyers.place_limit_sell(symbol, self.lot_size, entry, "STRAT4ENTRY")
                                        raw_sl = self.fyers.place_stoploss_buy(symbol, self.lot_size, sl, "STRAT4SL")
                                        sl_order_id = _extract_order_id(raw_sl)
                                        entry_order_id = _extract_order_id(raw_entry)
                                        trade_id = log_to_journal(symbol, "ENTRY", "strat4", entry=entry, sl=sl, remarks="", trade_id=None, lot_size=self.lot_size)
                                        self.positions[symbol] = {
                                            "sl_order": {"id": sl_order_id, "resp": raw_sl},
                                            "entry_order": {"id": entry_order_id, "resp": raw_entry},
                                            "strategy": "strat4",
                                            "entry_price": entry,
                                            "sl_price": sl,
                                            "trade_id": trade_id
                                        }
                                        logging.info(f"[STRAT4] ENTRY @{entry} SL @{sl} for {symbol}")
                                        self.s4_pending.pop(symbol, None)
        except Exception as e:
            logging.exception(f"[STRAT4] error processing {symbol}: {e}")

        # -------- EXIT conditions (common) --------
        position = self.positions.get(symbol)
        # Exit if we have a position and we see a green candle that closes above VWMA20
        if position and (candle["close"] > candle["open"]) and (candle["close"] > vwma20):
            sl_order_info = position.get("sl_order") or {}
            sl_order_id = sl_order_info.get("id")
            exit_price = candle["close"]

            cancel_resp = None
            if sl_order_id:
                cancel_resp = self.fyers.cancel_order(sl_order_id)
            else:
                logging.info(f"[EXIT] No SL order id found in position for {symbol}; attempting reconcile and exit.")

            if sl_order_id and _is_cancel_success(cancel_resp):
                # proceed with market buy exit
                self.fyers.place_market_buy(symbol, self.lot_size, "EXITCOND")
                entry_price = position.get("entry_price")
                sl_price = position.get("sl_price")
                trade_id = position.get("trade_id")
                # Interpret if loss on short -> SL_HIT; else normal EXIT
                if entry_price is not None and exit_price > entry_price:
                    self.sl_count += 1
                    log_to_journal(
                        symbol, "SL_HIT", position["strategy"],
                        entry=entry_price, sl=sl_price, exit=exit_price,
                        remarks=f"Exit after cancel (interpreted success). cancel_resp={cancel_resp}",
                        trade_id=trade_id,
                        lot_size=self.lot_size
                    )
                else:
                    log_to_journal(
                        symbol, "EXIT", position["strategy"],
                        entry=entry_price, sl=sl_price, exit=exit_price,
                        remarks=f"Exit condition met. cancel_resp={cancel_resp}",
                        trade_id=trade_id,
                        lot_size=self.lot_size
                    )
                self.positions[symbol] = None
            else:
                logging.warning(f"[EXIT SKIPPED] {symbol} - Cancel failed or unknown: {cancel_resp}")
                self.reconcile_position(symbol)

    def on_trade(self, msg):
        # msg may contain trades in different shapes; guard defensively
        try:
            trades = msg.get("trades") or msg.get("data") or msg
            if not trades:
                return
            symbol = None
            side = None
            if isinstance(trades, dict):
                symbol = trades.get("symbol") or trades.get("s") or trades.get("sym")
                side = trades.get("side")
            if isinstance(trades, list) and len(trades) > 0 and isinstance(trades[0], dict):
                t0 = trades[0]
                symbol = symbol or t0.get("symbol")
                side = side or t0.get("side")

            if not symbol:
                return

            position = self.positions.get(symbol)
            # SL Buy executed -> side == 1 (buy)
            if position and side == 1:
                trade_id = position.get("trade_id")
                entry_price = position.get("entry_price")
                sl_price = position.get("sl_price")
                log_to_journal(
                    symbol, "SL_HIT", position["strategy"],
                    entry=entry_price, sl=sl_price,
                    exit=None,  # we may not have exact exit price in msg; if available, include
                    remarks=str(msg),
                    trade_id=trade_id,
                    lot_size=self.lot_size
                )
                self.sl_count += 1
                self.positions[symbol] = None
        except Exception as e:
            logging.exception(f"Error in on_trade processing: {e}")

# ---------------- MAIN ----------------
if __name__ == "__main__":
    fyers_client = FyersClient(CLIENT_ID, ACCESS_TOKEN, LOT_SIZE)
    engine = StrategyEngine(fyers_client, LOT_SIZE)

    # Wait until 9:15 AM (light sleep to avoid busy-loop)
    while datetime.datetime.now().time() < TRADING_START:
        time.sleep(1)

    option_symbols = []
    try:
        option_symbols = get_atm_symbols(fyers_client)
    except Exception as e:
        logging.exception(f"Failed to build ATM symbols: {e}")
        raise

    engine.prefill_history(option_symbols, days_back=1)
    fyers_client.register_trade_callback(engine.on_trade)
    fyers_client.subscribe_market_data(option_symbols, engine.on_candle)
    fyers_client.start_order_socket()


