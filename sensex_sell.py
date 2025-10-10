import datetime
import logging
import csv
import os
import time
import uuid
import sys  # console StreamHandler
from collections import defaultdict, deque
from dotenv import load_dotenv
from fyers_apiv3 import fyersModel
from fyers_apiv3.FyersWebsocket import order_ws, data_ws

# ---------------- CONFIG ----------------
load_dotenv()

CLIENT_ID = os.getenv("FYERS_CLIENT_ID")
ACCESS_TOKEN = os.getenv("FYERS_ACCESS_TOKEN")  # Format: APPID-XXXXX:token
LOT_SIZE = 40
MAX_SLS_PER_DAY = 3
TRADING_START = datetime.time(9, 15)
TRADING_END = datetime.time(15, 0)
JOURNAL_FILE = "sensex_trades.csv"

# Candle settings
CANDLE_MINUTES = 15
HISTORY_RESOLUTION = "15"

# === NEW/UPDATED: warmup requirements ===
MIN_BARS_FOR_VWMA = 20          # need at least 20 bars for VWMA(20)
MAX_HISTORY_LOOKBACK_DAYS = 7   # try up to N days back to fetch enough bars

# === Absolute max SL in points ===
MAX_SL_POINTS = 100  # If computed SL width > 100, skip the trade

# Ensure journal file exists with headers (added trade_id and pnl)
if not os.path.exists(JOURNAL_FILE):
    with open(JOURNAL_FILE, mode="w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "timestamp", "trade_id", "symbol", "action", "strategy",
            "entry_price", "sl_price", "exit_price", "pnl", "remarks"
        ])

# ---------------- LOGGING (file + console) ----------------
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("strategy.log"),   # file logs
        logging.StreamHandler(sys.stdout)      # console logs
    ],
    force=True
)

logging.info("[BOOT] Logger initialized. Logs -> console + strategy.log")

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
    If action in ('EXIT','SL_HIT'), update the matching entry row and write exit/pnl/remarks.
    """
    if not os.path.exists(JOURNAL_FILE):
        with open(JOURNAL_FILE, mode="w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                "timestamp", "trade_id", "symbol", "action", "strategy",
                "entry_price", "sl_price", "exit_price", "pnl", "remarks"
            ])

    if action == "ENTRY":
        tid = trade_id or str(uuid.uuid4())
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(JOURNAL_FILE, mode="a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                timestamp, tid, symbol, action, strategy,
                entry, sl, "", "", remarks
            ])
        return tid

    updated = False
    rows = []
    with open(JOURNAL_FILE, mode="r", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        for r in reader:
            rows.append(r)

    target_idx = None
    if trade_id:
        for idx in range(len(rows)-1, -1, -1):
            r = rows[idx]
            if r.get("trade_id") == trade_id:
                target_idx = idx
                break

    if target_idx is None:
        for idx in range(len(rows)-1, -1, -1):
            r = rows[idx]
            if r.get("symbol") == symbol and (not r.get("exit_price")):
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
        r["action"] = action
        if exit is not None:
            r["exit_price"] = str(exit)
        if sl is not None:
            r["sl_price"] = str(sl)
        entry_val = _safe_float(r.get("entry_price"))
        exit_val = _safe_float(r.get("exit_price"))
        pnl = compute_pnl_for_short(entry_val, exit_val, lot_size)
        r["pnl"] = "" if pnl is None else str(pnl)
        prev_remarks = r.get("remarks") or ""
        combined = (prev_remarks + " | " + remarks).strip(" | ")
        r["remarks"] = combined
        rows[target_idx] = r
        updated = True
    else:
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
    if date_obj.weekday() != 3:
        return False
    next_week = date_obj + datetime.timedelta(days=7)
    return next_week.month != date_obj.month

def get_next_expiry():
    today = datetime.date.today()
    weekday = today.weekday()
    if weekday == 3:
        candidate_thu = today
    else:
        days_to_thu = (3 - weekday) % 7
        candidate_thu = today + datetime.timedelta(days=days_to_thu)
    if candidate_thu in SPECIAL_MARKET_HOLIDAYS:
        expiry = candidate_thu - datetime.timedelta(days=1)
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
    data = {"symbols": "BSE:SENSEX-INDEX"}
    resp = fyers_client.client.quotes(data)
    if not resp.get("d"):
        raise Exception(f"Failed to fetch SENSEX spot: {resp}")

    ltp = float(resp["d"][0]["v"]["lp"])
    atm_strike = round(ltp / 100) * 100

    expiry = get_next_expiry()
    expiry_str = format_expiry_for_symbol(expiry)

    ce_symbol = f"BSE:SENSEX{expiry_str}{atm_strike}CE"
    pe_symbol = f"BSE:SENSEX{expiry_str}{atm_strike}PE"

    print(f"[ATM SYMBOLS] CE={ce_symbol}, PE={pe_symbol}")
    logging.info(f"[ATM SYMBOLS] CE={ce_symbol}, PE={pe_symbol}")
    return [ce_symbol, pe_symbol]


def _extract_order_id(resp):
    if not isinstance(resp, dict):
        return None
    if "id" in resp and resp.get("id"):
        return str(resp.get("id"))
    orders = resp.get("orders")
    if isinstance(orders, dict):
        if "id" in orders and orders.get("id"):
            return str(orders.get("id"))
        if "order" in orders and isinstance(orders.get("order"), dict) and orders["order"].get("id"):
            return str(orders["order"].get("id"))
    ons = resp.get("orderNumStatus") or resp.get("orderNumStatusMessage")
    if isinstance(ons, str):
        if ":" in ons:
            return ons.split(":")[0]
        return ons
    if "d" in resp:
        try:
            d = resp["d"]
            if isinstance(d, list) and len(d) > 0 and isinstance(d[0], dict) and d[0].get("id"):
                return str(d[0].get("id"))
        except Exception:
            pass
    return None


def _is_cancel_success(resp):
    if not resp:
        return False
    if isinstance(resp, dict):
        if resp.get("s") == "ok":
            return True
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
                ts = int(tick.get("last_traded_time") or tick.get("tt") or tick.get("timestamp"))
                if ts > 10_000_000_000:
                    ts //= 1000
                cum_vol = int(tick.get("vol_traded_today", 0))
            except Exception as e:
                logging.error(f"Bad tick format: {tick}, {e}")
                return

            dt = datetime.datetime.fromtimestamp(ts)
            bucket_minute = (dt.minute // CANDLE_MINUTES) * CANDLE_MINUTES
            candle_open_time = dt.replace(second=0, microsecond=0, minute=bucket_minute)

            c = candle_buffers[symbol]
            if c is None or c["time"] != candle_open_time:
                # CLOSE previous candle and send it out
                if c is not None:
                    logging.info(f"[BAR CLOSE] {symbol} {c['time']} O={c['open']} H={c['high']} L={c['low']} C={c['close']} V={c['volume']}")
                    on_candle_callback(symbol, c)
                # start new candle
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
        self.positions = {}
        self.sl_count = 0
        self.candles = defaultdict(lambda: deque(maxlen=200))
        self.s3_fired_date = {}
        self.s4_pending = {}
        # Strategy 1 state
        self.s1_marked_low = {}
        self.s1_fired_date = {}
        self.s1_global_fired_date = None

    def prefill_history(self, symbols, days_back=1):
        """
        UPDATED: keep going farther back day-by-day until we collect at least MIN_BARS_FOR_VWMA candles
        (during trading hours), or hit MAX_HISTORY_LOOKBACK_DAYS. Log exactly how many per symbol.
        """
        for symbol in symbols:
            total_added = 0
            used_days = 0
            # Try from 'days_back' up to 'MAX_HISTORY_LOOKBACK_DAYS'
            while total_added < MIN_BARS_FOR_VWMA and used_days < MAX_HISTORY_LOOKBACK_DAYS:
                try:
                    to_date = datetime.datetime.now().strftime("%Y-%m-%d")
                    from_dt = datetime.datetime.now() - datetime.timedelta(days=days_back + used_days)
                    from_date = from_dt.strftime("%Y-%m-%d")

                    params = {
                        "symbol": symbol,
                        "resolution": HISTORY_RESOLUTION,
                        "date_format": "1",
                        "range_from": from_date,
                        "range_to": to_date,
                        "cont_flag": "1"
                    }
                    hist = self.fyers.client.history(params)

                    added_this_round = 0
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
                            if TRADING_START <= candle["time"].time() <= TRADING_END:
                                self.candles[symbol].append(candle)
                                total_added += 1
                                added_this_round += 1

                    logging.info(f"[PREFILL] {symbol}: +{added_this_round} bars (round {used_days+1}), total={total_added}")
                except Exception as e:
                    logging.error(f"[PREFILL] Failed to fetch history for {symbol}: {e}")
                finally:
                    used_days += 1

            if total_added == 0:
                logging.info(f"[PREFILL] {symbol}: no historical bars appended (total=0)")
            else:
                logging.info(f"[PREFILL DONE] {symbol}: total prefilled bars={total_added} (min needed={MIN_BARS_FOR_VWMA})")

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

    # === absolute SL width check ===
    def _sl_within_max_points(self, entry_price, sl_price):
        try:
            width = float(sl_price) - float(entry_price)
            return width <= MAX_SL_POINTS
        except Exception:
            return False

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
        """
        Strategy 2 (S2): Two greens then a red with EMA/VWMA conditions (bearish bias)
        Changes already logged in caller.
        """
        if len(candles) < 3:
            logging.info("[STRAT2] blocked: need at least 3 candles")
            return None
        if ema5 is None or vwma20 is None:
            logging.info("[STRAT2] blocked: missing indicator values")
            return None

        c1, c2, c3 = candles[-3], candles[-2], candles[-1]
        is_green = lambda c: c["close"] > c["open"]
        is_red   = lambda c: c["close"] < c["open"]

        if not (is_green(c1) and is_green(c2) and is_red(c3)):
            logging.info(f"[STRAT2] blocked: pattern mismatch -> "
                         f"c1({c1['open']},{c1['close']}), c2({c2['open']},{c2['close']}), c3({c3['open']},{c3['close']})")
            return None

        closes = [x["close"] for x in candles]
        vols   = [x.get("volume", 0) for x in candles]

        idx1, idx2, idx3 = len(closes) - 3, len(closes) - 2, len(closes) - 1

        ema_c1 = self.compute_ema(closes[:idx1+1], 5)
        vw_c1  = self.compute_vwma(closes[:idx1+1], vols[:idx1+1], 20)
        ema_c2 = self.compute_ema(closes[:idx2+1], 5)
        vw_c2  = self.compute_vwma(closes[:idx2+1], vols[:idx2+1], 20)
        ema_c3 = self.compute_ema(closes[:idx3+1], 5)
        vw_c3  = self.compute_vwma(closes[:idx3+1], vols[:idx3+1], 20)

        logging.info(f"[STRAT2] c1: ema={ema_c1}, vwma={vw_c1}; "
                     f"c2: ema={ema_c2}, vwma={vw_c2}; "
                     f"c3: ema={ema_c3}, vwma={vw_c3}")

        if None in (ema_c1, vw_c1, ema_c2, vw_c2, ema_c3, vw_c3):
            logging.info("[STRAT2] blocked: missing indicator values (per-index)")
            return None

        if not (ema_c1 < vw_c1):
            logging.info(f"[STRAT2] blocked: bearish bias failed on c1 (ema={ema_c1}, vwma={vw_c1})")
            return None
        if not (ema_c3 < vw_c3):
            logging.info(f"[STRAT2] blocked: bearish bias failed on c3 (ema={ema_c3}, vwma={vw_c3})")
            return None

        if not (vw_c2 > vw_c3):
            logging.info(f"[STRAT2] blocked: vwma(c2)={vw_c2} <= vwma(c3)={vw_c3}")
            return None

        if not (c3["high"] >= ema_c3):
            logging.info(f"[STRAT2] blocked: c3.high {c3['high']} < ema5(c3) {ema_c3}")
            return None

        if not (c3["close"] < ema_c3 and c3["close"] < vw_c3):
            logging.info(f"[STRAT2] blocked: c3.close {c3['close']} not below ema5 {ema_c3} and vwma20 {vw_c3}")
            return None

        entry = c3["close"]
        sl    = c3["high"] + 5

        # absolute SL cap
        if not self._sl_within_max_points(entry, sl):
            logging.info(f"[STRAT2] Skipped: SL width {sl-entry:.2f} > {MAX_SL_POINTS} points")
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

        if not (c1["time"].time() == datetime.time(9, 15) and c2["time"].time() == datetime.time(9, 30)):
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

        if not self._sl_within_max_points(entry, sl):
            logging.info(f"[STRAT3] Skipped: SL width {sl-entry:.2f} > {MAX_SL_POINTS} points")
            return None

        return {"entry": entry, "sl": sl, "date": today}

    def reconcile_position(self, symbol):
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

        # --- NEW: explicit warmup/skip logs before indicator calc
        if len(candles) < MIN_BARS_FOR_VWMA:
            logging.info(f"[SKIP] {symbol} {candle['time']}: only {len(candles)} bars; need {MIN_BARS_FOR_VWMA} for VWMA20")
            return

        closes = [c["close"] for c in candles]
        volumes = [c["volume"] for c in candles]
        ema5 = self.compute_ema(closes, 5)
        vwma20 = self.compute_vwma(closes, volumes, 20)

        if ema5 is None or vwma20 is None:
            logging.info(f"[SKIP] {symbol} {candle['time']}: indicators unavailable (EMA5={ema5}, VWMA20={vwma20})")
            return

        # Enhanced candle-close log with EMA/VWMA
        logging.info(
            f"[BAR CLOSE + IND] {symbol} {candle['time']} O={candle['open']} H={candle['high']} L={candle['low']} C={candle['close']} V={candle['volume']} | EMA5={ema5:.2f} VWMA20={vwma20:.2f}"
        )

        now = datetime.datetime.now().time()
        if not (TRADING_START <= now <= TRADING_END):
            logging.info(f"[SKIP] {symbol} outside trading window")
            return
        if self.sl_count >= MAX_SLS_PER_DAY:
            logging.info(f"[SKIP] {symbol} daily SL limit reached ({self.sl_count}/{MAX_SLS_PER_DAY})")
            return

        position = self.positions.get(symbol)

        # -------- STRATEGY 3 --------
        if position is None:
            s3 = self.detect_strategy3(symbol, candles)
            if s3:
                raw_entry_resp = self.fyers.place_limit_sell(symbol, self.lot_size, s3["entry"], "STRAT3ENTRY")
                raw_sl_resp = self.fyers.place_stoploss_buy(symbol, self.lot_size, s3["sl"], "STRAT3SL")
                sl_order_id = _extract_order_id(raw_sl_resp)
                entry_order_id = _extract_order_id(raw_entry_resp)
                trade_id = log_to_journal(symbol, "ENTRY", "strat3", entry=s3["entry"], sl=s3["sl"], remarks="", lot_size=self.lot_size)
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

        # -------- STRATEGY 1 (breakdown) --------
        now_date = datetime.date.today()
        first_mark = self.s1_marked_low.get(symbol)
        if (not first_mark) or (first_mark[0] != now_date):
            try:
                c_time = candle["time"].time()
                if c_time == TRADING_START:
                    self.s1_marked_low[symbol] = (now_date, candle["low"])
                    logging.info(f"[STRAT1] Marked first-candle low for {symbol} = {candle['low']}")
            except Exception as e:
                logging.exception(f"[STRAT1] error marking first candle low for {symbol}: {e}")

        position = self.positions.get(symbol)
        fired_today = self.s1_fired_date.get(symbol)
        if self.s1_global_fired_date == now_date:
            logging.debug(f"[STRAT1] Global S1 already taken today; skipping checks for {symbol}")
        else:
            if position is None and fired_today != now_date:
                marked = self.s1_marked_low.get(symbol)
                if marked and marked[0] == now_date:
                    marked_low = marked[1]
                    is_red = lambda c: c["close"] < c["open"]
                    if candle["close"] < marked_low and is_red(candle):
                        entry_price = candle["close"]
                        sl_price = candle["high"] + 5

                        if not self._sl_within_max_points(entry_price, sl_price):
                            logging.info(f"[STRAT1] Skipped: SL width {sl_price-entry_price:.2f} > {MAX_SL_POINTS} points for {symbol}")
                            self.s1_fired_date[symbol] = now_date
                        else:
                            raw_entry = self.fyers.place_limit_sell(symbol, self.lot_size, entry_price, "STRAT1ENTRY")
                            raw_sl = self.fyers.place_stoploss_buy(symbol, self.lot_size, sl_price, "STRAT1SL")
                            sl_order_id = _extract_order_id(raw_sl)
                            entry_order_id = _extract_order_id(raw_entry)
                            trade_id = log_to_journal(symbol, "ENTRY", "strat1", entry=entry_price, sl=sl_price, remarks="breakdown entry", lot_size=self.lot_size)
                            self.positions[symbol] = {
                                "sl_order": {"id": sl_order_id, "resp": raw_sl},
                                "entry_order": {"id": entry_order_id, "resp": raw_entry},
                                "strategy": "strat1",
                                "entry_price": entry_price,
                                "sl_price": sl_price,
                                "trade_id": trade_id
                            }
                            self.s1_fired_date[symbol] = now_date
                            self.s1_global_fired_date = now_date
                            logging.info(f"[STRAT1] ENTRY @{entry_price} SL @{sl_price} for {symbol} (marked_low={marked_low})")

        # -------- STRATEGY 2 --------
        position = self.positions.get(symbol)
        if position is None:
            logging.info(f"[DEBUG] Checking STRAT2 for {symbol} at {candle['time']}")
            strat2 = self.detect_new_short_strategy(candles, ema5, vwma20)
            if strat2:
                raw_entry = self.fyers.place_limit_sell(symbol, self.lot_size, strat2["entry"], "STRAT2ENTRY")
                raw_sl = self.fyers.place_stoploss_buy(symbol, self.lot_size, strat2["sl"], "STRAT2SL")
                sl_order_id = _extract_order_id(raw_sl)
                entry_order_id = _extract_order_id(raw_entry)
                trade_id = log_to_journal(symbol, "ENTRY", "strat2", entry=strat2["entry"], sl=strat2["sl"], remarks="", lot_size=self.lot_size)
                self.positions[symbol] = {
                    "sl_order": {"id": sl_order_id, "resp": raw_sl},
                    "entry_order": {"id": entry_order_id, "resp": raw_entry},
                    "strategy": "strat2",
                    "entry_price": strat2["entry"],
                    "sl_price": strat2["sl"],
                    "trade_id": trade_id
                }
                logging.info(f"[STRAT2] ENTRY @{strat2['entry']} SL @{strat2['sl']} for {symbol}")
        else:
            logging.info(f"[SKIP] {symbol} STRAT2 not evaluated: position already open")

        # -------- STRATEGY 4 --------
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
                                    if not self._sl_within_max_points(entry, sl):
                                        logging.info(f"[STRAT4] Skipped: SL width {sl-entry:.2f} > {MAX_SL_POINTS} points")
                                        self.s4_pending.pop(symbol, None)
                                    else:
                                        raw_entry = self.fyers.place_limit_sell(symbol, self.lot_size, entry, "STRAT4ENTRY")
                                        raw_sl = self.fyers.place_stoploss_buy(symbol, self.lot_size, sl, "STRAT4SL")
                                        sl_order_id = _extract_order_id(raw_sl)
                                        entry_order_id = _extract_order_id(raw_entry)
                                        trade_id = log_to_journal(symbol, "ENTRY", "strat4", entry=entry, sl=sl, remarks="", lot_size=self.lot_size)
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
                self.fyers.place_market_buy(symbol, self.lot_size, "EXITCOND")
                entry_price = position.get("entry_price")
                sl_price = position.get("sl_price")
                trade_id = position.get("trade_id")
                if entry_price is not None and exit_price > entry_price:
                    self.sl_count += 1
                    log_to_journal(
                        symbol, "SL_HIT", position["strategy"],
                        entry=entry_price, sl=sl_price, exit=exit_price,
                        remarks=f"Exit after cancel (interpreted success). cancel_resp={cancel_resp}",
                        lot_size=self.lot_size,
                        trade_id=trade_id
                    )
                else:
                    log_to_journal(
                        symbol, "EXIT", position["strategy"],
                        entry=entry_price, sl=sl_price, exit=exit_price,
                        remarks=f"Exit condition met. cancel_resp={cancel_resp}",
                        lot_size=self.lot_size,
                        trade_id=trade_id
                    )
                self.positions[symbol] = None
            else:
                logging.warning(f"[EXIT SKIPPED] {symbol} - Cancel failed or unknown: {cancel_resp}")
                self.reconcile_position(symbol)

    def on_trade(self, msg):
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
            if position and side == 1:
                trade_id = position.get("trade_id")
                entry_price = position.get("entry_price")
                sl_price = position.get("sl_price")
                log_to_journal(
                    symbol, "SL_HIT", position["strategy"],
                    entry=entry_price, sl=sl_price,
                    exit=None,
                    remarks=str(msg),
                    lot_size=self.lot_size,
                    trade_id=trade_id
                )
                self.sl_count += 1
                self.positions[symbol] = None
        except Exception as e:
            logging.exception(f"Error in on_trade processing: {e}")

# ---------------- MAIN ----------------
if __name__ == "__main__":
    logging.info("[BOOT] Starting main…")
    fyers_client = FyersClient(CLIENT_ID, ACCESS_TOKEN, LOT_SIZE)
    engine = StrategyEngine(fyers_client, LOT_SIZE)

    # Wait until 9:15 AM
    while datetime.datetime.now().time() < TRADING_START:
        time.sleep(1)

    option_symbols = []
    try:
        option_symbols = get_atm_symbols(fyers_client)
    except Exception as e:
        logging.exception(f"Failed to build ATM symbols: {e}")
        raise

    # UPDATED: dynamic prefill to ensure ≥ MIN_BARS_FOR_VWMA bars if possible
    engine.prefill_history(option_symbols, days_back=1)

    fyers_client.register_trade_callback(engine.on_trade)
    fyers_client.subscribe_market_data(option_symbols, engine.on_candle)
    fyers_client.start_order_socket()
