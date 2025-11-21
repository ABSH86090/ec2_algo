import datetime
import logging
import csv
import os
import time
import uuid
import sys
from collections import defaultdict, deque
from dotenv import load_dotenv
from fyers_apiv3 import fyersModel
from fyers_apiv3.FyersWebsocket import order_ws, data_ws

# ---------------- CONFIG ----------------
load_dotenv()

CLIENT_ID = os.getenv("FYERS_CLIENT_ID")
ACCESS_TOKEN = os.getenv("FYERS_ACCESS_TOKEN")  # Format: APPID-XXXXX:token
LOT_SIZE = 20
TRADING_START = datetime.time(9, 15)
TRADING_END = datetime.time(14, 59)  # 3:00 PM
JOURNAL_FILE = "sensex_trades.csv"

# Candle settings
CANDLE_MINUTES = 15
HISTORY_RESOLUTION = "15"

# Warmup requirements
MIN_BARS_FOR_EMA = 20
MAX_HISTORY_LOOKBACK_DAYS = 7

# Ensure journal file exists with headers
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
        logging.FileHandler("strategy.log"),
        logging.StreamHandler(sys.stdout)
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
    if entry_price is None or exit_price is None:
        return None
    try:
        pnl_points = float(entry_price) - float(exit_price)
        pnl_value = pnl_points * float(lot_size)
        return round(pnl_value, 2)
    except Exception:
        return None

# journaling (unchanged)
def log_to_journal(symbol, action, strategy=None, entry=None, sl=None, exit=None, remarks="", trade_id=None, lot_size=LOT_SIZE):
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

# ---------------- EXPIRY & SYMBOL UTILS (kept same) ----------------
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

# ---------------- FYERS CLIENT (kept same) ----------------
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
                if c is not None:
                    logging.info(f"[BAR CLOSE] {symbol} {c['time']} O={c['open']} H={c['high']} L={c['low']} C={c['close']} V={c['volume']}")
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

# ---------------- STRATEGY ENGINE (updated: only Strategy 1 & 2 implemented) ----------------
class StrategyEngine:
    def __init__(self, fyers_client: FyersClient, lot_size: int = 20):
        self.fyers = fyers_client
        self.lot_size = lot_size
        self.positions = {}  # symbol -> position dict
        self.candles = defaultdict(lambda: deque(maxlen=500))
        self.profitable_today = {}  # symbol -> bool (True if any trade produced profit for that symbol today)
        self.s1_marked_green = {}  # symbol -> (date, price) for first green closure above EMAs (or last such green)

    def prefill_history(self, symbols, days_back=1):
        for symbol in symbols:
            total_added = 0
            used_days = 0
            while total_added < MIN_BARS_FOR_EMA and used_days < MAX_HISTORY_LOOKBACK_DAYS:
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

            logging.info(f"[PREFILL DONE] {symbol}: total prefilled bars={total_added} (min needed={MIN_BARS_FOR_EMA})")

    def compute_ema(self, prices, period):
        if len(prices) < period: return None
        ema = prices[0]; k = 2/(period+1)
        for p in prices[1:]:
            ema = p*k + ema*(1-k)
        return ema

    def _cancel_sl_and_place_new(self, position, new_sl_price):
        # cancel existing SL order if present, then place a new stoploss buy at new_sl_price
        sl_info = position.get("sl_order") or {}
        sl_order_id = sl_info.get("id")
        if sl_order_id:
            try:
                cancel_resp = self.fyers.cancel_order(sl_order_id)
                logging.info(f"Cancelled old SL order: {cancel_resp}")
            except Exception as e:
                logging.exception(f"Failed to cancel SL order {sl_order_id}: {e}")
        new_sl_resp = self.fyers.place_stoploss_buy(position["symbol"], self.lot_size, new_sl_price, "TRAIL_SL")
        new_sl_id = _extract_order_id(new_sl_resp)
        position["sl_order"] = {"id": new_sl_id, "resp": new_sl_resp}
        position["sl_price"] = new_sl_price
        return new_sl_resp

    def on_candle(self, symbol, candle):
        self.candles[symbol].append(candle)
        candles = list(self.candles[symbol])

        if len(candles) < MIN_BARS_FOR_EMA:
            logging.info(f"[SKIP] {symbol} {candle['time']}: only {len(candles)} bars; need {MIN_BARS_FOR_EMA} for EMA20")
            return

        closes = [c["close"] for c in candles]
        volumes = [c["volume"] for c in candles]
        ema5 = self.compute_ema(closes, 5)
        ema20 = self.compute_ema(closes, 20)

        if ema5 is None or ema20 is None:
            logging.info(f"[SKIP] {symbol} {candle['time']}: indicators unavailable (EMA5={ema5}, EMA20={ema20})")
            return

        logging.info(f"[BAR CLOSE + IND] {symbol} {candle['time']} O={candle['open']} H={candle['high']} L={candle['low']} C={candle['close']} V={candle['volume']} | EMA5={ema5:.2f} EMA20={ema20:.2f}")

        now = datetime.datetime.now().time()
        if not (TRADING_START <= now <= TRADING_END):
            logging.info(f"[SKIP] {symbol} outside trading window")
            return

        # If symbol already had a profitable trade today, do not take any more trades for that symbol
        if self.profitable_today.get(symbol):
            logging.info(f"[SKIP] {symbol} already produced profit today; skipping further entries.")
            return

        position = self.positions.get(symbol)

        # ------------------ Strategy 1 ------------------
        # If a green candle closed above both EMA5 and EMA20 -> mark it
        is_green = lambda c: c["close"] > c["open"]
        is_red = lambda c: c["close"] < c["open"]

        if is_green(candle) and candle["close"] > ema5 and candle["close"] > ema20:
            self.s1_marked_green[symbol] = (candle["time"].date(), candle["close"])  # record date + price
            logging.info(f"[S1 MARK] {symbol} marked green close above EMAs at {candle['close']} on {candle['time'].date()}")

        # Strategy 1 entry: wait for red candle which closes below both EMA5 and EMA20
        if position is None and symbol in self.s1_marked_green:
            mark_date, _ = self.s1_marked_green[symbol]
            if mark_date == candle["time"].date():
                if is_red(candle) and candle["close"] < ema5 and candle["close"] < ema20:
                    entry = candle["close"]
                    sl = candle["high"]  # SL exactly at high of the red candle (user requirement)
                    target_points = abs(sl - entry)
                    target_price = entry - target_points  # since we are short

                    # Place entry and SL
                    raw_entry = self.fyers.place_limit_sell(symbol, self.lot_size, entry, "S1ENTRY")
                    raw_sl = self.fyers.place_stoploss_buy(symbol, self.lot_size, sl, "S1SL")
                    sl_order_id = _extract_order_id(raw_sl)
                    entry_order_id = _extract_order_id(raw_entry)
                    trade_id = log_to_journal(symbol, "ENTRY", "strat1", entry=entry, sl=sl, remarks="", lot_size=self.lot_size)
                    self.positions[symbol] = {
                        "symbol": symbol,
                        "strategy": "strat1",
                        "entry_price": entry,
                        "sl_price": sl,
                        "target_price": target_price,
                        "target_points": target_points,
                        "sl_order": {"id": sl_order_id, "resp": raw_sl},
                        "entry_order": {"id": entry_order_id, "resp": raw_entry},
                        "trade_id": trade_id
                    }
                    logging.info(f"[STRAT1] ENTRY @{entry} SL @{sl} TARGET @{target_price} for {symbol}")

        # ------------------ Strategy 2 ------------------
        # If a green candle formed that closes below EMA20 -> mark for strat2
        # (User: "If a green candle is formed such that it closes below ema20, then wait for a red candle which closes below both ema5 and ema20")
        # We'll reuse s1_marked_green structure but tag differently
        if is_green(candle) and candle["close"] < ema20:
            # mark for strat2
            key = (symbol, "s2")
            self.s1_marked_green[key] = (candle["time"].date(), candle["close"])  # reusing store; different key
            logging.info(f"[S2 MARK] {symbol} marked green-close-below-EMA20 at {candle['close']} on {candle['time'].date()}")

        # Strategy 2 entry: wait for red candle which closes below both ema5 and ema20
        key = (symbol, "s2")
        if position is None and key in self.s1_marked_green:
            mark_date, _ = self.s1_marked_green[key]
            if mark_date == candle["time"].date():
                if is_red(candle) and candle["close"] < ema5 and candle["close"] < ema20:
                    entry = candle["close"]
                    sl = candle["high"] + 5  # user requirement
                    target_points = abs(sl - entry)
                    target_price = entry - target_points

                    raw_entry = self.fyers.place_limit_sell(symbol, self.lot_size, entry, "S2ENTRY")
                    raw_sl = self.fyers.place_stoploss_buy(symbol, self.lot_size, sl, "S2SL")
                    sl_order_id = _extract_order_id(raw_sl)
                    entry_order_id = _extract_order_id(raw_entry)
                    trade_id = log_to_journal(symbol, "ENTRY", "strat2", entry=entry, sl=sl, remarks="", lot_size=self.lot_size)
                    self.positions[symbol] = {
                        "symbol": symbol,
                        "strategy": "strat2",
                        "entry_price": entry,
                        "sl_price": sl,
                        "target_price": target_price,
                        "target_points": target_points,
                        "sl_order": {"id": sl_order_id, "resp": raw_sl},
                        "entry_order": {"id": entry_order_id, "resp": raw_entry},
                        "trade_id": trade_id
                    }
                    logging.info(f"[STRAT2] ENTRY @{entry} SL @{sl} TARGET @{target_price} for {symbol}")

        # ------------------ Manage open position: target, move SL, and time-based exit ------------------
        position = self.positions.get(symbol)
        if position:
            entry = position.get("entry_price")
            sl = position.get("sl_price")
            target_price = position.get("target_price")
            tp_pts = position.get("target_points")

            # Check if target achieved in this bar (for shorts: price <= target)
            if candle["low"] <= target_price:
                # target hit -> exit at target_price (place market buy or limit?) place market buy for execution
                self.fyers.place_market_buy(symbol, self.lot_size, "TARGET_HIT")
                exit_price = target_price
                pnl = compute_pnl_for_short(entry, exit_price, self.lot_size)
                log_to_journal(symbol, "EXIT", position["strategy"], entry=entry, sl=sl, exit=exit_price, remarks="Target hit", lot_size=self.lot_size, trade_id=position.get("trade_id"))
                # mark profitable if pnl>0
                if pnl and pnl > 0:
                    self.profitable_today[symbol] = True
                self.positions[symbol] = None
                logging.info(f"[EXIT-TARGET] {symbol} exited at {exit_price} pnl={pnl}")
                return

            # Move SL to entry after 70%/80% of target achieved depending on strategy
            # For shorts, favorable move = price lowered by fraction of target_points
            fraction = 0.7 if position.get("strategy") == "strat1" else 0.8
            trigger_price = entry - tp_pts * fraction
            # if low of the candle <= trigger_price then move SL to entry
            if candle["low"] <= trigger_price and position.get("sl_price") != entry:
                try:
                    logging.info(f"[TRAIL] {symbol} reached {fraction*100:.0f}% of TP; moving SL to entry {entry}")
                    self._cancel_sl_and_place_new(position, entry)
                except Exception as e:
                    logging.exception(f"Failed to move SL for {symbol}: {e}")

            # Time-based exit at 3:00 PM if neither SL nor target achieved
            # We'll interpret "by 3:00 PM" as when the bar's time is >= TRADING_END (15:00)
            if candle["time"].time() >= TRADING_END:
                logging.info(f"[TIME EXIT] {symbol} forcing exit at market at {candle['time']}")
                # cancel SL order if exists
                sl_info = position.get("sl_order") or {}
                sl_order_id = sl_info.get("id")
                cancel_resp = None
                if sl_order_id:
                    cancel_resp = self.fyers.cancel_order(sl_order_id)
                # market buy to exit
                market_resp = self.fyers.place_market_buy(symbol, self.lot_size, "EOD_EXIT")
                exit_price = candle["close"]
                pnl = compute_pnl_for_short(entry, exit_price, self.lot_size)
                log_to_journal(symbol, "EXIT", position["strategy"], entry=entry, sl=sl, exit=exit_price, remarks=f"EOD exit; cancel_resp={cancel_resp}", lot_size=self.lot_size, trade_id=position.get("trade_id"))
                if pnl and pnl > 0:
                    self.profitable_today[symbol] = True
                self.positions[symbol] = None
                logging.info(f"[EXIT-EOD] {symbol} exited at {exit_price} pnl={pnl} cancel_resp={cancel_resp}")

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
            # If we receive a buy trade (side == 1) against an open short, treat as SL hit / exit
            if position and side == 1:
                trade_id = position.get("trade_id")
                entry_price = position.get("entry_price")
                sl_price = position.get("sl_price")
                # We don't have exit price in on_trade reliably; log SL_HIT and mark position closed
                log_to_journal(symbol, "SL_HIT", position["strategy"], entry=entry_price, sl=sl_price, exit=None, remarks=str(msg), lot_size=self.lot_size, trade_id=trade_id)
                # Attempt to compute pnl if we can extract price
                exit_price = None
                try:
                    if isinstance(trades, dict) and trades.get("price"):
                        exit_price = float(trades.get("price"))
                    elif isinstance(trades, list) and trades[0].get("price"):
                        exit_price = float(trades[0].get("price"))
                except Exception:
                    exit_price = None
                pnl = compute_pnl_for_short(entry_price, exit_price, self.lot_size) if exit_price is not None else None
                if pnl and pnl > 0:
                    self.profitable_today[symbol] = True
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

    engine.prefill_history(option_symbols, days_back=1)

    fyers_client.register_trade_callback(engine.on_trade)
    fyers_client.subscribe_market_data(option_symbols, engine.on_candle)
    fyers_client.start_order_socket()
