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

# We still won't send any orders before 9:25, but ATM will be fixed at 9:16
TRADING_START = datetime.time(9, 25)
TRADING_END = datetime.time(14, 45)  # 3:00 PM
ATM_DECISION_TIME = datetime.time(9, 16)

JOURNAL_FILE = "sensex_trades.csv"

# Candle settings –– 3-minute combined premium candles
CANDLE_MINUTES = 3
HISTORY_RESOLUTION = "3"

# Warmup requirements
MIN_BARS_FOR_EMA = 20
MAX_HISTORY_LOOKBACK_DAYS = 7

# ------------- max SL points safety (optional) -------------
MAX_SL_POINTS = 150
# ------------------------------------------------------------

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
    """
    We treat the combined straddle premium as a single instrument whose price is (CE+PE).
    PnL = (entry_combined - exit_combined) * lot_size
    """
    if entry_price is None or exit_price is None:
        return None
    try:
        pnl_points = float(entry_price) - float(exit_price)
        pnl_value = pnl_points * float(lot_size)
        return round(pnl_value, 2)
    except Exception:
        return None


# Helper to extract order id from various fyers responses (robust)
def _extract_order_id(resp):
    try:
        if not resp:
            return None
        if isinstance(resp, dict):
            for key in ("id", "order_id", "orderId", "orderID", "data"):
                if key in resp and resp[key]:
                    val = resp[key]
                    if isinstance(val, dict):
                        for subk in ("id", "order_id", "orderId"):
                            if subk in val and val[subk]:
                                return str(val[subk])
                    else:
                        return str(val)
        return None
    except Exception:
        return None


# journaling
def log_to_journal(symbol, action, strategy=None, entry=None, sl=None, exit=None,
                   remarks="", trade_id=None, lot_size=LOT_SIZE):
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

    # For everything else, try to update an existing row
    rows = []
    with open(JOURNAL_FILE, mode="r", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        for r in reader:
            rows.append(r)

    target_idx = None
    if trade_id:
        for idx in range(len(rows) - 1, -1, -1):
            r = rows[idx]
            if r.get("trade_id") == trade_id:
                target_idx = idx
                break

    if target_idx is None:
        for idx in range(len(rows) - 1, -1, -1):
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
        mon = (
            (expiry_date + datetime.timedelta(days=(3 - expiry_date.weekday())))
            .strftime("%b")
            .upper()
            if expiry_date.weekday() != 3
            else expiry_date.strftime("%b").upper()
        )
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
    """
    Called at 9:16 AM (after first 1-minute candle 9:15–9:16 closes).
    Uses SENSEX spot at that moment to decide ATM strike.
    """
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

    logging.info(f"[ATM SYMBOLS @9:16] CE={ce_symbol}, PE={pe_symbol}, spot={ltp}")
    print(f"[ATM SYMBOLS @9:16] CE={ce_symbol}, PE={pe_symbol}, spot={ltp}")
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

    def place_market_sell(self, symbol: str, qty: int, tag: str = ""):
        data = {
            "symbol": symbol, "qty": qty, "type": 2, "side": -1,
            "productType": "INTRADAY", "limitPrice": 0, "stopPrice": 0,
            "validity": "DAY", "disclosedQty": 0, "offlineOrder": False, "orderTag": tag
        }
        return self._log_order_resp("Market Sell", self.client.place_order(data))

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
        """
        Subscribe to the 2 option legs, but build *one* synthetic symbol
        representing the combined straddle premium (CE+PE) on 3-minute candles.

        The callback will always be called with symbol="STRADDLE".
        """
        if len(instrument_ids) != 2:
            raise ValueError("subscribe_market_data expects exactly 2 option symbols (CE & PE)")

        ce_symbol, pe_symbol = instrument_ids[0], instrument_ids[1]
        last_ltp = {}  # symbol -> last traded price
        straddle_candle = None

        def on_message(tick):
            nonlocal straddle_candle

            try:
                if "symbol" not in tick or "ltp" not in tick:
                    return
                symbol = tick["symbol"]
                if symbol not in (ce_symbol, pe_symbol):
                    return

                ltp = float(tick["ltp"])
                ts = int(tick.get("last_traded_time") or tick.get("tt") or tick.get("timestamp"))
                if ts > 10_000_000_000:
                    ts //= 1000
            except Exception as e:
                logging.error(f"Bad tick format: {tick}, {e}")
                return

            last_ltp[symbol] = ltp

            # Need both legs' last prices to build combined premium
            if ce_symbol not in last_ltp or pe_symbol not in last_ltp:
                return

            combined_ltp = last_ltp[ce_symbol] + last_ltp[pe_symbol]
            dt = datetime.datetime.fromtimestamp(ts)
            bucket_minute = (dt.minute // CANDLE_MINUTES) * CANDLE_MINUTES
            candle_open_time = dt.replace(second=0, microsecond=0, minute=bucket_minute)

            # If new 3-minute bucket starts -> close previous bar (if any) and start a new one
            if straddle_candle is None or straddle_candle["time"] != candle_open_time:
                if straddle_candle is not None:
                    logging.info(f"[BAR CLOSE] STRADDLE {straddle_candle['time']} "
                                 f"O={straddle_candle['open']} H={straddle_candle['high']} "
                                 f"L={straddle_candle['low']} C={straddle_candle['close']} V={straddle_candle['volume']}")
                    try:
                        on_candle_callback("STRADDLE", straddle_candle, closed=True)
                    except TypeError:
                        on_candle_callback("STRADDLE", straddle_candle)

                straddle_candle = {
                    "time": candle_open_time,
                    "open": combined_ltp,
                    "high": combined_ltp,
                    "low": combined_ltp,
                    "close": combined_ltp,
                    "volume": 0,
                }
                # Also notify about the new live candle (closed=False)
                try:
                    on_candle_callback("STRADDLE", straddle_candle, closed=False)
                except TypeError:
                    on_candle_callback("STRADDLE", straddle_candle)
            else:
                # Update current candle intrabar
                straddle_candle["high"] = max(straddle_candle["high"], combined_ltp)
                straddle_candle["low"] = min(straddle_candle["low"], combined_ltp)
                straddle_candle["close"] = combined_ltp
                # we aren't tracking volume for combined instrument; keep 0
                try:
                    on_candle_callback("STRADDLE", straddle_candle, closed=False)
                except TypeError:
                    on_candle_callback("STRADDLE", straddle_candle)

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

    def register_order_callback(self, cb):
        self.order_callbacks.append(cb)

    def register_trade_callback(self, cb):
        self.trade_callbacks.append(cb)


# ---------------- STRATEGY ENGINE (new combined-straddle logic) ----------------
class StrategyEngine:
    """
    All EMA and candle logic runs on the synthetic symbol "STRADDLE" whose
    price is CE+PE premium. Orders are placed on the two legs at the same time.
    """

    def __init__(self, fyers_client: FyersClient, lot_size: int = 20,
                 ce_symbol: str = None, pe_symbol: str = None):
        self.fyers = fyers_client
        self.lot_size = lot_size

        # option legs for the ATM straddle decided at 9:16
        self.ce_symbol = ce_symbol
        self.pe_symbol = pe_symbol

        # only one trading symbol in strategy (combined premium)
        self.straddle_symbol = "STRADDLE"

        # candle history for STRADDLE
        self.candles = defaultdict(lambda: deque(maxlen=500))

        # trading state
        self.positions = {}      # key: "STRADDLE" -> position dict
        self.profitable_today = {}  # "STRADDLE" -> bool
        self.taken_today = {}       # "STRADDLE" -> bool (max 1 trade / day)

        # pattern-state for entry
        # stage: 0 = waiting for red close below both EMAs
        #        1 = red done, waiting for a green candle
        #        2 = green done, waiting for final red candle (with special open / close conditions)
        self.pattern_state = {
            self.straddle_symbol: {
                "stage": 0,
                "first_red_time": None,
                "green_time": None,
                "last_date": None,
            }
        }

    def set_leg_symbols(self, ce_symbol: str, pe_symbol: str):
        self.ce_symbol = ce_symbol
        self.pe_symbol = pe_symbol

    def prefill_history(self, symbols, days_back=1):
        """
        We no longer need per-leg history for logic because EMAs are computed on the
        live combined premium candles. This method is kept only for compatibility.
        You can extend it later to backfill STRADDLE history if you want.
        """
        logging.info("[PREFILL] Skipping history prefill for combined-straddle strategy.")

    def compute_ema(self, prices, period):
        if len(prices) < period:
            return None
        ema = prices[0]
        k = 2 / (period + 1)
        for p in prices[1:]:
            ema = p * k + ema * (1 - k)
        return ema

    def _reset_pattern_if_new_day(self, symbol, candle_time):
        state = self.pattern_state.get(symbol)
        if state is None:
            self.pattern_state[symbol] = {
                "stage": 0,
                "first_red_time": None,
                "green_time": None,
                "last_date": candle_time.date(),
            }
            return

        if state["last_date"] != candle_time.date():
            self.pattern_state[symbol] = {
                "stage": 0,
                "first_red_time": None,
                "green_time": None,
                "last_date": candle_time.date(),
            }

    def on_candle(self, symbol, candle, closed=False):
        """
        symbol is always "STRADDLE".
        closed == True  -> closed bar (EMAs & entry logic)
        closed == False -> intrabar updates (for SL checks)
        """
        if symbol != self.straddle_symbol:
            # Ignore anything else
            return

        # Append closed bars to history; intrabar updates are not appended
        if closed:
            self.candles[symbol].append(candle)

        candles = list(self.candles[symbol])

        # If not enough closed bars for EMA, allow intrabar SL checks but skip entry logic
        if len(candles) < MIN_BARS_FOR_EMA:
            if not closed and self.positions.get(symbol):
                # intrabar SL checks later
                pass
            else:
                if closed:
                    logging.info(f"[SKIP] {symbol} {candle['time']}: only {len(candles)} bars; "
                                 f"need {MIN_BARS_FOR_EMA} for EMA20")
                return

        closes = [c["close"] for c in candles]
        ema5 = self.compute_ema(closes, 5) if len(closes) >= 5 else None
        ema20 = self.compute_ema(closes, 20) if len(closes) >= 20 else None

        now = datetime.datetime.now().time()
        if not (TRADING_START <= now <= TRADING_END):
            logging.info(f"[SKIP] {symbol} outside trading window")
            return

        # pattern state reset if new day
        self._reset_pattern_if_new_day(symbol, candle["time"])
        state = self.pattern_state[symbol]

        # helper bull / bear
        is_green = lambda c: c["close"] > c["open"]
        is_red = lambda c: c["close"] < c["open"]

        position = self.positions.get(symbol)

        # ---------------- ENTRY LOGIC (only on closed bars) ----------------
        if closed and ema5 is not None and ema20 is not None:
            logging.info(f"[BAR CLOSE + IND] {symbol} {candle['time']} "
                         f"O={candle['open']} H={candle['high']} L={candle['low']} "
                         f"C={candle['close']} V={candle['volume']} "
                         f"| EMA5={ema5:.2f} EMA20={ema20:.2f}")

            if self.profitable_today.get(symbol):
                logging.info(f"[SKIP] {symbol} already had a profitable trade today.")
            else:
                # Stage-machine for your described pattern
                if state["stage"] == 0:
                    # Stage 0: wait for first RED candle which closes below both EMA5 and EMA20
                    if is_red(candle) and candle["close"] < ema5 and candle["close"] < ema20:
                        state["stage"] = 1
                        state["first_red_time"] = candle["time"]
                        logging.info(f"[PATTERN] Stage 0 -> 1 (first red below EMAs) at {candle['time']}")
                elif state["stage"] == 1:
                    # Stage 1: we have that red, now wait for a GREEN candle (no EMA condition specified)
                    if is_green(candle):
                        state["stage"] = 2
                        state["green_time"] = candle["time"]
                        logging.info(f"[PATTERN] Stage 1 -> 2 (green candle) at {candle['time']}")
                    # If we get another strong red below EMAs, we can treat it as a fresh starting point
                    elif is_red(candle) and candle["close"] < ema5 and candle["close"] < ema20:
                        state["stage"] = 1
                        state["first_red_time"] = candle["time"]
                        logging.info(f"[PATTERN] Stage 1 refreshed by new red below EMAs at {candle['time']}")
                elif state["stage"] == 2:
                    # Stage 2: wait for FINAL red candle:
                    # open is above EMA5 but less than EMA20
                    # close is below both EMA5 and EMA20
                    cond_open = candle["open"] > ema5 and candle["open"] < ema20
                    cond_close = is_red(candle) and candle["close"] < ema5 and candle["close"] < ema20

                    if cond_open and cond_close and position is None and not self.taken_today.get(symbol):
                        entry = candle["close"]
                        sl = candle["high"]  # SL is high of this final red candle

                        sl_points = sl - entry
                        if sl_points <= 0:
                            logging.info(f"[SKIP] SL not above entry (entry={entry}, sl={sl}); invalid.")
                            state["stage"] = 0
                        elif sl_points > MAX_SL_POINTS:
                            logging.info(f"[SKIP] SL points {sl_points:.2f} > MAX_SL_POINTS={MAX_SL_POINTS}")
                            log_to_journal(symbol, "SKIP", "combined_straddle",
                                           entry=entry, sl=sl,
                                           remarks=f"SL_points={sl_points:.2f} > MAX_SL_POINTS",
                                           lot_size=self.lot_size)
                            state["stage"] = 0
                        else:
                            # -------------- EXECUTE ENTRY --------------
                            logging.info(f"[ENTRY SIGNAL] {symbol} entry={entry} sl={sl} "
                                         f"(SL pts={sl_points:.2f})")

                            # Sell both CE and PE at market
                            if not self.ce_symbol or not self.pe_symbol:
                                logging.error("[ENTRY] CE/PE symbols not set; cannot place straddle.")
                            else:
                                ce_resp = self.fyers.place_market_sell(
                                    self.ce_symbol, self.lot_size, tag="STRADDLE_ENTRY_CE"
                                )
                                pe_resp = self.fyers.place_market_sell(
                                    self.pe_symbol, self.lot_size, tag="STRADDLE_ENTRY_PE"
                                )
                                trade_id = log_to_journal(
                                    symbol, "ENTRY", "combined_straddle",
                                    entry=entry, sl=sl, remarks="",
                                    lot_size=self.lot_size
                                )

                                self.positions[symbol] = {
                                    "symbol": symbol,
                                    "strategy": "combined_straddle",
                                    "entry_price": entry,
                                    "sl_price": sl,
                                    "trade_id": trade_id,
                                    "exiting": False,
                                    "legs": [self.ce_symbol, self.pe_symbol],
                                    "exit_trades_count": 0,
                                    "exit_prices_sum": 0.0,
                                }
                                self.taken_today[symbol] = True
                                logging.info(f"[POSITION OPENED] {symbol} ENTRY={entry} SL={sl} "
                                             f"CE_order={ce_resp} PE_order={pe_resp}")

                            # After a full pattern + entry, reset pattern state
                            state["stage"] = 0
                            state["first_red_time"] = None
                            state["green_time"] = None
                    else:
                        # Pattern invalidated / something else -> you might choose to reset
                        if is_red(candle) and candle["close"] < ema5 and candle["close"] < ema20:
                            # restart from new first red
                            state["stage"] = 1
                            state["first_red_time"] = candle["time"]
                            state["green_time"] = None
                            logging.info(f"[PATTERN] Stage 2 reset to Stage 1 (new red) at {candle['time']}")

        # ---------------- MANAGE OPEN POSITION: SL on combined premium + EOD exit ----------------
        position = self.positions.get(symbol)
        if not position:
            return

        entry = position.get("entry_price")
        sl = position.get("sl_price")

        # Intrabar SL check on combined premium:
        # "If the combined premium at anytime reaches the high of the red candle, exit the trade."
        if candle["high"] >= sl and not position.get("exiting"):
            try:
                logging.info(f"[SL HIT] {symbol}: candle_high={candle['high']} >= SL={sl}. Exiting straddle.")

                # Exit both legs at market
                if self.ce_symbol:
                    self.fyers.place_market_buy(self.ce_symbol, self.lot_size, tag="STRADDLE_SL_EXIT_CE")
                if self.pe_symbol:
                    self.fyers.place_market_buy(self.pe_symbol, self.lot_size, tag="STRADDLE_SL_EXIT_PE")

                position["exiting"] = True

                # We log a provisional exit at SL for journal; final actual prices will come via on_trade
                log_to_journal(
                    symbol, "EXIT_INITIATED", position["strategy"],
                    entry=entry, sl=sl, exit=sl,
                    remarks="SL hit on combined premium; market exit initiated",
                    lot_size=self.lot_size,
                    trade_id=position.get("trade_id"),
                )
            except Exception as e:
                logging.exception(f"Failed to execute SL exit for {symbol}: {e}")
            return

        # End-of-day safety exit
        if candle["time"].time() >= TRADING_END and not position.get("exiting"):
            try:
                logging.info(f"[TIME EXIT] {symbol} forcing exit at market at {candle['time']}")
                if self.ce_symbol:
                    self.fyers.place_market_buy(self.ce_symbol, self.lot_size, tag="STRADDLE_EOD_EXIT_CE")
                if self.pe_symbol:
                    self.fyers.place_market_buy(self.pe_symbol, self.lot_size, tag="STRADDLE_EOD_EXIT_PE")

                position["exiting"] = True
                log_to_journal(
                    symbol, "EXIT_INITIATED", position["strategy"],
                    entry=entry, sl=sl, exit=candle["close"],
                    remarks="EOD exit initiated",
                    lot_size=self.lot_size,
                    trade_id=position.get("trade_id"),
                )
            except Exception as e:
                logging.exception(f"Failed to execute EOD exit for {symbol}: {e}")
            return

    def on_trade(self, msg):
        """
        We treat BUY trades on either leg as exits for the open straddle position.
        Once both legs are bought back, the position is considered fully closed.
        """
        try:
            trades = msg.get("trades") or msg.get("data") or msg
            if not trades:
                return

            # Normalize to list of dicts
            if isinstance(trades, dict):
                trades_list = [trades]
            else:
                trades_list = trades

            for t in trades_list:
                symbol = t.get("symbol") or t.get("s") or t.get("sym")
                side = t.get("side")

                if not symbol:
                    continue

                # Only care about BUY side=1 which closes our short legs
                if side != 1:
                    continue

                # We currently only have one combined position ("STRADDLE"), if any
                pos = self.positions.get(self.straddle_symbol)
                if not pos:
                    continue

                if symbol not in pos.get("legs", []):
                    continue

                # Extract executed price
                exit_price_leg = None
                try:
                    if t.get("price") is not None:
                        exit_price_leg = float(t["price"])
                except Exception:
                    exit_price_leg = None

                if exit_price_leg is not None:
                    pos["exit_trades_count"] += 1
                    pos["exit_prices_sum"] += exit_price_leg

                # When both legs are bought back, finalize journal as EXIT
                if pos["exit_trades_count"] >= len(pos.get("legs", [])):
                    combined_exit_price = pos["exit_prices_sum"]
                    trade_id = pos.get("trade_id")
                    entry_price = pos.get("entry_price")
                    sl_price = pos.get("sl_price")

                    log_to_journal(
                        self.straddle_symbol, "EXIT", pos["strategy"],
                        entry=entry_price, sl=sl_price, exit=combined_exit_price,
                        remarks=str(msg),
                        lot_size=self.lot_size,
                        trade_id=trade_id,
                    )

                    pnl = compute_pnl_for_short(entry_price, combined_exit_price, self.lot_size)
                    if pnl is not None and pnl > 0:
                        self.profitable_today[self.straddle_symbol] = True

                    # Remove position
                    self.positions.pop(self.straddle_symbol, None)
                    logging.info(f"[POSITION CLOSED] STRADDLE exit_price={combined_exit_price} pnl={pnl}")
                    break

        except Exception as e:
            logging.exception(f"Error in on_trade processing: {e}")


# ---------------- MAIN ----------------
if __name__ == "__main__":
    logging.info("[BOOT] Starting main…")
    fyers_client = FyersClient(CLIENT_ID, ACCESS_TOKEN, LOT_SIZE)

    # 1) Wait until 9:16 AM (after first 1-minute candle closes) to fix ATM strike
    logging.info("Waiting for 9:16 AM to decide ATM premium…")
    while datetime.datetime.now().time() < ATM_DECISION_TIME:
        time.sleep(1)

    try:
        option_symbols = get_atm_symbols(fyers_client)  # [CE, PE]
    except Exception as e:
        logging.exception(f"Failed to build ATM symbols at 9:16: {e}")
        raise

    ce_symbol, pe_symbol = option_symbols[0], option_symbols[1]

    engine = StrategyEngine(fyers_client, LOT_SIZE, ce_symbol=ce_symbol, pe_symbol=pe_symbol)

    # Prefill no-op for now (can be extended if you want historical EMAs)
    engine.prefill_history(["STRADDLE"], days_back=1)

    # 2) Wait until trading start time for entries
    logging.info(f"Waiting for TRADING_START={TRADING_START} to begin processing candles…")
    while datetime.datetime.now().time() < TRADING_START:
        time.sleep(1)

    fyers_client.register_trade_callback(engine.on_trade)
    fyers_client.subscribe_market_data(option_symbols, engine.on_candle)
    fyers_client.start_order_socket()
