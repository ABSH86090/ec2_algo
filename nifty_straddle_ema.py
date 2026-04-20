import datetime
import logging
import os
import time
import sys
import requests
from collections import deque
from dotenv import load_dotenv
from fyers_apiv3 import fyersModel
from fyers_apiv3.FyersWebsocket import data_ws

# =========================================================
# CONFIG
# =========================================================
load_dotenv()

CLIENT_ID = os.getenv("FYERS_CLIENT_ID")
ACCESS_TOKEN = os.getenv("FYERS_ACCESS_TOKEN")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

LOT_SIZE = 130
HEDGE_OFFSET = 1000          # Points away from ATM for hedges

ATM_DECISION_TIME = datetime.time(9, 20)
TRADING_END = datetime.time(15, 0)

HISTORY_RESOLUTION = "3"

EMA_FAST = 5
EMA_SLOW = 20
EMA_GAP_MIN = 5              # Minimum gap between EMA20 and EMA5 (not used for entry now but kept for reference)

MIN_BARS_FOR_EMA = 25
HIST_PREFILL_BARS = 80
MAX_SL_POINTS = 50

LOG_FILE = "nifty_strangle_revised.log"

# =========================================================
# LOGGING
# =========================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout)
    ],
    force=True
)
logger = logging.getLogger(__name__)


def send_telegram(msg):
    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        try:
            requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={"chat_id": TELEGRAM_CHAT_ID, "text": msg[:4000]},
                timeout=3
            )
        except Exception:
            pass


# =========================================================
# SYMBOL UTILS
# =========================================================

SPECIAL_MARKET_HOLIDAYS = {
    datetime.date(2026, 1, 26),
    datetime.date(2026, 3, 3),
    datetime.date(2026, 3, 26),
    datetime.date(2026, 3, 31),
    datetime.date(2026, 4, 14),
    datetime.date(2026, 5, 1),
    datetime.date(2026, 5, 28),
    datetime.date(2026, 6, 26),
    datetime.date(2026, 9, 14),
    datetime.date(2026, 10, 2),
    datetime.date(2026, 11, 24),
    datetime.date(2026, 12, 25)
}

def is_last_tuesday(d, holidays=SPECIAL_MARKET_HOLIDAYS):
    # Check if 'd' is the last Tuesday of the month
    is_tuesday = d.weekday() == 1
    is_last_week = (d + datetime.timedelta(days=7)).month != d.month

    if is_tuesday and is_last_week:
        return not (d in holidays)  # Return False if Tuesday itself is a holiday

    # If 'd' is Monday, check if tomorrow (Tuesday) is a holiday and would've been last Tuesday
    if d.weekday() == 0:
        next_day = d + datetime.timedelta(days=1)
        is_last_week_tuesday = (next_day + datetime.timedelta(days=7)).month != next_day.month
        if next_day in holidays and is_last_week_tuesday:
            return True

    return False


def get_next_expiry():
    today = datetime.date.today()
    days = (1 - today.weekday()) % 7
    expiry = today + datetime.timedelta(days=days)
    if today.weekday() == 1 and datetime.datetime.now().time() >= datetime.time(15, 30):
        expiry += datetime.timedelta(days=7)
    if expiry in SPECIAL_MARKET_HOLIDAYS:
        expiry -= datetime.timedelta(days=1)
    return expiry


def format_expiry(expiry):
    yy = expiry.strftime("%y")
    if is_last_tuesday(expiry):
        return f"{yy}{expiry.strftime('%b').upper()}"
    m, d = expiry.month, expiry.day
    m_token = {10: "O", 11: "N", 12: "D"}.get(m, str(m))
    return f"{yy}{m_token}{d:02d}"


def get_nifty_strangle_symbols(fyers, spot=None):
    """
    Returns (ce_symbol, pe_symbol, ce_hedge_symbol, pe_hedge_symbol, atm)
    ce/pe are the main sell strikes; ce_hedge/pe_hedge are 1000 pts away for buying.
    """
    if spot is None:
        r = fyers.quotes({"symbols": "NSE:NIFTY50-INDEX"})
        spot = float(r["d"][0]["v"]["lp"])

    atm = round(spot / 50) * 50

    # Main straddle strikes (sell)
    ce_strike = atm
    pe_strike = atm

    # Hedge strikes (buy) — 1000 points away from ATM
    ce_hedge_strike = atm + 1000
    pe_hedge_strike = atm - 1000

    expiry = format_expiry(get_next_expiry())

    ce_symbol       = f"NSE:NIFTY{expiry}{ce_strike}CE"
    pe_symbol       = f"NSE:NIFTY{expiry}{pe_strike}PE"
    ce_hedge_symbol = f"NSE:NIFTY{expiry}{ce_hedge_strike}CE"
    pe_hedge_symbol = f"NSE:NIFTY{expiry}{pe_hedge_strike}PE"

    logger.info(
        f"[STRANGLE SYMBOLS] "
        f"CE={ce_symbol} | PE={pe_symbol} | "
        f"CE_HEDGE={ce_hedge_symbol} | PE_HEDGE={pe_hedge_symbol}"
    )
    send_telegram(
        f"📌 NIFTY STRANGLE SYMBOLS\n"
        f"CE (sell)  : {ce_symbol}\n"
        f"PE (sell)  : {pe_symbol}\n"
        f"CE HEDGE   : {ce_hedge_symbol}\n"
        f"PE HEDGE   : {pe_hedge_symbol}"
    )

    return ce_symbol, pe_symbol, ce_hedge_symbol, pe_hedge_symbol, atm


# =========================================================
# FYERS CLIENT
# =========================================================
class FyersClient:
    def __init__(self):
        self.client = fyersModel.FyersModel(
            client_id=CLIENT_ID,
            token=ACCESS_TOKEN,
            is_async=False,
            log_path=""
        )
        self.auth = f"{CLIENT_ID}:{ACCESS_TOKEN}"

    def sell_market(self, symbol, tag):
        return self.client.place_order({
            "symbol": symbol,
            "qty": LOT_SIZE,
            "type": 2,
            "side": -1,
            "productType": "INTRADAY",
            "validity": "DAY",
            "orderTag": tag
        })

    def buy_market(self, symbol, tag):
        return self.client.place_order({
            "symbol": symbol,
            "qty": LOT_SIZE,
            "type": 2,
            "side": 1,
            "productType": "INTRADAY",
            "validity": "DAY",
            "orderTag": tag
        })


# =========================================================
# HISTORICAL PREFILL (PREVIOUS SESSIONS)
# =========================================================
def fetch_historical_premium(fyers, ce, pe):
    to_dt = datetime.datetime.now()
    from_dt = to_dt - datetime.timedelta(days=7)

    def fetch(symbol):
        r = fyers.client.history({
            "symbol": symbol,
            "resolution": HISTORY_RESOLUTION,
            "date_format": "1",
            "range_from": from_dt.strftime("%Y-%m-%d"),
            "range_to": to_dt.strftime("%Y-%m-%d"),
            "cont_flag": "1"
        })
        return r.get("candles", [])

    ce_hist = fetch(ce)
    pe_hist = fetch(pe)

    candles = []
    for c1, c2 in zip(ce_hist, pe_hist):
        ts = datetime.datetime.fromtimestamp(c1[0])
        if ts.time() < datetime.time(9, 15) or ts.time() > datetime.time(15, 30):
            continue
        candles.append({
            "time": ts,
            "open": c1[1] + c2[1],
            "high": c1[2] + c2[2],
            "low": c1[3] + c2[3],
            "close": c1[4] + c2[4]
        })

    logger.info(f"[PREFILL] Loaded {len(candles)} candles")
    return candles


# =========================================================
# STRATEGY ENGINE
# =========================================================
class StrategyEngine:
    """
    Entry Logic (on closed candles):
    ---------------------------------
    1. EMA5 < EMA20 (mandatory throughout)
    2. Previous closed candle must be GREEN (close > open)
    3. Current closed candle must be RED (close < open)
       AND its high >= EMA20 (touches EMA20)
       AND its close < EMA20 (closes below EMA20)
    4. SL = high of the entry red candle
       SL on premium = combined premium at candle high
       (approximated as entry_premium + (candle_high - candle_close))
    5. Buy hedges first, then sell main strikes.

    Exit Logic:
    -----------
    Exit Condition 1 (SL): Live combined premium >= sl_premium
    Exit Condition 2 (Green candles above EMA20):
        Two consecutive closed green candles with close > EMA20
    On exit: Buy CE+PE main first, then Sell CE+PE hedges.
    """

    def __init__(self, fyers, ce, pe, ce_hedge, pe_hedge):
        self.fyers = fyers
        self.ce = ce
        self.pe = pe
        self.ce_hedge = ce_hedge
        self.pe_hedge = pe_hedge

        self.candles = deque(maxlen=2000)
        self.position = None          # dict with entry/sl/sl_premium when in trade
        self.disabled_for_day = False
        self.last_logged_ema_time = None
        self.consecutive_green_above_ema = 0   # counter for exit condition 2

    # ----------------------------------------------------------
    def ema(self, values, period):
        if len(values) < period:
            return None
        sma = sum(values[:period]) / period
        ema = sma
        k = 2 / (period + 1)
        for v in values[period:]:
            ema = v * k + ema * (1 - k)
        return ema

    def get_emas(self):
        closes = [c["close"] for c in list(self.candles)[-HIST_PREFILL_BARS:]]
        ema5  = self.ema(closes, EMA_FAST)
        ema20 = self.ema(closes, EMA_SLOW)
        return ema5, ema20

    # ----------------------------------------------------------
    def _enter_trade(self, entry_candle, ema5, ema20):
        entry_close = entry_candle["close"]
        entry_high  = entry_candle["high"]

        # SL premium = combined premium when candle touches its high.
        # Since we only have OHLC, approximate:
        #   sl_premium = entry_close + (entry_high - entry_close)
        #              = entry_high of combined premium candle
        sl_premium = entry_candle["high"]   # high of the combined premium candle

        if entry_high - entry_close > MAX_SL_POINTS:
            logger.warning(
                f"[SKIP SIGNAL] SL too wide: "
                f"entry_close={entry_close:.2f} entry_high={entry_high:.2f} "
                f"diff={entry_high - entry_close:.2f} > MAX {MAX_SL_POINTS} — "
                f"skipping this candle, will keep scanning."
            )
            send_telegram(
                f"\u26a0\ufe0f SIGNAL SKIPPED (SL too wide)\n"
                f"Candle High={entry_high:.2f} Close={entry_close:.2f}\n"
                f"Diff={entry_high - entry_close:.2f} > max {MAX_SL_POINTS}"
            )
            return   # skip this signal, do NOT disable for the day

        msg = (
            f"📉 STRANGLE ENTRY\n"
            f"Entry Close  = {entry_close:.2f}\n"
            f"SL (premium) = {sl_premium:.2f}\n"
            f"EMA5={ema5:.2f} | EMA20={ema20:.2f}\n"
            f"Time: {entry_candle['time']}"
        )
        logger.info(f"[ENTRY] entry_close={entry_close:.2f} sl_premium={sl_premium:.2f}")
        send_telegram(msg)

        # ---- ORDER SEQUENCE: Hedges first, then sell main ----
        logger.info("[ORDER] Buying CE hedge")
        self.fyers.buy_market(self.ce_hedge, "CEHEDGEBUY")

        logger.info("[ORDER] Buying PE hedge")
        self.fyers.buy_market(self.pe_hedge, "PEHEDGEBUY")

        logger.info("[ORDER] Selling CE main")
        self.fyers.sell_market(self.ce, "STRANGLECE")

        logger.info("[ORDER] Selling PE main")
        self.fyers.sell_market(self.pe, "STRANGLEPE")

        self.position = {
            "entry_close": entry_close,
            "sl_premium": sl_premium,
        }
        self.consecutive_green_above_ema = 0

    def _exit_trade(self, reason):
        logger.info(f"[EXIT] Reason: {reason}")
        send_telegram(f"🛑 STRANGLE EXIT\nReason: {reason}")

        # ---- ORDER SEQUENCE: Buy main first, then sell hedges ----
        logger.info("[ORDER] Buying CE main (exit)")
        self.fyers.buy_market(self.ce, "EXITCE")

        logger.info("[ORDER] Buying PE main (exit)")
        self.fyers.buy_market(self.pe, "EXITPE")

        logger.info("[ORDER] Selling CE hedge (exit)")
        self.fyers.sell_market(self.ce_hedge, "CEHEDGESELL")

        logger.info("[ORDER] Selling PE hedge (exit)")
        self.fyers.sell_market(self.pe_hedge, "PEHEDGESELL")

        self.disabled_for_day = True
        self.position = None
        self.consecutive_green_above_ema = 0

    # ----------------------------------------------------------
    def on_candle(self, candle, closed, live_premium=None):
        """
        closed=True  -> bar has closed, run entry/exit logic on completed candle
        closed=False -> bar still forming, only check live SL
        live_premium -> current combined premium (for SL check on live ticks)
        """
        if self.disabled_for_day:
            return

        # ---- Live SL check (tick-level) ----
        if self.position and live_premium is not None:
            if live_premium >= self.position["sl_premium"]:
                self._exit_trade(
                    f"SL HIT (live premium {live_premium:.2f} >= sl {self.position['sl_premium']:.2f})"
                )
                return

        if not closed:
            return

        # ---- From here: only closed candles ----
        self.candles.append(candle)

        if len(self.candles) < MIN_BARS_FOR_EMA:
            return

        ema5, ema20 = self.get_emas()
        if ema5 is None or ema20 is None:
            return

        # EMA log (once per closed candle)
        if candle["time"] != self.last_logged_ema_time:
            logger.info(
                f"[EMA] {candle['time']} | "
                f"close={candle['close']:.2f} | "
                f"EMA5={ema5:.2f} EMA20={ema20:.2f} GAP={ema20 - ema5:.2f}"
            )
            self.last_logged_ema_time = candle["time"]

        # ---- EXIT condition 2: two consecutive green candles above EMA20 ----
        if self.position:
            if candle["close"] > candle["open"] and candle["close"] > ema20:
                self.consecutive_green_above_ema += 1
                logger.info(
                    f"[GREEN ABOVE EMA20] Count={self.consecutive_green_above_ema} "
                    f"close={candle['close']:.2f} EMA20={ema20:.2f}"
                )
                if self.consecutive_green_above_ema >= 2:
                    self._exit_trade("2 consecutive green candles closed above EMA20")
                    return
            else:
                # Reset counter if condition breaks
                self.consecutive_green_above_ema = 0

        # ---- ENTRY LOGIC ----
        if self.position:
            return   # already in trade, no new entry

        # Need at least 2 candles to check previous candle
        if len(self.candles) < 2:
            return

        candle_list  = list(self.candles)
        prev_candle  = candle_list[-2]   # second-to-last (previous closed candle)
        curr_candle  = candle_list[-1]   # latest closed candle (= current `candle`)

        # Condition A: EMA5 < EMA20
        if ema5 >= ema20:
            return

        # Condition B: Previous candle is GREEN
        is_prev_green = prev_candle["close"] > prev_candle["open"]
        if not is_prev_green:
            return

        # Condition C: Current candle is RED, touches EMA20 (high >= EMA20), closes below EMA20
        is_curr_red           = curr_candle["close"] < curr_candle["open"]
        touches_ema20         = curr_candle["high"] >= ema20
        closes_below_ema20    = curr_candle["close"] < ema20

        if is_curr_red and touches_ema20 and closes_below_ema20:
            logger.info(
                f"[ENTRY SIGNAL] "
                f"prev={prev_candle['time']} green | "
                f"curr={curr_candle['time']} red touches+closes below EMA20"
            )
            self._enter_trade(curr_candle, ema5, ema20)


# =========================================================
# MAIN
# =========================================================
if __name__ == "__main__":
    logger.info("[BOOT] NIFTY STRANGLE EMA STRATEGY STARTED (REVISED)")
    send_telegram("🚀 NIFTY STRANGLE EMA STRATEGY STARTED (REVISED)")

    fyers = FyersClient()

    # Wait until ATM decision time
    while datetime.datetime.now().time() < ATM_DECISION_TIME:
        time.sleep(1)

    # Get symbols — spot price fetched inside
    ce, pe, ce_hedge, pe_hedge, atm = get_nifty_strangle_symbols(fyers.client)
    engine = StrategyEngine(fyers, ce, pe, ce_hedge, pe_hedge)

    # Prefill historical candles (using CE+PE main strikes)
    for c in fetch_historical_premium(fyers, ce, pe):
        engine.candles.append(c)

    # EMA boot log
    if len(engine.candles) >= MIN_BARS_FOR_EMA:
        ema5, ema20 = engine.get_emas()
        if ema5 and ema20:
            logger.info(
                f"[EMA BOOT] EMA5={ema5:.2f} EMA20={ema20:.2f} GAP={ema20 - ema5:.2f}"
            )
            send_telegram(
                f"📊 EMA READY (BOOT)\n"
                f"EMA5={ema5:.2f}\n"
                f"EMA20={ema20:.2f}\n"
                f"GAP={ema20 - ema5:.2f}"
            )

    # ========= WEBSOCKET =========
    last = {}       # latest LTP per symbol
    candle = None   # current forming candle (combined premium)

    # We also need hedge LTPs to subscribe, but hedges are only traded, not charted.
    # Subscribe to main CE + PE for charting; hedges subscribed for confirmation only.
    SUBSCRIBED_SYMBOLS = [ce, pe]

    def extract_tick_epoch(msg):
        ts = msg.get("last_traded_time") or msg.get("timestamp") or msg.get("tt")
        return int(ts // 1000) if ts and ts > 10_000_000_000 else int(ts)

    def on_tick(msg):
        global candle

        if "symbol" not in msg or "ltp" not in msg:
            return

        sym = msg["symbol"]
        if sym not in (ce, pe):
            return

        last[sym] = msg["ltp"]
        if ce not in last or pe not in last:
            return

        now = datetime.datetime.now()
        if now.time() >= TRADING_END:
            return

        premium = last[ce] + last[pe]
        epoch   = extract_tick_epoch(msg)
        dt      = datetime.datetime.fromtimestamp(epoch)
        bucket  = dt.replace(second=0, microsecond=0, minute=(dt.minute // 3) * 3)

        if candle is None or candle["time"] != bucket:
            if candle:
                # Candle just closed — pass it with closed=True
                engine.on_candle(candle, closed=True, live_premium=None)
            # Start new candle
            candle = {
                "time":  bucket,
                "open":  premium,
                "high":  premium,
                "low":   premium,
                "close": premium
            }
        else:
            candle["high"]  = max(candle["high"], premium)
            candle["low"]   = min(candle["low"], premium)
            candle["close"] = premium

        # Live tick — pass live premium for SL check
        engine.on_candle(candle, closed=False, live_premium=premium)

    def on_open():
        ws.subscribe(symbols=SUBSCRIBED_SYMBOLS, data_type="SymbolUpdate")
        ws.keep_running()

    ws = data_ws.FyersDataSocket(
        access_token=fyers.auth,
        on_connect=on_open,
        on_message=on_tick,
        log_path=""
    )
    ws.connect()
