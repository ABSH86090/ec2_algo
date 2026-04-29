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

CLIENT_ID        = os.getenv("FYERS_CLIENT_ID")
ACCESS_TOKEN     = os.getenv("FYERS_ACCESS_TOKEN")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID")

LOT_SIZE         = 65
HEDGE_OFFSET     = 1000          # Points away from ATM for hedges

ATM_DECISION_TIME = datetime.time(9, 20)
ENTRY_CUTOFF      = datetime.time(10, 0)   # No new entries at or after 10:00 AM
TRADING_END       = datetime.time(15, 0)   # Force-exit at 3:00 PM

HISTORY_RESOLUTION = "5"

EMA_FAST         = 5
EMA_SLOW         = 20
MIN_BARS_FOR_EMA = 25
HIST_PREFILL_BARS = 80

MAX_SL_POINTS    = 25            # SL = entry premium + 25 (combined CE+PE)

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
    is_tuesday   = d.weekday() == 1
    is_last_week = (d + datetime.timedelta(days=7)).month != d.month

    if is_tuesday and is_last_week:
        return not (d in holidays)

    if d.weekday() == 0:
        next_day = d + datetime.timedelta(days=1)
        is_last_week_tuesday = (next_day + datetime.timedelta(days=7)).month != next_day.month
        if next_day in holidays and is_last_week_tuesday:
            return True

    return False


def get_next_expiry():
    today = datetime.date.today()
    days  = (1 - today.weekday()) % 7
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
    if spot is None:
        r    = fyers.quotes({"symbols": "NSE:NIFTY50-INDEX"})
        spot = float(r["d"][0]["v"]["lp"])

    atm = round(spot / 50) * 50

    ce_strike       = atm
    pe_strike       = atm
    ce_hedge_strike = atm + HEDGE_OFFSET
    pe_hedge_strike = atm - HEDGE_OFFSET

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
            "symbol":      symbol,
            "qty":         LOT_SIZE,
            "type":        2,
            "side":        -1,
            "productType": "INTRADAY",
            "validity":    "DAY",
            "orderTag":    tag
        })

    def buy_market(self, symbol, tag):
        return self.client.place_order({
            "symbol":      symbol,
            "qty":         LOT_SIZE,
            "type":        2,
            "side":        1,
            "productType": "INTRADAY",
            "validity":    "DAY",
            "orderTag":    tag
        })


# =========================================================
# HISTORICAL PREFILL
# =========================================================
def fetch_historical_premium(fyers, ce, pe):
    to_dt   = datetime.datetime.now()
    from_dt = to_dt - datetime.timedelta(days=7)

    def fetch(symbol):
        r = fyers.client.history({
            "symbol":     symbol,
            "resolution": HISTORY_RESOLUTION,
            "date_format": "1",
            "range_from": from_dt.strftime("%Y-%m-%d"),
            "range_to":   to_dt.strftime("%Y-%m-%d"),
            "cont_flag":  "1"
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
            "time":  ts,
            "open":  c1[1] + c2[1],
            "high":  c1[2] + c2[2],
            "low":   c1[3] + c2[3],
            "close": c1[4] + c2[4]
        })

    logger.info(f"[PREFILL] Loaded {len(candles)} candles")
    return candles


# =========================================================
# STRATEGY ENGINE
# =========================================================
class StrategyEngine:
    """
    Strategy Rules
    ==============

    PRE-CONDITION (day-level gate, checked on 9:20 candle close):
    ---------------------------------------------------------------
    The 9:20 AM candle (first 3-min candle of the session) must be:
      - RED  (close < open)
      - Close below EMA20
    If this condition fails → disabled_for_day = True, no trades today.

    ENTRY (after 9:20 gate passes):
    --------------------------------
    Wait for a RED candle (on close) that:
      - Closes BELOW the low of the 9:20 candle's combined premium
      - Candle closes before 10:00 AM (entry_cutoff)
    Entry = candle close premium.
    SL    = entry_premium + MAX_SL_POINTS (25 points combined)

    ORDER SEQUENCE on entry:
      1. Buy CE hedge
      2. Buy PE hedge
      3. Sell CE main
      4. Sell PE main

    EXIT CONDITIONS (whichever hits first):
    ----------------------------------------
    1. Live premium >= sl_premium  (SL hit)
    2. Two consecutive CLOSED green candles with close > EMA20
    3. Time >= 3:00 PM (EOD force-exit)

    ORDER SEQUENCE on exit:
      1. Buy CE main
      2. Buy PE main
      3. Sell CE hedge
      4. Sell PE hedge

    Only ONE trade per day. No re-entry after exit.
    """

    def __init__(self, fyers, ce, pe, ce_hedge, pe_hedge):
        self.fyers    = fyers
        self.ce       = ce
        self.pe       = pe
        self.ce_hedge = ce_hedge
        self.pe_hedge = pe_hedge

        self.candles   = deque(maxlen=2000)
        self.position  = None       # dict with entry/sl details when in trade

        # --- Day-level state ---
        self.disabled_for_day          = False   # True after any exit or failed gate
        self.gate_checked              = False   # True once 9:20 candle has been evaluated
        self.gate_passed               = False   # True if 9:20 candle passed the pre-condition
        self.straddle_920_low          = None    # Low of the 9:20 combined premium candle
        self.consecutive_green_above_ema = 0     # Counter for exit condition 2

        self.last_logged_ema_time      = None

    # ----------------------------------------------------------
    def compute_ema_series(self, period):
        """Full EMA series seeded with SMA, returns latest value."""
        closes = [c["close"] for c in self.candles]
        if len(closes) < period:
            return None
        k   = 2 / (period + 1)
        ema = sum(closes[:period]) / period
        for close in closes[period:]:
            ema = round(close * k + ema * (1 - k), 2)
        return ema

    def get_emas(self):
        ema5  = self.compute_ema_series(EMA_FAST)
        ema20 = self.compute_ema_series(EMA_SLOW)
        return ema5, ema20

    # ----------------------------------------------------------
    def _enter_trade(self, entry_candle, ema5, ema20):
        entry_premium = entry_candle["close"]
        sl_premium    = entry_premium + MAX_SL_POINTS

        msg = (
            f"📉 STRANGLE ENTRY\n"
            f"Entry Premium = {entry_premium:.2f}\n"
            f"SL (premium)  = {sl_premium:.2f}  (+{MAX_SL_POINTS} pts)\n"
            f"EMA5={ema5:.2f} | EMA20={ema20:.2f}\n"
            f"Time: {entry_candle['time']}"
        )
        logger.info(
            f"[ENTRY] entry_premium={entry_premium:.2f} sl_premium={sl_premium:.2f} "
            f"EMA5={ema5:.2f} EMA20={ema20:.2f}"
        )
        send_telegram(msg)

        # ORDER SEQUENCE: Hedges first, then sell main
        logger.info("[ORDER] Buying CE hedge")
        self.fyers.buy_market(self.ce_hedge, "CEHEDGEBUY")

        logger.info("[ORDER] Buying PE hedge")
        self.fyers.buy_market(self.pe_hedge, "PEHEDGEBUY")

        logger.info("[ORDER] Selling CE main")
        self.fyers.sell_market(self.ce, "STRANGLECE")

        logger.info("[ORDER] Selling PE main")
        self.fyers.sell_market(self.pe, "STRANGLEPE")

        self.position = {
            "entry_premium": entry_premium,
            "sl_premium":    sl_premium,
        }
        self.consecutive_green_above_ema = 0

    def _exit_trade(self, reason):
        logger.info(f"[EXIT] Reason: {reason}")
        send_telegram(f"🛑 STRANGLE EXIT\nReason: {reason}")

        # ORDER SEQUENCE: Buy main first, then sell hedges
        logger.info("[ORDER] Buying CE main (exit)")
        self.fyers.buy_market(self.ce, "EXITCE")

        logger.info("[ORDER] Buying PE main (exit)")
        self.fyers.buy_market(self.pe, "EXITPE")

        logger.info("[ORDER] Selling CE hedge (exit)")
        self.fyers.sell_market(self.ce_hedge, "CEHEDGESELL")

        logger.info("[ORDER] Selling PE hedge (exit)")
        self.fyers.sell_market(self.pe_hedge, "PEHEDGESELL")

        self.disabled_for_day            = True
        self.position                    = None
        self.consecutive_green_above_ema = 0

    # ----------------------------------------------------------
    def _check_920_gate(self, candle, ema20):
        """
        Called exactly once, when the 9:20 candle closes.
        Gate passes if candle is RED and closes below EMA20.
        Stores straddle_920_low for use in entry condition.
        """
        self.gate_checked = True

        is_red           = candle["close"] < candle["open"]
        below_ema20      = candle["close"] < ema20

        if is_red and below_ema20:
            self.gate_passed     = True
            self.straddle_920_low = candle["low"]
            logger.info(
                f"[9:20 GATE] PASSED — red candle closes below EMA20. "
                f"920_low={self.straddle_920_low:.2f} EMA20={ema20:.2f}"
            )
            send_telegram(
                f"✅ 9:20 GATE PASSED\n"
                f"Candle: O={candle['open']:.2f} H={candle['high']:.2f} "
                f"L={candle['low']:.2f} C={candle['close']:.2f}\n"
                f"EMA20={ema20:.2f}\n"
                f"920 Low (trigger level) = {self.straddle_920_low:.2f}"
            )
        else:
            self.gate_passed    = False
            self.disabled_for_day = True
            reason = []
            if not is_red:
                reason.append("candle not RED")
            if not below_ema20:
                reason.append("close NOT below EMA20")
            logger.info(
                f"[9:20 GATE] FAILED ({', '.join(reason)}) — skipping today."
            )
            send_telegram(
                f"❌ 9:20 GATE FAILED ({', '.join(reason)})\n"
                f"No trades today."
            )

    # ----------------------------------------------------------
    def on_candle(self, candle, closed, live_premium=None):
        """
        closed=True  → bar has closed; run gate / entry / exit logic
        closed=False → bar still forming; only check live SL
        live_premium → current combined premium for live SL check
        """
        now = datetime.datetime.now()

        # ---- EOD force-exit (live tick level) ----
        if self.position and now.time() >= TRADING_END:
            self._exit_trade("EOD 3:00 PM force-exit")
            return

        # ---- Live SL check (tick-level) ----
        if self.position and live_premium is not None:
            if live_premium >= self.position["sl_premium"]:
                self._exit_trade(
                    f"SL HIT (live premium {live_premium:.2f} "
                    f">= sl {self.position['sl_premium']:.2f})"
                )
                return

        if not closed:
            return

        # ---- From here: closed candles only ----
        if self.disabled_for_day:
            return

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

        # ---- 9:20 GATE CHECK (run exactly once on 9:20 candle close) ----
        # The 9:20 candle bucket is 09:18 (since 3-min buckets: 09:15, 09:18, 09:21…)
        # Actually for 3-min bars starting at 09:15:
        #   09:15 candle closes at 09:18, 09:18 closes at 09:21 — so the bar whose
        #   bucket time == 09:18 is the one that covers 09:15-09:18 (first bar).
        #   The bar that STARTS at 09:18 covers 09:18-09:21 and is the "9:20 candle".
        # We identify the 9:20 candle as the bar with bucket time 09:18
        # (its tick range is 09:18:00–09:20:59, closing at 09:21).
        # More simply: we mark the gate when we see the first candle whose
        # time.minute is 18 (bucket 09:18) — this is the bar active at 9:20.
        if not self.gate_checked:
            c_time = candle["time"]
            # The candle bucket that contains 9:20 AM:
            # 5-min buckets: 09:15, 09:20, 09:25 ...
            # 09:20 bucket spans 09:20:00 to 09:24:59 → this is "the 9:20 candle"
            is_920_candle = (
                c_time.hour == 9 and c_time.minute == 20
            )
            if is_920_candle:
                self._check_920_gate(candle, ema20)
                return   # Don't attempt entry on the gate candle itself

            # If we've somehow passed 09:21 without seeing the 09:18 candle,
            # mark gate as failed to be safe.
            if c_time.hour > 9 or (c_time.hour == 9 and c_time.minute > 20):
                logger.warning("[9:20 GATE] 9:20 candle never seen — disabling for day.")
                send_telegram("⚠️ 9:20 candle not observed — no trades today.")
                self.gate_checked     = True
                self.gate_passed      = False
                self.disabled_for_day = True
            return   # Never enter on or before the gate candle

        if not self.gate_passed:
            return   # Gate failed earlier; already disabled

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
                self.consecutive_green_above_ema = 0

        # ---- ENTRY LOGIC ----
        if self.position:
            return   # Already in trade; one entry per day only

        # Entry window: only before 10:00 AM
        if candle["time"].time() >= ENTRY_CUTOFF:
            logger.info(
                f"[ENTRY SKIPPED] After 10:00 AM cutoff — "
                f"candle time {candle['time']}, no more entries today."
            )
            send_telegram("⏰ 10:00 AM cutoff reached — no entry taken today.")
            self.disabled_for_day = True
            return

        # Entry candle must be RED
        is_red = candle["close"] < candle["open"]
        if not is_red:
            return

        # Entry candle must close BELOW the low of the 9:20 candle
        if candle["close"] >= self.straddle_920_low:
            logger.info(
                f"[ENTRY SCAN] Red candle at {candle['time']} but "
                f"close={candle['close']:.2f} >= 920_low={self.straddle_920_low:.2f} "
                f"— not below 9:20 low, skipping."
            )
            return

        # All conditions met → enter
        logger.info(
            f"[ENTRY SIGNAL] Red candle close {candle['close']:.2f} "
            f"< 9:20 low {self.straddle_920_low:.2f} at {candle['time']}"
        )
        self._enter_trade(candle, ema5, ema20)


# =========================================================
# MAIN
# =========================================================
if __name__ == "__main__":
    logger.info("[BOOT] NIFTY STRANGLE EMA STRATEGY STARTED (REVISED v2)")
    send_telegram("🚀 NIFTY STRANGLE EMA STRATEGY STARTED (REVISED v2)")

    fyers = FyersClient()

    # Wait until ATM decision time (9:20 AM)
    while datetime.datetime.now().time() < ATM_DECISION_TIME:
        time.sleep(1)

    # Get symbols — spot price fetched inside
    ce, pe, ce_hedge, pe_hedge, atm = get_nifty_strangle_symbols(fyers.client)
    engine = StrategyEngine(fyers, ce, pe, ce_hedge, pe_hedge)

    # Prefill historical candles for EMA warm-up (previous sessions only)
    hist = fetch_historical_premium(fyers, ce, pe)
    # Only load candles from BEFORE today so today's gate candle is seen live
    today = datetime.date.today()
    for c in hist:
        if c["time"].date() < today:
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
    last   = {}      # latest LTP per symbol
    candle = None    # current forming candle (combined premium)

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

        # Feed EOD exit at tick level too (belt-and-suspenders)
        if now.time() >= TRADING_END:
            if engine.position:
                engine.on_candle(candle or {}, closed=False, live_premium=last[ce] + last[pe])
            return

        premium = last[ce] + last[pe]
        epoch   = extract_tick_epoch(msg)
        dt      = datetime.datetime.fromtimestamp(epoch)
        bucket  = dt.replace(second=0, microsecond=0, minute=(dt.minute // 5) * 5)

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

        # Live tick — pass live premium for SL check only
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
