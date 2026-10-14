"""
NIFTY / SENSEX ATM EMA21 OPTION-SELLING STRATEGY
================================================
5-minute candles. EMA21 is computed on the INDIVIDUAL option premium of the
ATM CE and the ATM PE. The two legs run as fully independent single-leg
SELL strategies.

━━━ STRIKE SELECTION ━━━
  Index by weekday: Mon/Tue/Fri = NIFTY, Wed/Thu = SENSEX (DAY_TO_INDEX).
  Uses the CLOSE of that index's 9:15 5-min candle (closes at 9:20 AM).
  ATM = round(close_920 / step) * step (50 NIFTY, 100 SENSEX) → ATM CE & PE.

━━━ SCENARIO DETECTION (per leg) ━━━
  Look at the option's own 9:15–9:20 candle ("first candle"):
    • close > EMA21 (GREEN or RED)            → Scenario 1
    • RED   (close < open) AND close < EMA21  → Scenario 2
    • anything else                           → no trade on that leg today
  EMA21 uses previous days' 5-min candles too (continuous, like a chart),
  since 21 candles of today's data don't exist at 9:20.

━━━ SCENARIO 1 ━━━
  Entry  : a later RED candle that closes below EMA21 → SELL at market.
  SL     : the option's day high (as of the entry candle), checked every tick.
  Exit   : SL hit, or 3:00 PM force-exit.
  Trades : up to S1_MAX_TRADES per leg (re-entry needs a fresh red candle
           closing below EMA21 after the previous trade has exited).

━━━ SCENARIO 2 ━━━
  A) Breakdown [S2B] — entry candle must CLOSE by 9:45 AM
     Entry : RED candle closing below the first candle's LOW
             (S2_BREAK_LEVEL = "close" to use the first candle's close).
     SL    : high of the entry candle (every tick).
     Exit  : SL, OR a 5-min candle closes above EMA21, OR 3:00 PM.

  B) Pin-bar re-entry [S2P] — only after A
     If A's SL is hit on the very next candle AND that candle closes as a
     RED PIN BAR (red, open in the bottom PIN_BAR_OPEN_MAX_FRAC = 50% of
     its high-low range), SELL again at its close.
     SL    : high of the pin-bar candle.   Exit : SL or 3:00 PM.

  C) Fallback [S2F] — only if A gave no entry by 9:45 AM
     Wait for a GREEN candle that OPENS below EMA21 and CLOSES above it.
     If, within the next 5 candles, a RED candle OPENS above EMA21 and
     CLOSES below it, SELL.
     SL    : high of that red candle.      Exit : SL or 3:00 PM.
     If 5 candles pass, wait for a new such green candle.
     A newer such green candle restarts the 5-candle window.
     Only 1 fallback trade.

━━━ HEDGE (expiry day only) ━━━
  Expiry: NIFTY = Tuesday, SENSEX = Thursday; moved to the previous trading
  day when that day is a holiday.
  On expiry day, before each SELL, BUY a same-type option HEDGE_DISTANCE
  (300) points further OTM:  CE hedge = ATM + 300 CE,  PE hedge = ATM − 300 PE.
  If the hedge order fails, the short is NOT placed.
  On exit the short is bought back first, then the hedge is sold.
  On non-expiry days no hedge is bought.
"""

import datetime
import logging
import os
import sys
import threading
import time
from collections import deque

import requests
from dotenv import load_dotenv
from fyers_apiv3 import fyersModel
from fyers_apiv3.FyersWebsocket import data_ws

# =========================================================
# CONFIG
# =========================================================
load_dotenv()

CLIENT_ID          = os.getenv("FYERS_CLIENT_ID")
ACCESS_TOKEN       = os.getenv("FYERS_ACCESS_TOKEN")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID")

# ── Which index to trade, by weekday (Mon=0 … Fri=4) ──
DAY_TO_INDEX = {
    0: "NIFTY",    # Monday
    1: "NIFTY",    # Tuesday
    2: "SENSEX",   # Wednesday
    3: "SENSEX",   # Thursday
    4: "NIFTY",    # Friday
}

INDEX_CONFIG = {
    "NIFTY": {
        "index_symbol":   "NSE:NIFTY50-INDEX",
        "option_prefix":  "NSE:NIFTY",
        "strike_step":    50,
        "lot_size":       65,     # qty sent to the API
        "expiry_weekday": 1,      # Tuesday
        "hedge_distance": 300,
    },
    "SENSEX": {
        "index_symbol":   "BSE:SENSEX-INDEX",
        "option_prefix":  "BSE:SENSEX",
        "strike_step":    100,
        "lot_size":       20,     # qty sent to the API
        "expiry_weekday": 3,      # Thursday
        "hedge_distance": 300,
    },
}

_today_wd = datetime.date.today().weekday()
if _today_wd not in DAY_TO_INDEX:
    print("Weekend — no index configured for today. Exiting.")
    sys.exit(0)

INDEX_NAME     = DAY_TO_INDEX[_today_wd]
CFG            = INDEX_CONFIG[INDEX_NAME]
INDEX_SYMBOL   = CFG["index_symbol"]
OPTION_PREFIX  = CFG["option_prefix"]
STRIKE_STEP    = CFG["strike_step"]
LOT_SIZE       = CFG["lot_size"]
EXPIRY_WEEKDAY = CFG["expiry_weekday"]
HEDGE_DISTANCE = CFG["hedge_distance"]

DECISION_TIME   = datetime.time(9, 20, 5)  # just after 9:20 candle close
FIRST_CANDLE    = datetime.time(9, 15)     # 5-min candle 9:15–9:20
TRADING_END     = datetime.time(15, 0)     # 3:00 PM EOD force-exit

HISTORY_RESOLUTION = "5"
CANDLE_MINUTES     = 5

EMA_PERIOD = 21

S1_MAX_TRADES   = 2        # Scenario 1 trades per leg per day
S2_BREAK_LEVEL  = "low"    # "low" or "close" of the first candle
S2_BREAKDOWN_DEADLINE = datetime.time(9, 45)   # breakdown entry candle must CLOSE by this
S2_FALLBACK_WINDOW    = 5                      # red candle must come within N candles of green
PIN_BAR_OPEN_MAX_FRAC = 0.5   # red pin bar: open within the bottom 50% of the high-low range


LOG_FILE = "atm_ema21_option_sell.log"

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
# EXPIRY / SYMBOL UTILS
# =========================================================
SPECIAL_MARKET_HOLIDAYS = {          # NSE trading holidays 2026
    datetime.date(2026, 1, 15),      # Maharashtra municipal elections
    datetime.date(2026, 1, 26),      # Republic Day
    datetime.date(2026, 3, 3),       # Holi
    datetime.date(2026, 3, 26),      # Ram Navami
    datetime.date(2026, 3, 31),      # Mahavir Jayanti
    datetime.date(2026, 4, 3),       # Good Friday
    datetime.date(2026, 4, 14),      # Ambedkar Jayanti
    datetime.date(2026, 5, 1),       # Maharashtra Day
    datetime.date(2026, 5, 28),      # Bakri Id
    datetime.date(2026, 6, 26),      # Muharram
    datetime.date(2026, 9, 14),      # Ganesh Chaturthi
    datetime.date(2026, 10, 2),      # Gandhi Jayanti
    datetime.date(2026, 10, 20),     # Dussehra (Tue → expiry Mon 19 Oct)
    datetime.date(2026, 11, 10),     # Diwali Balipratipada (Tue → expiry Mon 9 Nov)
    datetime.date(2026, 11, 24),     # Guru Nanak Jayanti (Tue → expiry Mon 23 Nov)
    datetime.date(2026, 12, 25),     # Christmas
}


def _shift_for_holidays(d):
    """Move an expiry back to the previous trading day if it is a holiday/weekend."""
    while d in SPECIAL_MARKET_HOLIDAYS or d.weekday() >= 5:
        d -= datetime.timedelta(days=1)
    return d


def _expiry_info():
    """
    Returns (actual_expiry_date, is_monthly) for the active index.
    Scheduled expiry = next EXPIRY_WEEKDAY (Tue for NIFTY, Thu for SENSEX);
    if that day is a holiday the expiry moves to the previous trading day.
    Monthly = the scheduled day is the last such weekday of its month.
    """
    today = datetime.date.today()
    scheduled = today + datetime.timedelta(days=(EXPIRY_WEEKDAY - today.weekday()) % 7)
    actual = _shift_for_holidays(scheduled)

    # Already past this week's (possibly shifted) expiry → next week
    if actual < today or (actual == today and
                          datetime.datetime.now().time() >= datetime.time(15, 30)):
        scheduled += datetime.timedelta(days=7)
        actual = _shift_for_holidays(scheduled)

    is_monthly = (scheduled + datetime.timedelta(days=7)).month != scheduled.month
    return actual, is_monthly


def get_next_expiry():
    return _expiry_info()[0]


def is_expiry_day():
    return datetime.date.today() == get_next_expiry()


def format_expiry():
    expiry, is_monthly = _expiry_info()
    yy = expiry.strftime("%y")
    if is_monthly:
        return f"{yy}{expiry.strftime('%b').upper()}"
    m, d  = expiry.month, expiry.day
    m_tok = {10: "O", 11: "N", 12: "D"}.get(m, str(m))
    return f"{yy}{m_tok}{d:02d}"


def build_symbol(strike, opt_type):
    return f"{OPTION_PREFIX}{format_expiry()}{strike}{opt_type}"


# =========================================================
# 9:20 INDEX CANDLE CLOSE
# =========================================================
def get_920_index_close(fyers_client, max_wait_sec=90):
    today = datetime.date.today()
    deadline = time.time() + max_wait_sec

    while time.time() < deadline:
        try:
            r = fyers_client.history({
                "symbol":      INDEX_SYMBOL,
                "resolution":  HISTORY_RESOLUTION,
                "date_format": "1",
                "range_from":  today.strftime("%Y-%m-%d"),
                "range_to":    today.strftime("%Y-%m-%d"),
                "cont_flag":   "1",
            })
            for c in r.get("candles", []):
                dt = datetime.datetime.fromtimestamp(c[0])
                if dt.date() == today and dt.time() == FIRST_CANDLE:
                    close = c[4]
                    logger.info(f"[SPOT] {INDEX_NAME} 9:20 candle close = {close:.2f}")
                    return close
        except Exception as e:
            logger.warning(f"[SPOT] history fetch failed: {e}")
        time.sleep(3)

    r = fyers_client.quotes({"symbols": INDEX_SYMBOL})
    try:
        ltp = r["d"][0]["v"]["lp"]
    except (KeyError, IndexError, TypeError) as e:
        raise RuntimeError(f"Could not read spot from quotes response: {r}") from e
    logger.warning(f"[SPOT] 9:20 candle not available — using live LTP {ltp:.2f}")
    send_telegram(f"⚠️ 9:20 index candle not available, using live LTP {ltp:.2f}")
    return ltp


# =========================================================
# FYERS CLIENT
# =========================================================
def order_ok(resp):
    return isinstance(resp, dict) and resp.get("s") == "ok"


class FyersClient:
    def __init__(self):
        self.client = fyersModel.FyersModel(
            client_id=CLIENT_ID,
            token=ACCESS_TOKEN,
            is_async=False,
            log_path=""
        )
        self.auth = f"{CLIENT_ID}:{ACCESS_TOKEN}"

    def _order(self, symbol, side, tag):
        try:
            resp = self.client.place_order({
                "symbol":      symbol,
                "qty":         LOT_SIZE,
                "type":        2,          # market
                "side":        side,       # 1 = buy, -1 = sell
                "productType": "INTRADAY",
                "validity":    "DAY",
                "orderTag":    tag,
            })
            logger.info(f"[ORDER] {symbol} side={side} tag={tag} resp={resp}")
            if not order_ok(resp):
                send_telegram(f"❗ ORDER REJECTED {symbol} side={side}: {resp}")
            return resp
        except Exception as e:
            logger.error(f"[ORDER ERROR] {symbol} side={side} tag={tag}: {e}")
            send_telegram(f"❗ ORDER ERROR {symbol} side={side}: {e}")
            return None

    def buy_market(self, symbol, tag):
        return self._order(symbol, 1, tag)

    def sell_market(self, symbol, tag):
        return self._order(symbol, -1, tag)


# =========================================================
# HISTORICAL PREFILL
# =========================================================
def fetch_historical_candles(fyers_client, symbol, wait_for_first=True, max_wait_sec=90):
    """
    5-min candles for one option over the past 7 days. Retries briefly until
    today's 9:15 candle shows up (it can lag a few seconds after 9:20).
    """
    today    = datetime.date.today()
    deadline = time.time() + max_wait_sec
    candles  = []
    r        = None

    while True:
        to_dt   = datetime.datetime.now()
        from_dt = to_dt - datetime.timedelta(days=7)
        try:
            r = fyers_client.history({
                "symbol":      symbol,
                "resolution":  HISTORY_RESOLUTION,
                "date_format": "1",
                "range_from":  from_dt.strftime("%Y-%m-%d"),
                "range_to":    to_dt.strftime("%Y-%m-%d"),
                "cont_flag":   "1",
            })
            raw = r.get("candles", []) or []
        except Exception as e:
            logger.warning(f"[PREFILL] {symbol} history fetch failed: {e}")
            raw = []

        candles = []
        for c in raw:
            dt = datetime.datetime.fromtimestamp(c[0])
            if dt.time() < datetime.time(9, 15) or dt.time() > datetime.time(15, 30):
                continue
            candles.append({"time": dt, "open": c[1], "high": c[2],
                            "low": c[3], "close": c[4]})
        candles.sort(key=lambda c: c["time"])

        has_first = any(c["time"].date() == today and c["time"].time() == FIRST_CANDLE
                        for c in candles)
        if has_first or not wait_for_first or time.time() >= deadline:
            break
        time.sleep(3)

    today_c = [c for c in candles if c["time"].date() == today]
    logger.info(
        f"[PREFILL] {symbol}: {len(candles)} candles loaded "
        f"({len(today_c)} today"
        + (f", {today_c[0]['time']:%H:%M}–{today_c[-1]['time']:%H:%M}" if today_c else "")
        + ")"
    )

    if not candles:
        reason = (f"Fyers returned NO data for {symbol} — symbol probably wrong "
                  f"(check expiry/holiday list). API response: {str(r)[:300]}")
    elif not today_c:
        reason = f"history has old candles but none for today for {symbol}"
    else:
        reason = (f"today's candles start at {today_c[0]['time']:%H:%M}, "
                  f"no 9:15 candle for {symbol}")
    return candles, reason


# =========================================================
# EMA HELPER
# =========================================================
def compute_ema(closes, period):
    if len(closes) < period:
        return None
    k   = 2 / (period + 1)
    ema = sum(closes[:period]) / period
    for close in closes[period:]:
        ema = close * k + ema * (1 - k)
    return ema


# =========================================================
# LEG STRATEGY (one instance per option: ATM CE, ATM PE)
# =========================================================
class LegStrategy:
    """
    Trade kinds (also used in order tags, letters/digits only):
      S1  – Scenario 1 trade
      S2B – Scenario 2 first-candle breakdown
      S2P – Scenario 2 pin-bar re-entry after S2B SL hit on the next candle
      S2F – Scenario 2 fallback (green above EMA21 → red below EMA21 within 5)
    """

    def __init__(self, fyers, symbol, label, hedge_symbol=None):
        self.fyers        = fyers
        self.symbol       = symbol
        self.label        = label          # "CE" or "PE"
        self.hedge_symbol = hedge_symbol   # only set on expiry day

        self.candles   = deque(maxlen=2000)
        self.day_high  = None
        self.today     = datetime.date.today()

        self.scenario     = None   # 1, 2, or 0 (no trade today)
        self.first_candle = None

        self.position     = None
        self.trades_taken = 0      # all trades on this leg (for tag numbering)
        self.s1_trades    = 0
        self.lock         = threading.RLock()

        # Scenario 2 state machine
        #   BREAKDOWN      – waiting for red close below first candle (till 9:45)
        #   BREAKDOWN_OPEN – S2B trade running
        #   PINBAR_WATCH   – S2B stopped out; check if that candle is a red pin bar
        #   FALLBACK       – no breakdown by 9:45; green>EMA21 then red<EMA21 within 5
        #   DONE           – no more Scenario 2 trades today
        self.s2_phase      = None
        self.fb_green      = None    # green candle that armed the fallback window
        self.fb_count      = 0       # candles seen since that green candle
        self.last_sl_exit  = None    # info about the most recent SL exit

        # While replaying today's already-closed candles at startup (e.g. a
        # restart at 10:30) we only rebuild state — no orders are placed on
        # old candles. Live candles turn this off.
        self.replaying    = False

    # ----------------------------------------------------------
    @staticmethod
    def _bucket(dt):
        return dt.replace(second=0, microsecond=0,
                          minute=(dt.minute // CANDLE_MINUTES) * CANDLE_MINUTES)

    @staticmethod
    def _is_red_pin_bar(c):
        """Red candle whose OPEN sits in the lower part of its full high-low range."""
        rng = c["high"] - c["low"]
        if c["close"] >= c["open"] or rng <= 0:
            return False
        return (c["open"] - c["low"]) <= PIN_BAR_OPEN_MAX_FRAC * rng

    def _notify(self, msg):
        logger.info(f"[{self.label}] {msg}")
        if not self.replaying:
            send_telegram(f"ℹ️ {self.label} {self.symbol}\n{msg}")

    # ----------------------------------------------------------
    def _classify_first_candle(self, candle, ema):
        self.first_candle = candle
        is_red = candle["close"] < candle["open"]
        colour = "RED" if is_red else ("GREEN" if candle["close"] > candle["open"] else "DOJI")

        if ema is None:
            self.scenario = 0
            reason = "not enough history for EMA21"
        elif candle["close"] > ema:
            self.scenario = 1
            reason = f"{colour} & closed above EMA21 → wait for red close below EMA21"
        elif is_red and candle["close"] < ema:
            self.scenario = 2
            self.s2_phase = "BREAKDOWN"
            lvl = candle[S2_BREAK_LEVEL]
            reason = (f"RED & closed below EMA21 → wait for red close below "
                      f"first-candle {S2_BREAK_LEVEL} {lvl:.2f} "
                      f"(entry candle must close by {S2_BREAKDOWN_DEADLINE:%H:%M})")
        else:
            self.scenario = 0
            reason = "first candle fits neither scenario → no trade today"

        ema_txt = f"{ema:.2f}" if ema is not None else "n/a"
        logger.info(
            f"[{self.label} FIRST CANDLE] O={candle['open']:.2f} H={candle['high']:.2f} "
            f"L={candle['low']:.2f} C={candle['close']:.2f} EMA21={ema_txt} "
            f"→ scenario {self.scenario}: {reason}"
        )
        send_telegram(
            f"🔎 {self.label} {self.symbol}\n"
            f"9:15 candle O={candle['open']:.2f} C={candle['close']:.2f} EMA21={ema_txt}\n"
            f"Scenario {self.scenario}: {reason}"
        )

    # ----------------------------------------------------------
    def _enter_trade(self, candle, sl, ema, kind, why):
        """Returns True if the short was placed."""
        with self.lock:
            if self.position:
                return False
            entry = candle["close"]
            risk  = sl - entry
            if risk <= 0:
                logger.info(f"[{self.label} SKIP {kind}] SL {sl:.2f} <= entry {entry:.2f}")
                return False

            n   = self.trades_taken + 1
            tag = f"ATM{self.label}{kind}T{n}"

            # Expiry-day hedge first — never sell naked if it fails
            hedge_bought = False
            if self.hedge_symbol:
                resp = self.fyers.buy_market(self.hedge_symbol, f"{tag}H")
                if not order_ok(resp):
                    logger.error(f"[{self.label}] hedge buy failed — short NOT placed")
                    send_telegram(f"❗ {self.label} hedge buy failed — skipping short")
                    return False
                hedge_bought = True

            resp = self.fyers.sell_market(self.symbol, tag)
            if not order_ok(resp):
                logger.error(f"[{self.label}] short sell failed")
                if hedge_bought:
                    self.fyers.sell_market(self.hedge_symbol, f"{tag}HX")
                return False

            self.trades_taken = n
            self.position = {
                "kind":         kind,
                "entry_price":  entry,
                "sl_price":     sl,
                "entry_candle": candle["time"],
                "tag":          tag,
                "hedge":        hedge_bought,
            }

            hedge_txt = f"\nHedge bought: {self.hedge_symbol}" if hedge_bought else ""
            exit_txt  = ("SL / close above EMA21 / 3:00 PM" if kind == "S2B"
                         else "SL / 3:00 PM")
            logger.info(
                f"[{self.label} SELL #{n} {kind}] symbol={self.symbol} "
                f"entry={entry:.2f} sl={sl:.2f} risk={risk:.2f} EMA21={ema:.2f} "
                f"candle={candle['time']:%H:%M} ({why})"
                f"{' hedge=' + self.hedge_symbol if hedge_bought else ''}"
            )
            send_telegram(
                f"📉 {self.label} SELL #{n} [{kind}] — {self.symbol}\n"
                f"Entry = {entry:.2f}\n"
                f"SL    = {sl:.2f} ({why})\n"
                f"EMA21 = {ema:.2f}\n"
                f"Exit  : {exit_txt}\n"
                f"Signal candle: {candle['time']:%H:%M}{hedge_txt}"
            )
            return True

    def _exit_trade(self, reason, price=None, sl_hit=False):
        with self.lock:
            if not self.position:
                return
            pos = self.position
            self.position = None

            pnl_txt = ""
            if price is not None:
                pts = pos["entry_price"] - price      # short P&L
                pnl_txt = f" | approx P&L {pts:+.2f} pts ({pts * LOT_SIZE:+.0f})"

            # Buy back the short first, then release the hedge
            self.fyers.buy_market(self.symbol, f"{pos['tag']}X")
            if pos["hedge"]:
                self.fyers.sell_market(self.hedge_symbol, f"{pos['tag']}HX")

            logger.info(f"[{self.label} EXIT {pos['kind']}] symbol={self.symbol} {reason}{pnl_txt}")
            send_telegram(f"🛑 {self.label} EXIT [{pos['kind']}] — {self.symbol}\n"
                          f"Reason: {reason}{pnl_txt}")

            # Scenario 2 phase after an exit
            if pos["kind"] == "S2B":
                if sl_hit:
                    self.last_sl_exit = {
                        "entry_candle": pos["entry_candle"],
                        "exit_candle":  self._bucket(datetime.datetime.now()),
                    }
                    self.s2_phase = "PINBAR_WATCH"
                else:
                    self.s2_phase = "DONE"
            elif pos["kind"] in ("S2P", "S2F"):
                self.s2_phase = "DONE"

    def force_eod_exit(self):
        if self.position:
            self._exit_trade("EOD 3:00 PM force-exit")

    # ----------------------------------------------------------
    def on_tick(self, ltp):
        """Tick-level SL / EOD checks."""
        if not self.position:
            return
        if datetime.datetime.now().time() >= TRADING_END:
            self._exit_trade("EOD 3:00 PM force-exit", ltp)
            return
        if ltp >= self.position["sl_price"]:
            self._exit_trade(
                f"SL HIT — LTP {ltp:.2f} >= SL {self.position['sl_price']:.2f}",
                ltp, sl_hit=True
            )

    def on_closed_candle(self, candle):
        self.candles.append(candle)
        is_today = candle["time"].date() == self.today
        if is_today:
            self.day_high = candle["high"] if self.day_high is None \
                else max(self.day_high, candle["high"])

        closes = [c["close"] for c in self.candles]
        ema    = compute_ema(closes, EMA_PERIOD)
        t      = candle["time"].time()

        ema_txt = f"{ema:.2f}" if ema is not None else "n/a"
        logger.info(
            f"[{self.label} CANDLE] {candle['time']:%H:%M} "
            f"O={candle['open']:.2f} H={candle['high']:.2f} "
            f"L={candle['low']:.2f} C={candle['close']:.2f} EMA21={ema_txt} "
            f"dayHigh={self.day_high} scenario={self.scenario} s2={self.s2_phase} "
            f"pos={self.position['kind'] if self.position else 'no'} trades={self.trades_taken}"
        )

        if not is_today:
            return

        # First candle decides the scenario
        if t == FIRST_CANDLE:
            self._classify_first_candle(candle, ema)
            return

        if not self.scenario or ema is None:
            return

        # Candle-level EOD guard
        if t >= TRADING_END:
            if self.position:
                self._exit_trade("EOD 3:00 PM (candle-level)", candle["close"])
            return

        close_time = (candle["time"] + datetime.timedelta(minutes=CANDLE_MINUTES)).time()
        # No new trades from replayed candles or from the candle closing at 3:00 PM
        can_enter  = (not self.replaying) and close_time < TRADING_END

        if self.scenario == 1:
            self._scenario1(candle, ema, can_enter)
        else:
            self._scenario2(candle, ema, close_time, can_enter)

    # ----------------------------------------------------------
    def _scenario1(self, candle, ema, can_enter):
        if self.position or self.s1_trades >= S1_MAX_TRADES or not can_enter:
            return
        if candle["close"] < candle["open"] and candle["close"] < ema:
            if self._enter_trade(candle, self.day_high, ema, "S1", "SL = option day high"):
                self.s1_trades += 1

    def _scenario2(self, candle, ema, close_time, can_enter):
        is_red   = candle["close"] < candle["open"]
        is_green = candle["close"] > candle["open"]

        # ── Exit rule that only applies to the breakdown trade ──
        if self.position:
            if self.position["kind"] == "S2B" and candle["close"] > ema:
                self._exit_trade(
                    f"candle closed above EMA21 ({candle['close']:.2f} > {ema:.2f})",
                    candle["close"]
                )
            return

        # ── Pin-bar re-entry check (runs on the candle in which the S2B SL hit) ──
        if self.s2_phase == "PINBAR_WATCH":
            self.s2_phase = "DONE"
            info = self.last_sl_exit or {}
            next_candle = info.get("entry_candle", candle["time"]) + \
                datetime.timedelta(minutes=CANDLE_MINUTES)
            sl_on_next  = (info.get("exit_candle") == next_candle == candle["time"])

            if not sl_on_next:
                self._notify("S2 breakdown SL was not hit on the very next candle "
                             "→ no pin-bar re-entry; Scenario 2 done for today")
                return
            if not self._is_red_pin_bar(candle):
                self._notify(f"SL candle {candle['time']:%H:%M} is not a red pin bar "
                             f"→ no re-entry; Scenario 2 done for today")
                return
            if can_enter:
                self._enter_trade(candle, candle["high"], ema, "S2P",
                                  "red pin bar re-entry, SL = pin bar high")
            return

        # ── First-candle breakdown, entry candle must close by 9:45 ──
        if self.s2_phase == "BREAKDOWN":
            if close_time <= S2_BREAKDOWN_DEADLINE:
                level = self.first_candle[S2_BREAK_LEVEL]
                if is_red and candle["close"] < level and can_enter:
                    if self._enter_trade(candle, candle["high"], ema, "S2B",
                                         "SL = entry candle high"):
                        self.s2_phase = "BREAKDOWN_OPEN"
                        return
            if close_time >= S2_BREAKDOWN_DEADLINE:
                self.s2_phase = "FALLBACK"
                self._notify(f"No breakdown entry by {S2_BREAKDOWN_DEADLINE:%H:%M} → "
                             f"switching to fallback: wait for green candle crossing EMA21 upward, "
                             f"then red candle crossing it downward within {S2_FALLBACK_WINDOW} candles")
            return

        # ── Fallback: green close above EMA21, then red close below EMA21 within 5 ──
        if self.s2_phase == "FALLBACK":
            if self.fb_green is not None:
                self.fb_count += 1
                if is_red and candle["open"] > ema and candle["close"] < ema:
                    if can_enter and self._enter_trade(
                            candle, candle["high"], ema, "S2F",
                            f"red crossed EMA21 down {self.fb_count} candle(s) after green "
                            f"{self.fb_green['time']:%H:%M}, SL = red candle high"):
                        self.s2_phase = "FALLBACK_OPEN"
                        return

            if is_green and candle["open"] < ema and candle["close"] > ema:
                self.fb_green = candle
                self.fb_count = 0
                self._notify(f"Fallback armed: green {candle['time']:%H:%M} crossed EMA21 "
                             f"upward (O {candle['open']:.2f} < {ema:.2f} < C {candle['close']:.2f})"
                             f" → watching next {S2_FALLBACK_WINDOW} candles for a red candle "
                             f"opening above and closing below EMA21")
                return

            if self.fb_green is not None and self.fb_count >= S2_FALLBACK_WINDOW:
                self._notify(f"Fallback window from green {self.fb_green['time']:%H:%M} "
                             f"expired → waiting for a new green candle crossing EMA21 upward")
                self.fb_green = None
                self.fb_count = 0


# =========================================================
# MAIN
# =========================================================
if __name__ == "__main__":
    logger.info(f"[BOOT] ATM EMA21 OPTION-SELLING STRATEGY STARTED — {INDEX_NAME}")
    send_telegram(f"🚀 ATM EMA21 OPTION-SELLING STRATEGY STARTED — {INDEX_NAME}")

    fyers = FyersClient()

    # ── Wait for the 9:20 candle to close ──
    logger.info(f"[WAIT] Waiting until {DECISION_TIME} for the 9:20 index candle...")
    while datetime.datetime.now().time() < DECISION_TIME:
        time.sleep(1)

    # ── ATM strikes ──
    spot = get_920_index_close(fyers.client)
    atm  = round(spot / STRIKE_STEP) * STRIKE_STEP
    ce_symbol = build_symbol(atm, "CE")
    pe_symbol = build_symbol(atm, "PE")

    expiry_today = is_expiry_day()
    ce_hedge = build_symbol(atm + HEDGE_DISTANCE, "CE") if expiry_today else None
    pe_hedge = build_symbol(atm - HEDGE_DISTANCE, "PE") if expiry_today else None

    logger.info(
        f"[STRIKES] spot={spot:.2f} ATM={atm} CE={ce_symbol} PE={pe_symbol} "
        f"expiry_day={expiry_today} CE_hedge={ce_hedge} PE_hedge={pe_hedge}"
    )
    send_telegram(
        f"📌 {INDEX_NAME} STRIKE SELECTION (9:20 candle close)\n"
        f"Expiry = {get_next_expiry():%a %d %b} | Lot qty = {LOT_SIZE}\n"
        f"Spot = {spot:.2f} | ATM = {atm}\n"
        f"CE (sell): {ce_symbol}\nPE (sell): {pe_symbol}\n"
        + (f"Expiry day — hedges:\nCE: {ce_hedge}\nPE: {pe_hedge}"
           if expiry_today else "Not expiry day — no hedge")
    )

    engines = {
        ce_symbol: LegStrategy(fyers, ce_symbol, "CE", ce_hedge),
        pe_symbol: LegStrategy(fyers, pe_symbol, "PE", pe_hedge),
    }
    live_candle = {ce_symbol: None, pe_symbol: None}

    # ── Prefill: past days warm up EMA21; today's closed candles go through
    #    the strategy (first candle → scenario, later ones → signals) ──
    today = datetime.date.today()
    for symbol, engine in engines.items():
        hist, reason = fetch_historical_candles(fyers.client, symbol)
        now = datetime.datetime.now()
        for c in hist:
            if c["time"].date() < today:
                engine.candles.append(c)

        engine.replaying = True
        for c in hist:
            if c["time"].date() != today:
                continue
            if c["time"] + datetime.timedelta(minutes=CANDLE_MINUTES) <= now:
                engine.on_closed_candle(c)
            else:
                live_candle[symbol] = dict(c)   # still-forming candle
        engine.replaying = False

        if engine.scenario is None:
            engine.scenario = 0
            logger.warning(f"[{engine.label}] 9:15 candle not found — no trade today. {reason}")
            send_telegram(f"⚠️ {engine.label} 9:15 candle not found — no trade today.\n{reason}")

    # ── EOD watchdog (in case ticks dry up near 3:00 PM) ──
    def eod_watchdog():
        while datetime.datetime.now().time() < TRADING_END:
            time.sleep(1)
        for e in engines.values():
            e.force_eod_exit()
        logger.info("[EOD] 3:00 PM reached — all positions closed")
        send_telegram("🏁 EOD 3:00 PM — strategy done for the day")

    threading.Thread(target=eod_watchdog, daemon=True).start()

    # ── Websocket for live ticks ──
    def extract_tick_epoch(msg):
        ts = msg.get("last_traded_time") or msg.get("exch_feed_time") or msg.get("timestamp")
        if not ts:
            return int(time.time())
        return int(ts // 1000) if ts > 10_000_000_000 else int(ts)

    def on_tick(msg):
        if "symbol" not in msg or "ltp" not in msg:
            return
        sym = msg["symbol"]
        if sym not in engines:
            return

        engine = engines[sym]
        ltp    = msg["ltp"]

        if datetime.datetime.now().time() >= TRADING_END:
            engine.on_tick(ltp)
            return

        dt     = datetime.datetime.fromtimestamp(extract_tick_epoch(msg))
        bucket = dt.replace(second=0, microsecond=0,
                            minute=(dt.minute // CANDLE_MINUTES) * CANDLE_MINUTES)

        candle = live_candle[sym]
        if candle is None or candle["time"] != bucket:
            if candle is not None and candle["time"] < bucket:
                engine.on_closed_candle(candle)
            candle = {"time": bucket, "open": ltp, "high": ltp, "low": ltp, "close": ltp}
            live_candle[sym] = candle
        else:
            candle["high"]  = max(candle["high"], ltp)
            candle["low"]   = min(candle["low"],  ltp)
            candle["close"] = ltp

        engine.on_tick(ltp)   # SL check

    def on_open():
        ws.subscribe(symbols=list(engines.keys()), data_type="SymbolUpdate")
        ws.keep_running()

    ws = data_ws.FyersDataSocket(
        access_token=fyers.auth,
        on_connect=on_open,
        on_message=on_tick,
        log_path=""
    )
    ws.connect()
