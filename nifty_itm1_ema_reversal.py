"""
NIFTY ATM 62%-RETRACEMENT SELL STRATEGY  (2-min EMA20 confirmation)
=====================================================================
Levels come from the first 30-minute candle (9:15-9:45) on the ATM strike.
Entry is confirmed on 2-minute candles using EMA20 of that SAME option's
premium. CE and PE are checked and traded independently (same ATM strike).

━━━ SETUP (once, at 9:45 AM) ━━━
  1. ATM strike = round(NIFTY50 30-min candle (9:15-9:45) CLOSE / 50) * 50
  2. For EACH of ATM-CE and ATM-PE, independently:
       - Pull that option's own 9:15-9:45 30-min candle.
       - If it is NOT red (close >= open)  → no trade on that leg.
       - If it IS red:
           high, low  = candle high/low
           length     = high - low
           level_62   = low + 0.62 * length     (62% retracement level)
           sl_price   = high                    (stop, above entry)
  3. Prefill 2-min candles for the strike (last few days + today so far)
     so EMA20 is warmed up before live trading starts (same method as the
     ITM1 EMA script).

━━━ ENTRY ━━━
  Step A (arm)    : after 9:45, live LTP must first rise to >= level_62.
  Step B (confirm): after that, the first CLOSED 2-min candle that is
                      - RED (close < open), AND
                      - close < EMA20 (2-min, on the option premium), AND
                      - close < level_62
                    → SELL at market immediately.
  (Set REQUIRE_LEVEL_TOUCH = False to skip step A.)

━━━ SL / TARGET (fixed at entry) ━━━
  sl_price     = 30-min candle high
  risk         = sl_price - actual entry price
  target_price = entry - 2 * risk              (2:1 reward:risk)

━━━ EXIT (only three ways, once in a position) ━━━
  1. Target hit : live LTP <= target_price
  2. SL hit     : live LTP >= sl_price
  3. Safety net : 3:14 PM — force market-exit if still in a position;
                  if entry never triggered, the leg is abandoned for the day.

Only ONE trade per leg (CE / PE) per day.
"""

import datetime
import logging
import os
import sys
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

LOT_SIZE    = 65     # qty sent to the API
STRIKE_STEP = 50

RETRACEMENT         = 0.62   # 62% retracement of the red 30-min candle
REQUIRE_LEVEL_TOUCH = True   # price must reach the 62% level before a 2-min
                             # red candle can trigger the entry
REWARD_RISK         = 2.0    # target = entry - 2 * (SL - entry)

MARKET_OPEN        = datetime.time(9, 15)
MARKET_CLOSE       = datetime.time(15, 30)
FIRST_CANDLE_START = datetime.time(9, 15)
SETUP_WAIT_TIME    = datetime.time(9, 45)   # wait for the 30-min candle to close
SAFETY_TIME        = datetime.time(15, 14)  # EOD safety net

FIRST_CANDLE_RESOLUTION = "30"   # levels from the 30-min candle
EMA_RESOLUTION          = "2"    # EMA20 on 2-min candles
EMA_TF_MINUTES          = 2
EMA_PERIOD              = 20
PREFILL_DAYS            = 7      # calendar days of 2-min history for EMA warm-up

LOG_FILE = "nifty_atm_62_retracement_ema_sell.log"

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
# EXPIRY / SYMBOL UTILS  (unchanged)
# =========================================================
SPECIAL_MARKET_HOLIDAYS = {
    datetime.date(2026, 1, 26), datetime.date(2026, 3, 3), datetime.date(2026, 3, 26),
    datetime.date(2026, 3, 31), datetime.date(2026, 4, 14), datetime.date(2026, 5, 1),
    datetime.date(2026, 5, 28), datetime.date(2026, 6, 26), datetime.date(2026, 9, 14),
    datetime.date(2026, 10, 2), datetime.date(2026, 11, 24), datetime.date(2026, 12, 25),
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
    m, d  = expiry.month, expiry.day
    m_tok = {10: "O", 11: "N", 12: "D"}.get(m, str(m))
    return f"{yy}{m_tok}{d:02d}"


def build_symbol(strike, opt_type):
    expiry = format_expiry(get_next_expiry())
    return f"NSE:NIFTY{expiry}{strike}{opt_type}"


# =========================================================
# CANDLE HELPERS
# =========================================================
def fetch_candles(fyers_client, symbol, resolution, from_date, to_date):
    """Fetch candles for `symbol` between two dates (inclusive), market hours only."""
    r = fyers_client.history({
        "symbol":      symbol,
        "resolution":  resolution,
        "date_format": "1",
        "range_from":  from_date.strftime("%Y-%m-%d"),
        "range_to":    to_date.strftime("%Y-%m-%d"),
        "cont_flag":   "1",
    })
    candles = []
    for c in r.get("candles", []):
        dt = datetime.datetime.fromtimestamp(c[0])
        if dt.time() < MARKET_OPEN or dt.time() >= MARKET_CLOSE:
            continue
        candles.append({"time": dt, "open": c[1], "high": c[2], "low": c[3], "close": c[4]})
    candles.sort(key=lambda c: c["time"])
    return candles


def get_first_30min_candle(fyers_client, symbol, retries=6, wait_s=5):
    """Fetch today's 9:15-9:45 30-min candle (retries briefly right after 9:45)."""
    today = datetime.date.today()
    for attempt in range(retries):
        for c in fetch_candles(fyers_client, symbol, FIRST_CANDLE_RESOLUTION, today, today):
            if c["time"].time() == FIRST_CANDLE_START:
                return c
        time.sleep(wait_s)
    raise RuntimeError(f"9:15 30-min candle not found for {symbol} — check market is open")


def get_atm_from_first_candle(fyers_client):
    candle = get_first_30min_candle(fyers_client, "NSE:NIFTY50-INDEX")
    atm = round(candle["close"] / STRIKE_STEP) * STRIKE_STEP
    logger.info(f"[ATM] 30-min candle (9:15-9:45) close={candle['close']:.2f} → ATM={atm}")
    send_telegram(f"📊 30-MIN CANDLE (9:15-9:45)\nClose={candle['close']:.2f} → ATM Strike={atm}")
    return atm


def bucket_start(dt):
    """Start time of the 2-min candle containing `dt`, aligned to 9:15
    (9:15, 9:17, 9:19 ... — same grid Fyers uses for 2-min history)."""
    session_open = dt.replace(hour=MARKET_OPEN.hour, minute=MARKET_OPEN.minute,
                              second=0, microsecond=0)
    mins = int((dt - session_open).total_seconds() // 60)
    mins = (mins // EMA_TF_MINUTES) * EMA_TF_MINUTES
    return session_open + datetime.timedelta(minutes=mins)


def tick_datetime(msg):
    """Exchange time of a websocket tick; falls back to local clock."""
    ts = msg.get("exch_feed_time") or msg.get("last_traded_time")
    if not ts:
        return datetime.datetime.now()
    ts = int(ts)
    if ts > 10_000_000_000:        # milliseconds → seconds
        ts //= 1000
    return datetime.datetime.fromtimestamp(ts)


# =========================================================
# EMA HELPER  (same as ITM1 EMA script)
# =========================================================
def compute_ema(closes, period):
    if len(closes) < period:
        return None
    k   = 2 / (period + 1)
    ema = sum(closes[:period]) / period
    for close in closes[period:]:
        ema = round(close * k + ema * (1 - k), 2)
    return ema


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

    def _order(self, symbol, side, tag):
        resp = self.client.place_order({
            "symbol":      symbol,
            "qty":         LOT_SIZE,
            "type":        2,          # 2 = Market order
            "side":        side,
            "productType": "INTRADAY",
            "validity":    "DAY",
            "orderTag":    tag,
        })
        logger.info(f"[ORDER] {symbol} side={side} tag={tag} response={resp}")
        return resp

    def sell_market(self, symbol, tag):
        return self._order(symbol, -1, tag)

    def buy_market(self, symbol, tag):
        return self._order(symbol, 1, tag)


# =========================================================
# OPTION LEG STRATEGY  (one instance per leg — CE and PE run independently)
# =========================================================
class OptionLegStrategy:
    """
    states: "waiting"     (live price has not yet reached the 62% level)
            "armed"       (62% level reached; waiting for a 2-min red candle
                           to close below EMA20 and the 62% level)
            "in_position" (sold at market, monitoring SL/target)
            "done"        (exited, or abandoned at EOD without entering)
    """

    def __init__(self, fyers, symbol, label, high, low, level, sl_price):
        self.fyers  = fyers
        self.symbol = symbol
        self.label  = label   # "CE" or "PE"

        self.high     = high
        self.low      = low
        self.level    = level       # 62% retracement level
        self.sl_price = sl_price

        self.target_price = None    # fixed at entry
        self.entry_price  = None
        self.state        = "waiting" if REQUIRE_LEVEL_TOUCH else "armed"

        self.candles         = deque(maxlen=3000)   # closed 2-min candles
        self.live_candle     = None
        self.live_is_partial = False

        logger.info(
            f"[{self.label} SETUP] symbol={self.symbol} H={high:.2f} L={low:.2f} "
            f"62%_level={level:.2f} sl={sl_price:.2f} state={self.state}"
        )
        send_telegram(
            f"📌 {self.label} SETUP — {self.symbol}\n"
            f"30-min candle: H={high:.2f} L={low:.2f}\n"
            f"62% level = {level:.2f}\n"
            f"SL        = {sl_price:.2f}\n"
            f"Entry: price must reach {level:.2f}, then a 2-min red candle must close "
            f"below EMA20 and below {level:.2f}"
        )

    # ---------------- EMA warm-up ----------------
    def prefill(self):
        """Load closed 2-min candles (previous days + today so far) for EMA20."""
        now     = datetime.datetime.now()
        from_dt = now.date() - datetime.timedelta(days=PREFILL_DAYS)
        hist    = fetch_candles(self.fyers.client, self.symbol, EMA_RESOLUTION, from_dt, now.date())
        tf      = datetime.timedelta(minutes=EMA_TF_MINUTES)
        for c in hist:
            if c["time"] + tf <= now:          # skip the candle still forming
                self._append_closed(c)
        ema20 = self._ema20()
        logger.info(
            f"[{self.label} PREFILL] {len(self.candles)} closed 2-min candles loaded, "
            f"EMA20={ema20 if ema20 is None else f'{ema20:.2f}'}"
        )

    def _backfill_gap(self, upto_time):
        """If 2-min candles are missing before `upto_time`, fetch them from history."""
        tf = datetime.timedelta(minutes=EMA_TF_MINUTES)
        if not self.candles or self.candles[-1]["time"] + tf >= upto_time:
            return
        try:
            today = datetime.date.today()
            hist  = fetch_candles(self.fyers.client, self.symbol, EMA_RESOLUTION, today, today)
            added = 0
            for c in hist:
                if self.candles[-1]["time"] < c["time"] < upto_time:
                    self._append_closed(c)
                    added += 1
            if added:
                logger.info(f"[{self.label} BACKFILL] added {added} missing 2-min candle(s)")
        except Exception as e:
            logger.warning(f"[{self.label} BACKFILL] failed: {e}")

    def _append_closed(self, candle):
        if self.candles and candle["time"] <= self.candles[-1]["time"]:
            return False
        self.candles.append(dict(candle))
        return True

    def _ema20(self):
        return compute_ema([c["close"] for c in self.candles], EMA_PERIOD)

    # ---------------- orders ----------------
    def _enter(self, ltp, candle, ema20):
        risk = self.sl_price - ltp
        if risk <= 0:
            logger.info(f"[{self.label}] entry skipped — live {ltp:.2f} already >= SL {self.sl_price:.2f}")
            return
        self.entry_price  = ltp
        self.target_price = round(ltp - REWARD_RISK * risk, 2)

        logger.info(
            f"[{self.label} ENTRY] symbol={self.symbol} 2-min red candle {candle['time']:%H:%M} "
            f"C={candle['close']:.2f} < EMA20={ema20:.2f} & < 62%={self.level:.2f} — "
            f"selling at market, live={ltp:.2f} sl={self.sl_price:.2f} target={self.target_price:.2f}"
        )
        send_telegram(
            f"📉 {self.label} ENTRY — {self.symbol}\n"
            f"2-min red candle ({candle['time']:%H:%M}) closed {candle['close']:.2f}\n"
            f"below EMA20 {ema20:.2f} and 62% level {self.level:.2f} — SOLD at market\n"
            f"Entry  ≈ {ltp:.2f}\n"
            f"SL     = {self.sl_price:.2f}\n"
            f"Target = {self.target_price:.2f}  ({REWARD_RISK:g}x risk)"
        )
        self.fyers.sell_market(self.symbol, f"ATM62{self.label}SELL")
        self.state = "in_position"

    def _exit(self, reason):
        logger.info(f"[{self.label} EXIT] symbol={self.symbol} {reason}")
        send_telegram(f"🛑 {self.label} EXIT — {self.symbol}\nReason: {reason}")
        self.fyers.buy_market(self.symbol, f"ATM62{self.label}BUY")
        self.state = "done"

    # ---------------- candle close ----------------
    def _on_candle_close(self, candle, partial, ltp):
        self._backfill_gap(candle["time"])
        if not self._append_closed(candle):
            return
        ema20 = self._ema20()
        is_red = candle["close"] < candle["open"]
        logger.info(
            f"[{self.label} 2MIN] {candle['time']:%H:%M} O={candle['open']:.2f} "
            f"H={candle['high']:.2f} L={candle['low']:.2f} C={candle['close']:.2f} "
            f"red={is_red} EMA20={ema20 if ema20 is None else f'{ema20:.2f}'} state={self.state}"
            f"{' (partial)' if partial else ''}"
        )
        if self.state != "armed" or ema20 is None:
            return
        if partial:
            # first live candle started mid-bucket, so its open isn't reliable
            return
        close_time = (candle["time"] + datetime.timedelta(minutes=EMA_TF_MINUTES)).time()
        if close_time >= SAFETY_TIME:
            return
        if is_red and candle["close"] < ema20 and candle["close"] < self.level:
            self._enter(ltp, candle, ema20)

    # ---------------- tick ----------------
    def on_tick(self, ltp, tick_dt, now_dt):
        if self.state == "done":
            return

        # ── EOD safety net ──
        if now_dt.time() >= SAFETY_TIME:
            if self.state == "in_position":
                self._exit("3:14 PM safety-net force-exit")
            else:
                logger.info(f"[{self.label} SAFETY] entry never triggered — no trade")
                send_telegram(f"🕒 {self.label} SAFETY NET — entry never triggered, no trade ({self.symbol})")
                self.state = "done"
            return

        # ── Build 2-min candles from ticks ──
        if tick_dt.time() >= MARKET_OPEN:
            bucket = bucket_start(tick_dt)
            lc = self.live_candle
            if lc is None or bucket > lc["time"]:
                if lc is not None:
                    self._on_candle_close(lc, self.live_is_partial, ltp)
                self.live_is_partial = lc is None
                self.live_candle = {"time": bucket, "open": ltp, "high": ltp, "low": ltp, "close": ltp}
            elif bucket == lc["time"]:
                lc["high"]  = max(lc["high"], ltp)
                lc["low"]   = min(lc["low"], ltp)
                lc["close"] = ltp

        # ── Arm once the 62% level is reached ──
        if self.state == "waiting" and ltp >= self.level:
            self.state = "armed"
            logger.info(f"[{self.label} ARMED] live {ltp:.2f} >= 62% level {self.level:.2f} — "
                        f"waiting for 2-min red candle below EMA20 & 62% level")
            send_telegram(f"🎯 {self.label} ARMED — {self.symbol}\nLive {ltp:.2f} reached 62% level "
                          f"{self.level:.2f}\nWaiting for 2-min red candle close below EMA20 & level")
            return

        # ── SL / target ──
        if self.state == "in_position":
            if ltp <= self.target_price:
                self._exit(f"TARGET HIT — live price {ltp:.2f} <= target {self.target_price:.2f}")
            elif ltp >= self.sl_price:
                self._exit(f"SL HIT — live price {ltp:.2f} >= SL {self.sl_price:.2f}")


# =========================================================
# MAIN
# =========================================================
if __name__ == "__main__":
    logger.info("[BOOT] NIFTY ATM 62%-RETRACEMENT + 2-MIN EMA20 SELL STRATEGY STARTED")
    send_telegram("🚀 NIFTY ATM 62%-RETRACEMENT + 2-MIN EMA20 SELL STRATEGY STARTED")

    fyers = FyersClient()

    # ── Step 1: wait for the first 30-min candle (9:15-9:45) to close ──
    logger.info(f"[WAIT] Waiting until {SETUP_WAIT_TIME} for the 30-min candle to close...")
    while datetime.datetime.now().time() < SETUP_WAIT_TIME:
        time.sleep(1)

    # ── Step 2: ATM strike from NIFTY's own first 30-min candle ──
    atm = get_atm_from_first_candle(fyers.client)
    ce_symbol = build_symbol(atm, "CE")
    pe_symbol = build_symbol(atm, "PE")
    logger.info(f"[SYMBOLS] ATM={atm} CE={ce_symbol} PE={pe_symbol}")
    send_telegram(f"📌 ATM={atm}\nCE: {ce_symbol}\nPE: {pe_symbol}")

    # ── Step 3: evaluate each leg's own first 30-min candle, then warm up EMA20 ──
    engines = {}
    for label, symbol in [("CE", ce_symbol), ("PE", pe_symbol)]:
        candle = get_first_30min_candle(fyers.client, symbol)
        is_red = candle["close"] < candle["open"]
        logger.info(
            f"[{label} 30MIN] O={candle['open']:.2f} H={candle['high']:.2f} "
            f"L={candle['low']:.2f} C={candle['close']:.2f} red={is_red}"
        )
        if not is_red:
            logger.info(f"[{label}] first candle not red — no trade on this leg")
            send_telegram(f"⚪ {label} — first 30-min candle not red, skipping this leg")
            continue

        high, low = candle["high"], candle["low"]
        level     = round(low + RETRACEMENT * (high - low), 2)
        engine    = OptionLegStrategy(fyers, symbol, label, high, low, level, sl_price=high)
        engine.prefill()
        engines[symbol] = engine

    if not engines:
        logger.info("[DONE] Neither CE nor PE had a red first candle — nothing to trade today.")
        send_telegram("⚪ Neither CE nor PE had a red first candle — no trades today.")
        sys.exit(0)

    # ── Step 4: websocket — live ticks build 2-min candles and drive entry / SL / target ──
    SUBSCRIBED_SYMBOLS = list(engines.keys())

    def on_tick(msg):
        if "symbol" not in msg or "ltp" not in msg:
            return
        sym = msg["symbol"]
        if sym not in engines:
            return
        engines[sym].on_tick(msg["ltp"], tick_datetime(msg), datetime.datetime.now())

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
