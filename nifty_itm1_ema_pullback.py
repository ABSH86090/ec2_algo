"""
NIFTY ITM1 EMA-PULLBACK BUY STRATEGY
=====================================
5-minute candles. EMA5 & EMA20 are computed on the INDIVIDUAL option premium
of the ITM1 CE and the ITM1 PE. The two strikes run as fully independent
single-leg BUY strategies.

━━━ STRIKE SELECTION ━━━
  Uses the CLOSE of the NIFTY50 index 9:15 5-min candle (the candle that
  closes at 9:20 AM).
  ATM      = round(close_920 / 50) * 50
  ITM1 CE  = ATM - 50
  ITM1 PE  = ATM + 50
  Strikes are fixed for the day.

━━━ ENTRY LOGIC (per strike, on each closed 5-min candle) ━━━
  Step 1 (ARMING) : At least 15 of TODAY's 5-min candles must close above
                    BOTH EMA5 and EMA20.
                    (CONSECUTIVE_REQUIRED=True → the 15 must be in a row;
                     a candle that does not close above both resets the count.)
  Step 2 (RED)    : After arming, a RED candle (close < open) with
                      low   < EMA5  AND low < EMA20
                      close < EMA5
                      EMA5  > EMA20
  Step 3 (GREEN)  : The VERY NEXT candle is GREEN (close > open) with
                      close > EMA20  AND  EMA5 > EMA20
  Step 4 (BUY)    : Buy the option at market.
                      SL     = lower of (red candle low, green candle low)
                      Target = entry + 2 × (entry − SL)      (1:2 R:R)

  Reset rule      : If EMA5 goes below EMA20 at any time before the
                    confirmation (green) candle, the 15-candle count is
                    reset to 0 and Step 1 must be satisfied again.

  If the candle after the red candle is not a valid green confirmation,
  the setup is discarded (if that candle itself qualifies as a red setup
  candle, it becomes the new red candle).

━━━ EXITS ━━━
  1. SL     : live LTP <= SL        (checked every tick)
  2. Target : live LTP >= Target    (checked every tick)
  3. EOD    : 3:00 PM hard force-exit

  Max 2 trades per strike (CE and PE separately) per day.
  After a trade exits, the strike stays armed if EMA5 is still above EMA20
  (set RESET_COUNT_AFTER_TRADE=True to demand a fresh 15 candles instead).
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

LOT_SIZE      = 65     # qty sent to the API
STRIKE_STEP   = 50     # Nifty strike interval
INDEX_SYMBOL  = "NSE:NIFTY50-INDEX"

DECISION_TIME      = datetime.time(9, 20, 5)  # just after 9:20 candle close
DECISION_CANDLE    = datetime.time(9, 15)     # 5-min candle starting 9:15, closing 9:20
TRADING_END        = datetime.time(15, 0)     # 3:00 PM EOD force-exit

HISTORY_RESOLUTION = "5"
CANDLE_MINUTES     = 5

EMA_FAST         = 5
EMA_SLOW         = 20
MIN_BARS_FOR_EMA = EMA_SLOW

MIN_CANDLES_ABOVE_EMAS  = 15     # Step 1 requirement
CONSECUTIVE_REQUIRED    = True   # 15 candles must be in a row
RESET_COUNT_AFTER_TRADE = False  # require fresh 15 candles after each trade?
MAX_TRADES_PER_STRIKE   = 2
TARGET_RR               = 2.0    # target = 2 × SL distance

LOG_FILE = "nifty_itm1_ema_pullback_buy.log"

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
    datetime.date(2026, 12, 25),
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
# 9:20 INDEX CANDLE CLOSE
# =========================================================
def get_920_index_close(fyers_client, max_wait_sec=90):
    """
    Return the close of today's NIFTY50 9:15 5-min candle (closes at 9:20).
    Retries for a short while because the candle can take a few seconds to
    appear in the history API. Falls back to live LTP if it never shows up.
    """
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
                if dt.date() == today and dt.time() == DECISION_CANDLE:
                    close = c[4]
                    logger.info(f"[SPOT] NIFTY50 9:20 candle close = {close:.2f}")
                    return close
        except Exception as e:
            logger.warning(f"[SPOT] history fetch failed: {e}")
        time.sleep(3)

    # Fallback: live LTP
    r = fyers_client.quotes({"symbols": INDEX_SYMBOL})
    try:
        ltp = r["d"][0]["v"]["lp"]
    except (KeyError, IndexError, TypeError) as e:
        raise RuntimeError(f"Could not read spot from quotes response: {r}") from e
    logger.warning(f"[SPOT] 9:20 candle not available — using live LTP {ltp:.2f}")
    send_telegram(f"⚠️ 9:20 index candle not available, using live LTP {ltp:.2f}")
    return ltp


def compute_itm1_strikes(spot):
    atm       = round(spot / STRIKE_STEP) * STRIKE_STEP
    ce_strike = atm - STRIKE_STEP   # ITM1 call
    pe_strike = atm + STRIKE_STEP   # ITM1 put
    logger.info(f"[STRIKES] spot={spot:.2f} ATM={atm} ITM1_CE={ce_strike} ITM1_PE={pe_strike}")
    send_telegram(
        f"📌 STRIKE SELECTION (9:20 candle close)\n"
        f"Spot     = {spot:.2f}\n"
        f"ATM      = {atm}\n"
        f"ITM1 CE  = {ce_strike}\n"
        f"ITM1 PE  = {pe_strike}"
    )
    return atm, ce_strike, pe_strike


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
def fetch_historical_candles(fyers_client, symbol):
    """Fetch 5-min candles for one option symbol over the past 7 days."""
    to_dt   = datetime.datetime.now()
    from_dt = to_dt - datetime.timedelta(days=7)

    r = fyers_client.history({
        "symbol":      symbol,
        "resolution":  HISTORY_RESOLUTION,
        "date_format": "1",
        "range_from":  from_dt.strftime("%Y-%m-%d"),
        "range_to":    to_dt.strftime("%Y-%m-%d"),
        "cont_flag":   "1",
    })

    candles = []
    for c in r.get("candles", []):
        dt = datetime.datetime.fromtimestamp(c[0])
        if dt.time() < datetime.time(9, 15) or dt.time() > datetime.time(15, 30):
            continue
        candles.append({
            "time":  dt,
            "open":  c[1],
            "high":  c[2],
            "low":   c[3],
            "close": c[4],
        })

    candles.sort(key=lambda c: c["time"])
    logger.info(f"[PREFILL] {symbol}: {len(candles)} candles loaded")
    return candles


# =========================================================
# EMA HELPER
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
# STRIKE STRATEGY (one instance per strike)
# =========================================================
class StrikeStrategy:
    def __init__(self, fyers, symbol, label):
        self.fyers  = fyers
        self.symbol = symbol
        self.label  = label   # "CE" or "PE"

        self.candles = deque(maxlen=2000)

        # Trend / setup state
        self.above_count = 0      # today's candles closed above both EMAs
        self.armed       = False  # Step 1 satisfied
        self.pending_red = None   # Step 2 red candle awaiting confirmation

        # Trade state
        self.position     = None
        self.trades_taken = 0

    @property
    def done(self):
        return self.trades_taken >= MAX_TRADES_PER_STRIKE and self.position is None

    # ----------------------------------------------------------
    def _reset_trend(self, why):
        if self.above_count or self.armed or self.pending_red:
            logger.info(f"[{self.label} RESET] {why} — 15-candle count reset")
        self.above_count = 0
        self.armed       = False
        self.pending_red = None

    def _update_trend_state(self, candle, ema5, ema20):
        """
        Maintain the Step-1 counter. Returns False if the trend is broken
        (EMA5 < EMA20) on this candle.
        """
        if ema5 < ema20:
            self._reset_trend(f"EMA5 {ema5:.2f} < EMA20 {ema20:.2f}")
            return False

        if not self.armed:
            closes_above = candle["close"] > ema5 and candle["close"] > ema20
            if closes_above:
                self.above_count += 1
            elif CONSECUTIVE_REQUIRED:
                self.above_count = 0

            if self.above_count >= MIN_CANDLES_ABOVE_EMAS:
                self.armed = True
                logger.info(
                    f"[{self.label} ARMED] {self.above_count} candles closed above "
                    f"EMA5 & EMA20 — watching for red/green pattern"
                )
                send_telegram(
                    f"✅ {self.label} ARMED — {self.symbol}\n"
                    f"{self.above_count} candles closed above EMA5 & EMA20.\n"
                    f"Watching for red pullback + green confirmation."
                )
        return True

    # ----------------------------------------------------------
    def _enter_trade(self, red, green, ema5, ema20):
        entry  = green["close"]
        # SL = lower of the red candle's low and the green candle's low
        if green["low"] < red["low"]:
            sl, sl_src = green["low"], f"green candle low @ {green['time'].strftime('%H:%M')}"
        else:
            sl, sl_src = red["low"], f"red candle low @ {red['time'].strftime('%H:%M')}"
        risk   = entry - sl
        if risk <= 0:
            logger.info(
                f"[{self.label} SKIP] entry {entry:.2f} <= SL {sl:.2f}, invalid risk"
            )
            return
        target = round(entry + TARGET_RR * risk, 2)
        self.trades_taken += 1
        tag = f"ITM1{self.label}B{self.trades_taken}"

        logger.info(
            f"[{self.label} ENTRY #{self.trades_taken}] symbol={self.symbol} "
            f"entry={entry:.2f} sl={sl:.2f} ({sl_src}) target={target:.2f} risk={risk:.2f} "
            f"red={red['time']} green={green['time']} EMA5={ema5:.2f} EMA20={ema20:.2f}"
        )
        send_telegram(
            f"📈 {self.label} BUY #{self.trades_taken} — {self.symbol}\n"
            f"Entry  = {entry:.2f}\n"
            f"SL     = {sl:.2f} ({sl_src})\n"
            f"Target = {target:.2f} (2 × {risk:.2f})\n"
            f"EMA5={ema5:.2f} | EMA20={ema20:.2f}\n"
            f"Signal candle: {green['time'].strftime('%H:%M')}"
        )

        self.fyers.buy_market(self.symbol, tag)

        self.position = {
            "entry_price": entry,
            "sl_price":    sl,
            "target":      target,
            "entry_time":  green["time"],
            "tag":         tag,
        }

    def _exit_trade(self, reason, price=None):
        pnl_txt = ""
        if price is not None and self.position:
            pts = price - self.position["entry_price"]
            pnl_txt = f" | approx P&L {pts:+.2f} pts ({pts * LOT_SIZE:+.0f})"

        logger.info(f"[{self.label} EXIT] symbol={self.symbol} {reason}{pnl_txt}")
        send_telegram(f"🛑 {self.label} EXIT — {self.symbol}\nReason: {reason}{pnl_txt}")

        self.fyers.sell_market(self.symbol, f"{self.position['tag']}X")
        self.position    = None
        self.pending_red = None

        if RESET_COUNT_AFTER_TRADE:
            self._reset_trend("trade closed (RESET_COUNT_AFTER_TRADE)")

        if self.trades_taken >= MAX_TRADES_PER_STRIKE:
            logger.info(f"[{self.label}] max {MAX_TRADES_PER_STRIKE} trades done for the day")

    # ----------------------------------------------------------
    def on_tick(self, ltp):
        """Tick-level SL / target / EOD checks."""
        if not self.position:
            return
        if datetime.datetime.now().time() >= TRADING_END:
            self._exit_trade("EOD 3:00 PM force-exit", ltp)
            return
        if ltp <= self.position["sl_price"]:
            self._exit_trade(
                f"SL HIT — LTP {ltp:.2f} <= SL {self.position['sl_price']:.2f}", ltp
            )
        elif ltp >= self.position["target"]:
            self._exit_trade(
                f"TARGET HIT — LTP {ltp:.2f} >= target {self.position['target']:.2f}", ltp
            )

    def on_closed_candle(self, candle):
        self.candles.append(candle)
        closes = [c["close"] for c in self.candles]
        if len(closes) < MIN_BARS_FOR_EMA:
            logger.info(f"[{self.label}] not enough candles for EMA yet ({len(closes)})")
            return

        ema5  = compute_ema(closes, EMA_FAST)
        ema20 = compute_ema(closes, EMA_SLOW)
        if ema5 is None or ema20 is None:
            return

        logger.info(
            f"[{self.label} CANDLE] {candle['time'].strftime('%H:%M')} "
            f"O={candle['open']:.2f} H={candle['high']:.2f} "
            f"L={candle['low']:.2f} C={candle['close']:.2f} "
            f"EMA5={ema5:.2f} EMA20={ema20:.2f} count={self.above_count} "
            f"armed={self.armed} pending_red={'YES' if self.pending_red else 'no'} "
            f"pos={'YES' if self.position else 'no'} trades={self.trades_taken}"
        )

        # Candle-level EOD guard
        if candle["time"].time() >= TRADING_END:
            if self.position:
                self._exit_trade("EOD 3:00 PM (candle-level)", candle["close"])
            return

        # Step 1 bookkeeping (always runs, even while in a trade)
        trend_ok = self._update_trend_state(candle, ema5, ema20)

        if self.position or self.trades_taken >= MAX_TRADES_PER_STRIKE:
            return
        if not trend_ok or not self.armed:
            return

        is_green = candle["close"] > candle["open"]
        is_red   = candle["close"] < candle["open"]

        # Step 3: confirmation candle immediately after the red candle
        if self.pending_red is not None:
            red = self.pending_red
            self.pending_red = None
            if is_green and candle["close"] > ema20 and ema5 > ema20:
                self._enter_trade(red, candle, ema5, ema20)
                return
            logger.info(
                f"[{self.label}] candle after red ({red['time'].strftime('%H:%M')}) "
                f"did not confirm — setup discarded"
            )

        # Step 2: red pullback candle
        red_setup = (
            is_red and
            candle["low"] < ema5 and
            candle["low"] < ema20 and
            candle["close"] < ema5 and
            ema5 > ema20
        )
        if red_setup:
            self.pending_red = candle
            logger.info(
                f"[{self.label} RED SETUP] {candle['time'].strftime('%H:%M')} "
                f"low={candle['low']:.2f} close={candle['close']:.2f} — "
                f"waiting for green confirmation on next candle"
            )


# =========================================================
# MAIN
# =========================================================
if __name__ == "__main__":
    logger.info("[BOOT] NIFTY ITM1 EMA-PULLBACK BUY STRATEGY STARTED")
    send_telegram("🚀 NIFTY ITM1 EMA-PULLBACK BUY STRATEGY STARTED")

    fyers = FyersClient()

    # ── Step 1: wait for the 9:20 candle to close ──
    logger.info(f"[WAIT] Waiting until {DECISION_TIME} for the 9:20 index candle...")
    while datetime.datetime.now().time() < DECISION_TIME:
        time.sleep(1)

    # ── Step 2: 9:20 close → ITM1 strikes ──
    spot = get_920_index_close(fyers.client)
    atm, ce_strike, pe_strike = compute_itm1_strikes(spot)

    ce_symbol = build_symbol(ce_strike, "CE")
    pe_symbol = build_symbol(pe_strike, "PE")
    logger.info(f"[SYMBOLS] CE={ce_symbol} | PE={pe_symbol}")
    send_telegram(f"📌 SYMBOLS\nCE (buy): {ce_symbol}\nPE (buy): {pe_symbol}")

    engines = {
        ce_symbol: StrikeStrategy(fyers, ce_symbol, "CE"),
        pe_symbol: StrikeStrategy(fyers, pe_symbol, "PE"),
    }
    live_candle = {ce_symbol: None, pe_symbol: None}

    # ── Step 3: prefill — past days warm up EMAs only; today's closed
    #    candles go through the strategy so they count toward the 15 ──
    today = datetime.date.today()
    now   = datetime.datetime.now()
    for symbol, engine in engines.items():
        hist = fetch_historical_candles(fyers.client, symbol)
        for c in hist:
            if c["time"].date() < today:
                engine.candles.append(c)

        closes = [c["close"] for c in engine.candles]
        if len(closes) >= MIN_BARS_FOR_EMA:
            logger.info(
                f"[EMA BOOT] {symbol}: EMA5={compute_ema(closes, EMA_FAST):.2f} "
                f"EMA20={compute_ema(closes, EMA_SLOW):.2f}"
            )

        for c in hist:
            if c["time"].date() != today:
                continue
            candle_close_time = c["time"] + datetime.timedelta(minutes=CANDLE_MINUTES)
            if candle_close_time <= now:
                engine.on_closed_candle(c)
            else:
                # current, still-forming candle — seed the live builder with it
                live_candle[symbol] = dict(c)

    # ── Step 4: websocket for live ticks ──
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
            engine.on_tick(ltp)   # EOD exit if still holding
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

        engine.on_tick(ltp)   # SL / target

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
