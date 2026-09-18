"""
NIFTY ITM1 EMA-REVERSAL SELL STRATEGY
=======================================
5-minute candles, EMA5 & EMA20 computed on the INDIVIDUAL option premium
(not a combined straddle). CE and PE strikes are chosen independently and
run as two fully independent single-leg strategies.

━━━ STRIKE SELECTION ━━━
  At 9:16 AM, fetch NIFTY50 spot price.
  ATM      = round(spot / 50) * 50
  ITM1 CE  = ATM - 50   (one strike in-the-money for a call)
  ITM1 PE  = ATM + 50   (one strike in-the-money for a put)
  These two strikes are fixed for the day. Each is tracked/traded on its
  OWN 5-min candle series and its OWN EMA5/EMA20 — no combined premium.

━━━ ENTRY SIGNAL (checked on each closed 5-min candle, per strike) ━━━
  On the CLOSING candle (N):
    close(N) < EMA5(N)  AND  close(N) < EMA20(N)  AND  EMA5(N) < EMA20(N)
  On the PRECEDING candle (N-1):
    EMA5(N-1) > EMA20(N-1)          (i.e. the EMA5/EMA20 crossover happens
                                      exactly on candle N)
  → SELL the strike (naked, single leg, no hedge).

━━━ EXIT (the ONLY ways out) ━━━
  1. SL      : high of the entry/signal candle + 2 points (checked on every
               live tick against the option's live LTP).
  2. Reversal: on any later closed candle that is GREEN (close > open) AND
               close > EMA5 AND close > EMA20 AND EMA5 > EMA20.
  3. EOD     : 3:00 PM hard force-exit safety net, regardless of 1/2.

  Only ONE trade is taken per strike per day. Once a strike's trade exits
  (for any reason above), that strike is done for the day — no re-entry.

Qty per order = 1 (as configured in LOT_SIZE below — adjust to your
broker's actual per-lot quantity if the API expects total share count
rather than a lot count).
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

LOT_SIZE      = 1     # qty sent to the API — adjust if your broker needs
                       # the actual share count for 1 lot instead of "1".
STRIKE_STEP   = 50    # Nifty strike interval

DECISION_TIME      = datetime.time(9, 16)   # spot checked at 9:16 AM
TRADING_END         = datetime.time(15, 0)  # 3:00 PM EOD force-exit (safety net)

HISTORY_RESOLUTION = "5"    # 5-minute candles

EMA_FAST         = 5
EMA_SLOW         = 20
MIN_BARS_FOR_EMA = 21        # need at least EMA_SLOW+1 candles for prev+curr EMA20

SL_OFFSET_POINTS = 2          # SL = signal-candle high + 2 points

LOG_FILE = "nifty_itm1_ema_reversal.log"

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
# EXPIRY / SYMBOL UTILS  (unchanged from reference script)
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


def get_spot_price(fyers_client):
    """Fetch live NIFTY50 spot LTP via the quotes endpoint."""
    r = fyers_client.quotes({"symbols": "NSE:NIFTY50-INDEX"})
    try:
        ltp = r["d"][0]["v"]["lp"]
    except (KeyError, IndexError, TypeError) as e:
        raise RuntimeError(f"Could not read spot LTP from quotes response: {r}") from e
    logger.info(f"[SPOT] NIFTY50 LTP = {ltp:.2f}")
    return ltp


def compute_itm1_strikes(spot):
    atm     = round(spot / STRIKE_STEP) * STRIKE_STEP
    ce_strike = atm - STRIKE_STEP   # ITM1 call: one step below ATM
    pe_strike = atm + STRIKE_STEP   # ITM1 put:  one step above ATM
    logger.info(f"[STRIKES] spot={spot:.2f} ATM={atm} ITM1_CE={ce_strike} ITM1_PE={pe_strike}")
    send_telegram(
        f"📌 STRIKE SELECTION (9:16 AM)\n"
        f"Spot     = {spot:.2f}\n"
        f"ATM      = {atm}\n"
        f"ITM1 CE  = {ce_strike}\n"
        f"ITM1 PE  = {pe_strike}"
    )
    return atm, ce_strike, pe_strike


def build_symbol(strike, opt_type):
    expiry = format_expiry(get_next_expiry())
    return f"NSE:NIFTY{expiry}{strike}{opt_type}"


# =========================================================
# FYERS CLIENT  (naked sell / buy only — no hedge legs)
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
            "orderTag":    tag,
        })

    def buy_market(self, symbol, tag):
        return self.client.place_order({
            "symbol":      symbol,
            "qty":         LOT_SIZE,
            "type":        2,
            "side":        1,
            "productType": "INTRADAY",
            "validity":    "DAY",
            "orderTag":    tag,
        })


# =========================================================
# HISTORICAL PREFILL  (per-strike, individual premium)
# =========================================================
def fetch_historical_candles(fyers_client, symbol):
    """Fetch 5-min candles for a single option symbol over the past 7 days."""
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
# EMA HELPER (pure function — recomputed over an explicit closes list,
# so we can independently evaluate EMA "as of the previous candle" vs
# EMA "as of the current candle" for crossover detection)
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
# STRIKE STRATEGY  (one instance per strike — CE and PE run independently)
# =========================================================
class StrikeStrategy:
    """
    Single-leg, naked-sell EMA-reversal strategy for ONE option strike.

    Entry (on a closed candle N):
        close(N) < EMA5(N) and close(N) < EMA20(N) and EMA5(N) < EMA20(N)
        and EMA5(N-1) > EMA20(N-1)                     (crossover on N)
      → SELL the strike.

    Exit (only three ways out):
        1. SL       : live price >= entry_signal_candle.high + SL_OFFSET_POINTS
                      (checked every tick)
        2. Reversal : closed candle is green, close > EMA5 & EMA20, EMA5 > EMA20
        3. EOD      : 3:00 PM force-exit safety net

    Only one trade per strike per day (self.done flag).
    """

    def __init__(self, fyers, symbol, label):
        self.fyers  = fyers
        self.symbol = symbol
        self.label  = label   # "CE" or "PE", used in logs/order tags

        self.candles  = deque(maxlen=2000)
        self.position = None   # dict when in a trade, None otherwise
        self.done     = False  # True once this strike's one trade has exited

    # ----------------------------------------------------------
    def _enter_trade(self, candle, ema5, ema20):
        entry    = candle["close"]
        sl_price = round(candle["high"] + SL_OFFSET_POINTS, 2)
        tag      = f"ITM1{self.label}"

        logger.info(
            f"[{self.label} ENTRY] symbol={self.symbol} entry={entry:.2f} "
            f"sl={sl_price:.2f} (signal_high={candle['high']:.2f}+{SL_OFFSET_POINTS}) "
            f"EMA5={ema5:.2f} EMA20={ema20:.2f} time={candle['time']}"
        )
        send_telegram(
            f"📉 {self.label} ENTRY — {self.symbol}\n"
            f"Sell price = {entry:.2f}\n"
            f"SL         = {sl_price:.2f}  (signal-candle high {candle['high']:.2f} + {SL_OFFSET_POINTS})\n"
            f"EMA5={ema5:.2f} | EMA20={ema20:.2f}\n"
            f"Time: {candle['time']}"
        )

        self.fyers.sell_market(self.symbol, f"{tag}SELL")

        self.position = {
            "entry_price": entry,
            "sl_price":    sl_price,
            "entry_time":  candle["time"],
        }

    def _exit_trade(self, reason):
        logger.info(f"[{self.label} EXIT] symbol={self.symbol} {reason}")
        send_telegram(f"🛑 {self.label} EXIT — {self.symbol}\nReason: {reason}")

        tag = f"ITM1{self.label}"
        self.fyers.buy_market(self.symbol, f"{tag}BUY")

        self.position = None
        self.done     = True   # only 1 trade per strike per day

    # ----------------------------------------------------------
    def on_candle(self, candle, closed, live_price=None):
        now = datetime.datetime.now()

        # ── EOD force-exit safety net (tick-level, always checked first) ──
        if now.time() >= TRADING_END:
            if self.position:
                self._exit_trade("EOD 3:00 PM force-exit (safety net)")
            return

        # ── Live tick: SL check ──
        if live_price is not None and self.position:
            if live_price >= self.position["sl_price"]:
                self._exit_trade(
                    f"SL HIT — live price {live_price:.2f} >= SL {self.position['sl_price']:.2f}"
                )

        if not closed:
            return

        # ── Closed candle: update history ──
        self.candles.append(candle)
        closes = [c["close"] for c in self.candles]

        if len(closes) < MIN_BARS_FOR_EMA:
            return

        ema5_curr  = compute_ema(closes, EMA_FAST)
        ema20_curr = compute_ema(closes, EMA_SLOW)
        prev_closes = closes[:-1]
        ema5_prev   = compute_ema(prev_closes, EMA_FAST)
        ema20_prev  = compute_ema(prev_closes, EMA_SLOW)

        if ema5_curr is None or ema20_curr is None:
            return

        logger.info(
            f"[{self.label} CANDLE] {candle['time']} "
            f"O={candle['open']:.2f} H={candle['high']:.2f} "
            f"L={candle['low']:.2f} C={candle['close']:.2f} "
            f"EMA5={ema5_curr:.2f} EMA20={ema20_curr:.2f} "
            f"position={'YES' if self.position else 'no'} done={self.done}"
        )

        # Candle-level EOD guard (belt-and-suspenders alongside tick-level)
        if candle["time"].time() >= TRADING_END:
            if self.position:
                self._exit_trade("EOD 3:00 PM (candle-level)")
            return

        # ── If in a position, only the reversal-exit check applies ──
        if self.position:
            is_green = candle["close"] > candle["open"]
            reversal = (
                is_green and
                candle["close"] > ema5_curr and
                candle["close"] > ema20_curr and
                ema5_curr > ema20_curr
            )
            if reversal:
                self._exit_trade(
                    f"Reversal signal — green candle closed above both EMAs "
                    f"(EMA5={ema5_curr:.2f} > EMA20={ema20_curr:.2f})"
                )
            return

        # ── Not in a position: check for entry (only once per day) ──
        if self.done:
            return
        if ema5_prev is None or ema20_prev is None:
            return

        entry_signal = (
            candle["close"] < ema5_curr and
            candle["close"] < ema20_curr and
            ema5_curr < ema20_curr and
            ema5_prev > ema20_prev
        )
        if entry_signal:
            self._enter_trade(candle, ema5_curr, ema20_curr)


# =========================================================
# MAIN
# =========================================================
if __name__ == "__main__":
    logger.info("[BOOT] NIFTY ITM1 EMA-REVERSAL STRATEGY STARTED")
    send_telegram("🚀 NIFTY ITM1 EMA-REVERSAL STRATEGY STARTED")

    fyers = FyersClient()

    # ── Step 1: wait until 9:16 AM ──
    logger.info(f"[WAIT] Waiting until {DECISION_TIME} to check spot & pick strikes...")
    while datetime.datetime.now().time() < DECISION_TIME:
        time.sleep(1)

    # ── Step 2: spot → ITM1 CE / PE strikes ──
    spot = get_spot_price(fyers.client)
    atm, ce_strike, pe_strike = compute_itm1_strikes(spot)

    ce_symbol = build_symbol(ce_strike, "CE")
    pe_symbol = build_symbol(pe_strike, "PE")
    logger.info(f"[SYMBOLS] CE={ce_symbol} | PE={pe_symbol}")
    send_telegram(f"📌 SYMBOLS\nCE (sell): {ce_symbol}\nPE (sell): {pe_symbol}")

    ce_engine = StrikeStrategy(fyers, ce_symbol, "CE")
    pe_engine = StrikeStrategy(fyers, pe_symbol, "PE")
    engines   = {ce_symbol: ce_engine, pe_symbol: pe_engine}

    # ── Step 3: prefill historical 5-min candles (past days only, EMA warm-up) ──
    today = datetime.date.today()
    for symbol, engine in engines.items():
        hist = fetch_historical_candles(fyers.client, symbol)
        for c in hist:
            if c["time"].date() < today:
                engine.candles.append(c)
        closes = [c["close"] for c in engine.candles]
        if len(closes) >= MIN_BARS_FOR_EMA:
            e5, e20 = compute_ema(closes, EMA_FAST), compute_ema(closes, EMA_SLOW)
            logger.info(f"[EMA BOOT] {symbol}: EMA5={e5:.2f} EMA20={e20:.2f}")

        # feed any of today's candles that already exist (e.g. 9:15 candle,
        # if it happened to close before 9:16 decision time)
        for c in hist:
            if c["time"].date() == today:
                engine.on_candle(c, closed=True)

    # ── Step 4: websocket for live ticks ──
    SUBSCRIBED_SYMBOLS = [ce_symbol, pe_symbol]
    live_candle = {ce_symbol: None, pe_symbol: None}

    def extract_tick_epoch(msg):
        ts = msg.get("last_traded_time") or msg.get("timestamp") or msg.get("tt")
        return int(ts // 1000) if ts and ts > 10_000_000_000 else int(ts)

    def on_tick(msg):
        if "symbol" not in msg or "ltp" not in msg:
            return

        sym = msg["symbol"]
        if sym not in engines:
            return

        engine = engines[sym]
        ltp    = msg["ltp"]
        now    = datetime.datetime.now()

        # EOD: pass live price for tick-level force-exit
        if now.time() >= TRADING_END:
            engine.on_candle(live_candle[sym] or {}, closed=False, live_price=ltp)
            return

        epoch  = extract_tick_epoch(msg)
        dt     = datetime.datetime.fromtimestamp(epoch)
        bucket = dt.replace(second=0, microsecond=0, minute=(dt.minute // 5) * 5)

        candle = live_candle[sym]
        if candle is None or candle["time"] != bucket:
            if candle:
                engine.on_candle(candle, closed=True, live_price=None)
            candle = {
                "time":  bucket,
                "open":  ltp,
                "high":  ltp,
                "low":   ltp,
                "close": ltp,
            }
            live_candle[sym] = candle
        else:
            candle["high"]  = max(candle["high"], ltp)
            candle["low"]   = min(candle["low"],  ltp)
            candle["close"] = ltp

        # Live tick: SL check
        engine.on_candle(candle, closed=False, live_price=ltp)

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
