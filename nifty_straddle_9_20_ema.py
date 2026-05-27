"""
NIFTY STRADDLE LIVE STRATEGY
==============================
3-minute candles, EMA5 & EMA20 on combined CE+PE straddle premium.
ATM strike is determined from the CLOSE of the first 15-minute candle (9:15-9:30).
Two strategies run independently; each places and manages its own trade.

━━━ STRATEGY 1 — EMA Compression Sell ━━━
  Case 1: Premium > 200    →  EMA5 ≤ 0.9 × EMA20
  Case 2: 100 < P ≤ 200   →  EMA5 ≤ 0.8 × EMA20
  Case 3: P ≤ 100         →  EMA5 ≤ 0.7 × EMA20
  Signal: red candle that opens above EMA5 AND closes below EMA5

━━━ STRATEGY 2 — Opening-Gap Rejection Sell ━━━
  Sequential (must occur in order):
  1. First 3-min candle (9:15): red, close < EMA5 & EMA20, EMA5 < EMA20
  2. Any later candle: close > EMA5 & EMA20, and EMA5 > EMA20  (recovery)
  3. Red candle closes below BOTH EMA5 & EMA20  (rejection)
  4. ≥2 candles close below BOTH EMA5 & EMA20
  5. Signal: red candle opens above EMA5, closes below BOTH, EMA5 ≤ EMA20

Trade (both strategies):
  Order sequence on ENTRY : buy CE hedge → buy PE hedge → sell CE → sell PE
  Order sequence on EXIT  : buy CE → buy PE → sell CE hedge → sell PE hedge
  SL        : entry_premium + 15 pts (checked on every live tick)
  Cost-lock : if live premium drops 20 pts from entry, SL is moved to entry (breakeven)
  Exit      : 3:00 PM force-exit if SL not hit
"""

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

CLIENT_ID          = os.getenv("FYERS_CLIENT_ID")
ACCESS_TOKEN       = os.getenv("FYERS_ACCESS_TOKEN")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID")

LOT_SIZE        = 130
HEDGE_OFFSET    = 1000           # Points away from ATM for hedges

ATM_DECISION_TIME = datetime.time(9, 30)   # Wait for 15-min candle to close
ENTRY_CUTOFF      = datetime.time(14, 30)  # No new entries at or after 2:30 PM
TRADING_END       = datetime.time(15, 0)   # Force-exit at 3:00 PM

HISTORY_RESOLUTION = "3"         # 3-minute candles

EMA_FAST         = 5
EMA_SLOW         = 20
MIN_BARS_FOR_EMA = 25

SL_POINTS        = 15            # Combined CE+PE premium SL offset
COST_LOCK_PTS    = 20            # Profit pts to trigger SL move to entry (breakeven)

LOG_FILE = "nifty_straddle_live.log"

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


def get_atm_from_15min_candle(fyers_client):
    """
    Fetch the 9:15 AM 15-minute candle for NIFTY50 and return the ATM
    strike (close rounded to nearest 50). Call this at or after 9:30 AM.
    """
    today = datetime.date.today()
    r = fyers_client.history({
        "symbol":      "NSE:NIFTY50-INDEX",
        "resolution":  "15",
        "date_format": "1",
        "range_from":  today.strftime("%Y-%m-%d"),
        "range_to":    today.strftime("%Y-%m-%d"),
        "cont_flag":   "1",
    })
    for c in r.get("candles", []):
        ts = datetime.datetime.fromtimestamp(c[0])
        if ts.hour == 9 and ts.minute == 15:
            close = c[4]
            atm   = round(close / 50) * 50
            logger.info(f"[ATM] 15-min candle close={close:.2f} → ATM={atm}")
            send_telegram(
                f"📊 15-MIN CANDLE (9:15-9:30)\n"
                f"Close={close:.2f} → ATM Strike={atm}"
            )
            return atm
    raise RuntimeError("15-min candle at 9:15 not found in API response — check market is open")


def build_straddle_symbols(atm):
    """Build CE, PE, and hedge symbols from ATM strike."""
    expiry = format_expiry(get_next_expiry())

    ce_sym       = f"NSE:NIFTY{expiry}{atm}CE"
    pe_sym       = f"NSE:NIFTY{expiry}{atm}PE"
    ce_hedge_sym = f"NSE:NIFTY{expiry}{atm + HEDGE_OFFSET}CE"
    pe_hedge_sym = f"NSE:NIFTY{expiry}{atm - HEDGE_OFFSET}PE"

    logger.info(
        f"[SYMBOLS] CE={ce_sym} | PE={pe_sym} | "
        f"CE_HEDGE={ce_hedge_sym} | PE_HEDGE={pe_hedge_sym}"
    )
    send_telegram(
        f"📌 STRADDLE SYMBOLS\n"
        f"CE (sell)  : {ce_sym}\n"
        f"PE (sell)  : {pe_sym}\n"
        f"CE HEDGE   : {ce_hedge_sym}\n"
        f"PE HEDGE   : {pe_hedge_sym}"
    )
    return ce_sym, pe_sym, ce_hedge_sym, pe_hedge_sym


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
# HISTORICAL PREFILL
# =========================================================
def fetch_historical_premium(fyers_client, ce, pe):
    """
    Fetch 3-min combined CE+PE candles for the past 7 days.
    Returns list of dicts sorted by time.
    """
    to_dt   = datetime.datetime.now()
    from_dt = to_dt - datetime.timedelta(days=7)

    def fetch(symbol):
        r = fyers_client.history({
            "symbol":      symbol,
            "resolution":  HISTORY_RESOLUTION,
            "date_format": "1",
            "range_from":  from_dt.strftime("%Y-%m-%d"),
            "range_to":    to_dt.strftime("%Y-%m-%d"),
            "cont_flag":   "1",
        })
        return {c[0]: c for c in r.get("candles", [])}

    ce_map = fetch(ce)
    pe_map = fetch(pe)

    candles = []
    for ts in sorted(set(ce_map) & set(pe_map)):
        dt = datetime.datetime.fromtimestamp(ts)
        if dt.time() < datetime.time(9, 15) or dt.time() > datetime.time(15, 30):
            continue
        c1, c2 = ce_map[ts], pe_map[ts]
        o = c1[1] + c2[1]
        c = c1[4] + c2[4]
        candles.append({
            "time":  dt,
            "open":  o,
            "high":  round(max(o, c), 2),
            "low":   round(min(o, c), 2),
            "close": c,
        })

    logger.info(f"[PREFILL] {len(candles)} combined candles loaded")
    return candles


# =========================================================
# STRATEGY ENGINE
# =========================================================
class StrategyEngine:
    """
    Manages two independent straddle sell strategies on 3-minute candles.

    on_candle() is called:
      - closed=True  : bar just closed; run all phase/entry logic
      - closed=False : bar forming; only check live SL / cost-lock / EOD exit

    Position dict keys:
      entry_premium  : combined premium at entry
      sl_premium     : current SL level (entry + SL_POINTS, then moves to entry on cost-lock)
      cost_locked    : True once SL has been moved to breakeven
    """

    def __init__(self, fyers, ce, pe, ce_hedge, pe_hedge):
        self.fyers    = fyers
        self.ce       = ce
        self.pe       = pe
        self.ce_hedge = ce_hedge
        self.pe_hedge = pe_hedge

        self.candles = deque(maxlen=2000)

        # ── Strategy 1 state ──
        self.position1 = None   # dict when in trade, None otherwise
        self.s1_done   = False  # True after S1 trade completes (one trade per day)

        # ── Strategy 2 state ──
        self.position2 = None
        self.s2_done   = False
        # Phases: 0=init, 1=wait recovery, 2=wait rejection,
        #         3=count below, 4=wait signal, -1=inactive
        self.s2_phase  = 0
        self.s2_count  = 0      # candles below both EMAs counted in phase 3

    # ----------------------------------------------------------
    # EMA
    # ----------------------------------------------------------
    def _compute_ema(self, period):
        closes = [c["close"] for c in self.candles]
        if len(closes) < period:
            return None
        k   = 2 / (period + 1)
        ema = sum(closes[:period]) / period
        for close in closes[period:]:
            ema = round(close * k + ema * (1 - k), 2)
        return ema

    def _get_emas(self):
        return self._compute_ema(EMA_FAST), self._compute_ema(EMA_SLOW)

    # ----------------------------------------------------------
    # ORDER HELPERS
    # ----------------------------------------------------------
    def _enter_trade(self, strategy_num, candle, ema5, ema20, note=""):
        entry = candle["close"]
        sl    = round(entry + SL_POINTS, 2)
        tag   = f"S{strategy_num}"

        logger.info(
            f"[S{strategy_num} ENTRY] entry={entry:.2f} sl={sl:.2f} "
            f"cost_lock_at={entry - COST_LOCK_PTS:.2f} "
            f"EMA5={ema5:.2f} EMA20={ema20:.2f} time={candle['time']} {note}"
        )
        send_telegram(
            f"📉 STRATEGY {strategy_num} ENTRY\n"
            f"Premium  = {entry:.2f}\n"
            f"SL       = {sl:.2f}  (+{SL_POINTS} pts)\n"
            f"CostLock = {entry - COST_LOCK_PTS:.2f}  (SL→cost if premium hits this)\n"
            f"EMA5={ema5:.2f} | EMA20={ema20:.2f}\n"
            f"Time: {candle['time']}"
            + (f"\n{note}" if note else "")
        )

        self.fyers.buy_market(self.ce_hedge, f"{tag}CEHEDGEBUY")
        self.fyers.buy_market(self.pe_hedge, f"{tag}PEHEDGEBUY")
        self.fyers.sell_market(self.ce,      f"{tag}SELLCE")
        self.fyers.sell_market(self.pe,      f"{tag}SELLPE")

        position = {
            "entry_premium": entry,
            "sl_premium":    sl,
            "cost_locked":   False,   # becomes True once cost-lock triggers
        }
        if strategy_num == 1:
            self.position1 = position
        else:
            self.position2 = position

    def _exit_trade(self, strategy_num, reason):
        logger.info(f"[S{strategy_num} EXIT] {reason}")
        send_telegram(f"🛑 STRATEGY {strategy_num} EXIT\nReason: {reason}")

        tag = f"S{strategy_num}"
        self.fyers.buy_market(self.ce,        f"{tag}BUYCE")
        self.fyers.buy_market(self.pe,        f"{tag}BUYPE")
        self.fyers.sell_market(self.ce_hedge, f"{tag}CEHEDGESELL")
        self.fyers.sell_market(self.pe_hedge, f"{tag}PEHEDGESELL")

        if strategy_num == 1:
            self.position1 = None
            self.s1_done   = True
        else:
            self.position2 = None
            self.s2_done   = True

    # ----------------------------------------------------------
    # COST-LOCK: move SL to breakeven once 20 pts in profit
    # ----------------------------------------------------------
    def _check_cost_lock(self, strategy_num, position, live_premium):
        """
        If premium has dropped COST_LOCK_PTS from entry (market moved in our favour)
        and cost-lock has not fired yet, move sl_premium to entry_premium (breakeven).
        Called on every live tick while position is open.
        """
        if position["cost_locked"]:
            return
        if live_premium <= position["entry_premium"] - COST_LOCK_PTS:
            position["sl_premium"]  = position["entry_premium"]
            position["cost_locked"] = True
            logger.info(
                f"[S{strategy_num} COST LOCK] "
                f"Premium {live_premium:.2f} dropped {COST_LOCK_PTS} pts from entry "
                f"{position['entry_premium']:.2f} — SL moved to entry (breakeven)"
            )
            send_telegram(
                f"🔒 STRATEGY {strategy_num} — SL MOVED TO COST\n"
                f"Live premium : {live_premium:.2f}\n"
                f"Entry        : {position['entry_premium']:.2f}\n"
                f"New SL       : {position['entry_premium']:.2f}  (breakeven)"
            )

    # ----------------------------------------------------------
    # STRATEGY 1 — EMA Compression Sell
    # ----------------------------------------------------------
    @staticmethod
    def _s1_case_threshold(premium):
        if premium > 200:
            return 1, 0.9
        elif premium > 100:
            return 2, 0.9
        else:
            return 3, 0.9

    def _run_s1(self, candle, ema5, ema20, t):
        if self.s1_done or self.position1:
            return
        if t >= ENTRY_CUTOFF:
            return

        cn, thr = self._s1_case_threshold(candle["close"])
        if ema5 > thr * ema20:
            return

        is_red = candle["close"] < candle["open"]
        if is_red and candle["open"] > ema5 and candle["close"] < ema5:
            self._enter_trade(1, candle, ema5, ema20, note=f"Case {cn} | EMA5≤{int(thr*100)}%×EMA20")

    # ----------------------------------------------------------
    # STRATEGY 2 — Opening-Gap Rejection Sell
    # ----------------------------------------------------------
    def _run_s2_phases(self, candle, ema5, ema20, t):
        """Advance Strategy 2 state machine on a closed candle."""
        if self.s2_done or self.s2_phase == -1:
            return

        is_first = (
            candle["time"].date() == datetime.date.today() and
            candle["time"].hour   == 9 and
            candle["time"].minute == 15
        )

        # ── Phase 0: first 3-min candle of the day ──
        if self.s2_phase == 0:
            if is_first:
                is_red      = candle["close"] < candle["open"]
                below_both  = candle["close"] < ema5 and candle["close"] < ema20
                ema_bearish = ema5 < ema20
                if is_red and below_both and ema_bearish:
                    self.s2_phase = 1
                    logger.info(
                        f"[S2] P0→P1: first candle red, close={candle['close']:.2f} "
                        f"< EMA5={ema5:.2f} & EMA20={ema20:.2f}, EMA5<EMA20"
                    )
                else:
                    self.s2_phase = -1
                    reasons = []
                    if not is_red:       reasons.append("not red")
                    if not below_both:   reasons.append("close not below both EMAs")
                    if not ema_bearish:  reasons.append("EMA5 not < EMA20")
                    logger.info(f"[S2] P0→N/A: first candle failed ({', '.join(reasons)})")
            elif candle["time"].date() == datetime.date.today():
                # 9:15 candle was missed — invalidate
                self.s2_phase = -1
                logger.info("[S2] P0→N/A: 9:15 candle not seen, invalidating")
            return

        # ── Phase 1: wait for green candle closing above BOTH EMA5 & EMA20,
        #             with EMA5 also above EMA20  (true recovery confirmation) ──
        if self.s2_phase == 1:
            recovery = (
                candle["close"] > ema5 and   # close above EMA5
                candle["close"] > ema20 and  # close above EMA20
                ema5 > ema20                 # EMA5 crossed above EMA20
            )
            if recovery:
                self.s2_phase = 2
                logger.info(
                    f"[S2] P1→P2: recovery at {candle['time']}, "
                    f"close={candle['close']:.2f} > EMA5={ema5:.2f} & EMA20={ema20:.2f}, "
                    f"EMA5 > EMA20"
                )
            return

        # ── Phase 2: wait for rejection red candle below both EMAs ──
        if self.s2_phase == 2:
            is_red     = candle["close"] < candle["open"]
            below_both = candle["close"] < ema5 and candle["close"] < ema20
            if is_red and below_both:
                self.s2_phase = 3
                self.s2_count = 0
                logger.info(
                    f"[S2] P2→P3: rejection red candle at {candle['time']}, "
                    f"close={candle['close']:.2f}"
                )
            return

        # ── Phase 3: count candles closing below both EMAs (need ≥2) ──
        if self.s2_phase == 3:
            if candle["close"] < ema5 and candle["close"] < ema20:
                self.s2_count += 1
                logger.info(
                    f"[S2] P3: count={self.s2_count} at {candle['time']}, "
                    f"close={candle['close']:.2f}"
                )
                if self.s2_count >= 2:
                    self.s2_phase = 4
                    logger.info("[S2] P3→P4: 2 candles confirmed, awaiting entry signal")
            return

        # ── Phase 4: wait for entry signal candle ──
        if self.s2_phase == 4:
            if self.position2 or t >= ENTRY_CUTOFF:
                return
            is_red         = candle["close"] < candle["open"]
            opens_above_e5 = candle["open"]  > ema5
            below_both     = candle["close"] < ema5 and candle["close"] < ema20
            compressed     = ema5 <= ema20
            if is_red and opens_above_e5 and below_both and compressed:
                self._enter_trade(
                    2, candle, ema5, ema20,
                    note=f"EMA5={ema5:.2f} ≤ EMA20={ema20:.2f}"
                )

    # ----------------------------------------------------------
    # MAIN CANDLE HANDLER
    # ----------------------------------------------------------
    def on_candle(self, candle, closed, live_premium=None):
        now = datetime.datetime.now()

        # ── EOD force-exit (tick-level) ──
        if now.time() >= TRADING_END:
            if self.position1:
                self._exit_trade(1, "EOD 3:00 PM force-exit")
            if self.position2:
                self._exit_trade(2, "EOD 3:00 PM force-exit")
            return

        # ── Live tick processing (SL check + cost-lock) ──
        if live_premium is not None:

            # Cost-lock check first — may tighten SL before the SL check below
            if self.position1:
                self._check_cost_lock(1, self.position1, live_premium)
            if self.position2:
                self._check_cost_lock(2, self.position2, live_premium)

            # SL check (uses potentially updated sl_premium after cost-lock)
            if self.position1 and live_premium >= self.position1["sl_premium"]:
                self._exit_trade(
                    1,
                    f"SL HIT — live premium {live_premium:.2f} "
                    f">= sl {self.position1['sl_premium']:.2f}"
                    + (" (cost-lock SL)" if self.position1 and self.position1.get("cost_locked") else "")
                )
            if self.position2 and live_premium >= self.position2["sl_premium"]:
                self._exit_trade(
                    2,
                    f"SL HIT — live premium {live_premium:.2f} "
                    f">= sl {self.position2['sl_premium']:.2f}"
                    + (" (cost-lock SL)" if self.position2 and self.position2.get("cost_locked") else "")
                )

        if not closed:
            return

        # ── Closed candle: add to history ──
        self.candles.append(candle)

        if len(self.candles) < MIN_BARS_FOR_EMA:
            return

        ema5, ema20 = self._get_emas()
        if ema5 is None or ema20 is None:
            return

        logger.info(
            f"[CANDLE] {candle['time']} "
            f"O={candle['open']:.2f} H={candle['high']:.2f} "
            f"L={candle['low']:.2f} C={candle['close']:.2f} "
            f"EMA5={ema5:.2f} EMA20={ema20:.2f} "
            f"S2-phase={self.s2_phase}"
            + (f"(count={self.s2_count})" if self.s2_phase == 3 else "")
        )

        t = candle["time"].time()

        # Candle-level 3PM exit (belt-and-suspenders alongside tick-level check)
        if t >= TRADING_END:
            if self.position1:
                self._exit_trade(1, "EOD 3:00 PM (candle-level)")
            if self.position2:
                self._exit_trade(2, "EOD 3:00 PM (candle-level)")
            return

        # ── Strategy 2 phase transitions ──
        self._run_s2_phases(candle, ema5, ema20, t)

        # ── Strategy 1 entry check ──
        self._run_s1(candle, ema5, ema20, t)


# =========================================================
# MAIN
# =========================================================
if __name__ == "__main__":
    logger.info("[BOOT] NIFTY STRADDLE LIVE STRATEGY STARTED")
    send_telegram("🚀 NIFTY STRADDLE LIVE STRATEGY STARTED")

    fyers = FyersClient()

    # ── Step 1: Wait until 9:30 AM (first 15-min candle closes) ──
    logger.info(f"[WAIT] Waiting until {ATM_DECISION_TIME} for 15-min candle to close...")
    while datetime.datetime.now().time() < ATM_DECISION_TIME:
        time.sleep(1)

    # ── Step 2: Determine ATM from 15-min candle close ──
    atm = get_atm_from_15min_candle(fyers.client)

    # ── Step 3: Build symbols ──
    ce, pe, ce_hedge, pe_hedge = build_straddle_symbols(atm)
    engine = StrategyEngine(fyers, ce, pe, ce_hedge, pe_hedge)

    # ── Step 4: Prefill historical 3-min candles ──
    hist  = fetch_historical_premium(fyers.client, ce, pe)
    today = datetime.date.today()

    # Previous days → seed EMA deque only (don't run strategy logic)
    for c in hist:
        if c["time"].date() < today:
            engine.candles.append(c)

    # Log EMA state after prefill
    if len(engine.candles) >= MIN_BARS_FOR_EMA:
        e5, e20 = engine._get_emas()
        if e5 and e20:
            logger.info(f"[EMA BOOT] EMA5={e5:.2f} EMA20={e20:.2f} GAP={e20 - e5:.2f}")
            send_telegram(
                f"📊 EMA READY\nEMA5={e5:.2f}\nEMA20={e20:.2f}\nGAP={e20 - e5:.2f}"
            )

    # ── Step 5: Feed today's historical candles (9:15–9:30) through engine ──
    # This triggers S2 phase 0 check on the 9:15 candle and warms today's EMA.
    for c in hist:
        if c["time"].date() == today:
            engine.on_candle(c, closed=True)

    # ── Step 6: Websocket for live ticks ──
    last   = {}     # latest LTP per symbol
    candle = None   # current forming 3-min candle (combined premium)

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

        now     = datetime.datetime.now()
        premium = last[ce] + last[pe]

        # EOD: pass live premium for tick-level exit
        if now.time() >= TRADING_END:
            engine.on_candle(candle or {}, closed=False, live_premium=premium)
            return

        epoch  = extract_tick_epoch(msg)
        dt     = datetime.datetime.fromtimestamp(epoch)
        # 3-minute bucket: round minute down to nearest 3
        bucket = dt.replace(second=0, microsecond=0, minute=(dt.minute // 3) * 3)

        if candle is None or candle["time"] != bucket:
            if candle:
                # Previous candle just closed
                engine.on_candle(candle, closed=True, live_premium=None)
            # Open new candle
            candle = {
                "time":  bucket,
                "open":  premium,
                "high":  premium,
                "low":   premium,
                "close": premium,
            }
        else:
            candle["high"]  = max(candle["high"], premium)
            candle["low"]   = min(candle["low"],  premium)
            candle["close"] = premium

        # Live tick: cost-lock + SL check
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
