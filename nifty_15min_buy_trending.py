"""
NIFTY OPTION BUY — LIVE STRATEGY (CE + PE, both sides)
========================================================
15-minute candles, EMA5 & EMA20 on each side's ATM option close price.
ATM strike is determined from the CLOSE of the 9:15 AM 15-minute candle.

Entry Signal (evaluated independently for CE and PE on each closed candle):
  1. Green candle (Close > Open)
  2. Candle OPENS below EMA5
  3. Candle CLOSES above EMA5
  4. EMA5 >= 101% of EMA20  (EMA5 at least 1% above EMA20)

Strike selection at entry:
  • If ATM premium >= MIN_PREMIUM (120): trade ATM
  • Else: scan deeper ITM strikes in steps of 50 and pick the one with the
    LOWEST premium that is still >= MIN_PREMIUM (closest to 120 from above)
      – CE side: ATM-50, ATM-100, ...  (ITM = lower strike = higher premium)
      – PE side: ATM+50, ATM+100, ...  (ITM = higher strike = higher premium)

Constraints:
  • Max MAX_TRADES_PER_STRIKE (2) trades on any one strike per session
  • One open position per side at a time; re-entry is allowed after close

Entry   : market order placed on the FIRST TICK of the NEXT 15-min candle
Target  : entry_price × (1 + TARGET_PCT)  — checked on every live tick
SL      : entry_price × (1 − SL_PCT)      — checked on every live tick
Exit    : force-close at 3:00 PM if SL/target not already hit
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

LOT_SIZE              = 75
NUM_LOTS              = 1

EMA_FAST              = 5
EMA_SLOW              = 20
MIN_BARS              = 25

EMA5_ABOVE_EMA20_MIN_PCT = 1.01  # EMA5 must be at least 1% ABOVE EMA20
TARGET_PCT            = 0.30   # +30% profit target
SL_PCT                = 0.30   # −30% stop loss
MIN_PREMIUM           = 120    # minimum option premium; scan ITM if below this
MAX_TRADES_PER_STRIKE = 2      # max trades per strike symbol per session
MAX_ITM_SCAN          = 20     # how many ITM strikes to scan when searching

ATM_DECISION_TIME = datetime.time(9, 30)
ENTRY_CUTOFF      = datetime.time(14, 45)
TRADING_END       = datetime.time(15, 0)

LOG_FILE = "nifty_option_buy_live.log"

# =========================================================
# LOGGING
# =========================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout),
    ],
    force=True,
)
logger = logging.getLogger(__name__)


def send_telegram(msg):
    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        try:
            requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={"chat_id": TELEGRAM_CHAT_ID, "text": msg[:4000]},
                timeout=3,
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
    today  = datetime.date.today()
    days   = (1 - today.weekday()) % 7
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
    """Fetch 9:15 AM 15-min candle for NIFTY50 and return ATM strike (rounded to 50)."""
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
            logger.info(f"[ATM] 15-min close={close:.2f} → ATM={atm}")
            send_telegram(f"📊 15-MIN CANDLE (9:15-9:30)\nClose={close:.2f} → ATM={atm}")
            return atm
    raise RuntimeError("15-min candle at 9:15 not found — is market open?")


# =========================================================
# 15-MIN CANDLE BUCKET
# =========================================================
_MARKET_OPEN_MINS = 9 * 60 + 15

def to_15min_bucket(dt):
    total_mins  = dt.hour * 60 + dt.minute
    offset      = ((total_mins - _MARKET_OPEN_MINS) // 15) * 15
    bucket_mins = _MARKET_OPEN_MINS + offset
    return dt.replace(hour=bucket_mins // 60, minute=bucket_mins % 60,
                      second=0, microsecond=0)


# =========================================================
# FYERS CLIENT
# =========================================================
class FyersClient:
    def __init__(self):
        self.client = fyersModel.FyersModel(
            client_id=CLIENT_ID,
            token=ACCESS_TOKEN,
            is_async=False,
            log_path="",
        )
        self.auth = f"{CLIENT_ID}:{ACCESS_TOKEN}"

    def buy_market(self, symbol, tag):
        return self.client.place_order({
            "symbol":      symbol,
            "qty":         LOT_SIZE * NUM_LOTS,
            "type":        2,
            "side":        1,
            "productType": "INTRADAY",
            "validity":    "DAY",
            "orderTag":    tag,
        })

    def sell_market(self, symbol, tag):
        return self.client.place_order({
            "symbol":      symbol,
            "qty":         LOT_SIZE * NUM_LOTS,
            "type":        2,
            "side":        -1,
            "productType": "INTRADAY",
            "validity":    "DAY",
            "orderTag":    tag,
        })

    def get_ltp_batch(self, symbols):
        """Fetch LTPs for a list of symbols. Returns {symbol: ltp}."""
        if not symbols:
            return {}
        try:
            r   = self.client.quotes({"symbols": ",".join(symbols)})
            out = {}
            for item in r.get("d", []):
                n   = item.get("n", "")
                ltp = item.get("v", {}).get("lp")
                if n and ltp is not None:
                    out[n] = float(ltp)
            return out
        except Exception as e:
            logger.warning(f"[QUOTES] batch fetch error: {e}")
            return {}


# =========================================================
# HISTORICAL PREFILL
# =========================================================
def fetch_historical_option(fyers_client, symbol):
    """Fetch 15-min candles for past 7 days to seed EMA history."""
    to_dt   = datetime.datetime.now()
    from_dt = to_dt - datetime.timedelta(days=7)
    r = fyers_client.history({
        "symbol":      symbol,
        "resolution":  "15",
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
        candles.append({"time": dt, "open": c[1], "high": c[2], "low": c[3], "close": c[4]})
    candles.sort(key=lambda x: x["time"])
    logger.info(f"[PREFILL] {len(candles)} candles for {symbol}")
    return candles


# =========================================================
# PER-SIDE STRATEGY ENGINE
# =========================================================
class OptionSideEngine:
    """
    Manages signal detection, strike selection, and position for one side (CE or PE).

    Signal tracking runs on the ATM option (base_sym).
    The actual trade may be placed on a different strike (position["sym"])
    if the ATM premium is below MIN_PREMIUM.
    """

    def __init__(self, fyers, subscribe_fn, side, atm, expiry_str):
        self.fyers        = fyers
        self._subscribe   = subscribe_fn   # callable(sym) to add WS subscription
        self.side         = side           # "CE" or "PE"
        self.atm          = atm
        self.expiry_str   = expiry_str
        self.base_sym     = f"NSE:NIFTY{expiry_str}{atm}{side}"

        self.candles       = deque(maxlen=2000)
        self.position      = None    # {sym, entry_price, target_price, sl_price}
        self.pending_entry = False
        self._sig_ema5     = None
        self._sig_ema20    = None

        # Number of trades taken on each strike today: {symbol: count}
        self.trade_counts  = {}

    # ----------------------------------------------------------
    # EMA helpers
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
    # Strike selection
    # ----------------------------------------------------------
    def _find_eligible_strike(self):
        """
        Build candidate strikes in ITM direction, batch-fetch their LTPs,
        then return (sym, ltp) with the LOWEST ltp that is still >= MIN_PREMIUM
        and has < MAX_TRADES_PER_STRIKE trades today.

        CE: scan ATM → ATM-50 → ATM-100 ...  (lower strike = deeper ITM = higher premium)
        PE: scan ATM → ATM+50 → ATM+100 ...  (higher strike = deeper ITM = higher premium)
        """
        step = -50 if self.side == "CE" else 50

        # Build candidate symbols (skip strikes already at trade limit)
        candidate_syms = []
        for i in range(MAX_ITM_SCAN):
            strike = self.atm + step * i
            sym    = f"NSE:NIFTY{self.expiry_str}{strike}{self.side}"
            if self.trade_counts.get(sym, 0) < MAX_TRADES_PER_STRIKE:
                candidate_syms.append(sym)

        if not candidate_syms:
            return None, None

        ltp_map = self.fyers.get_ltp_batch(candidate_syms)

        # Filter to those with premium >= MIN_PREMIUM and pick minimum (closest to 120)
        eligible = [
            (ltp_map[s], s)
            for s in candidate_syms
            if s in ltp_map and ltp_map[s] >= MIN_PREMIUM
        ]
        if not eligible:
            return None, None

        eligible.sort()          # ascending by ltp → first entry is closest to MIN_PREMIUM
        ltp, sym = eligible[0]
        return sym, ltp

    # ----------------------------------------------------------
    # Orders
    # ----------------------------------------------------------
    def _enter_trade(self, sym, entry_price):
        target = round(entry_price * (1 + TARGET_PCT), 2)
        sl     = round(entry_price * (1 - SL_PCT), 2)

        replaced = sym != self.base_sym
        logger.info(
            f"[{self.side} ENTRY] {sym} entry={entry_price:.2f} "
            f"target={target:.2f} sl={sl:.2f} "
            f"ema5={self._sig_ema5} ema20={self._sig_ema20}"
            + (" [REPLACEMENT STRIKE]" if replaced else "")
        )
        send_telegram(
            f"📈 {self.side} ENTRY\n"
            f"Symbol : {sym}"
            + (" ← replacement strike" if replaced else "") + "\n"
            f"Entry  : {entry_price:.2f}\n"
            f"Target : {target:.2f}  (+{TARGET_PCT*100:.0f}%)\n"
            f"SL     : {sl:.2f}  (−{SL_PCT*100:.0f}%)\n"
            f"EMA5={self._sig_ema5:.2f} | EMA20={self._sig_ema20:.2f} "
            f"(EMA5={self._sig_ema5/self._sig_ema20*100:.1f}% of EMA20)"
        )

        self.fyers.buy_market(sym, f"{self.side}BUY")

        self.position = {
            "sym":          sym,
            "entry_price":  entry_price,
            "target_price": target,
            "sl_price":     sl,
        }
        self.trade_counts[sym] = self.trade_counts.get(sym, 0) + 1

        # Subscribe replacement strike to WebSocket for tick-level monitoring
        if replaced:
            self._subscribe(sym)

    def _exit_trade(self, reason, ltp=None):
        if self.position is None:
            return
        sym   = self.position["sym"]
        entry = self.position["entry_price"]
        pnl   = round((ltp - entry) * LOT_SIZE * NUM_LOTS, 2) if ltp is not None else None

        logger.info(
            f"[{self.side} EXIT] {sym} reason={reason}"
            + (f" ltp={ltp:.2f} pnl={pnl:+.2f}" if ltp is not None else "")
        )
        send_telegram(
            f"🔴 {self.side} EXIT — {reason}\n"
            f"Symbol : {sym}\n"
            f"Entry  : {entry:.2f}"
            + (f"\nLTP    : {ltp:.2f}\nP&L    : {pnl:+.2f}" if ltp is not None else "")
        )

        self.fyers.sell_market(sym, f"{self.side}SELL")
        self.position      = None
        self.pending_entry = False

    # ----------------------------------------------------------
    # Candle close → signal detection
    # ----------------------------------------------------------
    def on_closed_candle(self, candle):
        self.candles.append(candle)

        if len(self.candles) < MIN_BARS:
            return

        ema5, ema20 = self._get_emas()
        if ema5 is None or ema20 is None:
            return

        t = candle["time"].time()
        logger.info(
            f"[{self.side} CANDLE] {candle['time'].strftime('%H:%M')} "
            f"O={candle['open']:.2f} C={candle['close']:.2f} EMA5={ema5:.2f} EMA20={ema20:.2f} "
            f"ratio={ema5/ema20*100:.1f}%"
            + (" [IN POS]" if self.position else "")
            + (" [PENDING]" if self.pending_entry else "")
        )

        if t >= TRADING_END:
            if self.position:
                self._exit_trade("EOD 3:00 PM (candle-level)", candle["close"])
            self.pending_entry = False
            return

        # Signal check — only when flat and no pending entry already queued
        # Conditions: green candle, opens below EMA5, closes above EMA5, EMA5 >= 101% of EMA20
        is_green          = candle["close"] > candle["open"]
        opens_below_ema5  = candle["open"]  < ema5
        closes_above_ema5 = candle["close"] > ema5
        ema5_above_ema20  = ema5 >= EMA5_ABOVE_EMA20_MIN_PCT * ema20

        if (
            self.position is None
            and not self.pending_entry
            and t < ENTRY_CUTOFF
            and is_green
            and opens_below_ema5
            and closes_above_ema5
            and ema5_above_ema20
        ):
            self.pending_entry = True
            self._sig_ema5     = ema5
            self._sig_ema20    = ema20
            logger.info(
                f"[{self.side} SIGNAL] Green candle O={candle['open']:.2f} C={candle['close']:.2f} "
                f"crosses EMA5={ema5:.2f} | "
                f"EMA5/EMA20={ema5/ema20*100:.1f}% >= {EMA5_ABOVE_EMA20_MIN_PCT*100:.0f}% "
                f"→ pending entry at next candle open"
            )
            send_telegram(
                f"⚡ {self.side} SIGNAL\n"
                f"Green candle: O={candle['open']:.2f} → C={candle['close']:.2f}\n"
                f"EMA5={ema5:.2f} (crossed above)\n"
                f"EMA5/EMA20 = {ema5/ema20*100:.1f}% (>= {EMA5_ABOVE_EMA20_MIN_PCT*100:.0f}%)\n"
                f"Awaiting next candle open..."
            )

    # ----------------------------------------------------------
    # First tick of next candle → execute pending entry
    # ----------------------------------------------------------
    def on_new_candle_open(self):
        """Called once on the first tick of each new 15-min candle (base_sym tick)."""
        if not self.pending_entry or self.position is not None:
            return
        if datetime.datetime.now().time() >= TRADING_END:
            logger.info(f"[{self.side} PENDING] Cancelled — past TRADING_END")
            self.pending_entry = False
            return

        sym, ltp = self._find_eligible_strike()
        if sym is None:
            logger.info(
                f"[{self.side} PENDING] No eligible strike found "
                f"(all candidates below MIN_PREMIUM={MIN_PREMIUM} or at trade limit)"
            )
            self.pending_entry = False
            return

        self._enter_trade(sym, ltp)
        self.pending_entry = False

    # ----------------------------------------------------------
    # Live tick on position symbol → target / SL / EOD
    # ----------------------------------------------------------
    def on_position_tick(self, ltp):
        if self.position is None:
            return
        if datetime.datetime.now().time() >= TRADING_END:
            self._exit_trade("EOD 3:00 PM force-exit", ltp)
            return
        if ltp >= self.position["target_price"]:
            self._exit_trade("TARGET HIT", ltp)
        elif ltp <= self.position["sl_price"]:
            self._exit_trade("SL HIT", ltp)


# =========================================================
# MAIN
# =========================================================
if __name__ == "__main__":
    logger.info("[BOOT] NIFTY OPTION BUY LIVE — CE + PE — STARTED")
    send_telegram("🚀 NIFTY OPTION BUY LIVE\nTracking: CE + PE\nStarting up...")

    fyers = FyersClient()

    # ── Step 1: Wait until 9:30 AM ──
    logger.info(f"[WAIT] Waiting until {ATM_DECISION_TIME} for 15-min candle to close...")
    while datetime.datetime.now().time() < ATM_DECISION_TIME:
        time.sleep(1)

    # ── Step 2: Determine ATM ──
    atm         = get_atm_from_15min_candle(fyers.client)
    expiry_str  = format_expiry(get_next_expiry())
    ce_base_sym = f"NSE:NIFTY{expiry_str}{atm}CE"
    pe_base_sym = f"NSE:NIFTY{expiry_str}{atm}PE"

    logger.info(f"[SYMBOLS] CE base={ce_base_sym} | PE base={pe_base_sym}")
    send_telegram(
        f"📌 BASE SYMBOLS\nCE: {ce_base_sym}\nPE: {pe_base_sym}\n"
        f"(replacement strikes will be found dynamically if premium < {MIN_PREMIUM})"
    )

    # ── WebSocket subscription set (managed before engines so subscribe_fn is ready) ──
    subscribed_syms = set()
    ws = None   # assigned after engines are built; subscribe_fn closes over it

    def subscribe_sym(sym):
        if sym not in subscribed_syms and ws is not None:
            ws.subscribe(symbols=[sym], data_type="SymbolUpdate")
            subscribed_syms.add(sym)
            logger.info(f"[WS] Subscribed to {sym}")

    # ── Step 3: Build engines ──
    ce_engine = OptionSideEngine(fyers, subscribe_sym, "CE", atm, expiry_str)
    pe_engine = OptionSideEngine(fyers, subscribe_sym, "PE", atm, expiry_str)

    # ── Step 4: Prefill historical 15-min candles ──
    today = datetime.date.today()
    for engine, sym in [(ce_engine, ce_base_sym), (pe_engine, pe_base_sym)]:
        hist = fetch_historical_option(fyers.client, sym)
        # Previous days → seed EMA only
        for c in hist:
            if c["time"].date() < today:
                engine.candles.append(c)
        # Today's already-closed candles → run full engine logic
        for c in hist:
            if c["time"].date() == today:
                engine.on_closed_candle(c)

    for engine, label in [(ce_engine, "CE"), (pe_engine, "PE")]:
        e5, e20 = engine._get_emas()
        if e5 and e20:
            logger.info(f"[EMA BOOT {label}] EMA5={e5:.2f} EMA20={e20:.2f} ratio={e5/e20*100:.1f}%")
    send_telegram(
        f"📊 EMA READY\n"
        f"CE — EMA5={ce_engine._get_emas()[0]} EMA20={ce_engine._get_emas()[1]}\n"
        f"PE — EMA5={pe_engine._get_emas()[0]} EMA20={pe_engine._get_emas()[1]}"
    )

    # ── Step 5: WebSocket ──
    ce_candle = None
    pe_candle = None

    def on_tick(msg):
        global ce_candle, pe_candle

        if "symbol" not in msg or "ltp" not in msg:
            return

        sym = msg["symbol"]
        ltp = float(msg["ltp"])
        now = datetime.datetime.now()

        # ── EOD: only check open positions ──
        if now.time() >= TRADING_END:
            if ce_engine.position and sym == ce_engine.position["sym"]:
                ce_engine.on_position_tick(ltp)
            if pe_engine.position and sym == pe_engine.position["sym"]:
                pe_engine.on_position_tick(ltp)
            return

        bucket = to_15min_bucket(now)

        # ── CE side: candle tracking ──
        if sym == ce_base_sym:
            if ce_candle is None or ce_candle["time"] != bucket:
                if ce_candle is not None:
                    ce_engine.on_closed_candle(ce_candle)
                ce_candle = {"time": bucket, "open": ltp, "high": ltp, "low": ltp, "close": ltp}
                ce_engine.on_new_candle_open()
            else:
                ce_candle["high"]  = max(ce_candle["high"], ltp)
                ce_candle["low"]   = min(ce_candle["low"],  ltp)
                ce_candle["close"] = ltp

        # ── PE side: candle tracking ──
        if sym == pe_base_sym:
            if pe_candle is None or pe_candle["time"] != bucket:
                if pe_candle is not None:
                    pe_engine.on_closed_candle(pe_candle)
                pe_candle = {"time": bucket, "open": ltp, "high": ltp, "low": ltp, "close": ltp}
                pe_engine.on_new_candle_open()
            else:
                pe_candle["high"]  = max(pe_candle["high"], ltp)
                pe_candle["low"]   = min(pe_candle["low"],  ltp)
                pe_candle["close"] = ltp

        # ── Position tick: target/SL check (works for both ATM and replacement strikes) ──
        if ce_engine.position and sym == ce_engine.position["sym"]:
            ce_engine.on_position_tick(ltp)
        if pe_engine.position and sym == pe_engine.position["sym"]:
            pe_engine.on_position_tick(ltp)

    def on_open():
        initial_syms = [ce_base_sym, pe_base_sym]
        ws.subscribe(symbols=initial_syms, data_type="SymbolUpdate")
        subscribed_syms.update(initial_syms)
        ws.keep_running()

    ws = data_ws.FyersDataSocket(
        access_token=fyers.auth,
        on_connect=on_open,
        on_message=on_tick,
        log_path="",
    )
    ws.connect()
