"""
NIFTY ATM EMA21 OPTION-SELLING STRATEGY
=======================================
5-minute candles. EMA21 is computed on the INDIVIDUAL option premium of the
ATM CE and the ATM PE. The two legs run as fully independent single-leg
SELL strategies.

━━━ STRIKE SELECTION ━━━
  Uses the CLOSE of the NIFTY50 index 9:15 5-min candle (closes at 9:20 AM).
  ATM = round(close_920 / 50) * 50   → ATM CE and ATM PE, fixed for the day.

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
  Entry  : a later RED candle that closes below the first candle's LOW
           (set S2_BREAK_LEVEL = "close" to use the first candle's close)
           → SELL at market.
  SL     : high of the entry candle, checked every tick.
  Exit   : SL hit, OR a 5-min candle closes above EMA21, OR 3:00 PM.
  Trades : only 1 per leg.

━━━ HEDGE (expiry day only) ━━━
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

LOT_SIZE      = 65     # qty sent to the API
STRIKE_STEP   = 50     # Nifty strike interval
INDEX_SYMBOL  = "NSE:NIFTY50-INDEX"

DECISION_TIME   = datetime.time(9, 20, 5)  # just after 9:20 candle close
FIRST_CANDLE    = datetime.time(9, 15)     # 5-min candle 9:15–9:20
TRADING_END     = datetime.time(15, 0)     # 3:00 PM EOD force-exit

HISTORY_RESOLUTION = "5"
CANDLE_MINUTES     = 5

EMA_PERIOD = 21

S1_MAX_TRADES   = 2        # Scenario 1 trades per leg per day
S2_MAX_TRADES   = 1        # Scenario 2: only 1 trade
S2_BREAK_LEVEL  = "low"    # "low" or "close" of the first candle

HEDGE_DISTANCE  = 300      # points away for the expiry-day hedge

LOG_FILE = "nifty_atm_ema21_option_sell.log"

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


def is_expiry_day():
    return datetime.date.today() == get_next_expiry()


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
                    logger.info(f"[SPOT] NIFTY50 9:20 candle close = {close:.2f}")
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
        self.trades_taken = 0
        self.lock         = threading.RLock()

        # While replaying today's already-closed candles at startup (e.g. a
        # restart at 10:30) we only rebuild scenario / day high — no orders
        # are placed on old candles. Live candles turn this off.
        self.replaying    = False

    @property
    def max_trades(self):
        return {1: S1_MAX_TRADES, 2: S2_MAX_TRADES}.get(self.scenario, 0)

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
            lvl = candle[S2_BREAK_LEVEL]
            reason = (f"RED & closed below EMA21 → wait for red close below "
                      f"first-candle {S2_BREAK_LEVEL} {lvl:.2f}")
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
    def _enter_trade(self, candle, sl, ema, why):
        with self.lock:
            if self.position:
                return
            entry = candle["close"]
            risk  = sl - entry
            if risk <= 0:
                logger.info(f"[{self.label} SKIP] SL {sl:.2f} <= entry {entry:.2f}")
                return

            n   = self.trades_taken + 1
            tag = f"ATM{self.label}S{self.scenario}T{n}"

            # Expiry-day hedge first — never sell naked if it fails
            hedge_bought = False
            if self.hedge_symbol:
                resp = self.fyers.buy_market(self.hedge_symbol, f"{tag}H")
                if not order_ok(resp):
                    logger.error(f"[{self.label}] hedge buy failed — short NOT placed")
                    send_telegram(f"❗ {self.label} hedge buy failed — skipping short")
                    return
                hedge_bought = True

            resp = self.fyers.sell_market(self.symbol, tag)
            if not order_ok(resp):
                logger.error(f"[{self.label}] short sell failed")
                if hedge_bought:
                    self.fyers.sell_market(self.hedge_symbol, f"{tag}HX")
                return

            self.trades_taken = n
            self.position = {
                "entry_price":  entry,
                "sl_price":     sl,
                "entry_time":   candle["time"],
                "tag":          tag,
                "hedge":        hedge_bought,
            }

            hedge_txt = f"\nHedge bought: {self.hedge_symbol}" if hedge_bought else ""
            logger.info(
                f"[{self.label} SELL #{n}] scenario={self.scenario} symbol={self.symbol} "
                f"entry={entry:.2f} sl={sl:.2f} risk={risk:.2f} EMA21={ema:.2f} "
                f"candle={candle['time'].strftime('%H:%M')} ({why})"
                f"{' hedge=' + self.hedge_symbol if hedge_bought else ''}"
            )
            send_telegram(
                f"📉 {self.label} SELL #{n} (Scenario {self.scenario}) — {self.symbol}\n"
                f"Entry = {entry:.2f}\n"
                f"SL    = {sl:.2f} ({why})\n"
                f"EMA21 = {ema:.2f}\n"
                f"Signal candle: {candle['time'].strftime('%H:%M')}{hedge_txt}"
            )

    def _exit_trade(self, reason, price=None):
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

            logger.info(f"[{self.label} EXIT] symbol={self.symbol} {reason}{pnl_txt}")
            send_telegram(f"🛑 {self.label} EXIT — {self.symbol}\nReason: {reason}{pnl_txt}")

            if self.trades_taken >= self.max_trades:
                logger.info(f"[{self.label}] max trades for scenario {self.scenario} done")

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
                f"SL HIT — LTP {ltp:.2f} >= SL {self.position['sl_price']:.2f}", ltp
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
            f"[{self.label} CANDLE] {candle['time'].strftime('%H:%M')} "
            f"O={candle['open']:.2f} H={candle['high']:.2f} "
            f"L={candle['low']:.2f} C={candle['close']:.2f} EMA21={ema_txt} "
            f"dayHigh={self.day_high} scenario={self.scenario} "
            f"pos={'YES' if self.position else 'no'} trades={self.trades_taken}"
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

        # Scenario 2 exit: candle closes above EMA21
        if self.position:
            if self.scenario == 2 and candle["close"] > ema:
                self._exit_trade(
                    f"candle closed above EMA21 ({candle['close']:.2f} > {ema:.2f})",
                    candle["close"]
                )
            return

        if self.trades_taken >= self.max_trades:
            return

        if self.replaying:
            return   # never trade on a replayed (old) candle

        # A candle closing at 3:00 PM must not open a trade
        candle_close_time = (candle["time"] + datetime.timedelta(minutes=CANDLE_MINUTES)).time()
        if candle_close_time >= TRADING_END:
            return

        is_red = candle["close"] < candle["open"]
        if not is_red:
            return

        if self.scenario == 1 and candle["close"] < ema:
            self._enter_trade(candle, self.day_high, ema, "SL = option day high")

        elif self.scenario == 2:
            level = self.first_candle[S2_BREAK_LEVEL]
            if candle["close"] < level:
                self._enter_trade(candle, candle["high"], ema, "SL = entry candle high")


# =========================================================
# MAIN
# =========================================================
if __name__ == "__main__":
    logger.info("[BOOT] NIFTY ATM EMA21 OPTION-SELLING STRATEGY STARTED")
    send_telegram("🚀 NIFTY ATM EMA21 OPTION-SELLING STRATEGY STARTED")

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
        f"📌 STRIKE SELECTION (9:20 candle close)\n"
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
