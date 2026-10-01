# =========================================================
# SENSEX ATM CE/PE  +  CPR R1 BREAKOUT
# 5-MIN TIMEFRAME, LIVE (WEBSOCKET)
# =========================================================
#
# STRATEGY SUMMARY
# -----------------
# 1. At 09:16 AM, capture Sensex spot LTP and round to the nearest strike
#    (step 100) to get ATM. Both the ATM CE and the ATM PE are tracked,
#    each as an independent leg. No new strikes are picked later.
#
# 2. CPR is calculated for EACH option contract using the same method as
#    get_last_trading_day_cpr() in the NIFTY CPR multi-scenario script:
#       - Walk back one day at a time (max MAX_CPR_LOOKBACK_DAYS) until a
#         day with data is found -- that is the last trading day.
#       - That day's 5-MINUTE candles are combined into one day:
#           H = highest high, L = lowest low, C = last candle's close.
#       - calculate_cpr(H, L, C):
#           P  = (H + L + C) / 3
#           BC = (H + L) / 2,  TC = 2P - BC   (BC = min, TC = max)
#           R1 = 2P - L        S1 = 2P - H
#           R2 = P + (H - L)   S2 = P - (H - L)
#
# 3. ENTRY (on each leg's own 5-min candles):
#       A closed candle with OPEN < R1 and CLOSE > R1 (crossed R1 upward)
#       -> buy that option at the candle's close.
#       No new entries on candles closing at/after 3:00 PM.
#
# 4. RISK:
#       SL     = low of the breakout candle
#       Target = entry + 1.5 x (entry - SL)        (1 : 1.5 risk:reward)
#       SL / target are checked on every tick.
#
# 5. 3:00 PM: any open position is force-closed, pending orders for the
#    leg are cancelled, and the leg stops for the day.
#
# 6. SEED CANDLE: the forming candle at connect time is seeded from REST
#    history so the first candle reflects the full bucket's O/H/L/C.
# =========================================================

import datetime
import time as time_module
import os
import sys
import logging
import requests
from dotenv import load_dotenv
from fyers_apiv3 import fyersModel
from fyers_apiv3.FyersWebsocket import data_ws

# ================= CONFIG =================
load_dotenv()

CLIENT_ID = os.getenv("FYERS_CLIENT_ID")
ACCESS_TOKEN = os.getenv("FYERS_ACCESS_TOKEN")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

INDEX_SYMBOL = "BSE:SENSEX-INDEX"

TIMEFRAME_MIN = 5
STRIKE_STEP = 100

DECISION_TIME = datetime.time(9, 16, 0)
NO_NEW_TRADES_TIME = datetime.time(15, 0)
HARD_EXIT_TIME = datetime.time(15, 0)

TARGET_RR = 1.5              # target = 1.5 x risk (entry - SL)
MAX_TRADES_PER_LEG = 1       # trades allowed per leg (CE / PE) per day
MAX_CPR_LOOKBACK_DAYS = 10   # days walked back to find the last trading day
CPR_RESOLUTION = "5"         # 5-minute candles used to build prev-day H/L/C

EXIT_RETRY_SECONDS = 3       # wait between exit-order retries
MAX_EXIT_RETRIES = 5         # after this many failed exits, alert to close manually

LOT_SIZE = 20
LOTS = 1
QTY = LOT_SIZE * LOTS

LOG_FILE = "sensex_atm_cpr_r1_breakout.log"

# ================= LOGGING =================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler(sys.stdout)],
    force=True
)
logger = logging.getLogger(__name__)


# ================= TELEGRAM =================
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


# ================= FYERS =================
def sanitize_order_tag(tag):
    cleaned = "".join(ch for ch in str(tag) if ch.isalnum())
    return cleaned[:20] if cleaned else "TRADE"


class Fyers:
    def __init__(self):
        self.client = fyersModel.FyersModel(
            client_id=CLIENT_ID, token=ACCESS_TOKEN, is_async=False, log_path=""
        )
        self.auth = f"{CLIENT_ID}:{ACCESS_TOKEN}"

    def _market_order(self, symbol, qty, side, tag):
        try:
            return self.client.place_order({
                "symbol": symbol,
                "qty": qty,
                "type": 2,
                "side": side,
                "productType": "INTRADAY",
                "validity": "DAY",
                "orderTag": sanitize_order_tag(tag)
            })
        except Exception as e:
            logger.error(f"place_order failed for {symbol}: {e}")
            return None

    def buy(self, symbol, qty, tag):
        return self._market_order(symbol, qty, 1, tag)

    def sell(self, symbol, qty, tag):
        return self._market_order(symbol, qty, -1, tag)

    def get_pending_orders(self, symbol=None):
        """Status codes: 1=Cancelled, 2=Traded, 5=Rejected, 6=Pending."""
        try:
            resp = self.client.orderbook()
        except Exception as e:
            logger.warning(f"orderbook() failed: {e}")
            return []
        orders = (resp or {}).get("orderBook", []) or []
        pending = [o for o in orders if o.get("status") not in (1, 2, 5)]
        if symbol:
            pending = [o for o in pending if o.get("symbol") == symbol]
        return pending

    def get_net_qty(self, symbol):
        """Net open quantity for `symbol` from the positions book.
        Returns None if the positions call fails (unknown)."""
        try:
            resp = self.client.positions()
        except Exception as e:
            logger.warning(f"positions() failed: {e}")
            return None
        if not isinstance(resp, dict) or resp.get("s") != "ok":
            logger.warning(f"positions() returned: {resp}")
            return None
        for p in resp.get("netPositions", []) or []:
            if p.get("symbol") == symbol:
                return p.get("netQty", 0)
        return 0

    def cancel_pending_orders(self, symbol=None):
        pending = self.get_pending_orders(symbol)
        for o in pending:
            try:
                self.client.cancel_order({"id": o.get("id")})
                logger.info(f"Cancelled pending order {o.get('id')} for {o.get('symbol')}")
            except Exception as e:
                logger.warning(f"Failed to cancel order {o.get('id')}: {e}")
        return pending


def order_ok(resp):
    return isinstance(resp, dict) and resp.get("s") == "ok"


# ================= EXPIRY HELPERS =================
SPECIAL_MARKET_HOLIDAYS = {
    datetime.date(2026, 1, 26), datetime.date(2026, 3, 3), datetime.date(2026, 3, 26),
    datetime.date(2026, 3, 31), datetime.date(2026, 4, 14), datetime.date(2026, 5, 1),
    datetime.date(2026, 5, 28), datetime.date(2026, 6, 26), datetime.date(2026, 9, 14),
    datetime.date(2026, 10, 2), datetime.date(2026, 11, 24), datetime.date(2026, 12, 25),
}


def is_last_thursday(d):
    return d.weekday() == 3 and (d + datetime.timedelta(days=7)).month != d.month


def get_next_expiry():
    today = datetime.date.today()
    days_ahead = (3 - today.weekday()) % 7
    expiry = today + datetime.timedelta(days=days_ahead)
    if expiry in SPECIAL_MARKET_HOLIDAYS:
        expiry -= datetime.timedelta(days=1)
    return expiry


def format_expiry(expiry):
    yy = expiry.strftime("%y")
    if is_last_thursday(expiry):
        return f"{yy}{expiry.strftime('%b').upper()}"
    m_token = {10: "O", 11: "N", 12: "D"}.get(expiry.month, str(expiry.month))
    return f"{yy}{m_token}{expiry.day:02d}"


# ================= ATM STRIKE SELECTION =================
def get_atm_symbols(fyers):
    q = fyers.client.quotes({"symbols": INDEX_SYMBOL})
    index_ltp = float(q["d"][0]["v"]["lp"])
    atm = round(index_ltp / STRIKE_STEP) * STRIKE_STEP

    expiry = get_next_expiry()
    exp_token = format_expiry(expiry)

    atm_ce = f"BSE:SENSEX{exp_token}{atm}CE"
    atm_pe = f"BSE:SENSEX{exp_token}{atm}PE"

    msg = (
        f"📌 ATM STRIKE SELECTION (09:16 spot)\n"
        f"Index LTP : {index_ltp}\n"
        f"ATM       : {atm}\n"
        f"Expiry    : {expiry} ({exp_token})\n"
        f"ATM CE    : {atm_ce}\n"
        f"ATM PE    : {atm_pe}"
    )
    logger.info(msg)
    send_telegram(msg)
    return atm_ce, atm_pe, index_ltp


# ================= CPR FROM HISTORICAL DATA =================
# Same method as get_last_trading_day_cpr() in the NIFTY CPR script, applied
# to each option contract, using 5-minute candles instead of a daily candle.
def fetch_prev_day_ohlc(fyers, symbol):
    """Walk back day by day (max MAX_CPR_LOOKBACK_DAYS) until a day with
    5-minute candles is found, then combine them into that day's H/L/C."""
    today = datetime.date.today()

    for i in range(1, MAX_CPR_LOOKBACK_DAYS + 1):
        day = today - datetime.timedelta(days=i)
        day_str = day.strftime("%Y-%m-%d")
        try:
            resp = fyers.client.history({
                "symbol": symbol,
                "resolution": CPR_RESOLUTION,
                "date_format": "1",
                "range_from": day_str,
                "range_to": day_str,
                "cont_flag": "1"
            })
        except Exception as e:
            logger.warning(f"{CPR_RESOLUTION}-min history fetch failed for {symbol} on {day_str}: {e}")
            continue

        candles = (resp or {}).get("candles") or []
        if not candles:
            continue

        return {
            "date": day,
            "open": candles[0][1],
            "high": max(c[2] for c in candles),
            "low": min(c[3] for c in candles),
            "close": candles[-1][4],
            "count": len(candles),
        }

    logger.error(f"CPR not found for {symbol}: no {CPR_RESOLUTION}-min data in last "
                 f"{MAX_CPR_LOOKBACK_DAYS} days.")
    return None


def calculate_cpr(h, l, c):
    """Same formulas as calculate_cpr() in the NIFTY CPR script."""
    p = (h + l + c) / 3
    bc = (h + l) / 2
    tc = 2 * p - bc
    bc, tc = min(bc, tc), max(bc, tc)
    return {
        "pivot": round(p, 2),
        "bc": round(bc, 2),
        "tc": round(tc, 2),
        "r1": round(2 * p - l, 2),
        "s1": round(2 * p - h, 2),
        "r2": round(p + (h - l), 2),
        "s2": round(p - (h - l), 2),
    }


# ================= SEED CANDLE =================
def fetch_seed_candle(fyers, symbol, timeframe_min):
    today_str = datetime.date.today().strftime("%Y-%m-%d")
    try:
        resp = fyers.client.history({
            "symbol": symbol,
            "resolution": str(timeframe_min),
            "date_format": "1",
            "range_from": today_str,
            "range_to": today_str,
            "cont_flag": "1"
        })
    except Exception as e:
        logger.warning(f"Seed-candle history fetch failed for {symbol}: {e}")
        return None

    candles = (resp or {}).get("candles") or []
    now = datetime.datetime.now()
    bucket = now.replace(minute=(now.minute // timeframe_min) * timeframe_min,
                         second=0, microsecond=0)
    for ts_epoch, o, h, l, c, _vol in candles:
        if datetime.datetime.fromtimestamp(ts_epoch) == bucket:
            return {"time": bucket, "open": o, "high": h, "low": l, "close": c}
    return None


# ================= 5-MIN CANDLE BUILDER =================
class CandleBuilder:
    def __init__(self, timeframe_min, seed=None):
        self.timeframe_min = timeframe_min
        self.current = dict(seed) if seed else None

    def on_tick(self, ltp, ts):
        bucket = ts.replace(minute=(ts.minute // self.timeframe_min) * self.timeframe_min,
                            second=0, microsecond=0)
        closed = None
        if self.current is None:
            self.current = {"time": bucket, "open": ltp, "high": ltp, "low": ltp, "close": ltp}
        elif bucket < self.current["time"]:
            # Out-of-order / stale tick from an older bucket: ignore it so it
            # can't close the current candle early or open a backwards one.
            return None
        elif self.current["time"] != bucket:
            closed = dict(self.current)
            self.current = {"time": bucket, "open": ltp, "high": ltp, "low": ltp, "close": ltp}
        else:
            self.current["high"] = max(self.current["high"], ltp)
            self.current["low"] = min(self.current["low"], ltp)
            self.current["close"] = ltp
        return closed


# ================= LEG (CE or PE) =================
class CprLeg:
    def __init__(self, fyers, symbol, label, cpr):
        self.fyers = fyers
        self.symbol = symbol
        self.label = label
        self.cpr = cpr
        self.r1 = cpr["r1"] if cpr else None

        self.active = cpr is not None
        self.trades_taken = 0
        self.pos = None
        self.hard_exit_done = False

    # ---- candle close: look for R1 cross ----
    def on_candle_close(self, candle):
        if not self.active or self.pos:
            return
        if self.trades_taken >= MAX_TRADES_PER_LEG:
            return

        close_time = (candle["time"] + datetime.timedelta(minutes=TIMEFRAME_MIN)).time()
        if close_time >= NO_NEW_TRADES_TIME:
            return

        if candle["open"] < self.r1 < candle["close"]:
            logger.info(
                f"[{self.label}] R1 CROSS @ {candle['time']} -- "
                f"open={candle['open']} < R1={self.r1} < close={candle['close']}"
            )
            self.enter(candle)

    def enter(self, candle):
        entry = candle["close"]
        sl = candle["low"]
        risk = entry - sl
        if risk <= 0:
            logger.warning(f"[{self.label}] risk <= 0 (entry={entry}, low={sl}); skipping.")
            return

        target = round(entry + TARGET_RR * risk, 2)

        resp = self.fyers.buy(self.symbol, QTY, f"{self.label}ENTRY")
        if not order_ok(resp):
            logger.error(f"[{self.label}] ENTRY FAILED: {resp}")
            send_telegram(f"❌ [{self.label}] ENTRY FAILED: {resp}")
            return

        self.trades_taken += 1
        self.pos = {"entry": entry, "sl": sl, "target": target, "time": candle["time"]}

        msg = (
            f"🚀 ENTRY {self.label}\n"
            f"Symbol={self.symbol}\n"
            f"R1={self.r1}  Candle O={candle['open']} C={candle['close']} @ {candle['time']}\n"
            f"Entry={entry}  SL={sl}  Target={target}  (risk={round(risk,2)})"
        )
        logger.info(msg)
        send_telegram(msg)

    # ---- tick: SL / target / 3pm ----
    def on_tick(self, ltp, now):
        if not self.hard_exit_done and now.time() >= HARD_EXIT_TIME:
            self.hard_exit_done = True
            self.active = False
            if self.pos:
                self.exit("TIME", ltp)
            else:
                cancelled = self.fyers.cancel_pending_orders(self.symbol)
                logger.info(f"[{self.label}] 3:00 PM cutoff, no position; "
                            f"cancelled {len(cancelled)} pending order(s).")
            return

        if not self.pos:
            return

        # An earlier exit order failed: keep retrying (throttled) until the
        # positions book confirms the position is flat.
        if self.pos.get("exit_pending"):
            self.retry_exit(ltp, now)
            return

        if ltp <= self.pos["sl"]:
            self.exit("SL", ltp)
        elif ltp >= self.pos["target"]:
            self.exit("TARGET", ltp)

    def exit(self, reason, ltp):
        if not self.pos:
            return

        # Cancel any pending orders BEFORE sending the exit, so the 3pm
        # cleanup can never cancel the exit order itself.
        cancelled = self.fyers.cancel_pending_orders(self.symbol) if reason == "TIME" else []

        resp = self.fyers.sell(self.symbol, QTY, f"{self.label}EXIT{reason}")
        if not order_ok(resp):
            # Do NOT drop the position -- keep it and retry on later ticks.
            self.pos["exit_pending"] = True
            self.pos["exit_reason"] = reason
            self.pos["exit_attempts"] = 1
            self.pos["last_exit_try"] = datetime.datetime.now()
            logger.error(f"[{self.label}] EXIT ORDER FAILED ({reason}): {resp} -- will retry.")
            send_telegram(f"⚠️ [{self.label}] EXIT ORDER FAILED ({reason}) -- retrying. {resp}")
            return

        self._finish_exit(reason, ltp, cancelled)

    def retry_exit(self, ltp, now):
        wall_now = datetime.datetime.now()
        if (wall_now - self.pos["last_exit_try"]).total_seconds() < EXIT_RETRY_SECONDS:
            return
        self.pos["last_exit_try"] = wall_now
        reason = self.pos["exit_reason"]

        # Check the broker's positions first, so a retry can never sell twice
        # (e.g. if the first order actually went through despite the error).
        net_qty = self.fyers.get_net_qty(self.symbol)
        if net_qty == 0:
            logger.info(f"[{self.label}] position confirmed flat; exit complete.")
            self._finish_exit(reason, ltp, [])
            return
        if net_qty is None:
            logger.warning(f"[{self.label}] could not read positions; will try again.")
            return

        if self.pos["exit_attempts"] >= MAX_EXIT_RETRIES:
            if not self.pos.get("gave_up_alerted"):
                self.pos["gave_up_alerted"] = True
                send_telegram(f"🆘 [{self.label}] EXIT FAILED {MAX_EXIT_RETRIES} times -- "
                              f"{self.symbol} netQty={net_qty}. CLOSE IT MANUALLY!")
            return

        self.pos["exit_attempts"] += 1
        resp = self.fyers.sell(self.symbol, min(QTY, abs(net_qty)), f"{self.label}EXITRETRY")
        logger.info(f"[{self.label}] exit retry #{self.pos['exit_attempts']} -> {resp}")
        if order_ok(resp):
            self._finish_exit(reason, ltp, [])

    def _finish_exit(self, reason, ltp, cancelled):
        pnl_pts = round(ltp - self.pos["entry"], 2)
        msg = (
            f"🏁 EXIT {self.label} {reason}\n"
            f"Entry={self.pos['entry']}  Exit LTP={ltp}  P&L={pnl_pts} pts "
            f"(≈ ₹{round(pnl_pts * QTY, 2)})"
            + (f"\nCancelled {len(cancelled)} pending order(s)." if cancelled else "")
        )
        logger.info(msg)
        send_telegram(msg)
        self.pos = None


def build_leg(fyers, symbol, label):
    prev = fetch_prev_day_ohlc(fyers, symbol)
    if not prev:
        send_telegram(f"⚠️ [{label}] no previous-day data for {symbol}; leg disabled today.")
        return CprLeg(fyers, symbol, label, None)

    cpr = calculate_cpr(prev["high"], prev["low"], prev["close"])
    msg = (
        f"📐 CPR [{label}] {symbol}\n"
        f"Prev day {prev['date']} ({prev['count']} x {CPR_RESOLUTION}-min candles): "
        f"H={prev['high']} L={prev['low']} C={prev['close']}\n"
        f"P={cpr['pivot']}  BC={cpr['bc']}  TC={cpr['tc']}\n"
        f"R1={cpr['r1']}  R2={cpr['r2']}\n"
        f"S1={cpr['s1']}  S2={cpr['s2']}"
    )
    logger.info(msg)
    send_telegram(msg)
    return CprLeg(fyers, symbol, label, cpr)


# ================= MAIN =================
if __name__ == "__main__":
    # All times (09:16, 3:00 PM, candle buckets) use the machine's local
    # clock, so it MUST be set to IST (UTC+05:30).
    utc_offset = datetime.datetime.now().astimezone().utcoffset()
    if utc_offset != datetime.timedelta(hours=5, minutes=30):
        msg = (f"❌ Machine timezone is UTC{utc_offset}, not IST (UTC+05:30). "
               f"Set the system timezone to Asia/Kolkata (or TZ=Asia/Kolkata) and restart.")
        logger.error(msg)
        send_telegram(msg)
        sys.exit(1)

    send_telegram("🚀 SENSEX ATM CPR R1 BREAKOUT STARTED")
    fyers = Fyers()

    while datetime.datetime.now().time() < DECISION_TIME:
        time_module.sleep(1)

    atm_ce, atm_pe, spot = get_atm_symbols(fyers)

    legs = {
        atm_ce: build_leg(fyers, atm_ce, "CE"),
        atm_pe: build_leg(fyers, atm_pe, "PE"),
    }

    builders = {}
    for sym, leg in legs.items():
        seed = fetch_seed_candle(fyers, sym, TIMEFRAME_MIN)
        if seed:
            logger.info(f"[{leg.label}] seeded current candle -> {seed}")
        else:
            logger.warning(f"[{leg.label}] no seed candle; first live candle may be partial.")
        builders[sym] = CandleBuilder(TIMEFRAME_MIN, seed=seed)

    def on_message(msg):
        symbol = msg.get("symbol")
        if symbol not in legs or "ltp" not in msg:
            return

        ltp = msg["ltp"]
        ts = datetime.datetime.fromtimestamp(
            msg.get("last_traded_time") or msg.get("timestamp") or datetime.datetime.now().timestamp()
        )

        # The first snapshot for an option that hasn't traded yet today can
        # carry yesterday's last-trade time; such a tick must not build candles.
        if ts.date() != datetime.date.today():
            return

        leg = legs[symbol]
        leg.on_tick(ltp, ts)

        closed = builders[symbol].on_tick(ltp, ts)
        if closed:
            logger.info(f"[{leg.label}] candle {closed['time']:%H:%M} "
                        f"O={closed['open']} H={closed['high']} L={closed['low']} C={closed['close']}")
            leg.on_candle_close(closed)

    def on_open():
        ws.subscribe(symbols=list(legs.keys()), data_type="SymbolUpdate")
        ws.keep_running()

    ws = data_ws.FyersDataSocket(
        access_token=fyers.auth,
        on_connect=on_open,
        on_message=on_message,
        log_path=""
    )
    ws.connect()
