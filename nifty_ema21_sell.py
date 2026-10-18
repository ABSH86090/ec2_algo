# =====================================================================
# NIFTY EMA21 - SETUP B (SELL ATM OPTION) - LIVE
#
# Same rules as the backtest (ENABLED_SETUPS = {"B"}):
#   * ATM strike from the Nifty 9:15 1-min candle OPEN, nearest expiry
#     (expiry day itself included -> 0DTE).
#   * Both legs (ATM CE and ATM PE) are watched on 5-min candles.
#     EMA21 on the option's own 5-min closes, warmed up on the previous
#     WARMUP_DAYS trading days.
#   * A leg is a "B day" only if its 9:15 candle CLOSES BELOW EMA21
#     (above -> Setup A, which is disabled -> no trade on that leg).
#   * Setup B: a RED candle that closes below the 9:15 candle's LOW,
#     closing by 9:50 (9:20 ... 9:45 candles) -> SELL at its close.
#     SL = that candle's high + 1. Skip if SL - close > B_MAX_SL_POINTS.
#   * Only ONE position across both legs. While it is open, the other
#     leg is ignored; after the exit it watches again from the exit candle.
#     Both legs on the same candle -> tighter SL wins.
#   * Trailing SL: every 50% fall from entry moves the SL down 50% of its
#     original level. As in the backtest, a new SL becomes active from
#     the NEXT 5-min candle.
#   * Exit at EXIT_TIME if SL not hit.
#   * Every sell is hedged: buy the same type, same expiry, further OTM
#     (700 pts on expiry day, 1000 pts on other days) BEFORE the sell;
#     sell it AFTER the short is covered.
# =====================================================================

import os
import sys
import json
import time
import logging
import datetime
import threading
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from dotenv import load_dotenv
from fyers_apiv3 import fyersModel
from fyers_apiv3.FyersWebsocket import data_ws

# ======================================================
# CONFIG
# ======================================================
load_dotenv()
CLIENT_ID = os.getenv("FYERS_CLIENT_ID")
ACCESS_TOKEN = os.getenv("FYERS_ACCESS_TOKEN")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

IST = ZoneInfo("Asia/Kolkata")

# ---------- strategy (same values as the backtest) ----------
INDEX_SYMBOL = "NSE:NIFTY50-INDEX"
UNDERLYING = "NIFTY"
STRIKE_STEP = 50
TIMEFRAME = "5"
CANDLE_MIN = 5
EMA_PERIOD = 21
WARMUP_DAYS = 2

FIRST_CANDLE = datetime.time(9, 15)
B_LATEST_ENTRY = datetime.time(9, 50)      # last signal candle must CLOSE by this time
B_MAX_SL_POINTS = 25
B_SL_BUFFER = 1                            # SL = breakdown candle high + this
B_CONTINUE_AFTER_SKIP = False              # True -> after a skipped signal keep watching that leg
SAME_CANDLE_PRIORITY = "tighter_sl"        # or "CE" / "PE"

EXIT_TIME = datetime.time(15, 15)          # square off at 15:15

TRAIL_ENABLED = True
TRAIL_TRIGGER_PCT = 0.50
TRAIL_SL_PCT = 0.50
TRAIL_REPEAT = True

# ---------- 0DTE / hedge ----------
TRADE_0DTE = True                          # False -> no trades on expiry day
HEDGE_ENABLED = True                       # buy a hedge with EVERY sell (before it, exited after it)
HEDGE_DISTANCE_0DTE = 700                  # expiry day:  CE strike + 700,  PE strike - 700
HEDGE_DISTANCE_OTHER = 1000                # other days:  CE strike + 1000, PE strike - 1000


def hedge_distance(dte0):
    return HEDGE_DISTANCE_0DTE if dte0 else HEDGE_DISTANCE_OTHER

# ---------- execution ----------
PAPER_TRADE = False                        # LIVE: real orders. True = no real orders (fills at LTP).
LOTS = 1                                   # qty = LOTS x lot size from the symbol master
PRODUCT_TYPE = "INTRADAY"

MARKET_HOLIDAYS = {                        # NSE F&O holidays 2026 (weekdays) - keep updated
    datetime.date(2026, 1, 15), datetime.date(2026, 1, 26), datetime.date(2026, 3, 3),
    datetime.date(2026, 3, 26), datetime.date(2026, 3, 31), datetime.date(2026, 4, 3),
    datetime.date(2026, 4, 14), datetime.date(2026, 5, 1), datetime.date(2026, 5, 28),
    datetime.date(2026, 6, 26), datetime.date(2026, 9, 14), datetime.date(2026, 10, 2),
    datetime.date(2026, 10, 20), datetime.date(2026, 11, 10), datetime.date(2026, 11, 24),
    datetime.date(2026, 12, 25),
}

SYMBOL_MASTER_URL = "https://public.fyers.in/sym_details/NSE_FO.csv"

SPOT_FETCH_TIMEOUT_SEC = 90     # wait this long for the 9:15 spot candle before using quote open
CANDLE_FETCH_DELAY_SEC = 3      # wait after a candle closes before reading it
CANDLE_FETCH_TIMEOUT_SEC = 45   # give up on a candle if history doesn't return it by then
LATE_ENTRY_GRACE_SEC = 60       # a signal candle is ignored if read later than this after its close
LTP_STALE_SEC = 5               # REST quote fallback if no websocket tick for this long
FILL_WAIT_SEC = 10
EXIT_RETRIES = 3

STATE_FILE = "nifty_ema21_b_state.json"
LOG_FILE = "nifty_ema21_b_live.log"

# ======================================================
# LOGGING / TELEGRAM
# ======================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler(sys.stdout)],
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


def notify(msg, level=logging.INFO):
    logger.log(level, msg.replace("\n", " | "))
    send_telegram(msg)


def now_ist():
    return datetime.datetime.now(IST).replace(tzinfo=None)


def floor_candle(t):
    return t.replace(minute=t.minute - t.minute % CANDLE_MIN, second=0, microsecond=0)


def is_trading_day(d):
    return d.weekday() < 5 and d not in MARKET_HOLIDAYS


def prev_trading_days(d, n):
    days = []
    while len(days) < n:
        d -= datetime.timedelta(days=1)
        if is_trading_day(d):
            days.append(d)
    return sorted(days)


def candle_starts(day):
    """9:15 (first candle) plus every candle that closes by B_LATEST_ENTRY."""
    step = datetime.timedelta(minutes=CANDLE_MIN)
    cutoff = datetime.datetime.combine(day, B_LATEST_ENTRY)
    t = datetime.datetime.combine(day, FIRST_CANDLE)
    out = []
    while t + step <= cutoff:
        out.append(t)
        t += step
    return out


def candles_to_df(candles):
    df = pd.DataFrame([c[:6] for c in candles],
                      columns=["ts", "open", "high", "low", "close", "volume"])
    df["time"] = (pd.to_datetime(df["ts"], unit="s", utc=True)
                    .dt.tz_convert("Asia/Kolkata").dt.tz_localize(None))
    df = df.set_index("time").sort_index()
    df = df.between_time("09:15", "15:30")
    return df[~df.index.duplicated(keep="first")]


# ======================================================
# BROKER
# ======================================================
class Broker:
    def __init__(self):
        self.client = fyersModel.FyersModel(
            client_id=CLIENT_ID, token=ACCESS_TOKEN, is_async=False, log_path=""
        )

    def history(self, symbol, resolution, start, end):
        resp = self.client.history({
            "symbol": symbol,
            "resolution": resolution,
            "date_format": "1",
            "range_from": start.strftime("%Y-%m-%d"),
            "range_to": end.strftime("%Y-%m-%d"),
            "cont_flag": "1",
        })
        if resp.get("s") != "ok" or not resp.get("candles"):
            raise RuntimeError(f"history failed for {symbol}: {resp}")
        return candles_to_df(resp["candles"])

    def quote(self, symbol):
        r = self.client.quotes({"symbols": symbol})
        if r.get("s") != "ok":
            raise RuntimeError(f"quote failed for {symbol}: {r}")
        return r["d"][0]["v"]

    def ltp(self, symbol):
        return float(self.quote(symbol)["lp"])

    def market_order(self, symbol, qty, side, tag):
        """side: 1 = BUY, -1 = SELL. Returns dict(status=FILLED|REJECTED|UNKNOWN, price, order_id, info)."""
        if PAPER_TRADE:
            px = self.ltp(symbol)
            return {"status": "FILLED", "price": px, "order_id": "PAPER", "info": "paper"}

        resp = self.client.place_order({
            "symbol": symbol,
            "qty": int(qty),
            "type": 2,                 # MARKET
            "side": side,
            "productType": PRODUCT_TYPE,
            "limitPrice": 0,
            "stopPrice": 0,
            "validity": "DAY",
            "disclosedQty": 0,
            "offlineOrder": False,
            "orderTag": tag,
        })
        if not isinstance(resp, dict) or resp.get("s") != "ok":
            return {"status": "REJECTED", "price": None, "order_id": None, "info": resp}
        oid = resp.get("id")
        status, price, info = self._wait_fill(oid)
        return {"status": status, "price": price, "order_id": oid, "info": info}

    def _wait_fill(self, oid):
        # Fyers order status: 1 cancelled, 2 filled, 4 transit, 5 rejected, 6 pending, 7 expired
        deadline = time.time() + FILL_WAIT_SEC
        last = None
        while time.time() < deadline:
            try:
                r = self.client.orderbook(data={"id": oid})
                if r.get("s") == "ok" and r.get("orderBook"):
                    o = r["orderBook"][0]
                    last = o
                    st = o.get("status")
                    if st == 2:
                        return "FILLED", float(o.get("tradedPrice") or 0), o.get("message")
                    if st in (1, 5, 7):
                        return "REJECTED", None, o.get("message")
            except Exception as e:
                last = str(e)
            time.sleep(0.5)
        return "UNKNOWN", None, last


# ======================================================
# OPTION MASTER (real listed expiries / strikes / lot sizes)
# ======================================================
class OptionMaster:
    """Fyers NSE F&O symbol master. Columns used:
    3 lot size, 8 expiry (epoch), 9 ticker, 13 underlying, 15 strike, 16 option type."""

    def __init__(self):
        raw = pd.read_csv(SYMBOL_MASTER_URL, header=None, low_memory=False)
        df = raw[[3, 8, 9, 13, 15, 16]].copy()
        df.columns = ["lot", "expiry_ts", "ticker", "underlying", "strike", "opt"]
        df = df[(df["underlying"].astype(str).str.strip() == UNDERLYING)
                & (df["opt"].astype(str).str.strip().isin(["CE", "PE"]))].copy()
        if df.empty or not df["ticker"].astype(str).str.startswith(f"NSE:{UNDERLYING}").all():
            raise RuntimeError("Symbol master layout not as expected - no NIFTY options found")
        df["opt"] = df["opt"].astype(str).str.strip()
        df["strike"] = df["strike"].astype(float)
        df["lot"] = df["lot"].astype(int)
        df["expiry"] = (pd.to_datetime(df["expiry_ts"].astype("int64"), unit="s", utc=True)
                          .dt.tz_convert("Asia/Kolkata").dt.date)
        self.df = df
        logger.info(f"Symbol master loaded: {len(df)} NIFTY options")

    def nearest_expiry(self, today):
        exps = sorted(e for e in self.df["expiry"].unique() if e >= today)
        if not exps:
            raise RuntimeError("No upcoming NIFTY expiries in symbol master")
        return exps[0]

    def pick(self, expiry, opt, strike):
        chain = self.df[(self.df["expiry"] == expiry) & (self.df["opt"] == opt)]
        if chain.empty:
            raise RuntimeError(f"No {opt} contracts for expiry {expiry}")
        row = chain.loc[(chain["strike"] - strike).abs().idxmin()]
        return {"symbol": row["ticker"], "strike": float(row["strike"]), "lot": int(row["lot"])}


# ======================================================
# CONTRACT SELECTION (9:15 spot open -> ATM, nearest expiry)
# ======================================================
def spot_open_915(broker, today):
    ts = pd.Timestamp(datetime.datetime.combine(today, FIRST_CANDLE))
    deadline = time.time() + SPOT_FETCH_TIMEOUT_SEC
    while time.time() < deadline:
        try:
            df = broker.history(INDEX_SYMBOL, "1", today, today)
            if ts in df.index:
                return float(df.loc[ts, "open"]), "9:15 1-min candle open"
        except Exception as e:
            logger.warning(f"spot history: {e}")
        time.sleep(3)
    return float(broker.quote(INDEX_SYMBOL)["open_price"]), "quote open_price (fallback)"


def resolve_contract(broker, master, today):
    spot, src = spot_open_915(broker, today)
    atm = round(spot / STRIKE_STEP) * STRIKE_STEP
    expiry = master.nearest_expiry(today)
    legs = {}
    for opt in ("CE", "PE"):
        row = master.pick(expiry, opt, atm)
        legs[opt] = {"symbol": row["symbol"], "strike": row["strike"], "lot": row["lot"],
                     "code": None, "first_low": None, "done": False, "note": ""}
    contract = {"spot": spot, "spot_src": src, "atm": atm,
                "expiry": expiry.isoformat(), "dte0": expiry == today}
    return contract, legs


# ======================================================
# STATE (survives restarts within the same day)
# ======================================================
def load_state(today):
    blank = {"date": today.isoformat(), "contract": None, "legs": {}, "evaluated": [],
             "position": None, "free_from": None, "trades": []}
    try:
        with open(STATE_FILE) as f:
            s = json.load(f)
        if s.get("date") == today.isoformat():
            return s
    except Exception:
        pass
    return blank


def save_state(state):
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(state, f, indent=2, default=str)
    os.replace(tmp, STATE_FILE)


# ======================================================
# TRADER
# ======================================================
class LiveTrader:
    def __init__(self, broker, master, today, state):
        self.broker = broker
        self.master = master
        self.today = today
        self.state = state
        self.lock = threading.RLock()
        self.ticks = {}                # symbol -> (ltp, time)
        self.exit_blocked = False
        self.warmup_start = prev_trading_days(today, WARMUP_DAYS)[0]
        self.first_ts = datetime.datetime.combine(today, FIRST_CANDLE)

        p = self.state["position"]
        if p:
            notify(f"♻️ RESUMED open position\nSELL {p['symbol']} x{p['qty']} @ {p['entry_px']}\n"
                   f"SL={p['sl']} (trail steps {p['steps']})"
                   f"{chr(10) + 'Hedge: ' + p['hedge']['symbol'] if p.get('hedge') else ''}")

    # ---------- prices ----------
    def on_tick(self, symbol, ltp):
        self.ticks[symbol] = (ltp, time.time())
        pos = self.state["position"]
        if pos and pos["symbol"] == symbol:
            self.check_exit(ltp, now_ist())

    def ltp(self, symbol):
        t = self.ticks.get(symbol)
        if t and time.time() - t[1] <= LTP_STALE_SEC:
            return t[0]
        px = self.broker.ltp(symbol)
        self.ticks[symbol] = (px, time.time())
        return px

    # ---------- candles ----------
    def fetch_candle(self, symbol, start):
        """Closed 5-min candle at `start` with EMA21 (warmed up on previous days)."""
        ts = pd.Timestamp(start)
        deadline = time.time() + CANDLE_FETCH_TIMEOUT_SEC
        while time.time() < deadline:
            try:
                df = self.broker.history(symbol, TIMEFRAME, self.warmup_start, self.today)
                df["ema21"] = df["close"].ewm(span=EMA_PERIOD, adjust=False).mean()
                if ts in df.index:
                    return df.loc[ts]
            except Exception as e:
                logger.warning(f"{symbol} history: {e}")
            time.sleep(2)
        return None

    def classify(self, name, leg):
        c = self.fetch_candle(leg["symbol"], self.first_ts)
        if c is None:
            leg["done"], leg["note"] = True, "9:15 candle not available - leg skipped"
            notify(f"⚠️ {name}: 9:15 candle not available - leg not traded today", logging.WARNING)
            return
        info = (f"9:15 O={c['open']:.2f} H={c['high']:.2f} L={c['low']:.2f} "
                f"C={c['close']:.2f} EMA21={c['ema21']:.2f}")
        if c["close"] < c["ema21"]:
            leg["code"], leg["first_low"] = "B", float(c["low"])
            leg["note"] = "Setup B day - watching for breakdown of 9:15 low"
            notify(f"🔎 {name} {leg['symbol']}: closed BELOW EMA21 -> Setup B, "
                   f"watching for red close < {c['low']:.2f}\n{info}")
        else:
            leg["code"] = "A" if c["close"] > c["ema21"] else ""
            leg["done"] = True
            leg["note"] = "closed above EMA21 (Setup A, disabled)" if leg["code"] else "closed on EMA21"
            notify(f"⏸ {name} {leg['symbol']}: {leg['note']} - no trade on this leg\n{info}")

    def run_candles(self, now):
        for start in candle_starts(self.today):
            tag = start.strftime("%H:%M")
            if tag in self.state["evaluated"]:
                continue
            close = start + datetime.timedelta(minutes=CANDLE_MIN)
            if now < close + datetime.timedelta(seconds=CANDLE_FETCH_DELAY_SEC):
                return                                   # candles are processed in order

            with self.lock:                              # mark first: never act twice on restart
                self.state["evaluated"].append(tag)
                save_state(self.state)

            if start == self.first_ts:
                for name, leg in self.state["legs"].items():
                    if leg["code"] is None and not leg["done"]:
                        self.classify(name, leg)
                with self.lock:
                    save_state(self.state)
                continue

            if now > close + datetime.timedelta(seconds=LATE_ENTRY_GRACE_SEC):
                logger.info(f"{tag}: read too late to enter at its close - ignored")
                continue
            self.evaluate(start, tag)

    def evaluate(self, start, tag):
        with self.lock:
            if self.state["position"] is not None:
                logger.info(f"{tag}: position open - other leg ignored")
                return
            free_from = self.state.get("free_from")
        if free_from and start < datetime.datetime.fromisoformat(free_from):
            logger.info(f"{tag}: candle before the last exit candle - ignored")
            return

        cands = []
        for name, leg in self.state["legs"].items():
            if leg["done"]:
                continue
            if leg["code"] is None:
                self.classify(name, leg)
                if leg["done"]:
                    continue
            c = self.fetch_candle(leg["symbol"], start)
            if c is None:
                notify(f"⚠️ {tag} {name}: candle not available - not checked", logging.WARNING)
                continue
            if not (c["close"] < c["open"] and c["close"] < leg["first_low"]):
                continue
            sl = round(float(c["high"]) + B_SL_BUFFER, 2)
            risk = round(sl - float(c["close"]), 2)
            if risk > B_MAX_SL_POINTS:
                notify(f"⏭ {tag} {name} signal SKIPPED: SL {sl} is {risk} pts away "
                       f"(> {B_MAX_SL_POINTS})\nClose={c['close']} High={c['high']}")
                if not B_CONTINUE_AFTER_SKIP:
                    leg["done"], leg["note"] = True, f"{tag} signal skipped (risk {risk})"
                continue
            cands.append({"name": name, "leg": leg, "close": float(c["close"]),
                          "high": float(c["high"]), "sl": sl, "risk": risk})

        with self.lock:
            save_state(self.state)
        if not cands:
            return

        chosen = None
        if SAME_CANDLE_PRIORITY in ("CE", "PE"):
            chosen = next((x for x in cands if x["name"] == SAME_CANDLE_PRIORITY), None)
        chosen = chosen or min(cands, key=lambda x: x["risk"])
        for x in cands:
            if x is not chosen:
                notify(f"🚫 {tag} {x['name']} signal BLOCKED ({chosen['name']} chosen, tighter SL)")
        self.enter(chosen, tag)

    # ---------- entry ----------
    def enter(self, cand, tag):
        name, leg = cand["name"], cand["leg"]
        ct = self.state["contract"]
        qty = leg["lot"] * LOTS

        with self.lock:                                  # one trade per leg per day, even on crash
            leg["done"], leg["note"] = True, f"{tag} entry"
            save_state(self.state)

        hedge = None
        if HEDGE_ENABLED:
            dist = hedge_distance(ct["dte0"])
            target = leg["strike"] + (dist if name == "CE" else -dist)
            try:
                h = self.master.pick(datetime.date.fromisoformat(ct["expiry"]), name, target)
            except Exception as e:
                notify(f"❌ {tag} {name} ENTRY ABORTED: hedge lookup failed ({e})", logging.ERROR)
                return
            if h["strike"] != target:
                notify(f"⚠️ Hedge strike {target} not listed - using {h['strike']}", logging.WARNING)
            r = self.broker.market_order(h["symbol"], qty, 1, "B21HEDGE")
            if r["status"] != "FILLED":
                extra = ("\nStatus unknown - CHECK BROKER and sell the hedge if it filled"
                         if r["status"] == "UNKNOWN" else "")
                notify(f"❌ {tag} {name} ENTRY ABORTED: hedge buy {h['symbol']} {r['status']}\n"
                       f"{r['info']}{extra}", logging.ERROR)
                return
            hedge = {"symbol": h["symbol"], "strike": h["strike"], "entry": r["price"],
                     "order_id": r["order_id"]}

        r = self.broker.market_order(leg["symbol"], qty, -1, "B21SELL")
        if r["status"] == "REJECTED":
            notify(f"❌ {tag} SELL REJECTED {leg['symbol']}\n{r['info']}", logging.ERROR)
            if hedge:
                hr = self.order_with_retry(hedge["symbol"], qty, -1, "B21HEXIT")
                if hr["status"] != "FILLED":
                    notify(f"🆘 Could not unwind hedge {hedge['symbol']} x{qty} - SELL IT MANUALLY",
                           logging.CRITICAL)
            return

        entry_px = r["price"] or cand["close"]
        now = now_ist()
        position = {
            "leg": name, "setup_candle": tag, "symbol": leg["symbol"], "qty": qty,
            "signal_close": cand["close"], "entry_px": entry_px, "order_id": r["order_id"],
            "sl0": cand["sl"], "sl": cand["sl"], "steps": 0,
            "pending_steps": 0, "pending_from": None,
            "hedge": hedge, "entry_time": now.isoformat(timespec="seconds"),
        }
        with self.lock:
            self.state["position"] = position
            save_state(self.state)

        warn = "\n⚠️ Fill not confirmed - CHECK BROKER" if r["status"] == "UNKNOWN" else ""
        hedge_txt = (f"\nHedge: bought {hedge['symbol']} @ {hedge['entry']} "
                     f"({hedge_distance(ct['dte0'])} pts OTM)" if hedge else "")
        notify(
            f"🚀 SELL {name} {'(PAPER)' if PAPER_TRADE else ''}\n"
            f"Sold {leg['symbol']} x{qty} @ {entry_px} (signal close {cand['close']}, {tag} candle)\n"
            f"SL={cand['sl']} ({cand['risk']} pts) | expiry {ct['expiry']}"
            f"{' (0DTE)' if ct['dte0'] else ''}{hedge_txt}{warn}"
        )

    # ---------- exit ----------
    def check_exit(self, ltp, now):
        with self.lock:
            pos = self.state["position"]
            if not pos or self.exit_blocked:
                return

            # pending trail becomes active at the start of the next candle
            if (pos["pending_steps"] > pos["steps"] and pos["pending_from"]
                    and now >= datetime.datetime.fromisoformat(pos["pending_from"])):
                pos["steps"] = pos["pending_steps"]
                pos["sl"] = round(pos["sl0"] * (1 - TRAIL_SL_PCT) ** pos["steps"], 2)
                save_state(self.state)
                notify(f"🔻 Trail SL -> {pos['sl']} (step {pos['steps']}) {pos['symbol']}")

            reason = None
            if now.time() >= EXIT_TIME:
                reason = "TIME"
            elif ltp >= pos["sl"]:
                reason = "TRAIL_SL" if pos["steps"] else "SL"
            elif TRAIL_ENABLED:
                n = pos["pending_steps"]
                while (ltp <= pos["entry_px"] * (1 - TRAIL_TRIGGER_PCT) ** (n + 1)
                       and (TRAIL_REPEAT or n == 0)):
                    n += 1
                if n > pos["pending_steps"]:
                    pos["pending_steps"] = n
                    nxt = floor_candle(now) + datetime.timedelta(minutes=CANDLE_MIN)
                    pos["pending_from"] = nxt.isoformat()
                    save_state(self.state)
                    logger.info(f"trail trigger hit at {ltp}: SL -> "
                                f"{round(pos['sl0'] * (1 - TRAIL_SL_PCT) ** n, 2)} from {nxt:%H:%M}")
            if reason:
                self.exit(reason, ltp)

    def order_with_retry(self, symbol, qty, side, tag):
        r = None
        for attempt in range(EXIT_RETRIES):
            r = self.broker.market_order(symbol, qty, side, tag)
            if r["status"] in ("FILLED", "UNKNOWN"):
                break                  # UNKNOWN: never resend (could double the order)
            logger.error(f"{tag} attempt {attempt + 1} rejected: {r['info']}")
            time.sleep(1)
        return r

    def exit(self, reason, ltp):
        with self.lock:
            pos = self.state["position"]
            if not pos or self.exit_blocked:
                return

            # 1) cover the short FIRST (the hedge stays on until this is confirmed)
            r = self.order_with_retry(pos["symbol"], pos["qty"], 1, "B21COVER")
            if r["status"] != "FILLED":
                self.exit_blocked = True
                notify(f"🆘 EXIT {reason} NOT CONFIRMED - buy back {pos['symbol']} x{pos['qty']}\n"
                       f"Status={r['status']} {r['info']}\nSQUARE OFF MANUALLY"
                       f"{' (then sell hedge ' + pos['hedge']['symbol'] + ')' if pos.get('hedge') else ''}",
                       logging.CRITICAL)
                return
            exit_px = r["price"] or ltp
            sell_pts = round(pos["entry_px"] - exit_px, 2)

            # 2) then sell the hedge
            hedge_pts, hedge_txt = 0.0, ""
            h = pos.get("hedge")
            if h:
                hr = self.order_with_retry(h["symbol"], pos["qty"], -1, "B21HEXIT")
                if hr["status"] == "FILLED":
                    hedge_pts = round((hr["price"] or 0) - (h["entry"] or 0), 2)
                    hedge_txt = f"\nHedge: {h['entry']} -> {hr['price']} = {hedge_pts:+.2f} pts"
                else:
                    notify(f"🆘 HEDGE EXIT NOT CONFIRMED {h['symbol']} x{pos['qty']}\n"
                           f"Status={hr['status']} {hr['info']}\nSELL IT MANUALLY", logging.CRITICAL)
                    hedge_txt = "\nHedge: exit NOT confirmed"

            net = round(sell_pts + hedge_pts, 2)
            now = now_ist()
            self.state["trades"].append({**pos, "exit_px": exit_px, "exit_reason": reason,
                                         "exit_time": now.isoformat(timespec="seconds"),
                                         "sell_pts": sell_pts, "hedge_pts": hedge_pts,
                                         "net_pts": net, "pnl": round(net * pos["qty"], 2)})
            self.state["position"] = None
            # other leg may enter from the close of this exit candle onward (as in the backtest)
            self.state["free_from"] = floor_candle(now).isoformat()
            save_state(self.state)
            notify(
                f"🏁 EXIT {reason} {'(PAPER)' if PAPER_TRADE else ''}\n"
                f"{pos['symbol']} x{pos['qty']}: sold {pos['entry_px']} -> bought {exit_px} "
                f"= {sell_pts:+.2f} pts{hedge_txt}\n"
                f"Net {net:+.2f} pts = ₹{net * pos['qty']:,.0f}"
            )

    def day_done(self, now):
        with self.lock:
            if self.state["position"] is not None:
                return False
            if now.time() >= EXIT_TIME:
                return True
            if all(leg["done"] for leg in self.state["legs"].values()):
                return True
            tags = [s.strftime("%H:%M") for s in candle_starts(self.today)]
            return all(t in self.state["evaluated"] for t in tags)


# ======================================================
# WEBSOCKET
# ======================================================
def start_websocket(trader, symbols):
    def on_message(msg):
        try:
            if isinstance(msg, dict) and msg.get("symbol") in symbols and "ltp" in msg:
                trader.on_tick(msg["symbol"], float(msg["ltp"]))
        except Exception:
            logger.exception("tick handler error")

    def on_connect():
        ws.subscribe(symbols=list(symbols), data_type="SymbolUpdate")
        logger.info(f"Websocket connected, subscribed to {symbols}")

    ws = data_ws.FyersDataSocket(
        access_token=f"{CLIENT_ID}:{ACCESS_TOKEN}",
        log_path="",
        litemode=False,
        write_to_file=False,
        reconnect=True,
        on_connect=on_connect,
        on_close=lambda m: logger.warning(f"Websocket closed: {m}"),
        on_error=lambda m: logger.warning(f"Websocket error: {m}"),
        on_message=on_message,
    )
    ws.connect()
    return ws


# ======================================================
# MAIN
# ======================================================
def main():
    if not CLIENT_ID or not ACCESS_TOKEN:
        raise SystemExit("FYERS credentials not found in .env")

    today = now_ist().date()
    if not is_trading_day(today):
        logger.info("Market closed today - exiting")
        return

    broker = Broker()
    state = load_state(today)

    try:
        master = OptionMaster()
    except Exception as e:
        notify(f"❌ STARTUP FAILED: {e}", logging.ERROR)
        raise

    # wait for the 9:15 spot candle to exist before picking ATM
    ready = datetime.datetime.combine(today, datetime.time(9, 16, 5))
    if state["contract"] is None and now_ist() < ready:
        logger.info(f"Waiting until {ready:%H:%M:%S} to pick the ATM strike...")
        while now_ist() < ready:
            time.sleep(1)

    if state["contract"] is None:
        try:
            contract, legs = resolve_contract(broker, master, today)
        except Exception as e:
            notify(f"❌ STARTUP FAILED (contract): {e}", logging.ERROR)
            raise
        state["contract"], state["legs"] = contract, legs
        save_state(state)

    ct, legs = state["contract"], state["legs"]
    has_pos = state["position"] is not None

    if ct["dte0"] and not TRADE_0DTE and not has_pos:
        notify(f"⏸ NO TRADE TODAY - expiry day (0DTE) and TRADE_0DTE = False")
        return

    window_end = datetime.datetime.combine(today, B_LATEST_ENTRY) + \
        datetime.timedelta(seconds=LATE_ENTRY_GRACE_SEC)
    if now_ist() > window_end and not has_pos:
        notify("⏸ Started after the Setup B window (9:50) with no open position - nothing to do")
        return

    notify(
        f"🚀 EMA21 SETUP B STARTED {'(PAPER)' if PAPER_TRADE else '(LIVE)'}\n"
        f"Nifty open {ct['spot']} ({ct['spot_src']}) -> ATM {ct['atm']}\n"
        f"Expiry {ct['expiry']}{' (0DTE)' if ct['dte0'] else ''}"
        f"{' | hedge ' + str(hedge_distance(ct['dte0'])) + ' pts OTM' if HEDGE_ENABLED else ' | no hedge'}\n"
        f"CE {legs['CE']['symbol']} | PE {legs['PE']['symbol']} | lot {legs['CE']['lot']} x {LOTS}\n"
        f"Max SL {B_MAX_SL_POINTS} pts | entry by {B_LATEST_ENTRY:%H:%M} | exit {EXIT_TIME:%H:%M}"
    )

    trader = LiveTrader(broker, master, today, state)
    symbols = {legs["CE"]["symbol"], legs["PE"]["symbol"]}
    ws = start_websocket(trader, symbols)

    try:
        while True:
            now = now_ist()
            pos = trader.state["position"]
            if pos:                                   # backup exit check if ticks stop
                try:
                    trader.check_exit(trader.ltp(pos["symbol"]), now)
                except Exception as e:
                    logger.warning(f"LTP fallback failed: {e}")

            trader.run_candles(now)

            if trader.day_done(now):
                tr = trader.state["trades"]
                summary = "; ".join(f"{t['leg']} {t['net_pts']:+.2f} pts (₹{t['pnl']:,.0f})"
                                    for t in tr) or "no trade"
                notes = " | ".join(f"{k}: {v['note']}" for k, v in trader.state["legs"].items())
                notify(f"✅ Done for the day - {summary}\n{notes}")
                break
            time.sleep(1)
    except KeyboardInterrupt:
        if trader.state["position"]:
            notify("⚠️ Script stopped with an OPEN position - restart to resume or exit manually",
                   logging.WARNING)
    finally:
        try:
            ws.close_connection()
        except Exception:
            pass


if __name__ == "__main__":
    main()
