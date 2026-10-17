# =====================================================================
# SENSEX FIRST-CANDLE EMA / TREND-EMA / DAILY-EMA / CPR  ->  BUY ATM OPTION (LIVE)
#
# Entry logic  : identical to the backtest (5-min setup candles at 09:15 / 09:20)
# Day filter   : trade only if previous-day CPR width <= MAX_CPR_WIDTH_PCT
# BUY signal   : buy ATM CE      SELL signal : buy ATM PE
# Expiry rule  : nearest expiry is DTE0 or DTE1 -> use the NEXT expiry,
#                otherwise use the nearest expiry
# SL / Target  : on the INDEX
#                BUY  SL = setup low  + SL_CANDLE_FRACTION x range
#                SELL SL = setup high - SL_CANDLE_FRACTION x range
#                target = RR_MULTIPLE x risk
# Hard exit    : HARD_EXIT_TIME
# =====================================================================

import os
import sys
import json
import time
import logging
import datetime
import threading
from zoneinfo import ZoneInfo

import numpy as np
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
INDEX_SYMBOL = "BSE:SENSEX-INDEX"
TIMEFRAME = "5"
CANDLE_MIN = 5

EMA_PERIOD = 5                 # fast 5-min EMA (proximity + close filter)
EMA_TREND_PERIOD = 20          # 5-min trend EMA (column key kept as "ema200" like the backtest)

# Daily trend filter: BUY only if setup close > daily EMA, SELL only if < daily EMA.
DAILY_EMA_PERIOD = 100
# "live" -> yesterday's daily EMA updated with the setup candle's close (as a chart shows it)
# "prev" -> yesterday's final daily EMA, unchanged
DAILY_EMA_MODE = "live"

MAX_WICK_PCT = 0.25
EMA_PROXIMITY_PCT = 0.70
SETUP_CANDLE_TIMES = [datetime.time(9, 15), datetime.time(9, 20)]
MAX_TRADES_PER_DAY = 1

# SL inside the setup candle, measured from the end against the trade.
SL_CANDLE_FRACTION = 0.45

# Trade only on narrow-CPR days (TC - BC as % of pivot).
MAX_CPR_WIDTH_PCT = 0.30

RR_MULTIPLE = 2.5
HARD_EXIT_TIME = datetime.time(15, 0)

# 5-min history pulled for the 5-min EMAs at each setup (single request, < 100-day API cap)
HISTORY_DAYS = 35

# 5-min history pulled once at startup to build daily OHLC (for CPR + daily EMA),
# exactly like the backtest does. ~600 calendar days (~410 trading days) lets the
# daily EMA100 fully converge to the backtest's value. Fetched in chunks.
DAILY_HISTORY_DAYS = 600
CHUNK_DAYS = 90               # Fyers caps intraday history at 100 days per request

# ---------- execution ----------
PAPER_TRADE = False          # True = no real orders, fills simulated at option LTP. Set False to go live.
LOTS = 1                    # qty = LOTS x exchange lot size (lot size read from symbol master)
PRODUCT_TYPE = "INTRADAY"
STRIKE_STEP = 100

# Nearest expiry with trading-days-to-expiry <= this -> roll to the next expiry
DTE_ROLL_THRESHOLD = 1

# Exchange holidays (weekdays only) - used for the trading-day DTE count and to skip the day.
# Keep this updated from the BSE holiday list.
MARKET_HOLIDAYS = {                  # 2026 trading holidays
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
    datetime.date(2026, 10, 20),     # Dussehra
    datetime.date(2026, 11, 10),     # Diwali Balipratipada
    datetime.date(2026, 11, 24),     # Guru Nanak Jayanti
    datetime.date(2026, 12, 25),     # Christmas
}

SYMBOL_MASTER_URL = "https://public.fyers.in/sym_details/BSE_FO.csv"

SETUP_FETCH_DELAY_SEC = 2    # wait after a candle closes before reading it from history
SETUP_FETCH_TIMEOUT_SEC = 60 # give up on a setup candle if history doesn't return it by then
LTP_STALE_SEC = 5            # fall back to REST quote if no websocket tick for this long
FILL_WAIT_SEC = 10

STATE_FILE = "sensex_live_state.json"
LOG_FILE = "sensex_first_candle_live.log"

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


# ======================================================
# STRATEGY LOGIC (copied from the backtest)
# ======================================================
def ema(series, period):
    return series.ewm(span=period, adjust=False).mean()


def compute_cpr(day):
    """CPR from the previous day's high, low and close."""
    h, l, c = day["high"], day["low"], day["close"]
    p = (h + l + c) / 3
    bc = (h + l) / 2
    tc = 2 * p - bc
    top, bottom = max(bc, tc), min(bc, tc)
    width_pct = (top - bottom) / p * 100
    return {"BC": bottom, "TC": top, "P": p, "width_pct": width_pct}


def daily_ema_at_setup(prev_daily_ema, setup_close):
    """Daily EMA value as seen at the setup candle, without using today's final close."""
    if DAILY_EMA_MODE == "prev":
        return prev_daily_ema
    alpha = 2 / (DAILY_EMA_PERIOD + 1)
    return alpha * setup_close + (1 - alpha) * prev_daily_ema


def check_setup(c, cpr, daily_ema):
    """Returns (side, reason): side is "BUY", "SELL" or None. Same rules as the backtest."""
    rng = c["high"] - c["low"]
    if rng <= 0:
        return None, "zero range"

    # ---------- BUY ----------
    if c["close"] > c["open"]:
        if c["close"] <= c["ema200"]:
            return None, f"BUY: close not above EMA{EMA_TREND_PERIOD}"
        if c["close"] <= daily_ema:
            return None, f"BUY: close not above daily EMA{DAILY_EMA_PERIOD}"
        if c["close"] <= c["ema"]:
            return None, f"BUY: close not above EMA{EMA_PERIOD}"
        if c["close"] <= cpr["TC"]:
            return None, "BUY: close not above TC"
        if abs(c["low"] - c["ema"]) > EMA_PROXIMITY_PCT / 100 * c["ema"]:
            return None, "BUY: low not near EMA"
        if (c["high"] - c["close"]) > MAX_WICK_PCT * rng:
            return None, "BUY: upper wick too big"
        return "BUY", "ok"

    # ---------- SELL ----------
    if c["close"] < c["open"]:
        if c["close"] >= c["ema200"]:
            return None, f"SELL: close not below EMA{EMA_TREND_PERIOD}"
        if c["close"] >= daily_ema:
            return None, f"SELL: close not below daily EMA{DAILY_EMA_PERIOD}"
        if c["close"] >= c["ema"]:
            return None, f"SELL: close not below EMA{EMA_PERIOD}"
        if c["close"] >= cpr["BC"]:
            return None, "SELL: close not below BC"
        if abs(c["high"] - c["ema"]) > EMA_PROXIMITY_PCT / 100 * c["ema"]:
            return None, "SELL: high not near EMA"
        if (c["close"] - c["low"]) > MAX_WICK_PCT * rng:
            return None, "SELL: lower wick too big"
        return "SELL", "ok"

    return None, "doji (close = open)"


def sl_target(side, setup, entry):
    """SL inside the setup candle, target = RR x risk. Same as backtest simulate_trade."""
    rng = float(setup["high"]) - float(setup["low"])
    if side == "BUY":
        sl = float(setup["low"]) + SL_CANDLE_FRACTION * rng
        risk = entry - sl
        target = entry + RR_MULTIPLE * risk
    else:
        sl = float(setup["high"]) - SL_CANDLE_FRACTION * rng
        risk = sl - entry
        target = entry - RR_MULTIPLE * risk
    return sl, risk, target


# ======================================================
# BROKER
# ======================================================
class Broker:
    def __init__(self):
        self.client = fyersModel.FyersModel(
            client_id=CLIENT_ID, token=ACCESS_TOKEN, is_async=False, log_path=""
        )

    def _history_req(self, symbol, start, end):
        return self.client.history({
            "symbol": symbol,
            "resolution": TIMEFRAME,
            "date_format": "1",
            "range_from": start.strftime("%Y-%m-%d"),
            "range_to": end.strftime("%Y-%m-%d"),
            "cont_flag": "1",
        })

    def history(self, symbol, start, end):
        resp = self._history_req(symbol, start, end)
        if resp.get("s") != "ok" or not resp.get("candles"):
            raise RuntimeError(f"history failed: {resp}")
        return resp["candles"]

    def history_chunked(self, symbol, start, end):
        """Long 5-min history in CHUNK_DAYS pieces. Any API error aborts (so the daily
        EMA is never computed on a gappy series)."""
        out = []
        cur = start
        while cur <= end:
            chunk_end = min(cur + datetime.timedelta(days=CHUNK_DAYS), end)
            resp = self._history_req(symbol, cur, chunk_end)
            if resp.get("s") == "ok":
                out.extend(resp.get("candles") or [])
            elif resp.get("s") != "no_data":
                raise RuntimeError(f"history failed for {cur} -> {chunk_end}: {resp}")
            cur = chunk_end + datetime.timedelta(days=1)
            time.sleep(0.3)
        if not out:
            raise RuntimeError("history returned no candles")
        return out

    def ltp(self, symbol):
        r = self.client.quotes({"symbols": symbol})
        if r.get("s") != "ok":
            raise RuntimeError(f"quote failed for {symbol}: {r}")
        return float(r["d"][0]["v"]["lp"])

    def market_order(self, symbol, qty, side, tag):
        """
        side: 1 = BUY, -1 = SELL.
        Returns dict(status=FILLED|REJECTED|UNKNOWN, price, order_id, info)
        """
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
# OPTION SELECTION (from Fyers symbol master)
# ======================================================
def trading_dte(today, expiry):
    """Trading days from today to expiry. Expiry today -> 0, expiry next trading day -> 1."""
    hol = [d.isoformat() for d in MARKET_HOLIDAYS]
    return int(np.busday_count(today.isoformat(), expiry.isoformat(), holidays=hol))


class OptionMaster:
    """
    Reads the Fyers BSE F&O symbol master. Column positions used:
    3 lot size, 8 expiry (epoch), 9 ticker, 13 underlying, 15 strike, 16 option type.
    Using real listed expiries avoids hand-built symbol formats and holiday shifts.
    """

    def __init__(self):
        raw = pd.read_csv(SYMBOL_MASTER_URL, header=None, low_memory=False)
        df = raw[[3, 8, 9, 13, 15, 16]].copy()
        df.columns = ["lot", "expiry_ts", "ticker", "underlying", "strike", "opt"]
        df = df[(df["underlying"].astype(str).str.strip() == "SENSEX")
                & (df["opt"].astype(str).str.strip().isin(["CE", "PE"]))].copy()

        if df.empty or not df["ticker"].astype(str).str.startswith("BSE:SENSEX").all():
            raise RuntimeError("Symbol master layout not as expected - no SENSEX options found")

        df["opt"] = df["opt"].astype(str).str.strip()
        df["strike"] = df["strike"].astype(float)
        df["lot"] = df["lot"].astype(int)
        df["expiry"] = (
            pd.to_datetime(df["expiry_ts"].astype("int64"), unit="s", utc=True)
              .dt.tz_convert("Asia/Kolkata").dt.date
        )
        self.df = df
        logger.info(f"Symbol master loaded: {len(df)} SENSEX options, sample {df['ticker'].iloc[0]}")

    def expiries(self, today):
        return sorted(e for e in self.df["expiry"].unique() if e >= today)

    def choose_expiry(self, today):
        exps = self.expiries(today)
        if not exps:
            raise RuntimeError("No upcoming SENSEX expiries in symbol master")
        nearest = exps[0]
        dte = trading_dte(today, nearest)
        if dte <= DTE_ROLL_THRESHOLD:
            if len(exps) < 2:
                raise RuntimeError("Need next expiry but only one is listed")
            return exps[1], nearest, dte
        return nearest, nearest, dte

    def pick_option(self, side, index_ltp, today):
        expiry, nearest, dte = self.choose_expiry(today)
        opt_type = "CE" if side == "BUY" else "PE"
        chain = self.df[(self.df["expiry"] == expiry) & (self.df["opt"] == opt_type)]
        if chain.empty:
            raise RuntimeError(f"No {opt_type} contracts for expiry {expiry}")
        atm = round(index_ltp / STRIKE_STEP) * STRIKE_STEP
        row = chain.loc[(chain["strike"] - atm).abs().idxmin()]
        return {
            "symbol": row["ticker"],
            "strike": float(row["strike"]),
            "opt_type": opt_type,
            "expiry": expiry,
            "nearest_expiry": nearest,
            "nearest_dte": dte,
            "lot": int(row["lot"]),
        }


# ======================================================
# MARKET DATA
# ======================================================
def candles_to_df(candles):
    """Same processing as the backtest's fetch_history."""
    df = pd.DataFrame(candles, columns=["ts", "open", "high", "low", "close", "volume"])
    df["time"] = (
        pd.to_datetime(df["ts"], unit="s", utc=True)
          .dt.tz_convert("Asia/Kolkata")
          .dt.tz_localize(None)
    )
    df.set_index("time", inplace=True)
    df.sort_index(inplace=True)
    df = df.between_time("09:15", "15:30")
    df = df[~df.index.duplicated(keep="first")]
    return df


def load_candles(broker, today):
    """5-min index candles with the fast EMA and trend EMA."""
    candles = broker.history(INDEX_SYMBOL, today - datetime.timedelta(days=HISTORY_DAYS), today)
    df = candles_to_df(candles)
    df["ema"] = ema(df["close"], EMA_PERIOD)
    df["ema200"] = ema(df["close"], EMA_TREND_PERIOD)
    return df


def load_daily_context(broker, today):
    """
    Builds daily OHLC from 5-min candles (09:15-15:30) exactly like the backtest, then
    returns the previous day's CPR and the previous day's final daily EMA.
    """
    candles = broker.history_chunked(
        INDEX_SYMBOL, today - datetime.timedelta(days=DAILY_HISTORY_DAYS), today
    )
    df = candles_to_df(candles)
    past = df[df.index.date < today]
    daily = past.resample("1D").agg(
        {"open": "first", "high": "max", "low": "min", "close": "last"}
    ).dropna()
    if len(daily) < DAILY_EMA_PERIOD * 2:
        raise RuntimeError(
            f"Only {len(daily)} daily bars for daily EMA{DAILY_EMA_PERIOD} - need more history"
        )
    daily["dema"] = ema(daily["close"], DAILY_EMA_PERIOD)
    prev = daily.iloc[-1]
    return {
        "cpr": compute_cpr(prev),
        "prev_dema": float(prev["dema"]),
        "prev_day": daily.index[-1].date(),
        "n_days": len(daily),
    }


# ======================================================
# STATE (survives restarts within the same day)
# ======================================================
def load_state(today):
    blank = {"date": today.isoformat(), "trades_today": 0, "evaluated": [], "position": None}
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
    def __init__(self, broker, master, today, cpr, prev_dema):
        self.broker = broker
        self.master = master
        self.today = today
        self.cpr = cpr
        self.prev_dema = prev_dema
        self.lock = threading.RLock()
        self.last_ltp = None
        self.last_tick = 0.0
        self.exit_blocked = False      # set when an exit order's status is unknown
        self.state = load_state(today)

        if self.state["position"]:
            p = self.state["position"]
            notify(f"♻️ RESUMED open position\n{p['side']} {p['symbol']} qty={p['qty']}\n"
                   f"Index SL={p['sl']} Target={p['target']}")

    # ---------- prices ----------
    def on_tick(self, ltp):
        self.last_ltp = ltp
        self.last_tick = time.time()
        self.check_exit(ltp, now_ist())

    def index_ltp(self):
        if self.last_ltp is not None and time.time() - self.last_tick <= LTP_STALE_SEC:
            return self.last_ltp
        ltp = self.broker.ltp(INDEX_SYMBOL)
        self.last_ltp, self.last_tick = ltp, time.time()
        return ltp

    # ---------- setups ----------
    def run_setups(self, now):
        for st in SETUP_CANDLE_TIMES:
            tag = st.strftime("%H:%M")
            if tag in self.state["evaluated"]:
                continue

            setup_ts = datetime.datetime.combine(self.today, st)
            ready_at = setup_ts + datetime.timedelta(minutes=CANDLE_MIN, seconds=SETUP_FETCH_DELAY_SEC)
            entry_candle_end = setup_ts + datetime.timedelta(minutes=2 * CANDLE_MIN)

            if now < ready_at:
                return   # setups are processed in order

            # Mark first so a crash/restart can never take the same setup twice
            with self.lock:
                self.state["evaluated"].append(tag)
                save_state(self.state)
                busy = self.state["position"] is not None
                done = self.state["trades_today"] >= MAX_TRADES_PER_DAY

            if done:
                logger.info(f"{tag}: skipped (max trades reached)")
                continue
            if busy:
                logger.info(f"{tag}: skipped (previous trade still open)")
                continue
            if now >= entry_candle_end:
                notify(f"⏭ {tag}: skipped - started too late to enter on the next candle")
                continue

            candle = self.fetch_setup_candle(setup_ts)
            if candle is None:
                notify(f"⚠️ {tag}: setup candle not available from history - skipped", logging.WARNING)
                continue

            daily_ema = daily_ema_at_setup(self.prev_dema, float(candle["close"]))
            side, why = check_setup(candle, self.cpr, daily_ema)
            notify(
                f"🔎 {tag} setup: {side or 'NO TRADE'} ({why})\n"
                f"O={candle['open']:.2f} H={candle['high']:.2f} L={candle['low']:.2f} C={candle['close']:.2f}\n"
                f"EMA{EMA_PERIOD}={candle['ema']:.2f} EMA{EMA_TREND_PERIOD}={candle['ema200']:.2f} "
                f"Daily EMA{DAILY_EMA_PERIOD}={daily_ema:.2f}\n"
                f"TC={self.cpr['TC']:.2f} BC={self.cpr['BC']:.2f}"
            )
            if side:
                self.enter(side, candle, tag)

    def fetch_setup_candle(self, setup_ts):
        deadline = time.time() + SETUP_FETCH_TIMEOUT_SEC
        while time.time() < deadline:
            try:
                df = load_candles(self.broker, self.today)
                if setup_ts in df.index:
                    return df.loc[setup_ts]
            except Exception as e:
                logger.warning(f"history fetch failed: {e}")
            time.sleep(2)
        return None

    # ---------- entry ----------
    def enter(self, side, setup, tag):
        try:
            ltp = self.index_ltp()
        except Exception as e:
            notify(f"❌ ENTRY ABORTED: could not get index LTP ({e})", logging.ERROR)
            return

        sl, risk, target = sl_target(side, setup, ltp)
        if risk <= 0:
            notify(f"⏭ {tag} {side}: index {ltp} already at/past SL {sl:.2f} - skipped")
            return

        try:
            opt = self.master.pick_option(side, ltp, self.today)
        except Exception as e:
            notify(f"❌ ENTRY ABORTED: option selection failed ({e})", logging.ERROR)
            return

        qty = opt["lot"] * LOTS
        expiry_note = (
            f"nearest {opt['nearest_expiry']} is DTE{opt['nearest_dte']} -> next expiry {opt['expiry']}"
            if opt["expiry"] != opt["nearest_expiry"]
            else f"nearest expiry {opt['expiry']} (DTE{opt['nearest_dte']})"
        )

        r = self.broker.market_order(opt["symbol"], qty, 1, f"ENTRY{side}")
        if r["status"] == "REJECTED":
            notify(f"❌ ENTRY ORDER REJECTED {opt['symbol']}\n{r['info']}", logging.ERROR)
            return

        position = {
            "side": side,
            "setup": tag,
            "symbol": opt["symbol"],
            "qty": qty,
            "index_entry": round(ltp, 2),
            "sl": round(sl, 2),
            "target": round(target, 2),
            "risk": round(risk, 2),
            "opt_entry": r["price"],
            "order_id": r["order_id"],
            "entry_time": now_ist().isoformat(timespec="seconds"),
        }
        with self.lock:
            self.state["position"] = position
            self.state["trades_today"] += 1
            save_state(self.state)

        warn = "\n⚠️ Fill not confirmed - CHECK BROKER" if r["status"] == "UNKNOWN" else ""
        notify(
            f"🚀 ENTRY {side} {'(PAPER)' if PAPER_TRADE else ''}\n"
            f"Bought {opt['symbol']} x{qty} @ {r['price']}\n"
            f"Expiry: {expiry_note}\n"
            f"Index={ltp:.2f} SL={sl:.2f} Target={target:.2f} Risk={risk:.2f} pts{warn}"
        )

    # ---------- exit ----------
    def check_exit(self, ltp, now):
        with self.lock:
            pos = self.state["position"]
            if not pos or self.exit_blocked:
                return
            reason = None
            if pos["side"] == "BUY":
                if ltp <= pos["sl"]:
                    reason = "SL"
                elif ltp >= pos["target"]:
                    reason = "TARGET"
            else:
                if ltp >= pos["sl"]:
                    reason = "SL"
                elif ltp <= pos["target"]:
                    reason = "TARGET"
            if reason is None and now.time() >= HARD_EXIT_TIME:
                reason = "TIME_EXIT"
            if reason:
                self.exit(reason, ltp)

    def exit(self, reason, ltp):
        with self.lock:
            pos = self.state["position"]
            if not pos or self.exit_blocked:
                return

            r = None
            for attempt in range(3):
                r = self.broker.market_order(pos["symbol"], pos["qty"], -1, f"EXIT{reason.replace('_', '')}")
                if r["status"] == "FILLED":
                    break
                if r["status"] == "UNKNOWN":
                    break      # don't resend: a duplicate sell could open a short
                logger.error(f"exit attempt {attempt + 1} rejected: {r['info']}")
                time.sleep(1)

            if r["status"] != "FILLED":
                self.exit_blocked = True
                notify(
                    f"🆘 EXIT {reason} NOT CONFIRMED for {pos['symbol']} x{pos['qty']}\n"
                    f"Status={r['status']} {r['info']}\nSQUARE OFF MANUALLY",
                    logging.CRITICAL,
                )
                return

            idx_pts = (ltp - pos["index_entry"]) if pos["side"] == "BUY" else (pos["index_entry"] - ltp)
            opt_pnl = ""
            if pos.get("opt_entry") and r["price"]:
                pts = r["price"] - pos["opt_entry"]
                opt_pnl = f"\nOption: {pos['opt_entry']} -> {r['price']} = {pts:.2f} pts (₹{pts * pos['qty']:.0f})"

            self.state["position"] = None
            save_state(self.state)
            notify(
                f"🏁 EXIT {reason} {'(PAPER)' if PAPER_TRADE else ''}\n"
                f"{pos['symbol']} x{pos['qty']}\n"
                f"Index {pos['index_entry']} -> {ltp:.2f} ({idx_pts:+.2f} pts, "
                f"{idx_pts / pos['risk']:+.2f}R){opt_pnl}"
            )

    def day_done(self, now):
        with self.lock:
            if self.state["position"] is not None:
                return False
            all_checked = all(t.strftime("%H:%M") in self.state["evaluated"] for t in SETUP_CANDLE_TIMES)
            return all_checked or now.time() >= HARD_EXIT_TIME


# ======================================================
# WEBSOCKET
# ======================================================
def start_websocket(trader):
    def on_message(msg):
        try:
            if isinstance(msg, dict) and msg.get("symbol") == INDEX_SYMBOL and "ltp" in msg:
                trader.on_tick(float(msg["ltp"]))
        except Exception:
            logger.exception("tick handler error")

    def on_connect():
        ws.subscribe(symbols=[INDEX_SYMBOL], data_type="SymbolUpdate")
        logger.info("Websocket connected, subscribed to index")

    def on_error(msg):
        logger.warning(f"Websocket error: {msg}")

    def on_close(msg):
        logger.warning(f"Websocket closed: {msg}")

    ws = data_ws.FyersDataSocket(
        access_token=f"{CLIENT_ID}:{ACCESS_TOKEN}",
        log_path="",
        litemode=False,
        write_to_file=False,
        reconnect=True,
        on_connect=on_connect,
        on_close=on_close,
        on_error=on_error,
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
    if today.weekday() >= 5 or today in MARKET_HOLIDAYS:
        logger.info("Market closed today - exiting")
        return

    broker = Broker()

    try:
        ctx = load_daily_context(broker, today)
    except Exception as e:
        notify(f"❌ STARTUP FAILED: {e}", logging.ERROR)
        raise

    cpr, prev_dema, prev_day = ctx["cpr"], ctx["prev_dema"], ctx["prev_day"]

    # A restart with an open position must keep monitoring it, even on a wide-CPR day
    has_open_position = load_state(today)["position"] is not None

    if cpr["width_pct"] > MAX_CPR_WIDTH_PCT and not has_open_position:
        notify(
            f"⏸ NO TRADE TODAY - wide CPR\n"
            f"CPR from {prev_day}: width {cpr['width_pct']:.3f}% > {MAX_CPR_WIDTH_PCT}%\n"
            f"P={cpr['P']:.2f} TC={cpr['TC']:.2f} BC={cpr['BC']:.2f}"
        )
        return

    try:
        master = OptionMaster()
        load_candles(broker, today)          # sanity check that intraday history works
        expiry, nearest, dte = master.choose_expiry(today)
    except Exception as e:
        notify(f"❌ STARTUP FAILED: {e}", logging.ERROR)
        raise

    notify(
        f"🚀 STRATEGY STARTED {'(PAPER)' if PAPER_TRADE else '(LIVE)'}\n"
        f"CPR from {prev_day}: P={cpr['P']:.2f} TC={cpr['TC']:.2f} BC={cpr['BC']:.2f} "
        f"width={cpr['width_pct']:.3f}%\n"
        f"Daily EMA{DAILY_EMA_PERIOD} (prev close) = {prev_dema:.2f} [{ctx['n_days']} daily bars, mode={DAILY_EMA_MODE}]\n"
        f"EMA{EMA_PERIOD} / EMA{EMA_TREND_PERIOD} | SL frac={SL_CANDLE_FRACTION} | RR={RR_MULTIPLE}\n"
        f"Nearest expiry {nearest} (DTE{dte}) -> trading {expiry}\n"
        f"Lots={LOTS}"
    )

    trader = LiveTrader(broker, master, today, cpr, prev_dema)
    ws = start_websocket(trader)

    try:
        while True:
            now = now_ist()

            # Backup exit check (in case websocket ticks stop)
            if trader.state["position"]:
                try:
                    trader.check_exit(trader.index_ltp(), now)
                except Exception as e:
                    logger.warning(f"LTP fallback failed: {e}")

            trader.run_setups(now)

            if trader.day_done(now):
                notify("✅ Done for the day")
                break

            time.sleep(1)
    except KeyboardInterrupt:
        if trader.state["position"]:
            notify("⚠️ Script stopped with an OPEN position - restart to resume monitoring or exit manually",
                   logging.WARNING)
    finally:
        try:
            ws.close_connection()
        except Exception:
            pass


if __name__ == "__main__":
    main()
