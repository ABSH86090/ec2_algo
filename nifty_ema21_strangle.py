# =====================================================================
# NIFTY COMBINED-PREMIUM EMA21 SHORT STRANGLE (5-min) - LIVE
#
# Same rules as combined_ema21_backtest.py:
#   * Trades only on TRADE_DTE days (default {0, 1}: expiry day and the
#     session before it). DTE comes from the real expiry in the symbol master.
#   * Strikes from the Nifty 09:16 1-min candle OPEN -> ATM.
#       Expiry day : OTM2 CE + OTM3 PE      Other days : OTM4 CE + OTM4 PE
#     Nearest expiry (expiry day itself included -> 0DTE).
#   * COMBINED chart (CE + PE) in 5-min candles built from 1-min data
#     ("minute" method), EMA21 on the combined close, warmed up on the same
#     two contracts over the previous WARMUP_DAYS trading days.
#   * Entry: first RED combined candle of the day = reference. Then the first
#     later RED candle that CLOSES below the reference LOW and below EMA21
#     -> SELL CE and PE at its close. One trade per day.
#   * SL = entry (combined candle close) + 70% of the 09:15 candle's range,
#     capped at MAX_SL_POINTS above entry.
#   * Exit: combined LTP (CE LTP + PE LTP) >= SL, or EXIT_TIME.
#     Optional EMA_EXIT: green combined candle closing above EMA21.
#   * Optional hedge: buy far-OTM CE and PE BEFORE the sells, sell them AFTER
#     the shorts are covered.
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

# ---------- strikes (same as backtest) ----------
INDEX_SYMBOL = "NSE:NIFTY50-INDEX"
UNDERLYING = "NIFTY"
STRIKE_STEP = 50
CE_OTM_STEPS = 1                    # expiry day: OTM2 CE
PE_OTM_STEPS = 1                    # expiry day: OTM3 PE
NONEXP_CE_OTM_STEPS = 2             # other days: OTM4 CE
NONEXP_PE_OTM_STEPS = 2             # other days: OTM4 PE
STRIKE_TIME = datetime.time(9, 16)  # spot 1-min candle used to pick ATM ...
STRIKE_PRICE_FIELD = "open"         # ... its open = the price at 09:16:00
TRADE_DTE = {0, 1}                  # trading days to expiry to trade; None = every day

# ---------- candles / indicator ----------
RESOLUTION = 5
EMA_PERIOD = 21
WARMUP_DAYS = 2

# ---------- entry ----------
FIRST_CANDLE = datetime.time(9, 15)
ENTRY_LATEST = None                 # breakdown candle must CLOSE by this time; None = until EXIT_TIME
REQUIRE_BELOW_EMA = True
SL_MODE = "range"                   # "range" or "high" (see backtest)
SL_RANGE_PCT = 0.70
SL_FROM = "entry"                   # "entry" or "low"
SL_CANDLE = "first"                 # for SL_MODE="high": "first" / "first_red" / "breakdown"
SL_BUFFER = 0.0
MAX_SL_POINTS = 60                  # None = no limit
MAX_SL_MODE = "cap"                 # "cap" or "skip"
SL_FROM_FILL = True                 # True -> SL = actual combined fill + the SL distance
                                    # (if the fill slips, the SL moves with it, so a bad
                                    # fill can't land at or above the SL)

# ---------- exit ----------
EMA_EXIT = False                    # also exit on a green combined candle closing above EMA21
EXIT_TIME = datetime.time(15, 29)   # time exit = last tradable minute (orders at 15:30 are rejected)
SL_CHECK = "minute"                 # "tick"   -> exit the moment CE LTP + PE LTP >= SL
                                    # "minute" -> check only at each 1-min close (closer to the
                                    #             backtest's "minute" candles, fewer wick hits)

# ---------- size ----------
EXPIRY_LOTS = 3                     # lots on expiry day
NONEXP_LOTS = 4                     # lots on other days (lot size comes from the symbol master)

# ---------- hedge ----------
HEDGE_ENABLED = True                # buy far-OTM CE + PE before the sells (backtest had no hedge)
HEDGE_DISTANCE_0DTE = 700           # CE hedge = CE strike + this, PE hedge = PE strike - this
HEDGE_DISTANCE_OTHER = 1000

# ---------- execution ----------
PAPER_TRADE = False                  # True = no real orders (fills at LTP). Set False to go live.
PRODUCT_TYPE = "MARGIN"             # Fyers carry-forward (NRML) - no broker auto square-off

MARKET_HOLIDAYS = {                 # NSE F&O holidays 2026 (weekdays) - keep updated
    datetime.date(2026, 1, 15), datetime.date(2026, 1, 26), datetime.date(2026, 3, 3),
    datetime.date(2026, 3, 26), datetime.date(2026, 3, 31), datetime.date(2026, 4, 3),
    datetime.date(2026, 4, 14), datetime.date(2026, 5, 1), datetime.date(2026, 5, 28),
    datetime.date(2026, 6, 26), datetime.date(2026, 9, 14), datetime.date(2026, 10, 2),
    datetime.date(2026, 10, 20), datetime.date(2026, 11, 10), datetime.date(2026, 11, 24),
    datetime.date(2026, 12, 25),
}

SYMBOL_MASTER_URL = "https://public.fyers.in/sym_details/NSE_FO.csv"

SPOT_FETCH_TIMEOUT_SEC = 90     # wait this long for the 09:16 spot candle before using LTP
CANDLE_FETCH_DELAY_SEC = 3      # wait after a candle closes before reading it
CANDLE_FETCH_TIMEOUT_SEC = 45   # after this, use the candle even if the last minute is missing
CANDLE_RETRY_SEC = 2
LATE_ENTRY_GRACE_SEC = 60       # a breakdown read later than this after its close is not traded
LTP_STALE_SEC = 5               # REST quote fallback if no websocket tick for this long
FILL_WAIT_SEC = 10
EXIT_RETRIES = 3

STATE_FILE = "nifty_combined_ema21_state.json"
LOG_FILE = "nifty_combined_ema21_live.log"

OHLC = ["open", "high", "low", "close"]
AGG = {"open": "first", "high": "max", "low": "min", "close": "last"}
STEP = datetime.timedelta(minutes=RESOLUTION)

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
    return t.replace(minute=t.minute - t.minute % RESOLUTION, second=0, microsecond=0)


def is_trading_day(d):
    return d.weekday() < 5 and d not in MARKET_HOLIDAYS


def prev_trading_days(d, n):
    days = []
    while len(days) < n:
        d -= datetime.timedelta(days=1)
        if is_trading_day(d):
            days.append(d)
    return sorted(days)


def trading_days_to_expiry(day, expiry):
    """Trading sessions after `day` up to and including `expiry` (0 on expiry day)."""
    n, d = 0, day
    while d < expiry:
        d += datetime.timedelta(days=1)
        if is_trading_day(d):
            n += 1
    return n


def hedge_distance(dte0):
    return HEDGE_DISTANCE_0DTE if dte0 else HEDGE_DISTANCE_OTHER


def candles_to_df(candles):
    df = pd.DataFrame([c[:6] for c in candles],
                      columns=["ts", "open", "high", "low", "close", "volume"])
    df["time"] = (pd.to_datetime(df["ts"], unit="s", utc=True)
                    .dt.tz_convert("Asia/Kolkata").dt.tz_localize(None))
    df = df.set_index("time").sort_index()
    df = df.between_time("09:15", "15:29")
    return df[~df.index.duplicated(keep="first")]


def empty_ohlc():
    return pd.DataFrame(columns=OHLC, index=pd.DatetimeIndex([], name="time"), dtype=float)


# ======================================================
# COMBINED CHART (identical to the backtest, "minute" method)
# ======================================================
def align_legs(ce, pe):
    """Both legs on the same 1-min index; a missing minute on one leg is
    filled from its last close (same day only)."""
    idx = ce.index.union(pe.index)
    parts = {}
    for name, df in (("ce", ce), ("pe", pe)):
        x = df[OHLC].astype(float).reindex(idx)
        x["close"] = x.groupby(x.index.date)["close"].ffill()
        for f in ("open", "high", "low"):
            x[f] = x[f].fillna(x["close"])
        parts[name] = x
    return pd.concat(parts, axis=1).dropna()


def resample(df):
    return (df.resample(f"{RESOLUTION}min", label="left", closed="left")
              .agg(AGG).dropna())


def build_combined(ce, pe):
    m = align_legs(ce, pe)
    comb = pd.DataFrame({f: m[("ce", f)] + m[("pe", f)] for f in OHLC}, index=m.index)
    comb["high"] = comb[["open", "close"]].max(axis=1)
    comb["low"] = comb[["open", "close"]].min(axis=1)
    c5 = resample(comb)
    c5["ema21"] = c5["close"].ewm(span=EMA_PERIOD, adjust=False).mean()
    return c5


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

    def ltps(self, symbols):
        r = self.client.quotes({"symbols": ",".join(symbols)})
        if r.get("s") != "ok":
            raise RuntimeError(f"quote failed for {symbols}: {r}")
        return {item["n"]: float(item["v"]["lp"]) for item in r["d"]}

    def _place(self, symbol, qty, side, tag):
        resp = self.client.place_order({
            "symbol": symbol,
            "qty": int(qty),
            "type": 2,                 # MARKET
            "side": side,              # 1 BUY, -1 SELL
            "productType": PRODUCT_TYPE,
            "limitPrice": 0,
            "stopPrice": 0,
            "validity": "DAY",
            "disclosedQty": 0,
            "offlineOrder": False,
            "orderTag": tag,
        })
        if not isinstance(resp, dict) or resp.get("s") != "ok":
            return None, resp
        return resp.get("id"), None

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

    def market_orders(self, orders):
        """orders: list of (symbol, qty, side, tag). All are placed first, then
        each fill is confirmed. Returns one dict per order:
        status FILLED | REJECTED | UNKNOWN, price, order_id, info."""
        if PAPER_TRADE:
            px = self.ltps([o[0] for o in orders])
            return [{"status": "FILLED", "price": px.get(o[0]), "order_id": "PAPER",
                     "info": "paper"} for o in orders]
        placed = [self._place(*o) for o in orders]
        out = []
        for oid, err in placed:
            if oid is None:
                out.append({"status": "REJECTED", "price": None, "order_id": None, "info": err})
            else:
                st, px, info = self._wait_fill(oid)
                out.append({"status": st, "price": px, "order_id": oid, "info": info})
        return out

    def orders_with_retry(self, orders):
        """Like market_orders, but REJECTED orders are re-sent (never UNKNOWN ones,
        which could double the position)."""
        res = self.market_orders(orders)
        for attempt in range(1, EXIT_RETRIES):
            todo = [i for i, r in enumerate(res) if r["status"] == "REJECTED"]
            if not todo:
                break
            for i in todo:
                logger.error(f"{orders[i][3]} {orders[i][0]} rejected "
                             f"(attempt {attempt}): {res[i]['info']} - retrying")
            time.sleep(1)
            again = self.market_orders([orders[i] for i in todo])
            for i, r in zip(todo, again):
                res[i] = r
        return res


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
# CONTRACT SELECTION (09:16 spot open -> ATM -> OTM CE / PE)
# ======================================================
def spot_at_strike_time(broker, today):
    ts = pd.Timestamp(datetime.datetime.combine(today, STRIKE_TIME))
    deadline = time.time() + SPOT_FETCH_TIMEOUT_SEC
    while time.time() < deadline:
        try:
            df = broker.history(INDEX_SYMBOL, "1", today, today)
            if ts in df.index:
                return float(df.loc[ts, STRIKE_PRICE_FIELD]), f"{STRIKE_TIME:%H:%M} 1-min {STRIKE_PRICE_FIELD}"
        except Exception as e:
            logger.warning(f"spot history: {e}")
        time.sleep(3)
    return broker.ltps([INDEX_SYMBOL])[INDEX_SYMBOL], "LTP (fallback)"


def resolve_contract(broker, master, today, expiry):
    spot, src = spot_at_strike_time(broker, today)
    atm = round(spot / STRIKE_STEP) * STRIKE_STEP
    dte0 = expiry == today
    ce_steps, pe_steps = ((CE_OTM_STEPS, PE_OTM_STEPS) if dte0
                          else (NONEXP_CE_OTM_STEPS, NONEXP_PE_OTM_STEPS))
    legs = {}
    for opt, target in (("CE", atm + ce_steps * STRIKE_STEP),
                        ("PE", atm - pe_steps * STRIKE_STEP)):
        row = master.pick(expiry, opt, target)
        if row["strike"] != target:
            notify(f"⚠️ {opt} strike {target} not listed - using {row['strike']}", logging.WARNING)
        legs[opt] = row
    contract = {"spot": spot, "spot_src": src, "atm": atm, "expiry": expiry.isoformat(),
                "dte0": dte0, "dte": trading_days_to_expiry(today, expiry),
                "ce_steps": ce_steps, "pe_steps": pe_steps}
    return contract, legs


# ======================================================
# STATE (survives restarts within the same day)
# ======================================================
def load_state(today):
    blank = {"date": today.isoformat(), "contract": None, "legs": {},
             "last_candle": None,       # start of the last processed 5-min candle
             "first": None,             # 09:15 combined candle {high, low}
             "ref": None,               # first red combined candle {time, high, low}
             "signal_done": False, "note": "",
             "position": None, "trades": []}
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
        self.legs = state["legs"]
        self.lock = threading.RLock()
        self.ticks = {}                    # symbol -> (ltp, time)
        self.exit_blocked = False
        self.retry_at = 0.0
        self.last_minute_checked = None
        self.first_ts = datetime.datetime.combine(today, FIRST_CANDLE)
        self.warm = self.load_warmup()

        p = self.state["position"]
        if p:
            notify(f"♻️ RESUMED open position\nSELL {p['ce']['symbol']} + {p['pe']['symbol']} "
                   f"x{p['qty']} @ combined {p['entry_px']}\nSL={p['sl']}")

    # ---------- warm-up ----------
    def load_warmup(self):
        days = prev_trading_days(self.today, WARMUP_DAYS)
        warm = {}
        for name, leg in self.legs.items():
            try:
                df = self.broker.history(leg["symbol"], "1", days[0], days[-1])
                warm[name] = df[df.index.date < self.today][OHLC]
                logger.info(f"{name} warm-up: {len(warm[name])} 1-min candles")
            except Exception as e:
                notify(f"⚠️ {name} warm-up history failed ({e}) - EMA21 starts cold",
                       logging.WARNING)
                warm[name] = empty_ohlc()
        return warm

    # ---------- prices ----------
    def on_tick(self, symbol, ltp):
        self.ticks[symbol] = (ltp, time.time())
        if SL_CHECK != "tick":
            return
        pos = self.state["position"]
        if pos and symbol in (pos["ce"]["symbol"], pos["pe"]["symbol"]):
            other = pos["pe"]["symbol"] if symbol == pos["ce"]["symbol"] else pos["ce"]["symbol"]
            t = self.ticks.get(other)
            if t and time.time() - t[1] <= LTP_STALE_SEC:
                self.check_exit(ltp + t[0], now_ist())

    def combined_ltp(self, pos):
        syms = [pos["ce"]["symbol"], pos["pe"]["symbol"]]
        stale = [s for s in syms
                 if not (self.ticks.get(s) and time.time() - self.ticks[s][1] <= LTP_STALE_SEC)]
        if stale:
            for s, px in self.broker.ltps(stale).items():
                self.ticks[s] = (px, time.time())
        return round(sum(self.ticks[s][0] for s in syms), 2)

    # ---------- candles ----------
    def combined_until(self, close_dt):
        """Combined 5-min candles (with EMA21) that closed by close_dt.
        Returns (c5, complete) - complete = the last minute is in the data."""
        cutoff = pd.Timestamp(close_dt)
        today = {}
        for name, leg in self.legs.items():
            try:
                df = self.broker.history(leg["symbol"], "1", self.today, self.today)[OHLC]
            except Exception as e:
                logger.warning(f"{name} history: {e}")
                df = empty_ohlc()
            today[name] = df[(df.index.date == self.today) & (df.index < cutoff)]
        last_min = cutoff - pd.Timedelta(minutes=1)
        complete = any(len(d) and d.index.max() >= last_min for d in today.values())
        ce = pd.concat([self.warm["CE"], today["CE"]])
        pe = pd.concat([self.warm["PE"], today["PE"]])
        if ce.empty or pe.empty:
            return None, False
        return build_combined(ce, pe), complete

    def next_candle_start(self):
        last = self.state["last_candle"]
        return self.first_ts if last is None else datetime.datetime.fromisoformat(last) + STEP

    def needs_candles(self):
        if self.state["position"] is not None:
            return EMA_EXIT
        return not self.state["signal_done"]

    def run_candles(self, now):
        if not self.needs_candles() or time.time() < self.retry_at:
            return
        nxt = self.next_candle_start()
        if now < nxt + STEP + datetime.timedelta(seconds=CANDLE_FETCH_DELAY_SEC):
            return
        latest_close = floor_candle(now - datetime.timedelta(seconds=CANDLE_FETCH_DELAY_SEC))
        waited = (now - (latest_close)).total_seconds()

        c5, complete = self.combined_until(latest_close)
        if c5 is None or (not complete and waited < CANDLE_FETCH_TIMEOUT_SEC):
            self.retry_at = time.time() + CANDLE_RETRY_SEC
            return
        if not complete:
            logger.warning(f"{latest_close:%H:%M}: last minute missing on both legs - using "
                           f"available data")

        start = nxt
        while start + STEP <= latest_close and self.needs_candles():
            tag = start.strftime("%H:%M")
            with self.lock:                          # mark first: never act twice on restart
                self.state["last_candle"] = start.isoformat()
                save_state(self.state)
            ts = pd.Timestamp(start)
            if ts not in c5.index:
                if start == self.first_ts:
                    self.finish_signals(f"no {tag} combined candle - no trade today", warn=True)
                else:
                    logger.warning(f"{tag}: no combined candle")
            else:
                self.process(start, c5.loc[ts], now)
            start += STEP

    def finish_signals(self, note, warn=False):
        with self.lock:
            self.state["signal_done"] = True
            self.state["note"] = note
            save_state(self.state)
        notify(("⚠️ " if warn else "⏸ ") + note, logging.WARNING if warn else logging.INFO)

    def process(self, start, c, now):
        s = self.state
        tag = start.strftime("%H:%M")
        close_dt = start + STEP
        o, h, l, cl, ema = (float(c["open"]), float(c["high"]), float(c["low"]),
                            float(c["close"]), float(c["ema21"]))
        info = f"O={o:.2f} H={h:.2f} L={l:.2f} C={cl:.2f} EMA21={ema:.2f}"
        logger.info(f"{tag} combined {info}")

        if s["position"] is not None:                # only EMA_EXIT gets here
            if cl > o and cl > ema:
                pos = s["position"]
                self.exit("EMA", self.combined_ltp(pos))
            return

        if start == self.first_ts:
            s["first"] = {"high": h, "low": l}
            save_state(s)
            notify(f"🕘 09:15 combined candle {info}\nRange {h - l:.2f} -> "
                   f"SL distance {SL_RANGE_PCT:.0%} = {SL_RANGE_PCT * (h - l):.2f} pts")
        if s["first"] is None:
            self.finish_signals("09:15 combined candle not seen - no trade today", warn=True)
            return

        red = cl < o
        if s["ref"] is None:
            if red:
                s["ref"] = {"time": tag, "high": h, "low": l}
                save_state(s)
                notify(f"🔴 First red combined candle {tag} - watching for a red close "
                       f"< {l:.2f}{' and < EMA21' if REQUIRE_BELOW_EMA else ''}\n{info}")
            return

        ref = s["ref"]
        if not (red and cl < ref["low"] and (not REQUIRE_BELOW_EMA or cl < ema)):
            return

        # ---- breakdown candle ----
        if ENTRY_LATEST is not None and close_dt.time() > ENTRY_LATEST:
            self.finish_signals(f"late breakdown: {tag} candle closes after "
                                f"{ENTRY_LATEST:%H:%M} - no trade today")
            return
        if close_dt.time() >= EXIT_TIME:
            self.finish_signals(f"breakdown {tag} too close to exit time - no trade today")
            return
        if (now - close_dt).total_seconds() > LATE_ENTRY_GRACE_SEC:
            self.finish_signals(f"breakdown {tag} read too late to enter at its close "
                                f"(script restarted?) - no trade today", warn=True)
            return

        first = s["first"]
        if SL_MODE == "range":
            base = cl if SL_FROM == "entry" else first["low"]
            sl = base + SL_RANGE_PCT * (first["high"] - first["low"])
        else:
            sl = {"first": first["high"], "first_red": ref["high"], "breakdown": h}[SL_CANDLE]
        sl = round(sl + SL_BUFFER, 2)
        risk = round(sl - cl, 2)
        if risk <= 0:
            self.finish_signals(f"{tag} breakdown close {cl:.2f} is not below SL {sl} - skipped")
            return
        capped = False
        if MAX_SL_POINTS is not None and risk > MAX_SL_POINTS:
            if MAX_SL_MODE == "skip":
                self.finish_signals(f"{tag} SL {sl} is {risk} pts above entry "
                                    f"(> {MAX_SL_POINTS}) - skipped")
                return
            sl, risk, capped = round(cl + MAX_SL_POINTS, 2), MAX_SL_POINTS, True
        self.enter(tag, cl, ema, sl, risk, capped)

    # ---------- entry ----------
    def unwind(self, items, why):
        """Close already-opened legs/hedges. items: list of (symbol, qty, side)."""
        if not items:
            return
        res = self.broker.orders_with_retry([(sym, q, side, "C21UNWIND") for sym, q, side in items])
        for (sym, q, side), r in zip(items, res):
            if r["status"] != "FILLED":
                notify(f"🆘 Could not unwind {sym} x{q} ({why}) - "
                       f"{'BUY' if side == 1 else 'SELL'} IT MANUALLY\n{r['info']}",
                       logging.CRITICAL)

    def enter(self, tag, signal_close, ema, sl, risk, capped):
        ct = self.state["contract"]
        ce, pe = self.legs["CE"], self.legs["PE"]
        lots = EXPIRY_LOTS if ct["dte0"] else NONEXP_LOTS
        qty = ce["lot"] * lots

        with self.lock:                              # one trade per day, even on crash
            self.state["signal_done"] = True
            self.state["note"] = f"{tag} entry"
            save_state(self.state)

        # 1) hedges first
        hedges = []
        if HEDGE_ENABLED:
            dist = hedge_distance(ct["dte0"])
            expiry = datetime.date.fromisoformat(ct["expiry"])
            try:
                hce = self.master.pick(expiry, "CE", ce["strike"] + dist)
                hpe = self.master.pick(expiry, "PE", pe["strike"] - dist)
            except Exception as e:
                notify(f"❌ {tag} ENTRY ABORTED: hedge lookup failed ({e})", logging.ERROR)
                return
            res = self.broker.market_orders([(hce["symbol"], qty, 1, "C21HEDGE"),
                                             (hpe["symbol"], qty, 1, "C21HEDGE")])
            if any(r["status"] != "FILLED" for r in res):
                filled = [(h["symbol"], qty, -1) for h, r in zip((hce, hpe), res)
                          if r["status"] == "FILLED"]
                unknown = [h["symbol"] for h, r in zip((hce, hpe), res) if r["status"] == "UNKNOWN"]
                notify(f"❌ {tag} ENTRY ABORTED: hedge buy failed\n"
                       + "\n".join(f"{h['symbol']}: {r['status']} {r['info']}"
                                   for h, r in zip((hce, hpe), res))
                       + (f"\nStatus unknown for {unknown} - CHECK BROKER" if unknown else ""),
                       logging.ERROR)
                self.unwind(filled, "hedge buy failed")
                return
            hedges = [{"symbol": h["symbol"], "strike": h["strike"], "entry": r["price"],
                       "order_id": r["order_id"]} for h, r in zip((hce, hpe), res)]

        # 2) sell CE + PE together
        res = self.broker.market_orders([(ce["symbol"], qty, -1, "C21SELL"),
                                         (pe["symbol"], qty, -1, "C21SELL")])
        st = [r["status"] for r in res]
        hedge_unwind = [(h["symbol"], qty, -1) for h in hedges]
        if "REJECTED" in st:
            lines = "\n".join(f"{leg['symbol']}: {r['status']} {r['info']}"
                              for leg, r in zip((ce, pe), res))
            notify(f"❌ {tag} SELL FAILED - closing the other leg and hedges\n{lines}",
                   logging.ERROR)
            filled = [(leg["symbol"], qty, 1) for leg, r in zip((ce, pe), res)
                      if r["status"] == "FILLED"]
            if "UNKNOWN" in st:
                notify("🆘 One sell has UNKNOWN status - CHECK BROKER, square off manually, "
                       "hedges are left on", logging.CRITICAL)
                self.unwind(filled, "other leg rejected")
                return
            self.unwind(filled, "other leg rejected")
            self.unwind(hedge_unwind, "sell rejected")
            return

        # FILLED or UNKNOWN on both -> manage as an open position
        ce_px = res[0]["price"] or self.broker.ltps([ce["symbol"]])[ce["symbol"]]
        pe_px = res[1]["price"] or self.broker.ltps([pe["symbol"]])[pe["symbol"]]
        entry_px = round(ce_px + pe_px, 2)
        if SL_FROM_FILL and SL_MODE == "range" and SL_FROM == "entry":
            sl = round(entry_px + risk, 2)
        position = {
            "signal_candle": tag, "signal_close": signal_close, "signal_ema21": round(ema, 2),
            "ce": {"symbol": ce["symbol"], "entry": ce_px, "order_id": res[0]["order_id"]},
            "pe": {"symbol": pe["symbol"], "entry": pe_px, "order_id": res[1]["order_id"]},
            "qty": qty, "lots": lots, "entry_px": entry_px,
            "sl": sl, "sl_points": risk, "sl_capped": capped,
            "hedges": hedges, "entry_time": now_ist().isoformat(timespec="seconds"),
        }
        with self.lock:
            self.state["position"] = position
            save_state(self.state)

        warn = "\n⚠️ Fill not confirmed - CHECK BROKER" if "UNKNOWN" in st else ""
        hedge_txt = ("\nHedges: " + ", ".join(f"{h['symbol']} @ {h['entry']}" for h in hedges)
                     if hedges else "")
        notify(
            f"🚀 SELL STRANGLE {'(PAPER)' if PAPER_TRADE else ''}\n"
            f"{ce['symbol']} @ {ce_px} + {pe['symbol']} @ {pe_px} = {entry_px}\n"
            f"x{qty} ({lots} lots) | signal close {signal_close:.2f} ({tag} candle)\n"
            f"SL={sl} ({risk} pts{', capped' if capped else ''}) on combined LTP | "
            f"expiry {ct['expiry']}{' (0DTE)' if ct['dte0'] else ''}{hedge_txt}{warn}"
        )

    # ---------- exit ----------
    def check_exit(self, combined, now):
        with self.lock:
            pos = self.state["position"]
            if not pos or self.exit_blocked:
                return
            if now.time() >= EXIT_TIME:
                self.exit("TIME", combined)
            elif combined >= pos["sl"]:
                self.exit("SL", combined)

    def exit(self, reason, combined):
        with self.lock:
            pos = self.state["position"]
            if not pos or self.exit_blocked:
                return
            qty = pos["qty"]

            # 1) cover both shorts FIRST (hedges stay on until this is confirmed)
            res = self.broker.orders_with_retry([(pos["ce"]["symbol"], qty, 1, "C21COVER"),
                                                 (pos["pe"]["symbol"], qty, 1, "C21COVER")])
            if any(r["status"] != "FILLED" for r in res):
                self.exit_blocked = True
                bad = [(pos[k]["symbol"], r) for k, r in zip(("ce", "pe"), res)
                       if r["status"] != "FILLED"]
                notify(f"🆘 EXIT {reason} NOT CONFIRMED (combined {combined})\n"
                       + "\n".join(f"Buy back {sym} x{qty}: {r['status']} {r['info']}"
                                   for sym, r in bad)
                       + "\nSQUARE OFF MANUALLY"
                       + (" (then sell hedges " + ", ".join(h["symbol"] for h in pos["hedges"])
                          + ")" if pos["hedges"] else ""),
                       logging.CRITICAL)
                return
            ce_exit, pe_exit = res[0]["price"], res[1]["price"]
            exit_px = round(ce_exit + pe_exit, 2) if ce_exit and pe_exit else combined
            sell_pts = round(pos["entry_px"] - exit_px, 2)

            # 2) then sell the hedges
            hedge_pts, hedge_txt = 0.0, ""
            if pos["hedges"]:
                hres = self.broker.orders_with_retry([(h["symbol"], qty, -1, "C21HEXIT")
                                                      for h in pos["hedges"]])
                for h, r in zip(pos["hedges"], hres):
                    if r["status"] == "FILLED":
                        pts = round((r["price"] or 0) - (h["entry"] or 0), 2)
                        hedge_pts += pts
                        hedge_txt += f"\nHedge {h['symbol']}: {h['entry']} -> {r['price']} = {pts:+.2f}"
                    else:
                        notify(f"🆘 HEDGE EXIT NOT CONFIRMED {h['symbol']} x{qty}\n"
                               f"Status={r['status']} {r['info']}\nSELL IT MANUALLY",
                               logging.CRITICAL)
                        hedge_txt += f"\nHedge {h['symbol']}: exit NOT confirmed"
                hedge_pts = round(hedge_pts, 2)

            net = round(sell_pts + hedge_pts, 2)
            self.state["trades"].append({
                **pos, "exit_reason": reason, "exit_px": exit_px,
                "ce_exit": ce_exit, "pe_exit": pe_exit, "combined_ltp_at_trigger": combined,
                "exit_time": now_ist().isoformat(timespec="seconds"),
                "sell_pts": sell_pts, "hedge_pts": hedge_pts, "net_pts": net,
                "pnl": round(net * qty, 2)})
            self.state["position"] = None
            save_state(self.state)
            notify(
                f"🏁 EXIT {reason} {'(PAPER)' if PAPER_TRADE else ''}\n"
                f"Combined: sold {pos['entry_px']} -> bought {exit_px} = {sell_pts:+.2f} pts "
                f"(trigger LTP {combined})\n"
                f"CE {pos['ce']['entry']} -> {ce_exit} | PE {pos['pe']['entry']} -> {pe_exit}"
                f"{hedge_txt}\nNet {net:+.2f} pts x{qty} = ₹{net * qty:,.0f}"
            )

    def monitor(self, now):
        """Backup / minute-mode SL check and the time exit."""
        pos = self.state["position"]
        if not pos or self.exit_blocked:
            return
        minute = now.replace(second=0, microsecond=0)
        due = (now.time() >= EXIT_TIME or SL_CHECK == "tick"
               or (minute != self.last_minute_checked and now.second >= 1))
        if not due:
            return
        try:
            combined = self.combined_ltp(pos)
        except Exception as e:
            logger.warning(f"LTP fetch failed: {e}")
            return
        self.last_minute_checked = minute
        self.check_exit(combined, now)

    def day_done(self, now):
        with self.lock:
            if self.exit_blocked:
                return True
            if self.state["position"] is not None:
                return False
            return self.state["signal_done"] or now.time() >= EXIT_TIME


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

    if state["contract"] is None:
        expiry = master.nearest_expiry(today)
        dte = trading_days_to_expiry(today, expiry)
        if TRADE_DTE is not None and dte not in TRADE_DTE:
            notify(f"⏸ NO TRADE TODAY - {dte} DTE (expiry {expiry}), trading only DTE "
                   f"{sorted(TRADE_DTE)}")
            return

        # the 09:16 1-min candle must exist before picking ATM
        ready = datetime.datetime.combine(today, STRIKE_TIME) + datetime.timedelta(seconds=5)
        if now_ist() < ready:
            logger.info(f"Waiting until {ready:%H:%M:%S} to pick strikes...")
            while now_ist() < ready:
                time.sleep(1)
        try:
            contract, legs = resolve_contract(broker, master, today, expiry)
        except Exception as e:
            notify(f"❌ STARTUP FAILED (contract): {e}", logging.ERROR)
            raise
        state["contract"], state["legs"] = contract, legs
        save_state(state)

    ct, legs = state["contract"], state["legs"]
    has_pos = state["position"] is not None
    if now_ist().time() >= EXIT_TIME and not has_pos:
        notify("⏸ Started after exit time with no open position - nothing to do")
        return
    if state["signal_done"] and not has_pos:
        notify(f"⏸ Already done for today: {state['note'] or 'trade taken'}")
        return

    lots = EXPIRY_LOTS if ct["dte0"] else NONEXP_LOTS
    notify(
        f"🚀 COMBINED EMA21 STRANGLE STARTED {'(PAPER)' if PAPER_TRADE else '(LIVE)'}\n"
        f"Nifty {ct['spot']} ({ct['spot_src']}) -> ATM {ct['atm']}\n"
        f"Expiry {ct['expiry']} ({ct['dte']} DTE{', 0DTE' if ct['dte0'] else ''})\n"
        f"CE {legs['CE']['symbol']} (OTM{ct['ce_steps']}) + PE {legs['PE']['symbol']} "
        f"(OTM{ct['pe_steps']})\nQty {legs['CE']['lot']} x {lots} lots"
        f"{' | hedge ' + str(hedge_distance(ct['dte0'])) + ' pts OTM' if HEDGE_ENABLED else ' | no hedge'}\n"
        f"SL {SL_RANGE_PCT:.0%} of 09:15 range (max {MAX_SL_POINTS}) | SL check {SL_CHECK} | "
        f"exit {EXIT_TIME:%H:%M}"
    )

    trader = LiveTrader(broker, master, today, state)
    symbols = {legs["CE"]["symbol"], legs["PE"]["symbol"]}
    ws = start_websocket(trader, symbols)

    try:
        while True:
            now = now_ist()
            trader.monitor(now)
            try:
                trader.run_candles(now)
            except Exception:
                logger.exception("candle processing error")
                trader.retry_at = time.time() + CANDLE_RETRY_SEC

            if trader.day_done(now):
                tr = trader.state["trades"]
                summary = "; ".join(f"{t['exit_reason']} {t['net_pts']:+.2f} pts "
                                    f"(₹{t['pnl']:,.0f})" for t in tr) or "no trade"
                notify(f"✅ Done for the day - {summary}\n{trader.state['note']}")
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
