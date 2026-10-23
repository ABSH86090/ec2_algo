# =========================================================
# SENSEX COMBINED-PREMIUM EMA PATTERN SHORT STRANGLE  (+ HEDGES)
# LIVE TRADING (REAL ORDERS) - FYERS WEBSOCKET
# =========================================================
#
# Live version of the combined-premium EMA backtest. Same rules, same
# parameters (5-min candles, EMA5 / EMA21, 2 warm-up days, DTE 0/1 only).
#
# DAILY FLOW
# ----------
# 1. Runs only on days whose days-to-expiry is in TRADE_DTE (0 = expiry day).
# 2. 09:16:00  Sensex spot -> ATM (step 100), nearest expiry.
#       Expiry day : SELL OTM2 CE + OTM2 PE   (EXPIRY_LOTS lots)
#       Other days : SELL OTM5 CE + OTM5 PE   (NONEXP_LOTS lots)
#       HEDGES     : BUY CE at (short CE strike + 2000)
#                    BUY PE at (short PE strike - 2000)
# 3. Combined premium chart (short CE + short PE), 5-min candles, built from
#    1-min bars ("minute" method: each minute = CE+PE at the minute's open and
#    close). EMA5 / EMA21 on the combined close, warmed up on the same two
#    contracts over the previous WARMUP_DAYS trading days.
#       - Minutes before the websocket connected come from Fyers 1-min history.
#       - Minutes after that are built from live ticks (history fills any gaps).
# 4. Pattern (evaluated at every 5-min candle close, exactly as the backtest):
#       a. Day filter : 09:15 candle RED and closes BELOW EMA21, else no trade.
#                       Strength  : its close must also be in the bottom
#                       FIRST_CANDLE_CLOSE_PCT of its range,
#                       (close - low) <= FIRST_CANDLE_CLOSE_PCT x (high - low).
#       b. Setup      : later GREEN candle closing above EMA5 and EMA21.
#       c. Trigger    : next RED candle closing below EMA5 and EMA21 with
#                       EMA5 < EMA21 -> enter right after it closes.
#       Trigger candle must close by ENTRY_LATEST (15:00).
# 5. ENTRY ORDER : BUY both hedges first (margin benefit), confirm fills,
#                  then SELL CE + PE. If anything fails, whatever filled is
#                  flattened and the day is stopped.
# 6. SL          : day high of the combined premium (09:15 .. trigger candle)
#                  + SL_BUFFER, checked continuously on live CE LTP + PE LTP.
# 7. EXIT        : SL hit, or 15:29:00 time exit. Shorts are bought back
#                  FIRST, hedges are sold only after the shorts are flat.
#                  Orders use productType MARGIN so the broker's intraday
#                  auto square-off (~15:20 for INTRADAY) does not hit first.
#
# !!! REAL MONEY. MARGIN positions are NOT auto-squared off by the broker.
#     If an exit keeps failing you get a Telegram SOS - close it manually,
#     otherwise it carries overnight.
#
# A state file lets the script be restarted mid-day: it resumes the same
# strikes / open position instead of picking new ones.
# =========================================================

import os
import sys
import json
import time
import logging
import calendar
import datetime
import threading
import requests
import pandas as pd
from dotenv import load_dotenv
from fyers_apiv3 import fyersModel
from fyers_apiv3.FyersWebsocket import data_ws

# ================= CONFIG =================
load_dotenv()

CLIENT_ID = os.getenv("FYERS_CLIENT_ID")
ACCESS_TOKEN = os.getenv("FYERS_ACCESS_TOKEN")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

EXCHANGE = "BSE"
INDEX_SYMBOL = "BSE:SENSEX-INDEX"
UNDERLYING = "SENSEX"
STRIKE_STEP = 100

# Strikes (same as backtest)
CE_OTM_STEPS = 3                    # expiry day
PE_OTM_STEPS = 3
NONEXP_CE_OTM_STEPS = 4             # non-expiry days
NONEXP_PE_OTM_STEPS = 4
HEDGE_DISTANCE = 2000               # hedge strikes this many points beyond the shorts
STRIKE_TIME = datetime.time(9, 16)

# Expiry weekday history (BSE). From 1-Sep-2025: Thursday.
EXPIRY_SCHEDULE = [(datetime.date(2025, 1, 1), calendar.TUESDAY),
                   (datetime.date(2025, 9, 1), calendar.THURSDAY)]
EXPIRY_BEFORE = calendar.FRIDAY
TRADE_DTE = {0, 1}                  # trade only on these days-to-expiry; None = every day
SKIP_SAME_DAY_EXPIRY = False

# Candles / indicators
RESOLUTION = 5
EMA_FAST = 3
EMA_SLOW = 15
WARMUP_DAYS = 2

# Pattern / entry
FIRST_CANDLE = datetime.time(9, 15)
# Strong bearish first candle: its close must lie within this fraction of the
# candle's range from its LOW, i.e. (close - low) <= FIRST_CANDLE_CLOSE_PCT x (high - low).
# 0.10 -> close in the bottom 10% of the candle. None -> no strength check.
FIRST_CANDLE_CLOSE_PCT = 0.10
ENTRY_LATEST = datetime.time(15, 0)  # trigger candle must CLOSE by this time
MAX_TRADES_PER_DAY = 1
SL_BUFFER = 0.0

# Exit
EXIT_TIME = datetime.time(15, 29, 0)

# Size  (verify the current Sensex lot size with your broker)
LOT_SIZE = 20
EXPIRY_LOTS = 3
NONEXP_LOTS = 4

# Orders
PRODUCT_TYPE = "MARGIN"             # carry-forward product -> no 15:20 auto square-off
FILL_TIMEOUT = 15                   # seconds to wait for a market order to fill
EXIT_RETRY_SECONDS = 3
MAX_EXIT_RETRIES = 5                # after this many failed attempts -> Telegram SOS (keeps retrying)
CANDLE_CLOSE_DELAY = 2.0            # seconds after a candle boundary before evaluating it

STATE_FILE = "sensex_ema_strangle_state.json"
LOG_FILE = "sensex_ema_strangle_live.log"

NSE_HOLIDAYS = {                    # BSE follows the same trading holidays
    datetime.date(2026, 1, 15), datetime.date(2026, 1, 26), datetime.date(2026, 3, 3),
    datetime.date(2026, 3, 26), datetime.date(2026, 3, 31), datetime.date(2026, 4, 3),
    datetime.date(2026, 4, 14), datetime.date(2026, 5, 1), datetime.date(2026, 5, 28),
    datetime.date(2026, 6, 26), datetime.date(2026, 9, 14), datetime.date(2026, 10, 2),
    datetime.date(2026, 10, 20), datetime.date(2026, 11, 10), datetime.date(2026, 11, 24),
    datetime.date(2026, 12, 25),
}


# ================= LOGGING =================
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
            requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                          json={"chat_id": TELEGRAM_CHAT_ID, "text": msg[:4000]}, timeout=3)
        except Exception:
            pass


def alert(msg, level=logging.INFO):
    logger.log(level, msg)
    send_telegram(msg)


# ================= CALENDAR / CONTRACTS (same as backtest) =================
def expiry_weekday(d):
    wd = EXPIRY_BEFORE
    for start, day in EXPIRY_SCHEDULE:
        if d >= start:
            wd = day
    return wd


def is_trading_day(d):
    return d.weekday() < 5 and d not in NSE_HOLIDAYS


def prev_trading_days(d, n):
    days = []
    while len(days) < n:
        d -= datetime.timedelta(days=1)
        if is_trading_day(d):
            days.append(d)
    return sorted(days)


def trading_days_to_expiry(day, expiry):
    n, d = 0, day
    while d < expiry:
        d += datetime.timedelta(days=1)
        if is_trading_day(d):
            n += 1
    return n


def adjust_for_holiday(d):
    while not is_trading_day(d):
        d -= datetime.timedelta(days=1)
    return d


WEEKLY_MONTH_CODE = {1: "1", 2: "2", 3: "3", 4: "4", 5: "5", 6: "6",
                     7: "7", 8: "8", 9: "9", 10: "O", 11: "N", 12: "D"}


def next_expiry(trade_date, skip_same_day=False):
    """(expiry_date, is_monthly). Monthly = last expiry weekday of the month."""
    d = trade_date
    for _ in range(60):
        wd = expiry_weekday(d)
        if d.weekday() == wd:
            actual = adjust_for_holiday(d)
            if actual > trade_date or (actual == trade_date and not skip_same_day):
                ndays = calendar.monthrange(d.year, d.month)[1]
                last = max(datetime.date(d.year, d.month, x) for x in range(1, ndays + 1)
                           if datetime.date(d.year, d.month, x).weekday() == wd)
                return actual, d == last
        d += datetime.timedelta(days=1)
    raise RuntimeError("Could not find next expiry")


def contract_symbol(expiry, strike, monthly, opt):
    yy = expiry.strftime("%y")
    code = (f"{yy}{expiry.strftime('%b').upper()}" if monthly
            else f"{yy}{WEEKLY_MONTH_CODE[expiry.month]}{expiry.day:02d}")
    return f"{EXCHANGE}:{UNDERLYING}{code}{int(strike)}{opt}"


# ================= COMBINED CHART (same as backtest) =================
OHLC = ["open", "high", "low", "close"]
AGG = {"open": "first", "high": "max", "low": "min", "close": "last"}


def empty_ohlc():
    return pd.DataFrame(columns=OHLC, index=pd.DatetimeIndex([], name="time"), dtype=float)


def candles_to_df(candles):
    if not candles:
        return empty_ohlc()
    df = pd.DataFrame([c[:5] for c in candles], columns=["ts"] + OHLC)
    df["time"] = (pd.to_datetime(df["ts"], unit="s", utc=True)
                    .dt.tz_convert("Asia/Kolkata").dt.tz_localize(None))
    df = df.set_index("time").sort_index().between_time("09:15", "15:29")
    return df[~df.index.duplicated(keep="last")][OHLC].astype(float)


def align_legs(ce, pe):
    idx = ce.index.union(pe.index)
    parts = {}
    for name, df in (("ce", ce), ("pe", pe)):
        x = df[OHLC].reindex(idx)
        x["close"] = x.groupby(x.index.date)["close"].ffill()
        for f in ("open", "high", "low"):
            x[f] = x[f].fillna(x["close"])
        parts[name] = x
    return pd.concat(parts, axis=1).dropna()


def build_combined(ce, pe):
    m = align_legs(ce, pe)
    comb = pd.DataFrame({f: m[("ce", f)] + m[("pe", f)] for f in OHLC}, index=m.index)
    comb["high"] = comb[["open", "close"]].max(axis=1)      # COMBINE_METHOD = "minute"
    comb["low"] = comb[["open", "close"]].min(axis=1)
    c = (comb.resample(f"{RESOLUTION}min", label="left", closed="left")
             .agg(AGG).dropna())
    c["ema9"] = c["close"].ewm(span=EMA_FAST, adjust=False).mean()
    c["ema21"] = c["close"].ewm(span=EMA_SLOW, adjust=False).mean()
    c["green"] = c["close"] > c["open"]
    c["red"] = c["close"] < c["open"]
    return c


def first_candle_strength(row):
    """Where the close sits inside the candle's range, 0 = at the low, 1 = at the high.
    Returns (passed, fraction)."""
    rng = float(row["high"] - row["low"])
    if rng <= 0:
        return False, 1.0
    frac = float(row["close"] - row["low"]) / rng
    return (row["close"] - row["low"]) <= FIRST_CANDLE_CLOSE_PCT * rng + 1e-9, frac


def find_setup(d, start_ts):
    w = d[d.index >= start_ts]
    green = w[w["green"] & (w["close"] > w["ema9"]) & (w["close"] > w["ema21"])]
    if green.empty:
        return None, None
    g = green.index[0]
    w2 = w[w.index > g]
    red = w2[w2["red"] & (w2["close"] < w2["ema9"]) & (w2["close"] < w2["ema21"])
             & (w2["ema9"] < w2["ema21"])]
    return g, (red.index[0] if len(red) else None)


# ================= FYERS =================
def sanitize_order_tag(tag):
    cleaned = "".join(ch for ch in str(tag) if ch.isalnum())
    return cleaned[:20] if cleaned else "TRADE"


class Fyers:
    def __init__(self):
        if not CLIENT_ID or not ACCESS_TOKEN:
            raise SystemExit("FYERS_CLIENT_ID / FYERS_ACCESS_TOKEN missing in .env")
        self.client = fyersModel.FyersModel(client_id=CLIENT_ID, token=ACCESS_TOKEN,
                                            is_async=False, log_path="")
        self.auth = f"{CLIENT_ID}:{ACCESS_TOKEN}"

    def place_market(self, symbol, qty, side, tag):
        """side 1 = BUY, -1 = SELL. Returns (order_id or None, response)."""
        req = {"symbol": symbol, "qty": int(qty), "type": 2, "side": side,
               "productType": PRODUCT_TYPE, "limitPrice": 0, "stopPrice": 0,
               "validity": "DAY", "disclosedQty": 0, "offlineOrder": False,
               "orderTag": sanitize_order_tag(tag)}
        try:
            resp = self.client.place_order(req)
        except Exception as e:
            logger.error(f"place_order exception {symbol}: {e}")
            return None, str(e)
        logger.info(f"ORDER {'BUY' if side == 1 else 'SELL'} {qty} {symbol} -> {resp}")
        if isinstance(resp, dict) and resp.get("s") == "ok" and resp.get("id"):
            return resp["id"], resp
        return None, resp

    def get_order(self, order_id):
        try:
            resp = self.client.orderbook()
        except Exception as e:
            logger.warning(f"orderbook() failed: {e}")
            return None
        for o in (resp or {}).get("orderBook", []) or []:
            if str(o.get("id")) == str(order_id):
                return o
        return None

    def cancel(self, order_id):
        try:
            self.client.cancel_order({"id": order_id})
        except Exception as e:
            logger.warning(f"cancel {order_id} failed: {e}")

    def wait_fill(self, order_id, timeout=FILL_TIMEOUT):
        """Returns (filled_qty, avg_price or None, message).
        Status codes: 1=Cancelled 2=Traded 5=Rejected 6=Pending."""
        deadline = time.time() + timeout
        o = None
        while time.time() < deadline:
            o = self.get_order(order_id) or o
            if o and o.get("status") in (1, 2, 5):
                break
            time.sleep(0.5)
        else:
            logger.warning(f"order {order_id} not complete after {timeout}s -> cancelling")
            self.cancel(order_id)
            time.sleep(1)
            o = self.get_order(order_id) or o
        if not o:
            return 0, None, "order not found in orderbook"
        filled = int(o.get("filledQty") or 0)
        px = float(o.get("tradedPrice") or 0) or None
        return filled, px, o.get("message", "")

    def history(self, symbol, start, end, resolution="1"):
        req = {"symbol": symbol, "resolution": resolution, "date_format": "1",
               "range_from": start.strftime("%Y-%m-%d"),
               "range_to": end.strftime("%Y-%m-%d"), "cont_flag": "1"}
        last = None
        for attempt in range(3):
            try:
                resp = self.client.history(req)
                if isinstance(resp, dict) and resp.get("s") in ("ok", "no_data"):
                    return candles_to_df(resp.get("candles") or [])
                last = resp
            except Exception as e:
                last = e
            time.sleep(1 + attempt)
        raise RuntimeError(f"history {symbol}: {last}")


# ================= BOT =================
class Bot:
    def __init__(self):
        self.fy = Fyers()
        self.today = datetime.date.today()
        self.lock = threading.Lock()
        self.ltp = {}                 # symbol -> last price (today's trades only)
        self.bars = {}                # symbol -> {minute Timestamp: [o, h, l, c]}
        self.partial_minutes = set()  # minutes in which the socket (re)connected
        self.warm = {}
        self.state = None
        self.candle_len = pd.Timedelta(minutes=RESOLUTION)

    # ---------- state ----------
    def save(self):
        tmp = STATE_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(self.state, f, indent=2, default=str)
        os.replace(tmp, STATE_FILE)

    def load(self):
        if not os.path.exists(STATE_FILE):
            return None
        try:
            with open(STATE_FILE) as f:
                s = json.load(f)
            return s if s.get("date") == str(self.today) else None
        except Exception as e:
            logger.warning(f"state file unreadable: {e}")
            return None

    def ts(self, t):
        return pd.Timestamp(datetime.datetime.combine(self.today, t))

    def mark_done(self, note):
        self.state["done"] = True
        self.state["note"] = note
        self.save()
        alert(f"⏹ {note} -- no more trades today.")

    # ---------- setup ----------
    def get_spot(self):
        if datetime.datetime.now().time() < datetime.time(9, 16, 30):
            q = self.fy.client.quotes({"symbols": INDEX_SYMBOL})
            return float(q["d"][0]["v"]["lp"])
        # started late: use the 09:16 1-min candle open (same as the backtest)
        df = self.fy.history(INDEX_SYMBOL, self.today, self.today)
        t = self.ts(STRIKE_TIME)
        if t in df.index:
            return float(df.loc[t, "open"])
        return float(df[df.index <= t].iloc[-1]["close"])

    def resolve(self, spot):
        atm = round(spot / STRIKE_STEP) * STRIKE_STEP
        expiry, monthly = next_expiry(self.today, SKIP_SAME_DAY_EXPIRY)
        dte0 = expiry == self.today
        ce_steps, pe_steps = ((CE_OTM_STEPS, PE_OTM_STEPS) if dte0
                              else (NONEXP_CE_OTM_STEPS, NONEXP_PE_OTM_STEPS))
        ce_k, pe_k = atm + ce_steps * STRIKE_STEP, atm - pe_steps * STRIKE_STEP
        hce_k, hpe_k = ce_k + HEDGE_DISTANCE, pe_k - HEDGE_DISTANCE
        lots = EXPIRY_LOTS if dte0 else NONEXP_LOTS
        return {
            "spot": spot, "atm": atm, "expiry": str(expiry), "monthly": monthly,
            "dte0": dte0, "lots": lots, "qty": LOT_SIZE * lots,
            "ce": contract_symbol(expiry, ce_k, monthly, "CE"),
            "pe": contract_symbol(expiry, pe_k, monthly, "PE"),
            "hce": contract_symbol(expiry, hce_k, monthly, "CE"),
            "hpe": contract_symbol(expiry, hpe_k, monthly, "PE"),
        }

    def validate_symbols(self, syms):
        resp = self.fy.client.quotes({"symbols": ",".join(syms)})
        bad = [x.get("n") for x in (resp or {}).get("d", []) if x.get("s") != "ok"]
        if not isinstance(resp, dict) or resp.get("s") != "ok" or bad:
            raise RuntimeError(f"invalid symbol(s) {bad or syms}: {resp}")

    def setup(self):
        expiry, _ = next_expiry(self.today, SKIP_SAME_DAY_EXPIRY)
        dte = trading_days_to_expiry(self.today, expiry)
        if TRADE_DTE is not None and dte not in TRADE_DTE:
            alert(f"ℹ️ SENSEX EMA strangle: DTE {dte} (expiry {expiry}) not in "
                  f"{sorted(TRADE_DTE)} -- not trading today.")
            sys.exit(0)

        self.state = self.load()
        if self.state:
            alert(f"♻️ Resuming today's state from {STATE_FILE} "
                  f"(position: {'OPEN' if self.state.get('position') else 'none'}, "
                  f"done: {self.state.get('done')})", logging.WARNING)
            if self.state.get("done") and not self.state.get("position"):
                sys.exit(0)
        else:
            while datetime.datetime.now().time() < STRIKE_TIME:
                time.sleep(0.5)
            info = self.resolve(self.get_spot())
            info["dte"] = dte
            self.state = {"date": str(self.today), "info": info, "trades_taken": 0,
                          "search_start": None, "filter_passed": False,
                          "green_alerted": None, "position": None, "done": False,
                          "note": None, "day_pnl": 0.0}
            self.save()
            alert(
                f"📌 SENSEX EMA STRANGLE (LIVE) {self.today}\n"
                f"Spot {info['spot']}  ATM {info['atm']}  Expiry {info['expiry']}"
                f"{' (0DTE)' if info['dte0'] else ''}  DTE {dte}\n"
                f"SELL {info['ce']}\nSELL {info['pe']}\n"
                f"BUY  {info['hce']} (hedge)\nBUY  {info['hpe']} (hedge)\n"
                f"Qty {info['qty']} ({info['lots']} lots)  Product {PRODUCT_TYPE}")

        info = self.state["info"]
        self.syms = {k: info[k] for k in ("ce", "pe", "hce", "hpe")}
        self.validate_symbols(list(self.syms.values()))
        for s in self.syms.values():
            self.bars[s] = {}

        warm_days = prev_trading_days(self.today, WARMUP_DAYS)
        for k in ("ce", "pe"):
            df = self.fy.history(self.syms[k], warm_days[0], warm_days[-1])
            self.warm[k] = df[df.index.date < self.today]
        n_warm = len(build_combined(self.warm["ce"], self.warm["pe"])) \
            if len(self.warm["ce"]) and len(self.warm["pe"]) else 0
        logger.info(f"warm-up combined candles: {n_warm}")
        if n_warm < EMA_SLOW:
            alert(f"⚠️ only {n_warm} warm-up candles (< {EMA_SLOW}); EMAs are cold today.",
                  logging.WARNING)

    # ---------- websocket ----------
    def on_message(self, msg):
        sym = msg.get("symbol") if isinstance(msg, dict) else None
        if sym not in self.bars or "ltp" not in msg:
            return
        ltt = msg.get("last_traded_time")
        if ltt and datetime.date.fromtimestamp(ltt) != self.today:
            return                            # no trade yet today
        now = datetime.datetime.now()
        if not (datetime.time(9, 15) <= now.time() < datetime.time(15, 30)):
            return
        px = float(msg["ltp"])
        minute = pd.Timestamp(now.replace(second=0, microsecond=0))
        with self.lock:
            self.ltp[sym] = px
            b = self.bars[sym].get(minute)
            if b is None:
                self.bars[sym][minute] = [px, px, px, px]
            else:
                b[1], b[2], b[3] = max(b[1], px), min(b[2], px), px

    def on_connect(self):
        with self.lock:
            self.partial_minutes.add(pd.Timestamp(
                datetime.datetime.now().replace(second=0, microsecond=0)))
        self.ws.subscribe(symbols=list(self.syms.values()), data_type="SymbolUpdate")
        logger.info("websocket connected, subscribed")

    def start_socket(self):
        self.ws = data_ws.FyersDataSocket(
            access_token=self.fy.auth, log_path="", litemode=False,
            write_to_file=False, reconnect=True,
            on_connect=self.on_connect, on_message=self.on_message,
            on_error=lambda m: logger.warning(f"ws error: {m}"),
            on_close=lambda m: logger.warning(f"ws closed: {m}"))
        self.ws.connect()

    # ---------- chart ----------
    def leg_today(self, sym, upto):
        """Today's 1-min bars before `upto`: live ticks where we have complete
        minutes, Fyers history for everything else."""
        try:
            rest = self.fy.history(sym, self.today, self.today)
        except Exception as e:
            logger.warning(f"history refresh failed ({e}); using ticks only")
            rest = empty_ohlc()
        rest = rest[rest.index < upto]
        with self.lock:
            tb = {k: list(v) for k, v in self.bars[sym].items()
                  if k < upto and k not in self.partial_minutes}
        if tb:
            ticks = pd.DataFrame.from_dict(tb, orient="index", columns=OHLC).sort_index()
            ticks.index = pd.DatetimeIndex(ticks.index)
            return pd.concat([rest[~rest.index.isin(ticks.index)], ticks]).sort_index()
        return rest

    def chart(self, upto):
        legs = {k: pd.concat([self.warm[k], self.leg_today(self.syms[k], upto)])
                for k in ("ce", "pe")}
        c = build_combined(legs["ce"], legs["pe"])
        return c[c.index.date == self.today]

    # ---------- signal ----------
    def on_candle_close(self, boundary):
        S = boundary - self.candle_len
        d = self.chart(boundary)
        if S not in d.index:
            logger.warning(f"no combined candle for {S:%H:%M}")
            return
        r = d.loc[S]
        logger.info(f"candle {S:%H:%M} O={r['open']:.2f} H={r['high']:.2f} "
                    f"L={r['low']:.2f} C={r['close']:.2f} "
                    f"EMA{EMA_FAST}={r['ema9']:.2f} EMA{EMA_SLOW}={r['ema21']:.2f}")

        first_ts = self.ts(FIRST_CANDLE)
        if first_ts not in d.index:
            return self.mark_done(f"no {FIRST_CANDLE:%H:%M} combined candle")
        first = d.loc[first_ts]
        if not (first["red"] and first["close"] < first["ema21"]):
            why = "not red" if not first["red"] else "closed above EMA21"
            return self.mark_done(f"Day filter failed: {FIRST_CANDLE:%H:%M} candle {why} "
                                  f"(C={first['close']:.2f}, EMA21={first['ema21']:.2f})")

        # Strong bearish first candle: close within the bottom FIRST_CANDLE_CLOSE_PCT of its range
        strength_txt = ""
        if FIRST_CANDLE_CLOSE_PCT is not None:
            ok, frac = first_candle_strength(first)
            strength_txt = (f", close at {frac:.0%} of range "
                            f"(<= {FIRST_CANDLE_CLOSE_PCT:.0%})")
            if not ok:
                return self.mark_done(
                    f"Day filter failed: {FIRST_CANDLE:%H:%M} candle not strong enough -- "
                    f"close at {frac:.0%} of range from low, need <= {FIRST_CANDLE_CLOSE_PCT:.0%} "
                    f"(H={first['high']:.2f} L={first['low']:.2f} C={first['close']:.2f})")

        if not self.state["filter_passed"]:
            self.state["filter_passed"] = True
            self.save()
            alert(f"✅ Day filter passed: {FIRST_CANDLE:%H:%M} combined candle red, "
                  f"C={first['close']:.2f} < EMA21={first['ema21']:.2f}{strength_txt}")

        start = (pd.Timestamp(self.state["search_start"]) if self.state["search_start"]
                 else first_ts + self.candle_len)
        g, e = find_setup(d, start)
        if g is not None and self.state["green_alerted"] != str(g):
            self.state["green_alerted"] = str(g)
            self.save()
            alert(f"🟢 Setup: green candle {g:%H:%M} closed above both EMAs "
                  f"(C={d.loc[g, 'close']:.2f}). Waiting for red trigger.")

        latest_start = self.ts(ENTRY_LATEST) - self.candle_len
        if e is None:
            if S >= latest_start:
                self.mark_done(f"No trigger by {ENTRY_LATEST:%H:%M}")
            return
        if e > latest_start:
            return self.mark_done(f"Trigger candle {e:%H:%M} too late")
        if e != S:
            return self.mark_done(f"Trigger at {e:%H:%M} was missed (bot not running then)")

        trig = d.loc[e]
        day_high = float(d.loc[first_ts:e, "high"].max())
        sl = round(day_high + SL_BUFFER, 2)
        self.open_position(e, trig, day_high, sl)

    # ---------- orders ----------
    def execute_group(self, keys, closing):
        legs = self.state["position"]["legs"]
        placed = {}
        for k in keys:
            leg = legs[k]
            qty = leg["open_qty"] if closing else leg["qty"] - leg["open_qty"]
            if qty <= 0:
                continue
            opening_side = -1 if leg["dir"] == "SELL" else 1
            side = -opening_side if closing else opening_side
            oid, resp = self.fy.place_market(leg["symbol"], qty, side,
                                             f"EMA{k}{'X' if closing else 'E'}")
            if oid is None:
                alert(f"❌ order rejected {leg['symbol']} qty {qty}: {resp}", logging.ERROR)
                continue
            placed[k] = oid
        for k, oid in placed.items():
            leg = legs[k]
            filled, px, msg = self.fy.wait_fill(oid)
            if filled <= 0:
                alert(f"❌ {leg['symbol']} order {oid} not filled: {msg}", logging.ERROR)
                continue
            px = px if px else self.ltp.get(leg["symbol"], 0.0)
            if closing:
                leg["open_qty"] -= filled
                leg["exit_value"] += filled * px
                leg["exit_qty"] += filled
            else:
                prev = leg["open_qty"]
                leg["entry_px"] = ((leg["entry_px"] or 0) * prev + px * filled) / (prev + filled)
                leg["open_qty"] += filled
        self.save()

    def open_position(self, e, trig, day_high, sl):
        info = self.state["info"]
        qty = info["qty"]

        def leg(k, direction):
            return {"symbol": self.syms[k], "dir": direction, "qty": qty, "open_qty": 0,
                    "entry_px": None, "exit_value": 0.0, "exit_qty": 0}

        self.state["trades_taken"] += 1
        self.state["position"] = {
            "trade_no": self.state["trades_taken"], "trigger_candle": str(e),
            "chart_entry": round(float(trig["close"]), 2), "day_high": round(day_high, 2),
            "sl": sl, "live": False, "exiting": False, "exit_reason": None,
            "attempts": 0, "last_try": 0.0, "sos_sent": False,
            "legs": {"hce": leg("hce", "BUY"), "hpe": leg("hpe", "BUY"),
                     "ce": leg("ce", "SELL"), "pe": leg("pe", "SELL")}}
        self.save()
        alert(f"🔴 TRIGGER {e:%H:%M}: combined C={trig['close']:.2f} < "
              f"EMA{EMA_FAST}={trig['ema9']:.2f} & EMA{EMA_SLOW}={trig['ema21']:.2f}\n"
              f"SL (day high + {SL_BUFFER:g}) = {sl}. Buying hedges...")

        legs = self.state["position"]["legs"]
        self.execute_group(["hce", "hpe"], closing=False)
        if any(legs[k]["open_qty"] < qty for k in ("hce", "hpe")):
            return self.begin_exit("HEDGE ENTRY FAILED")

        self.execute_group(["ce", "pe"], closing=False)
        if any(legs[k]["open_qty"] < qty for k in ("ce", "pe")):
            return self.begin_exit("SHORT ENTRY FAILED")

        pos = self.state["position"]
        pos["live"] = True
        pos["entry_time"] = str(datetime.datetime.now())
        pos["entry_combined"] = round(legs["ce"]["entry_px"] + legs["pe"]["entry_px"], 2)
        self.save()
        alert(f"🚀 ENTRY (trade {pos['trade_no']})\n"
              + "\n".join(f"{'SELL' if l['dir'] == 'SELL' else 'BUY '} {l['symbol']} "
                          f"x{l['qty']} @ {l['entry_px']:.2f}" for l in legs.values())
              + f"\nCombined short premium {pos['entry_combined']} "
                f"(chart {pos['chart_entry']})  SL {sl}  Time exit {EXIT_TIME:%H:%M}")

    def begin_exit(self, reason, combined=None):
        pos = self.state["position"]
        pos["exiting"], pos["exit_reason"] = True, reason
        pos["attempts"], pos["last_try"] = 0, 0.0
        pos["exit_trigger_time"] = str(datetime.datetime.now())
        self.save()
        alert(f"🛑 EXIT {reason}" + (f" -- combined LTP {combined:.2f} >= SL {pos['sl']}"
                                     if combined is not None else "")
              + ". Buying back shorts, then selling hedges.")

    def progress_exit(self):
        pos = self.state["position"]
        if time.time() - pos["last_try"] < EXIT_RETRY_SECONDS:
            return
        pos["last_try"] = time.time()
        legs = pos["legs"]
        shorts = [k for k in ("ce", "pe") if legs[k]["open_qty"] > 0]
        hedges = [k for k in ("hce", "hpe") if legs[k]["open_qty"] > 0]
        group = shorts or hedges              # never sell hedges while shorts are open
        if not group:
            return self.finish_exit()
        pos["attempts"] += 1
        self.execute_group(group, closing=True)
        if all(legs[k]["open_qty"] == 0 for k in group):
            pos["last_try"] = 0.0             # move on to the next group at once
            pos["attempts"] = 0
            if not any(l["open_qty"] for l in legs.values()):
                return self.finish_exit()
        elif pos["attempts"] >= MAX_EXIT_RETRIES and not pos["sos_sent"]:
            pos["sos_sent"] = True
            left = ", ".join(f"{legs[k]['symbol']} {legs[k]['dir']} open {legs[k]['open_qty']}"
                             for k in group)
            alert(f"🆘 EXIT FAILED {MAX_EXIT_RETRIES} times ({pos['exit_reason']}): {left}. "
                  f"MARGIN position -- CLOSE MANUALLY NOW (bot keeps retrying).",
                  logging.CRITICAL)
        self.save()

    def finish_exit(self):
        pos = self.state["position"]
        lines, total = [], 0.0
        for l in pos["legs"].values():
            if not l["exit_qty"] or l["entry_px"] is None:
                continue
            xp = l["exit_value"] / l["exit_qty"]
            pnl = ((l["entry_px"] - xp) if l["dir"] == "SELL" else (xp - l["entry_px"])) * l["exit_qty"]
            total += pnl
            lines.append(f"{l['dir']:<4} {l['symbol']}  {l['entry_px']:.2f} -> {xp:.2f}  "
                         f"₹{pnl:,.0f}")
        self.state["day_pnl"] = round(self.state.get("day_pnl", 0.0) + total, 2)
        reason = pos["exit_reason"]
        alert(f"🏁 EXIT DONE ({reason}) trade {pos['trade_no']}\n" + "\n".join(lines)
              + f"\nTrade P&L ≈ ₹{total:,.0f} (before charges)  Day ≈ ₹{self.state['day_pnl']:,.0f}")
        self.state["position"] = None
        if reason == "SL" and self.state["trades_taken"] < MAX_TRADES_PER_DAY:
            now = pd.Timestamp(datetime.datetime.now())
            self.state["search_start"] = str(now.floor(f"{RESOLUTION}min"))
            self.state["green_alerted"] = None
            self.save()
        else:
            self.mark_done(f"Trade {pos['trade_no']} closed ({reason})")

    def check_position(self, now):
        pos = self.state["position"]
        if not pos:
            return
        if pos["exiting"]:
            return self.progress_exit()
        if not pos["live"]:
            return
        if now.time() >= EXIT_TIME:
            return self.begin_exit("TIME")
        with self.lock:
            a = self.ltp.get(self.syms["ce"])
            b = self.ltp.get(self.syms["pe"])
        if a is not None and b is not None and a + b >= pos["sl"]:
            self.begin_exit("SL", a + b)

    # ---------- main loop ----------
    def run(self):
        self.setup()
        self.start_socket()

        boundaries, b = [], self.ts(FIRST_CANDLE) + self.candle_len
        while b <= self.ts(ENTRY_LATEST):
            boundaries.append(b)
            b += self.candle_len
        delay = pd.Timedelta(seconds=CANDLE_CLOSE_DELAY)
        now = pd.Timestamp(datetime.datetime.now())
        pending = [b for b in boundaries if b + delay > now]

        while True:
            try:
                now = datetime.datetime.now()
                self.check_position(now)
                if pending and pd.Timestamp(now) >= pending[0] + delay:
                    b = pending.pop(0)
                    if not self.state["done"] and not self.state["position"]:
                        self.on_candle_close(b)
                pos = self.state["position"]
                if pos is None and (self.state["done"] or not pending
                                    or now.time() >= EXIT_TIME):
                    alert(f"✔️ SENSEX EMA strangle finished for {self.today}. "
                          f"{self.state.get('note') or ''}  Day P&L ≈ ₹{self.state.get('day_pnl', 0):,.0f}")
                    os._exit(0)
            except Exception as e:
                logger.exception(f"loop error: {e}")
                send_telegram(f"⚠️ loop error: {e}")
                time.sleep(1)
            time.sleep(0.2)


# ================= MAIN =================
if __name__ == "__main__":
    utc_offset = datetime.datetime.now().astimezone().utcoffset()
    if utc_offset != datetime.timedelta(hours=5, minutes=30):
        alert(f"❌ Machine timezone is UTC{utc_offset}, not IST. Set TZ=Asia/Kolkata and restart.",
              logging.ERROR)
        sys.exit(1)
    if not is_trading_day(datetime.date.today()):
        logger.info("Not a trading day.")
        sys.exit(0)
    alert("🚀 SENSEX EMA STRANGLE (LIVE, hedged, MARGIN) STARTED")
    Bot().run()
