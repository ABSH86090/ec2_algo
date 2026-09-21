# =========================================================
# SENSEX ITM2 CE/PE  +  CPR SWING-REVERSAL BREAKOUT
# 3-MIN TIMEFRAME, LIVE (WEBSOCKET)
# =========================================================
#
# STRATEGY SUMMARY
# -----------------
# 1. At 09:16 AM, capture Sensex spot LTP. Round to nearest strike (step 100)
#    to get ATM, then derive:
#       ITM2 CE strike = ATM - 200   (2 strikes in-the-money for a call)
#       ITM2 PE strike = ATM + 200   (2 strikes in-the-money for a put)
#    These two option symbols are then tracked independently for the
#    rest of the day. No new strikes are picked later in the day.
#
# 2. Strategy is restricted to S1 only, plus a conditional second level:
#    the OPTION's OWN previous-day LOW, included ONLY when that low sits
#    ABOVE its own S1. Both are computed SEPARATELY for each leg (CE and
#    PE) from that OPTION's OWN previous trading day's OHLC (NOT the
#    index) -- so these reference levels and Low1 are all option-premium
#    values, directly comparable. Formula (standard CPR):
#       P  = (prevHigh + prevLow + prevClose) / 3
#       S1 = 2*P - prevHigh
#    reference levels for a leg = {S1, [prevLow if prevLow > S1]}
#
# 3. On the 3-MIN candle series of EACH option's own premium (CE candles and
#    PE candles built independently from tick data), we look for a
#    "swing reversal breakout":
#       Low1   -> a candle whose low is lower than the previous candle's low
#       Low2   -> a LATER candle, AT LEAST 5 CANDLES AFTER LOW1, whose low is
#                 also lower than its own previous candle's low, but
#                 Low2.low > Low1.low (higher low). If a new low undercuts
#                 Low1 first, Low1 is replaced by it and the candle count
#                 restarts. A higher-low candidate arriving fewer than 5
#                 candles after Low1 is ignored (ptn keeps waiting).
#       Peak   -> the candle with the highest HIGH seen between Low1 and Low2
#                 (the "candle which made the first high" between the lows).
#       Breakout -> the first GREEN candle (close > open) after Low2 whose
#                 CLOSE breaks above the Peak candle's OPEN.
#
# 4. FILTER: at the moment a breakout confirms, Low1.low must be within 5%
#    of AT LEAST ONE of that leg's reference levels (S1, or S1 + prevLow
#    when prevLow > S1) -- above or below either level.
#
# 5. ENTRY: if the breakout passes the reference-level filter, BUY the
#    option (CE breakout -> buy CE, PE breakout -> buy PE) at market.
#       SL     = Low2.low - 2 points
#       TARGET = entry + (entry - SL)   i.e. same points as risked (1:1 RR)
#
# 6. Only ONE trade per strike per day (CE and PE tracked independently --
#    you can take one CE trade AND one PE trade, but never a second trade
#    on the same leg). Once a leg has traded, its pattern engine stops
#    scanning for the rest of the day.
#
# 7. Hard exit for any open position at 14:50, same as the original script.
#
# 8. The SENSEX INDEX is used ONLY ONCE, at 09:16, to compute ATM and derive
#    the ITM2 CE/PE strikes. Everything else -- reference levels, candles,
#    the swing pattern, SL and target -- is computed purely from each
#    individual option contract's own premium data. No index price is used
#    after the 09:16 strike selection.
#
# NOTE: previous-day OHLC for the exact ITM2 option CONTRACT may not exist
# (new weekly series, illiquid strike, etc). If Fyers returns no candles for
# a leg, that leg's reference levels can't be computed and the leg is
# skipped for the day (logged + Telegram warning) rather than trading
# without a filter.
# =========================================================

import datetime
import time as time_module
import os
import sys
import logging
import requests
import pandas as pd
from collections import deque
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

TIMEFRAME_MIN = 5          # candle timeframe for pattern detection
STRIKE_STEP = 100          # Sensex option strike interval
ITM_OFFSET = 100           # 2 strikes ITM = 200 points

DECISION_TIME = datetime.time(9, 16, 0)     # when ITM2 strikes are locked in
HARD_EXIT_TIME = datetime.time(14, 50)

PROXIMITY_PCT = 0.10        # "close to" = within 5% of the CPR level

LOT_SIZE = 20
LOTS = 1
QTY = LOT_SIZE * LOTS

LOG_FILE = "sensex_itm2_cpr_breakout.log"

# ================= LOGGING =================
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
class Fyers:
    def __init__(self):
        self.client = fyersModel.FyersModel(
            client_id=CLIENT_ID,
            token=ACCESS_TOKEN,
            is_async=False,
            log_path=""
        )
        self.auth = f"{CLIENT_ID}:{ACCESS_TOKEN}"

    def buy_mkt(self, symbol, qty, tag):
        return self.client.place_order({
            "symbol": symbol,
            "qty": qty,
            "type": 2,
            "side": 1,
            "productType": "INTRADAY",
            "validity": "DAY",
            "orderTag": tag
        })

    def sell_mkt(self, symbol, qty, tag):
        return self.client.place_order({
            "symbol": symbol,
            "qty": qty,
            "type": 2,
            "side": -1,
            "productType": "INTRADAY",
            "validity": "DAY",
            "orderTag": tag
        })

    def buy(self, symbol, qty, tag):
        return self.buy_mkt(symbol, qty, tag)

    def sell(self, symbol, qty, tag):
        return self.sell_mkt(symbol, qty, tag)


# ================= PREV-DAY LEVELS (PER-OPTION, FROM ITS OWN OHLC) =================
def compute_s1(prev):
    h, l, c = prev["high"], prev["low"], prev["close"]
    p = (h + l + c) / 3
    return 2 * p - h


def get_option_prev_day_levels(fyers, symbol):
    """Returns {'s1': ..., 'prev_low': ...} computed from `symbol`'s OWN
    previous trading day OHLC, or None if no candles are available for that
    contract (e.g. brand new weekly series with no trading history yet)."""
    today = datetime.date.today()
    start = (today - datetime.timedelta(days=7)).strftime("%Y-%m-%d")
    end = (today - datetime.timedelta(days=1)).strftime("%Y-%m-%d")

    resp = fyers.client.history({
        "symbol": symbol,
        "resolution": "15",
        "date_format": "1",
        "range_from": start,
        "range_to": end,
        "cont_flag": "1"
    })

    if not resp.get("candles"):
        return None

    df = pd.DataFrame(
        resp["candles"],
        columns=["ts", "open", "high", "low", "close", "volume"]
    )

    df["time"] = (
        pd.to_datetime(df["ts"], unit="s", utc=True)
          .dt.tz_convert("Asia/Kolkata")
          .dt.tz_localize(None)
    )

    df.set_index("time", inplace=True)
    df = df.between_time("09:15", "15:30")

    daily = df.resample("1D").agg({
        "high": "max",
        "low": "min",
        "close": "last"
    }).dropna()

    if daily.empty:
        return None

    prev_day = daily.iloc[-1]
    s1 = compute_s1({
        "high": prev_day["high"],
        "low": prev_day["low"],
        "close": prev_day["close"]
    })

    return {"s1": s1, "prev_low": float(prev_day["low"])}


def build_reference_levels(prev_day_levels):
    """S1 is always included. Previous day's LOW is included too, but only
    when it sits ABOVE S1 (per spec: 'if previous day low is above S1 then
    include previous day low'). Same 5% proximity rule applies to both."""
    s1 = prev_day_levels["s1"]
    prev_low = prev_day_levels["prev_low"]

    levels = {"S1": s1}
    if prev_low > s1:
        levels["PREV_LOW"] = prev_low
    return levels


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

    m = expiry.month
    d = expiry.day

    if m == 10:
        m_token = "O"
    elif m == 11:
        m_token = "N"
    elif m == 12:
        m_token = "D"
    else:
        m_token = str(m)

    return f"{yy}{m_token}{d:02d}"


# ================= ITM2 STRIKE SELECTION =================
def get_itm2_symbols(fyers):
    """Called once, at/after DECISION_TIME, using the current spot LTP."""
    q = fyers.client.quotes({"symbols": INDEX_SYMBOL})
    index_ltp = float(q["d"][0]["v"]["lp"])

    atm = round(index_ltp / STRIKE_STEP) * STRIKE_STEP

    itm2_ce_strike = atm - ITM_OFFSET   # ITM for a call = strike below spot
    itm2_pe_strike = atm + ITM_OFFSET   # ITM for a put  = strike above spot

    expiry = get_next_expiry()
    exp_token = format_expiry(expiry)

    itm2_ce = f"BSE:SENSEX{exp_token}{itm2_ce_strike}CE"
    itm2_pe = f"BSE:SENSEX{exp_token}{itm2_pe_strike}PE"

    msg = (
        f"📌 ITM2 STRIKE SELECTION (09:16 spot)\n"
        f"Index LTP : {index_ltp}\n"
        f"ATM       : {atm}\n"
        f"Expiry    : {expiry} ({exp_token})\n"
        f"ITM2 CE   : {itm2_ce}\n"
        f"ITM2 PE   : {itm2_pe}"
    )
    logger.info(msg)
    send_telegram(msg)

    return itm2_ce, itm2_pe, index_ltp


# ================= REFERENCE-LEVEL PROXIMITY FILTER =================
def low1_near_level(low1_value, levels):
    """levels = {'S1': val, ['PREV_LOW': val]}. Returns (name, value) for
    the first level Low1 is within PROXIMITY_PCT of (above or below),
    else (None, None)."""
    for name, level in levels.items():
        if level and abs(low1_value - level) / abs(level) <= PROXIMITY_PCT:
            return name, level
    return None, None


# ================= SWING REVERSAL BREAKOUT ENGINE =================
MIN_CANDLES_BETWEEN_LOWS = 2   # Low2 must be at least this many candles after Low1


class SwingReversalEngine:
    """
    Runs on ONE instrument's own 3-min candles (CE premium or PE premium).
    State machine: SEARCH_LOW1 -> SEARCH_LOW2 -> SEARCH_BREAKOUT

    Low1     -> a candle whose low is lower than the previous candle's low
    Low2     -> a LATER candle (>= MIN_CANDLES_BETWEEN_LOWS candles after Low1)
                whose low is also lower than its own previous candle's low,
                but Low2.low > Low1.low (higher low). A new low that undercuts
                Low1 first always replaces Low1 (and restarts the candle count).
    Peak     -> the candle with the highest 'high' seen between Low1 and Low2
                (the "candle which made the first high" between the two lows).
    Breakout -> the first GREEN candle after Low2 whose CLOSE breaks above the
                Peak candle's OPEN.
    """

    def __init__(self, label, levels):
        self.label = label
        self.levels = levels   # dict of this leg's OWN reference levels (option scale)
        self.candles = deque(maxlen=200)
        self.candle_index = -1
        self.state = "SEARCH_LOW1"
        self.low1 = None
        self.low1_index = None
        self.low2 = None
        self.peak_candle = None
        self.traded_today = False

    def reset_pattern(self):
        self.state = "SEARCH_LOW1"
        self.low1 = None
        self.low1_index = None
        self.low2 = None
        self.peak_candle = None

    def _start_low1(self, candle, idx):
        self.low1 = candle
        self.low1_index = idx
        self.peak_candle = candle
        self.low2 = None
        self.state = "SEARCH_LOW2"

    def on_new_candle(self, candle):
        """candle = the candle that JUST closed (dict: open/high/low/close/time)
        Returns a signal dict {sl, low1, low2, peak} if a valid, filtered
        breakout happens on this candle, else None."""

        self.candle_index += 1
        idx = self.candle_index
        self.candles.append(candle)

        if self.traded_today:
            return None

        prev = self.candles[-2] if len(self.candles) >= 2 else None
        if prev is None:
            return None

        if self.state == "SEARCH_LOW1":
            if candle["low"] < prev["low"]:
                self._start_low1(candle, idx)
                logger.info(f"[{self.label}] LOW1 candidate @ {candle['low']} ({candle['time']})")

        elif self.state == "SEARCH_LOW2":
            # keep tracking the highest-high candle seen since Low1
            if self.peak_candle is None or candle["high"] > self.peak_candle["high"]:
                self.peak_candle = candle

            if candle["low"] < prev["low"]:
                if candle["low"] < self.low1["low"]:
                    # new lower low -> replaces Low1, restarts the candle-gap count
                    logger.info(f"[{self.label}] LOW1 replaced (lower low) @ {candle['low']} ({candle['time']})")
                    self._start_low1(candle, idx)
                elif candle["low"] > self.low1["low"]:
                    gap = idx - self.low1_index
                    if gap >= MIN_CANDLES_BETWEEN_LOWS:
                        self.low2 = candle
                        self.state = "SEARCH_BREAKOUT"
                        logger.info(
                            f"[{self.label}] LOW2 (higher low, gap={gap}) @ {candle['low']} ({candle['time']}); "
                            f"peak candle open={self.peak_candle['open']} @ {self.peak_candle['time']}"
                        )
                    else:
                        logger.info(
                            f"[{self.label}] higher-low candidate @ {candle['low']} ({candle['time']}) "
                            f"rejected -- only {gap} candles since Low1 (need {MIN_CANDLES_BETWEEN_LOWS}). Ignoring."
                        )
                # equal low: ignore, keep waiting

        elif self.state == "SEARCH_BREAKOUT":
            # invalidation: a new low undercutting Low1 kills the pattern
            if candle["low"] < self.low1["low"]:
                logger.info(f"[{self.label}] pattern invalidated, low broke below Low1. Restarting from this candle.")
                self._start_low1(candle, idx)
                return None

            is_green = candle["close"] > candle["open"]
            breaks_out = candle["close"] > self.peak_candle["open"]

            if is_green and breaks_out:
                level_name, level_val = low1_near_level(self.low1["low"], self.levels)
                if level_name is None:
                    logger.info(
                        f"[{self.label}] breakout @ {candle['close']} ({candle['time']}) "
                        f"but Low1={self.low1['low']} not within {PROXIMITY_PCT*100:.0f}% of "
                        f"any reference level {self.levels} -- SKIPPED, resetting pattern."
                    )
                    self.reset_pattern()
                    return None

                sl = self.low2["low"] - 2
                signal = {
                    "entry_candle": candle,
                    "sl": sl,
                    "low1": self.low1,
                    "low2": self.low2,
                    "peak": self.peak_candle,
                    "level_name": level_name,
                    "level_val": level_val
                }
                logger.info(
                    f"[{self.label}] BREAKOUT CONFIRMED, Low1={self.low1['low']} near "
                    f"{level_name}={round(level_val,2)}"
                )
                return signal

        return None


# ================= 3-MIN CANDLE BUILDER =================
class CandleBuilder:
    def __init__(self, timeframe_min):
        self.timeframe_min = timeframe_min
        self.current = None

    def on_tick(self, ltp, ts):
        bucket = ts.replace(
            minute=(ts.minute // self.timeframe_min) * self.timeframe_min,
            second=0, microsecond=0
        )

        closed_candle = None

        if self.current is None:
            self.current = {"time": bucket, "open": ltp, "high": ltp, "low": ltp, "close": ltp}
        elif self.current["time"] != bucket:
            closed_candle = dict(self.current)
            self.current = {"time": bucket, "open": ltp, "high": ltp, "low": ltp, "close": ltp}
        else:
            self.current["high"] = max(self.current["high"], ltp)
            self.current["low"] = min(self.current["low"], ltp)
            self.current["close"] = ltp

        return closed_candle


# ================= TRADE MANAGER (single leg: CE or PE) =================
class LegTradeManager:
    def __init__(self, fyers, symbol, label):
        self.fyers = fyers
        self.symbol = symbol
        self.label = label
        self.pos = None

    def enter(self, signal):
        entry_price = signal["entry_candle"]["close"]
        sl = signal["sl"]
        risk = entry_price - sl
        if risk <= 0:
            logger.warning(f"[{self.label}] invalid risk ({risk}), skipping entry")
            return False
        target = entry_price + risk

        resp = self.fyers.buy(self.symbol, QTY, f"{self.label}ENTRY")
        if resp is None:
            send_telegram(f"❌ [{self.label}] ENTRY FAILED")
            return False

        self.pos = {
            "entry": entry_price,
            "sl": sl,
            "target": target
        }

        send_telegram(
            f"🚀 ENTRY {self.label}\n"
            f"Symbol={self.symbol}\n"
            f"Entry={entry_price}  SL={round(sl,2)}  Target={round(target,2)}\n"
            f"Low1={signal['low1']['low']}  Low2={signal['low2']['low']}  "
            f"Near {signal['level_name']}={round(signal['level_val'],2)}"
        )
        return True

    def on_tick(self, ltp, now):
        if not self.pos:
            return

        if now.time() >= HARD_EXIT_TIME:
            self.exit("TIME", ltp)
            return

        if ltp >= self.pos["target"]:
            self.exit("TARGET", ltp)
        elif ltp <= self.pos["sl"]:
            self.exit("SL", ltp)

    def exit(self, reason, ltp):
        if not self.pos:
            return
        self.fyers.sell(self.symbol, QTY, f"{self.label}EXIT{reason}")
        send_telegram(f"🏁 EXIT {self.label} {reason}\nLTP={ltp}")
        self.pos = None


# ================= MAIN =================
if __name__ == "__main__":
    send_telegram("🚀 ITM2 CPR SWING-BREAKOUT STRATEGY STARTED")

    fyers = Fyers()

    # ---- Wait for decision time (09:16) then lock ITM2 strikes ----
    while datetime.datetime.now().time() < DECISION_TIME:
        time_module.sleep(1)

    itm2_ce, itm2_pe, spot_at_decision = get_itm2_symbols(fyers)

    # ---- Reference levels per leg, from each option's OWN previous-day OHLC ----
    ce_prev = get_option_prev_day_levels(fyers, itm2_ce)
    pe_prev = get_option_prev_day_levels(fyers, itm2_pe)

    ce_levels = build_reference_levels(ce_prev) if ce_prev else {}
    pe_levels = build_reference_levels(pe_prev) if pe_prev else {}

    if not ce_prev:
        send_telegram(f"⚠️ No prev-day data for {itm2_ce} -- CE leg disabled for today")
    if not pe_prev:
        send_telegram(f"⚠️ No prev-day data for {itm2_pe} -- PE leg disabled for today")

    send_telegram(
        "REFERENCE LEVELS (per-leg, from own prev-day OHLC)\n"
        f"CE: {', '.join(f'{k}={round(v,2)}' for k, v in ce_levels.items()) or 'N/A'}\n"
        f"PE: {', '.join(f'{k}={round(v,2)}' for k, v in pe_levels.items()) or 'N/A'}"
    )

    ce_engine = SwingReversalEngine("CE", ce_levels)
    pe_engine = SwingReversalEngine("PE", pe_levels)

    ce_tm = LegTradeManager(fyers, itm2_ce, "CE")
    pe_tm = LegTradeManager(fyers, itm2_pe, "PE")

    ce_builder = CandleBuilder(TIMEFRAME_MIN)
    pe_builder = CandleBuilder(TIMEFRAME_MIN)

    # a leg with no usable reference levels never trades, but candles still get logged
    if not ce_prev:
        ce_engine.traded_today = True
    if not pe_prev:
        pe_engine.traded_today = True

    def on_tick(msg):
        symbol = msg.get("symbol")
        ts = datetime.datetime.fromtimestamp(
            msg.get("last_traded_time", msg.get("timestamp", datetime.datetime.now().timestamp()))
        )
        ltp = msg["ltp"]

        if symbol == itm2_ce:
            ce_tm.on_tick(ltp, ts)
            closed = ce_builder.on_tick(ltp, ts)
            if closed and not ce_engine.traded_today:
                signal = ce_engine.on_new_candle(closed)
                if signal:
                    if ce_tm.enter(signal):
                        ce_engine.traded_today = True

        elif symbol == itm2_pe:
            pe_tm.on_tick(ltp, ts)
            closed = pe_builder.on_tick(ltp, ts)
            if closed and not pe_engine.traded_today:
                signal = pe_engine.on_new_candle(closed)
                if signal:
                    if pe_tm.enter(signal):
                        pe_engine.traded_today = True

    def on_open():
        ws.subscribe(symbols=[itm2_ce, itm2_pe], data_type="SymbolUpdate")
        ws.keep_running()

    ws = data_ws.FyersDataSocket(
        access_token=fyers.auth,
        on_connect=on_open,
        on_message=on_tick,
        log_path=""
    )

    ws.connect()
