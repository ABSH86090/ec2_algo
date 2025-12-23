import datetime
import logging
import os
import time
import re
from collections import defaultdict, deque

from dotenv import load_dotenv
from fyers_apiv3 import fyersModel
from fyers_apiv3.FyersWebsocket import data_ws

# =========================================================
# CONFIG
# =========================================================
load_dotenv()

CLIENT_ID = os.getenv("FYERS_CLIENT_ID")
ACCESS_TOKEN = os.getenv("FYERS_ACCESS_TOKEN")

LOT_SIZE = 300
TICK_SIZE = 0.05

TRADING_START = datetime.time(9, 15)
TRADING_END = datetime.time(15, 0)

SCENARIO_123_END = datetime.time(9, 45)
SCENARIO_4_END = datetime.time(10, 0)
SCENARIO_3_END = datetime.time(14, 30)

EMA_FAST = 5
EMA_SLOW = 20
EMA_26 = 26
EMA_50 = 50

LOG_FILE = "nifty_cpr_option_strategy.log"

# =========================================================
# LOGGING
# =========================================================
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# =========================================================
# UTILS
# =========================================================
def round_to_tick(price):
    return round(round(price / TICK_SIZE) * TICK_SIZE, 2)

def ema(values, period):
    k = 2 / (period + 1)
    e = values[0]
    out = []
    for v in values:
        e = v * k + e * (1 - k)
        out.append(e)
    return out

# =========================================================
# CPR
# =========================================================
def calculate_cpr(h, l, c):
    p = (h + l + c) / 3
    bc = (h + l) / 2
    tc = 2 * p - bc
    bc, tc = min(bc, tc), max(bc, tc)
    return {
        "TC": tc,
        "BC": bc,
        "R1": 2 * p - l,
        "R2": p + (h - l),
        "S1": 2 * p - h,
        "S2": p - (h - l),
    }

# =========================================================
# NIFTY EXPIRY & ATM SYMBOL
# =========================================================
def get_next_nifty_expiry():
    today = datetime.date.today()
    wd = today.weekday()  # Tue = 1
    return today + datetime.timedelta(days=(1 - wd) % 7)

def format_expiry(expiry):
    yy = expiry.strftime("%y")
    m = expiry.month
    d = expiry.day
    mt = {10: "O", 11: "N", 12: "D"}.get(m, f"{m:02d}")
    return f"{yy}{mt}{d:02d}"

def get_atm_nifty_symbols(fyers):
    q = fyers.client.quotes({"symbols": "NSE:NIFTY50-INDEX"})
    ltp = float(q["d"][0]["v"]["lp"])
    atm = round(ltp / 50) * 50

    expiry = format_expiry(get_next_nifty_expiry())
    ce = f"NSE:NIFTY{expiry}{atm}CE"
    pe = f"NSE:NIFTY{expiry}{atm}PE"

    logger.info(f"ATM Symbols: {ce}, {pe}")
    return [ce, pe]

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
        self.auth_token = f"{CLIENT_ID}:{ACCESS_TOKEN}"

    def _tag(self, tag):
        return re.sub(r"[^A-Za-z0-9]", "", tag)[:20]

    def market(self, symbol, side, tag):
        return self.client.place_order({
            "symbol": symbol,
            "qty": LOT_SIZE,
            "type": 2,
            "side": side,
            "productType": "INTRADAY",
            "validity": "DAY",
            "orderTag": self._tag(tag)
        })

    def place_sl(self, symbol, side, trigger, tag):
        trigger = round_to_tick(trigger)
        limit_price = trigger + TICK_SIZE if side == 1 else trigger - TICK_SIZE

        return self.client.place_order({
            "symbol": symbol,
            "qty": LOT_SIZE,
            "type": 4,
            "side": side,
            "stopPrice": trigger,
            "limitPrice": round_to_tick(limit_price),
            "productType": "INTRADAY",
            "validity": "DAY",
            "orderTag": self._tag(tag)
        })

# =========================================================
# STRATEGY ENGINE
# =========================================================
class NiftyCPRStrategy:
    def __init__(self, fyers):
        self.fyers = fyers
        self.candles = defaultdict(lambda: deque(maxlen=500))
        self.cpr = {}
        self.positions = {}
        self.trades_taken = defaultdict(set)
        self.first_candle_above_bc = {}

        # Scenario 5 state
        self.s5_below_s1_count = defaultdict(int)
        self.s5_green_seen = defaultdict(bool)

    def init_cpr(self, symbols):
        prev = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        for s in symbols:
            h = self.fyers.client.history({
                "symbol": s,
                "resolution": "D",
                "date_format": "1",
                "range_from": prev,
                "range_to": prev,
                "cont_flag": "1"
            })
            if h.get("candles"):
                _, o, hi, lo, c, _ = h["candles"][0]
                self.cpr[s] = calculate_cpr(hi, lo, c)

    def on_candle(self, symbol, candle):
        self.candles[symbol].append(candle)
        closes = [c["close"] for c in self.candles[symbol]]

        ema26 = ema(closes, EMA_26)[-1]
        ema50 = ema(closes, EMA_50)[-1]

        green = candle["close"] > candle["open"]
        red = candle["close"] < candle["open"]
        now = candle["time"].time()

        cpr = self.cpr.get(symbol)
        if not cpr:
            return

        # ---------- FIRST CANDLE BC CHECK ----------
        if symbol not in self.first_candle_above_bc:
            self.first_candle_above_bc[symbol] = candle["close"] > cpr["BC"]

        # ---------- NO NEW ENTRY IF POSITION EXISTS ----------
        if symbol in self.positions:
            return

        # =====================================================
        # SCENARIO 1
        # =====================================================
        if (
            "S1" not in self.trades_taken[symbol]
            and now <= SCENARIO_123_END
            and green
            and candle["close"] > cpr["R1"]
        ):
            self.enter(symbol, "BUY", candle, "S1", rr=2, level_target=cpr["R2"])

        # =====================================================
        # SCENARIO 2
        # =====================================================
        if (
            "S2" not in self.trades_taken[symbol]
            and now <= SCENARIO_123_END
            and red
            and candle["close"] < cpr["S1"]
        ):
            self.enter(symbol, "SELL", candle, "S2", rr=2, level_target=cpr["S2"])

        # =====================================================
        # SCENARIO 3
        # =====================================================
        if (
            "S3" not in self.trades_taken[symbol]
            and now <= SCENARIO_3_END
            and red
            and candle["high"] > cpr["R1"]
            and candle["close"] < cpr["R1"]
            and candle["high"] > ema50
            and candle["close"] < ema50
        ):
            self.enter(symbol, "SELL", candle, "S3", rr=2, level_target=cpr["TC"])

        # =====================================================
        # SCENARIO 4
        # =====================================================
        if (
            "S4" not in self.trades_taken[symbol]
            and now <= SCENARIO_4_END
            and self.first_candle_above_bc.get(symbol)
            and red
            and candle["close"] < cpr["BC"]
        ):
            self.enter(symbol, "SELL", candle, "S4", rr=2, level_target=cpr["S1"])

        # =====================================================
        # SCENARIO 5
        # =====================================================
        if "S5" not in self.trades_taken[symbol]:

            # Phase 1: 30 mins below S1 & EMA26 < EMA50
            if candle["high"] < cpr["S1"] and candle["low"] > cpr["S2"] and ema26 < ema50:
                self.s5_below_s1_count[symbol] += 1
            else:
                self.s5_below_s1_count[symbol] = 0
                self.s5_green_seen[symbol] = False

            # Phase 2: Green candle above EMAs but below S1
            if (
                self.s5_below_s1_count[symbol] >= 2
                and green
                and candle["close"] > ema26
                and candle["close"] > ema50
                and candle["close"] < cpr["S1"]
            ):
                self.s5_green_seen[symbol] = True

            # Phase 3: Red rejection candle
            if (
                self.s5_green_seen[symbol]
                and red
                and candle["open"] > ema26
                and candle["open"] > ema50
                and candle["close"] < ema26
                and candle["close"] < ema50
                and candle["close"] < cpr["S1"]
            ):
                self.enter(symbol, "SELL", candle, "S5", rr=2, level_target=cpr["S2"])
                self.s5_green_seen[symbol] = False
                self.s5_below_s1_count[symbol] = 0

    # =====================================================
    # ENTRY
    # =====================================================
    def enter(self, symbol, side, candle, scenario, rr, level_target):
        entry = candle["close"]
        sl = candle["low"] if side == "BUY" else candle["high"]
        risk = abs(entry - sl)

        rr_target = entry + rr * risk if side == "BUY" else entry - rr * risk

        if level_target is None:
            final_target = rr_target
        else:
            final_target = (
                min(rr_target, level_target)
                if side == "BUY"
                else max(rr_target, level_target)
            )

        self.fyers.market(symbol, 1 if side == "BUY" else -1, f"{scenario}ENTRY")

        sl_side = -1 if side == "BUY" else 1
        sl_resp = self.fyers.place_sl(symbol, sl_side, sl, f"{scenario}SL")

        self.positions[symbol] = {
            "side": side,
            "sl_id": sl_resp.get("id"),
            "target": final_target,
            "scenario": scenario
        }

        self.trades_taken[symbol].add(scenario)

        logger.info(
            f"{scenario} {side} {symbol} ENTRY={entry} SL={sl} TARGET={final_target}"
        )

# =========================================================
# MAIN
# =========================================================
if __name__ == "__main__":
    fyers = FyersClient()
    engine = NiftyCPRStrategy(fyers)

    symbols = get_atm_nifty_symbols(fyers)
    engine.init_cpr(symbols)

    candle_buffers = defaultdict(lambda: None)

    def on_message(tick):
        symbol = tick.get("symbol")
        ltp = float(tick.get("ltp", 0))
        ts = int(tick.get("last_traded_time", time.time()))

        dt = datetime.datetime.fromtimestamp(ts)
        t = dt.replace(second=0, microsecond=0)

        c = candle_buffers[symbol]
        if c is None or c["time"] != t:
            if c:
                engine.on_candle(symbol, c)
            candle_buffers[symbol] = {
                "time": t,
                "open": ltp,
                "high": ltp,
                "low": ltp,
                "close": ltp,
            }
        else:
            c["high"] = max(c["high"], ltp)
            c["low"] = min(c["low"], ltp)
            c["close"] = ltp

    def on_open():
        fyers_ws.subscribe(symbols=symbols, data_type="SymbolUpdate")
        fyers_ws.keep_running()

    fyers_ws = data_ws.FyersDataSocket(
        access_token=fyers.auth_token,
        on_connect=on_open,
        on_message=on_message,
        log_path=""
    )

    fyers_ws.connect()
