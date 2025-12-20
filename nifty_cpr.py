import datetime
import logging
import os
import csv
import time
import json
import re
from collections import defaultdict, deque

from dotenv import load_dotenv
from fyers_apiv3 import fyersModel
from fyers_apiv3.FyersWebsocket import order_ws, data_ws

# =========================================================
# CONFIG
# =========================================================
load_dotenv()

CLIENT_ID = os.getenv("FYERS_CLIENT_ID")
ACCESS_TOKEN = os.getenv("FYERS_ACCESS_TOKEN")

LOT_SIZE = 75
TICK_SIZE = 0.05

TRADING_START = datetime.time(9, 15)
TRADING_END = datetime.time(15, 0)

SCENARIO_123_END = datetime.time(9, 45)
SCENARIO_4_END = datetime.time(10, 0)

EMA_FAST = 5
EMA_SLOW = 20

LOG_FILE = "nifty_cpr_option_strategy.log"

# =========================================================
# LOGGING
# =========================================================
for h in logging.root.handlers[:]:
    logging.root.removeHandler(h)

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
    if m == 10:
        mt = "O"
    elif m == 11:
        mt = "N"
    elif m == 12:
        mt = "D"
    else:
        mt = f"{m:02d}"
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

    def market_buy(self, symbol, tag):
        return self.client.place_order({
            "symbol": symbol,
            "qty": LOT_SIZE,
            "type": 2,
            "side": 1,
            "productType": "INTRADAY",
            "validity": "DAY",
            "orderTag": self._tag(tag)
        })

    def market_sell(self, symbol, tag):
        return self.client.place_order({
            "symbol": symbol,
            "qty": LOT_SIZE,
            "type": 2,
            "side": -1,
            "productType": "INTRADAY",
            "validity": "DAY",
            "orderTag": self._tag(tag)
        })

    def sl_sell(self, symbol, sl, tag):
        return self.client.place_order({
            "symbol": symbol,
            "qty": LOT_SIZE,
            "type": 3,
            "side": -1,
            "stopPrice": round_to_tick(sl),
            "productType": "INTRADAY",
            "validity": "DAY",
            "orderTag": self._tag(tag)
        })

    def cancel(self, oid):
        return self.client.cancel_order({"id": oid})

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

    # ---------- PREFILL CPR ----------
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
                logger.info(f"CPR {s}: {self.cpr[s]}")

    # ---------- CANDLE ----------
    def on_candle(self, symbol, candle):
        self.candles[symbol].append(candle)
        candles = list(self.candles[symbol])

        closes = [c["close"] for c in candles]
        ema5 = ema(closes, EMA_FAST)[-1]
        ema20 = ema(closes, EMA_SLOW)[-1]

        green = candle["close"] > candle["open"]
        red = candle["close"] < candle["open"]
        now = candle["time"].time()

        cpr = self.cpr.get(symbol)
        if not cpr:
            return

        # ---------- EXIT ----------
        if symbol in self.positions:
            if (
                (red and candle["close"] < ema5 and candle["close"] < ema20) or
                (green and candle["close"] > ema5 and candle["close"] > ema20)
            ):
                self.exit(symbol, "EMAEXIT")
            return

        # ---------- SCENARIO 1 ----------
        if (
            "S1" not in self.trades_taken[symbol]
            and now <= SCENARIO_123_END
            and green and candle["close"] > cpr["R1"]
        ):
            self.enter(symbol, "BUY", candle, "S1", 2, cpr["R2"])

        # ---------- SCENARIO 2 ----------
        if (
            "S2" not in self.trades_taken[symbol]
            and now <= SCENARIO_123_END
            and red and candle["close"] < cpr["S1"]
        ):
            self.enter(symbol, "SELL", candle, "S2", 2, cpr["S2"])

        # ---------- SCENARIO 3 ----------
        if (
            "S3" not in self.trades_taken[symbol]
            and now <= SCENARIO_123_END
            and red and candle["high"] > cpr["R1"]
            and candle["close"] < cpr["R1"]
        ):
            self.enter(symbol, "SELL", candle, "S3", 3, cpr["TC"])

        # ---------- SCENARIO 4 ----------
        if (
            "S4" not in self.trades_taken[symbol]
            and now <= SCENARIO_4_END
            and red and candle["close"] < cpr["BC"]
        ):
            self.enter(symbol, "SELL", candle, "S4", 2, cpr["S1"])

    # ---------- ENTRY ----------
    def enter(self, symbol, side, candle, scenario, rr, level_target):
        entry = candle["close"]
        sl = candle["low"] if side == "BUY" else candle["high"]
        risk = abs(entry - sl)
        target = entry + rr * risk if side == "BUY" else entry - rr * risk
        final_target = max(target, level_target) if side == "BUY" else min(target, level_target)

        if side == "BUY":
            self.fyers.market_buy(symbol, f"{scenario}ENTRY")
        else:
            self.fyers.market_sell(symbol, f"{scenario}ENTRY")

        sl_resp = self.fyers.sl_sell(symbol, sl, f"{scenario}SL")

        self.positions[symbol] = {
            "side": side,
            "sl_id": sl_resp.get("id"),
            "target": final_target
        }
        self.trades_taken[symbol].add(scenario)

        logger.info(f"{scenario} {side} {symbol} ENTRY={entry} SL={sl} TARGET={final_target}")

    # ---------- EXIT ----------
    def exit(self, symbol, reason):
        pos = self.positions.get(symbol)
        if not pos:
            return

        try:
            if pos.get("sl_id"):
                self.fyers.cancel(pos["sl_id"])
        except Exception as e:
            logger.warning(f"SL cancel failed {symbol}: {e}")

        if pos["side"] == "BUY":
            self.fyers.market_sell(symbol, f"{reason}EXIT")
        else:
            self.fyers.market_buy(symbol, f"{reason}EXIT")

        logger.info(f"EXIT {symbol} {reason}")
        del self.positions[symbol]

# =========================================================
# MAIN
# =========================================================
if __name__ == "__main__":
    fyers = FyersClient()
    engine = NiftyCPRStrategy(fyers)

    symbols = get_atm_nifty_symbols(fyers)
    engine.init_cpr(symbols)

    # ---------- DATA SOCKET ----------
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
