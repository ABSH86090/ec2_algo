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
DEBUG_LOGS = True

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
def is_last_tuesday(date_obj):
    return date_obj.weekday() == 1 and (date_obj + datetime.timedelta(days=7)).month != date_obj.month

def get_next_nifty_expiry():
    today = datetime.date.today()
    return today if today.weekday() == 1 else today + datetime.timedelta(days=(1 - today.weekday()) % 7)

def format_nifty_expiry(expiry):
    yy = expiry.strftime("%y")
    if is_last_tuesday(expiry):
        return f"{yy}{expiry.strftime('%b').upper()}"
    m, d = expiry.month, expiry.day
    m_token = {10: "O", 11: "N", 12: "D"}.get(m, f"{m:02d}")
    return f"{yy}{m_token}{d:02d}"

def get_atm_nifty_symbols(fyers):
    resp = fyers.client.quotes({"symbols": "NSE:NIFTY50-INDEX"})
    ltp = float(resp["d"][0]["v"]["lp"])
    atm = round(ltp / 50) * 50
    expiry = format_nifty_expiry(get_next_nifty_expiry())

    ce = f"NSE:NIFTY{expiry}{atm}CE"
    pe = f"NSE:NIFTY{expiry}{atm}PE"

    logger.info(
        f"[SYMBOL INIT] SPOT={ltp:.2f} ATM={atm} "
        f"CE={ce} PE={pe}"
    )

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

    def cancel_order(self, order_id):
        return self.client.cancel_order({"id": order_id})

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

            if not h.get("candles"):
                logger.warning(f"[CPR INIT] No data for {s}")
                continue

            _, _, hi, lo, c, _ = h["candles"][0]
            levels = calculate_cpr(hi, lo, c)
            self.cpr[s] = levels

            logger.info(
                f"[CPR INIT] {s} "
                f"H={hi:.2f} L={lo:.2f} C={c:.2f} | "
                f"TC={levels['TC']:.2f} BC={levels['BC']:.2f} | "
                f"R1={levels['R1']:.2f} R2={levels['R2']:.2f} | "
                f"S1={levels['S1']:.2f} S2={levels['S2']:.2f}"
            )

    def on_candle(self, symbol, candle):
        self.candles[symbol].append(candle)
        closes = [c["close"] for c in self.candles[symbol]]

        ema26 = ema(closes, EMA_26)[-1]
        ema50 = ema(closes, EMA_50)[-1]

        green = candle["close"] > candle["open"]
        red = candle["close"] < candle["open"]
        now = candle["time"].time()
        cpr = self.cpr.get(symbol)

        if not cpr or symbol in self.positions:
            return

        if DEBUG_LOGS:
            logger.info(
                f"[BAR CLOSE] {symbol} "
                f"O={candle['open']:.2f} H={candle['high']:.2f} "
                f"L={candle['low']:.2f} C={candle['close']:.2f} | "
                f"EMA26={ema26:.2f} EMA50={ema50:.2f}"
            )

        # =======================
        # SCENARIO 1
        # =======================
        if "S1" in self.trades_taken[symbol]:
            logger.info("[S1 SKIP] Already traded")
        elif now > SCENARIO_123_END:
            logger.info("[S1 SKIP] Time window over")
        elif not green:
            logger.info("[S1 SKIP] Candle not green")
        elif candle["close"] <= cpr["R1"]:
            logger.info(f"[S1 SKIP] Close <= R1 ({candle['close']:.2f} <= {cpr['R1']:.2f})")
        else:
            self.enter(symbol, "BUY", candle, "S1", 2, cpr["R2"])
            return

        # =======================
        # SCENARIO 2
        # =======================
        if "S2" in self.trades_taken[symbol]:
            logger.info("[S2 SKIP] Already traded")
        elif now > SCENARIO_123_END:
            logger.info("[S2 SKIP] Time window over")
        elif not red:
            logger.info("[S2 SKIP] Candle not red")
        elif candle["close"] >= cpr["S1"]:
            logger.info(f"[S2 SKIP] Close >= S1 ({candle['close']:.2f} >= {cpr['S1']:.2f})")
        else:
            self.enter(symbol, "SELL", candle, "S2", 2, cpr["S2"])
            return

        logger.info(f"[NO TRADE] {symbol} No scenario met")

    def enter(self, symbol, side, candle, scenario, rr, level_target):
        entry = candle["close"]
        sl = candle["low"] if side == "BUY" else candle["high"]
        risk = abs(entry - sl)

        if risk > 10:
            logger.info(
                f"[SKIP TRADE] {scenario} {symbol} "
                f"ENTRY={entry:.2f} SL={sl:.2f} SL_PTS={risk:.2f} > 10"
            )
            return

        target = (
            min(entry + rr * risk, level_target)
            if side == "BUY"
            else max(entry - rr * risk, level_target)
        )

        self.fyers.market(symbol, 1 if side == "BUY" else -1, f"{scenario}ENTRY")
        sl_resp = self.fyers.place_sl(symbol, -1 if side == "BUY" else 1, sl, f"{scenario}SL")

        self.positions[symbol] = {
            "side": side,
            "target": target,
            "sl_order_id": sl_resp.get("id")
        }

        self.trades_taken[symbol].add(scenario)

        logger.info(
            f"[ENTRY] {scenario} {symbol} "
            f"ENTRY={entry:.2f} SL={sl:.2f} TARGET={target:.2f}"
        )

    def on_tick(self, symbol, ltp):
        if symbol not in self.positions:
            return

        pos = self.positions[symbol]
        side = pos["side"]
        target = pos["target"]

        if (side == "BUY" and ltp >= target) or (side == "SELL" and ltp <= target):
            logger.info(
                f"[TARGET HIT] {symbol} LTP={ltp:.2f} TARGET={target:.2f}"
            )
            self.fyers.cancel_order(pos["sl_order_id"])
            self.fyers.market(symbol, -1 if side == "BUY" else 1, "TARGETEXIT")
            del self.positions[symbol]

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

        engine.on_tick(symbol, ltp)

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
                "close": ltp
            }
        else:
            c["high"] = max(c["high"], ltp)
            c["low"] = min(c["low"], ltp)
            c["close"] = ltp

    def on_open():
        logger.info("Websocket connected")
        fyers_ws.subscribe(symbols=symbols, data_type="SymbolUpdate")
        fyers_ws.keep_running()

    fyers_ws = data_ws.FyersDataSocket(
        access_token=fyers.auth_token,
        on_connect=on_open,
        on_message=on_message,
        log_path=""
    )

    fyers_ws.connect()
