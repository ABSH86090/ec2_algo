import datetime
import logging
import os
import time
import re
import requests
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

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

LOT_SIZE = 150
TICK_SIZE = 0.05

TRADING_START = datetime.time(9, 15)
TRADING_END = datetime.time(15, 0)

SCENARIO_123_END = datetime.time(9, 45)
SCENARIO_4_END = datetime.time(10, 0)
SCENARIO_3_END = datetime.time(14, 30)

EMA_26 = 26
EMA_50 = 50

LOG_FILE = "nifty_cpr_option_strategy.log"
MAX_CPR_LOOKBACK_DAYS = 10

S1_BUFFER = 0.5

# =========================================================
# LOGGING
# =========================================================
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)
logger.info("========== STRATEGY STARTED ==========")

# =========================================================
# TELEGRAM LOGGING
# =========================================================
def send_telegram_message(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        requests.post(
            url,
            json={"chat_id": TELEGRAM_CHAT_ID, "text": text[:4000]},
            timeout=3
        )
    except Exception:
        pass

class TelegramLogHandler(logging.Handler):
    def emit(self, record):
        try:
            send_telegram_message(self.format(record))
        except Exception:
            pass

telegram_handler = TelegramLogHandler()
telegram_handler.setLevel(logging.INFO)
telegram_handler.setFormatter(
    logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
)
logger.addHandler(telegram_handler)

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

def get_last_trading_day_candle(fyers, symbol):
    today = datetime.date.today()
    for i in range(1, MAX_CPR_LOOKBACK_DAYS + 1):
        check_date = today - datetime.timedelta(days=i)
        date_str = check_date.strftime("%Y-%m-%d")

        try:
            h = fyers.client.history({
                "symbol": symbol,
                "resolution": "D",
                "date_format": "1",
                "range_from": date_str,
                "range_to": date_str,
                "cont_flag": "1"
            })
        except Exception:
            continue

        candles = h.get("candles", [])
        if candles:
            _, _, high, low, close, _ = candles[0]
            return check_date, high, low, close

    return None, None, None, None

# =========================================================
# NIFTY ATM SYMBOL
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
    symbols = [f"NSE:NIFTY{expiry}{atm}CE", f"NSE:NIFTY{expiry}{atm}PE"]

    logger.info(f"[SYMBOLS] ATM NIFTY symbols selected: {symbols}")
    return symbols

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

        # --- Scenario 2 state ---
        self.s2_red_below_s1 = defaultdict(bool)
        self.s2_green_above_s1 = defaultdict(bool)

        # --- Scenario 5 state ---
        self.s5_below_s1_close_count = defaultdict(int)
        self.s5_green_above_emas = defaultdict(bool)

    def init_cpr(self, symbols):
        logger.info("========== CPR INITIALIZATION START ==========")
        for symbol in symbols:
            d, h, l, c = get_last_trading_day_candle(self.fyers, symbol)
            if d:
                self.cpr[symbol] = calculate_cpr(h, l, c)
        logger.info("========== CPR INITIALIZATION END ==========")

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

        if symbol not in self.first_candle_above_bc:
            self.first_candle_above_bc[symbol] = candle["close"] > cpr["BC"]

        # ---------------- S1 (UNCHANGED) ----------------
        if "S1" not in self.trades_taken[symbol] and now <= SCENARIO_123_END and green and candle["close"] > cpr["R1"]:
            self.enter(symbol, "BUY", candle, "S1", 2, cpr["R2"])

        # ---------------- S2 (UPDATED) ----------------
        S1 = cpr["S1"]

        if red and candle["close"] < S1 - S1_BUFFER:
            self.s2_red_below_s1[symbol] = True

        if self.s2_red_below_s1[symbol] and green and candle["close"] > S1 + S1_BUFFER:
            self.s2_green_above_s1[symbol] = True

        if (
            "S2" not in self.trades_taken[symbol]
            and now <= SCENARIO_123_END
            and self.s2_green_above_s1[symbol]
            and red
            and candle["open"] > S1
            and candle["close"] < S1
        ):
            self.enter(symbol, "SELL", candle, "S2", 2, cpr["S2"])
            self.s2_red_below_s1[symbol] = False
            self.s2_green_above_s1[symbol] = False

        # ---------------- S3 (UNCHANGED) ----------------
        if (
            "S3" not in self.trades_taken[symbol]
            and now <= SCENARIO_3_END
            and red
            and candle["open"] > cpr["R1"]
            and candle["close"] < cpr["R1"]
        ):
            self.enter(symbol, "SELL", candle, "S3", 5, cpr["TC"])

        # ---------------- S4 (UNCHANGED) ----------------
        if (
            "S4" not in self.trades_taken[symbol]
            and now <= SCENARIO_4_END
            and self.first_candle_above_bc.get(symbol)
            and red
            and candle["close"] < cpr["BC"]
        ):
            self.enter(symbol, "SELL", candle, "S4", 2, cpr["S1"])

        # ---------------- S5 (UPDATED) ----------------
        if candle["close"] < S1:
            self.s5_below_s1_close_count[symbol] += 1

        ema_gap_ok = abs(ema26 - ema50) <= 0.05 * ema50

        if (
            self.s5_below_s1_close_count[symbol] >= 30
            and ema_gap_ok
            and green
            and candle["close"] > ema26
            and candle["close"] > ema50
        ):
            self.s5_green_above_emas[symbol] = True

        if (
            "S5" not in self.trades_taken[symbol]
            and self.s5_green_above_emas[symbol]
            and red
            and candle["open"] > ema26
            and candle["open"] > ema50
            and candle["close"] < ema26
            and candle["close"] < ema50
        ):
            self.enter(symbol, "SELL", candle, "S5", 2, cpr["S2"])
            self.s5_green_above_emas[symbol] = False
            self.s5_below_s1_close_count[symbol] = 0

    def enter(self, symbol, side, candle, scenario, rr, level_target):
        entry = candle["close"]
        sl = candle["low"] if side == "BUY" else candle["high"]
        risk = abs(entry - sl)
        if risk > 15:
            return

        target = min(entry + rr * risk, level_target) if side == "BUY" else max(entry - rr * risk, level_target)

        logger.info(f"[ENTRY] {scenario} {symbol} {side} ENTRY={entry} SL={sl} TARGET={target}")

        self.fyers.market(symbol, 1 if side == "BUY" else -1, f"{scenario}ENTRY")
        sl_resp = self.fyers.place_sl(symbol, -1 if side == "BUY" else 1, sl, f"{scenario}SL")

        self.positions[symbol] = {
            "side": side,
            "target": target,
            "sl_price": sl,
            "sl_order_id": sl_resp.get("id")
        }

        self.trades_taken[symbol].add(scenario)

    def on_tick(self, symbol, ltp):
        if symbol not in self.positions:
            return

        pos = self.positions[symbol]
        side = pos["side"]

        if (side == "BUY" and ltp <= pos["sl_price"]) or (side == "SELL" and ltp >= pos["sl_price"]):
            logger.info(f"[EXIT] SL HIT {symbol} LTP={ltp}")
            del self.positions[symbol]
            return

        if (side == "BUY" and ltp >= pos["target"]) or (side == "SELL" and ltp <= pos["target"]):
            logger.info(f"[EXIT] TARGET HIT {symbol} LTP={ltp}")
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
