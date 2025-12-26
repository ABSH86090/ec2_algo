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

R1_BUFFER = 0.5
S1_BUFFER = 0.5

LOG_FILE = "nifty_cpr_option_strategy.log"
MAX_CPR_LOOKBACK_DAYS = 10

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
telegram_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(telegram_handler)

# =========================================================
# UTILS
# =========================================================
def round_to_tick(price):
    return round(round(price / TICK_SIZE) * TICK_SIZE, 2)

def ema(values, period):
    k = 2 / (period + 1)
    e = values[0]
    for v in values:
        e = v * k + e * (1 - k)
    return e

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
        d = today - datetime.timedelta(days=i)
        try:
            h = fyers.client.history({
                "symbol": symbol,
                "resolution": "D",
                "date_format": "1",
                "range_from": d.strftime("%Y-%m-%d"),
                "range_to": d.strftime("%Y-%m-%d"),
                "cont_flag": "1"
            })
            candles = h.get("candles", [])
            if candles:
                _, _, high, low, close, _ = candles[0]
                return d, high, low, close
        except Exception:
            continue
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
    m_token = {10: "O", 11: "N", 12: "D"}.get(expiry.month, f"{expiry.month:02d}")
    return f"{yy}{m_token}{expiry.day:02d}"

def get_atm_nifty_symbols(fyers):
    resp = fyers.client.quotes({"symbols": "NSE:NIFTY50-INDEX"})
    ltp = float(resp["d"][0]["v"]["lp"])
    atm = round(ltp / 50) * 50
    expiry = format_nifty_expiry(get_next_nifty_expiry())
    return [
        f"NSE:NIFTY{expiry}{atm}CE",
        f"NSE:NIFTY{expiry}{atm}PE"
    ]

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

    def init_cpr(self, symbols):
        for symbol in symbols:
            trade_date, h, l, c = get_last_trading_day_candle(self.fyers, symbol)
            if not trade_date:
                logger.warning(f"[CPR INIT] No previous candle found for {symbol}")
                continue

            cpr = calculate_cpr(h, l, c)
            self.cpr[symbol] = cpr

            logger.info(
                f"[CPR INIT] {symbol} | DATE={trade_date} | "
                f"TC={round(cpr['TC'],2)} BC={round(cpr['BC'],2)} | "
                f"R1={round(cpr['R1'],2)} R2={round(cpr['R2'],2)} | "
                f"S1={round(cpr['S1'],2)} S2={round(cpr['S2'],2)}"
            )

    def on_candle(self, symbol, candle):
        self.candles[symbol].append(candle)
        closes = [c["close"] for c in self.candles[symbol]]

        ema26 = ema(closes, EMA_26)
        ema50 = ema(closes, EMA_50)

        green = candle["close"] > candle["open"]
        red = candle["close"] < candle["open"]
        now = candle["time"].time()
        cpr = self.cpr.get(symbol)

        if not cpr or symbol in self.positions:
            return

        if symbol not in self.first_candle_above_bc:
            self.first_candle_above_bc[symbol] = candle["close"] > cpr["BC"]

        # Scenario 1
        if (
            "S1" not in self.trades_taken[symbol]
            and now <= SCENARIO_123_END
            and green
            and candle["close"] >= cpr["R1"] + R1_BUFFER
        ):
            self.enter(symbol, "BUY", candle, "S1", 2, cpr["R2"])

        # Scenario 2
        if (
            "S2" not in self.trades_taken[symbol]
            and now <= SCENARIO_123_END
            and red
            and candle["close"] <= cpr["S1"] - S1_BUFFER
        ):
            self.enter(symbol, "SELL", candle, "S2", 5, cpr["S2"])

        # Scenario 3
        if (
            "S3" not in self.trades_taken[symbol]
            and now <= SCENARIO_3_END
            and red
            and candle["open"] >= cpr["R1"] + R1_BUFFER
            and candle["close"] <= cpr["R1"] - R1_BUFFER
        ):
            self.enter(symbol, "SELL", candle, "S3", 5, cpr["TC"])

        # Scenario 4
        if (
            "S4" not in self.trades_taken[symbol]
            and now <= SCENARIO_4_END
            and self.first_candle_above_bc.get(symbol)
            and red
            and candle["close"] <= cpr["BC"] - R1_BUFFER
        ):
            self.enter(symbol, "SELL", candle, "S4", 2, cpr["S1"])

    def enter(self, symbol, side, candle, scenario, rr, level_target):
        entry = candle["close"]
        sl = candle["low"] if side == "BUY" else candle["high"]
        risk = abs(entry - sl)
        if risk > 15:
            return

        target = (
            min(entry + rr * risk, level_target)
            if side == "BUY"
            else max(entry - rr * risk, level_target)
        )

        self.fyers.market(symbol, 1 if side == "BUY" else -1, f"{scenario}ENTRY")
        sl_resp = self.fyers.place_sl(
            symbol,
            -1 if side == "BUY" else 1,
            sl,
            f"{scenario}SL"
        )

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
            del self.positions[symbol]
            return

        if (side == "BUY" and ltp >= pos["target"]) or (side == "SELL" and ltp <= pos["target"]):
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
        fyers_ws.subscribe(symbols=symbols, data_type="SymbolUpdate")
        fyers_ws.keep_running()

    fyers_ws = data_ws.FyersDataSocket(
        access_token=fyers.auth_token,
        on_connect=on_open,
        on_message=on_message,
        log_path=""
    )

    fyers_ws.connect()
