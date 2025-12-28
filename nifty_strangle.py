import datetime
import logging
import os
import time
import sys
import uuid
import requests
from collections import defaultdict, deque
from dotenv import load_dotenv
from fyers_apiv3 import fyersModel
from fyers_apiv3.FyersWebsocket import data_ws, order_ws

# =========================================================
# CONFIG
# =========================================================
load_dotenv()

CLIENT_ID = os.getenv("FYERS_CLIENT_ID")
ACCESS_TOKEN = os.getenv("FYERS_ACCESS_TOKEN")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

LOT_SIZE = 150
CANDLE_MINUTES = 3
HISTORY_RESOLUTION = "3"

EMA_FAST = 5
EMA_SLOW = 20
EMA_GAP_MIN = 5

ATM_DECISION_TIME = datetime.time(9, 16)
TRADING_START = datetime.time(9, 18)
TRADING_END = datetime.time(15, 0)

MIN_BARS_FOR_EMA = 25
MAX_HISTORY_LOOKBACK_DAYS = 7

LOG_FILE = "nifty_strangle_ema.log"

# =========================================================
# LOGGING + TELEGRAM
# =========================================================
for h in logging.root.handlers[:]:
    logging.root.removeHandler(h)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout)
    ],
    force=True
)

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

class TelegramHandler(logging.Handler):
    def emit(self, record):
        send_telegram(self.format(record))

logging.getLogger().addHandler(TelegramHandler())

# =========================================================
# UTILS
# =========================================================
def compute_ema(values, period):
    if len(values) < period:
        return None
    ema = values[0]
    k = 2 / (period + 1)
    for v in values[1:]:
        ema = v * k + ema * (1 - k)
    return ema

def is_green(c): return c["close"] > c["open"]
def is_red(c): return c["close"] < c["open"]

# =========================================================
# EXPIRY + SYMBOL
# =========================================================
def is_last_tuesday(d):
    return d.weekday() == 1 and (d + datetime.timedelta(days=7)).month != d.month

def get_next_expiry():
    today = datetime.date.today()
    return today if today.weekday() == 1 else today + datetime.timedelta(days=(1 - today.weekday()) % 7)

def format_expiry(d):
    yy = d.strftime("%y")
    if is_last_tuesday(d):
        return f"{yy}{d.strftime('%b').upper()}"
    m = {10: "O", 11: "N", 12: "D"}.get(d.month, f"{d.month:02d}")
    return f"{yy}{m}{d.day:02d}"

def get_strangle_symbols(fyers):
    q = fyers.quotes({"symbols": "NSE:NIFTY50-INDEX"})
    spot = float(q["d"][0]["v"]["lp"])
    atm = round(spot / 50) * 50

    ce = atm + 100
    pe = atm - 100

    expiry = format_expiry(get_next_expiry())

    ce_symbol = f"NSE:NIFTY{expiry}{ce}CE"
    pe_symbol = f"NSE:NIFTY{expiry}{pe}PE"

    logging.info(f"[SYMBOLS] ATM={atm} STRANGLE CE={ce_symbol} PE={pe_symbol}")
    return ce_symbol, pe_symbol

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
        self.auth = f"{CLIENT_ID}:{ACCESS_TOKEN}"

    def sell_market(self, symbol, tag):
        return self.client.place_order({
            "symbol": symbol,
            "qty": LOT_SIZE,
            "type": 2,
            "side": -1,
            "productType": "INTRADAY",
            "validity": "DAY",
            "orderTag": tag
        })

    def buy_market(self, symbol, tag):
        return self.client.place_order({
            "symbol": symbol,
            "qty": LOT_SIZE,
            "type": 2,
            "side": 1,
            "productType": "INTRADAY",
            "validity": "DAY",
            "orderTag": tag
        })

# =========================================================
# STRATEGY ENGINE
# =========================================================
class StrangleEMAEngine:
    def __init__(self, fyers, ce, pe):
        self.fyers = fyers
        self.ce = ce
        self.pe = pe

        self.candles = deque(maxlen=500)
        self.position = None
        self.state = 0
        self.green_seen = False

    def on_candle(self, candle, closed):
        if closed:
            self.candles.append(candle)

        if len(self.candles) < MIN_BARS_FOR_EMA:
            return

        closes = [c["close"] for c in self.candles]
        ema5 = compute_ema(closes, EMA_FAST)
        ema20 = compute_ema(closes, EMA_SLOW)

        logging.info(
            f"[BAR] {candle['time']} O={candle['open']} H={candle['high']} "
            f"L={candle['low']} C={candle['close']} EMA5={ema5:.2f} EMA20={ema20:.2f}"
        )

        if self.position:
            if candle["high"] >= self.position["sl"]:
                logging.info("[SL HIT] Exiting strangle")
                self.exit()
            elif candle["time"].time() >= TRADING_END:
                logging.info("[EOD EXIT]")
                self.exit()
            return

        if not closed:
            return

        if self.state == 0:
            if is_red(candle):
                self.state = 1
                self.green_seen = False

        elif self.state == 1:
            if is_green(candle):
                self.green_seen = True

        elif self.state == 1 and self.green_seen:
            if (
                is_red(candle)
                and candle["high"] >= ema5
                and candle["close"] < ema5
                and (ema20 - ema5) >= EMA_GAP_MIN
            ):
                self.enter(candle)
                self.state = 2

    def enter(self, candle):
        entry = candle["close"]
        sl = candle["high"]

        logging.info(f"[ENTRY] STRANGLE ENTRY={entry} SL={sl}")
        send_telegram(f"📉 NIFTY STRANGLE SELL\nEntry={entry}\nSL={sl}")

        self.fyers.sell_market(self.ce, "STRANGLECE")
        self.fyers.sell_market(self.pe, "STRANGLEPE")

        self.position = {"entry": entry, "sl": sl}

    def exit(self):
        self.fyers.buy_market(self.ce, "EXITCE")
        self.fyers.buy_market(self.pe, "EXITPE")
        send_telegram("✅ STRANGLE EXITED")
        self.position = None

# =========================================================
# MAIN
# =========================================================
if __name__ == "__main__":
    logging.info("[BOOT] NIFTY STRANGLE EMA STRATEGY STARTED")

    fyers = FyersClient()

    while datetime.datetime.now().time() < ATM_DECISION_TIME:
        time.sleep(1)

    ce, pe = get_strangle_symbols(fyers.client)
    engine = StrangleEMAEngine(fyers, ce, pe)

    # =======================
    # Websocket candle build
    # =======================
    last = {}
    candle = None

    def on_tick(msg):
        nonlocal candle
        s = msg.get("symbol")
        ltp = msg.get("ltp")
        ts = msg.get("timestamp")
        if not s or not ltp:
            return

        last[s] = ltp
        if ce not in last or pe not in last:
            return

        price = last[ce] + last[pe]
        dt = datetime.datetime.fromtimestamp(ts)
        bucket = dt.replace(second=0, microsecond=0,
                            minute=(dt.minute // 3) * 3)

        if candle is None or candle["time"] != bucket:
            if candle:
                engine.on_candle(candle, closed=True)
            candle = {
                "time": bucket,
                "open": price,
                "high": price,
                "low": price,
                "close": price
            }
        else:
            candle["high"] = max(candle["high"], price)
            candle["low"] = min(candle["low"], price)
            candle["close"] = price

        engine.on_candle(candle, closed=False)

    def on_open():
        ws.subscribe(symbols=[ce, pe], data_type="SymbolUpdate")
        ws.keep_running()

    ws = data_ws.FyersDataSocket(
        access_token=fyers.auth,
        on_connect=on_open,
        on_message=on_tick,
        log_path=""
    )
    ws.connect()
