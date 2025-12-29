import datetime
import logging
import csv
import os
import time
import uuid
import sys
import requests
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

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

LOT_SIZE = 150

ATM_DECISION_TIME = datetime.time(9, 16)
TRADING_START = datetime.time(9, 18)
TRADING_END = datetime.time(15, 0)

CANDLE_MINUTES = 3
HISTORY_RESOLUTION = "3"

EMA_FAST = 5
EMA_SLOW = 20
EMA_GAP_MIN = 5

MIN_BARS_FOR_EMA = 25
HIST_PREFILL_BARS = 60
MAX_SL_POINTS = 200

JOURNAL_FILE = "nifty_strangle_trades.csv"
LOG_FILE = "nifty_strangle_framework.log"

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

logger = logging.getLogger(__name__)

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

logger.addHandler(TelegramHandler())

# =========================================================
# SYMBOL UTILS
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

def get_nifty_strangle_symbols(fyers):
    r = fyers.quotes({"symbols": "NSE:NIFTY50-INDEX"})
    spot = float(r["d"][0]["v"]["lp"])
    atm = round(spot / 50) * 50

    ce = atm + 100
    pe = atm - 100
    expiry = format_expiry(get_next_expiry())

    ce_symbol = f"NSE:NIFTY{expiry}{ce}CE"
    pe_symbol = f"NSE:NIFTY{expiry}{pe}PE"

    logger.info(f"[STRANGLE SYMBOLS] CE={ce_symbol} PE={pe_symbol}")
    send_telegram(f"📌 NIFTY STRANGLE\nCE={ce_symbol}\nPE={pe_symbol}")

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
# HISTORICAL PREFILL
# =========================================================
def fetch_historical_premium(fyers, ce, pe):
    to_dt = datetime.datetime.now()
    from_dt = to_dt - datetime.timedelta(minutes=HIST_PREFILL_BARS * 3)

    def fetch(symbol):
        r = fyers.client.history({
            "symbol": symbol,
            "resolution": HISTORY_RESOLUTION,
            "date_format": "1",
            "range_from": from_dt.strftime("%Y-%m-%d"),
            "range_to": to_dt.strftime("%Y-%m-%d"),
            "cont_flag": "1"
        })
        return r.get("candles", [])

    ce_hist = fetch(ce)
    pe_hist = fetch(pe)

    candles = []
    for c1, c2 in zip(ce_hist, pe_hist):
        ts = datetime.datetime.fromtimestamp(c1[0])
        candles.append({
            "time": ts,
            "open": c1[1] + c2[1],
            "high": c1[2] + c2[2],
            "low": c1[3] + c2[3],
            "close": c1[4] + c2[4]
        })

    return candles

# =========================================================
# STRATEGY ENGINE
# =========================================================
class StrategyEngine:
    def __init__(self, fyers, ce, pe):
        self.fyers = fyers
        self.ce = ce
        self.pe = pe
        self.candles = deque(maxlen=2000)
        self.position = None
        self.stage = 0
        self.disabled_for_day = False

    def ema(self, values, period):
        ema = values[0]
        k = 2 / (period + 1)
        for v in values[1:]:
            ema = v * k + ema * (1 - k)
        return ema

    def on_candle(self, candle, closed):
        if closed:
            self.candles.append(candle)

        if len(self.candles) < MIN_BARS_FOR_EMA or self.disabled_for_day:
            return

        closes = [c["close"] for c in self.candles[-MIN_BARS_FOR_EMA:]]
        ema5 = self.ema(closes, EMA_FAST)
        ema20 = self.ema(closes, EMA_SLOW)

        # ENTRY LOGIC (UNCHANGED)
        if closed and self.stage == 0:
            if candle["close"] < candle["open"] and ema5 < ema20:
                self.stage = 1

        elif closed and self.stage == 1:
            if candle["close"] > candle["open"]:
                self.stage = 2

        elif closed and self.stage == 2 and not self.position:
            if (
                candle["close"] < candle["open"]
                and candle["high"] >= ema5
                and candle["close"] < ema5
                and (ema20 - ema5) >= EMA_GAP_MIN
            ):
                entry = candle["close"]
                sl = candle["high"]

                if sl <= entry or sl - entry > MAX_SL_POINTS:
                    self.disabled_for_day = True
                    return

                logger.info(f"[ENTRY] Entry={entry:.2f} SL={sl:.2f}")
                send_telegram(
                    f"📉 STRANGLE ENTRY\nEntry={entry:.2f}\nSL={sl:.2f}\nEMA5={ema5:.2f}\nEMA20={ema20:.2f}"
                )

                self.fyers.sell_market(self.ce, "STRANGLECE")
                self.fyers.sell_market(self.pe, "STRANGLEPE")

                self.position = {"entry": entry, "sl": sl}
                self.stage = 3

        if self.position:
            if candle["high"] >= self.position["sl"]:
                logger.info("[SL HIT]")
                send_telegram("🛑 STRANGLE SL HIT")

                self.fyers.buy_market(self.ce, "EXITCE")
                self.fyers.buy_market(self.pe, "EXITPE")
                self.disabled_for_day = True
                self.position = None

# =========================================================
# MAIN
# =========================================================
if __name__ == "__main__":
    logger.info("[BOOT] NIFTY STRANGLE EMA STRATEGY STARTED")
    send_telegram("🚀 NIFTY STRANGLE EMA STRATEGY STARTED")

    fyers = FyersClient()

    while datetime.datetime.now().time() < ATM_DECISION_TIME:
        time.sleep(1)

    ce, pe = get_nifty_strangle_symbols(fyers.client)
    engine = StrategyEngine(fyers, ce, pe)

    # ===== PREFILL HISTORY =====
    hist = fetch_historical_premium(fyers, ce, pe)
    for c in hist:
        engine.candles.append(c)

    closes = [c["close"] for c in engine.candles[-MIN_BARS_FOR_EMA:]]
    ema5 = engine.ema(closes, EMA_FAST)
    ema20 = engine.ema(closes, EMA_SLOW)

    logger.info(f"[EMA READY] EMA5={ema5:.2f} EMA20={ema20:.2f}")
    send_telegram(f"📊 EMA READY\nEMA5={ema5:.2f}\nEMA20={ema20:.2f}")

    # ===== WEBSOCKET =====
    last = {}
    candle = None

    def extract_tick_epoch(msg):
        ts = msg.get("last_traded_time") or msg.get("timestamp") or msg.get("tt")
        return int(ts // 1000) if ts and ts > 10_000_000_000 else int(ts)

    def on_tick(msg):
        nonlocal candle
        if "symbol" not in msg or "ltp" not in msg:
            return

        last[msg["symbol"]] = msg["ltp"]
        if ce not in last or pe not in last:
            return

        premium = last[ce] + last[pe]
        epoch = extract_tick_epoch(msg)
        dt = datetime.datetime.fromtimestamp(epoch)
        bucket = dt.replace(second=0, microsecond=0, minute=(dt.minute // 3) * 3)

        if candle is None or candle["time"] != bucket:
            if candle:
                engine.on_candle(candle, closed=True)
            candle = {"time": bucket, "open": premium, "high": premium, "low": premium, "close": premium}
        else:
            candle["high"] = max(candle["high"], premium)
            candle["low"] = min(candle["low"], premium)
            candle["close"] = premium

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
