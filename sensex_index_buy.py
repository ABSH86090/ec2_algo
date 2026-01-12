# =========================================================
# SENSEX CPR + EMA → ITM OPTIONS BUYING (INDEX SL / TARGET)
# LIVE = BACKTEST ALIGNED (FIXED)
# =========================================================

import datetime
import os
import sys
import logging
import requests
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

TIMEFRAME_MIN = 15
EMA_FAST = 5
EMA_SLOW = 20

LOT_SIZE = 20
LOTS = 1
TOTAL_QTY = LOT_SIZE * LOTS

INDEX_TARGET = 350
INDEX_SL = 75
HARD_EXIT_TIME = datetime.time(14, 45)

LOG_FILE = "sensex_index_sl_tp.log"

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

# ================= EMA =================
def ema(values, period):
    if len(values) < period:
        return None
    sma = sum(values[:period]) / period
    e = sma
    k = 2 / (period + 1)
    for v in values[period:]:
        e = v * k + e * (1 - k)
    return e

# ================= CPR =================
def compute_cpr(prev):
    h, l, c = prev["high"], prev["low"], prev["close"]
    p = (h + l + c) / 3
    bc = (h + l) / 2
    tc = 2 * p - bc
    r1 = 2 * p - l
    s1 = 2 * p - h
    return {"BC": min(bc, tc), "TC": max(bc, tc), "R1": r1, "S1": s1}

# ================= EMA WARMUP =================
def prefill_intraday_candles(fyers, candles, days=5):
    start = (datetime.date.today() - datetime.timedelta(days=days)).strftime("%Y-%m-%d")
    end = datetime.date.today().strftime("%Y-%m-%d")

    resp = fyers.client.history({
        "symbol": INDEX_SYMBOL,
        "resolution": "15",
        "date_format": "1",
        "range_from": start,
        "range_to": end,
        "cont_flag": "1"
    })

    if resp.get("candles"):
        for c in resp["candles"]:
            ts = datetime.datetime.fromtimestamp(c[0]).replace(second=0, microsecond=0)
            candles.append({
                "time": ts,
                "open": c[1],
                "high": c[2],
                "low": c[3],
                "close": c[4]
            })

    closes = [x["close"] for x in candles]
    send_telegram(
        f"📊 EMA WARMUP\nCandles={len(candles)}\n"
        f"EMA5={ema(closes, EMA_FAST)}\nEMA20={ema(closes, EMA_SLOW)}"
    )

SPECIAL_MARKET_HOLIDAYS = {
    datetime.date(2026, 1, 26),
    datetime.date(2026, 3, 3),
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

    # Monthly expiry
    if is_last_thursday(expiry):
        return f"{yy}{expiry.strftime('%b').upper()}"

    # Weekly expiry
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

def get_itm_symbols(fyers):
    q = fyers.client.quotes({"symbols": INDEX_SYMBOL})
    index_ltp = float(q["d"][0]["v"]["lp"])

    atm = round(index_ltp / 100) * 100

    expiry = get_next_expiry()
    exp_token = format_expiry(expiry)

    ce_strike = atm - 500
    pe_strike = atm + 500

    ce = f"BSE:SENSEX{exp_token}{ce_strike}CE"
    pe = f"BSE:SENSEX{exp_token}{pe_strike}PE"

    msg = (
        f"📌 SENSEX OPTION SELECTION\n"
        f"Index LTP : {index_ltp}\n"
        f"ATM       : {atm}\n"
        f"Expiry    : {expiry} ({exp_token})\n"
        f"CALL ITM  : {ce}\n"
        f"PUT  ITM  : {pe}"
    )

    logger.info(msg)
    send_telegram(msg)

    return ce, pe


# ================= SCENARIO ENGINE =================
class ScenarioEngine:
    def __init__(self, cpr):
        self.cpr = cpr

    def evaluate(self, p, c, ema5, ema20):
        if (
            p["close"] > self.cpr["BC"]
            and c["close"] < self.cpr["BC"]
            and c["close"] > self.cpr["S1"]
            and ema5 < ema20
        ):
            return "PUT"

        if (
            p["close"] > self.cpr["S1"]
            and c["close"] < self.cpr["S1"]
            and ema5 < ema20
        ):
            return "PUT"

        if (
            p["close"] < self.cpr["R1"]
            and c["close"] > self.cpr["R1"]
            and ema5 > ema20
        ):
            return "CALL"

        return None

# ================= TRADE MANAGER =================
class TradeManager:
    def __init__(self, fyers):
        self.fyers = fyers
        self.pos = None
        self.trade_day = None

    def enter(self, symbol, index_price):
        today = datetime.date.today()
        if self.trade_day == today:
            return
        self.trade_day = today

        self.fyers.buy_mkt(symbol, TOTAL_QTY, "ENTRY")
        self.pos = {"symbol": symbol, "entry": index_price}

        send_telegram(f"🚀 ENTRY\n{symbol}\nIndex={index_price}")

    def on_tick(self, index_ltp, now):
        if not self.pos:
            return

        if now.time() >= HARD_EXIT_TIME:
            self.exit("TIME EXIT")
            return

        entry = self.pos["entry"]

        if index_ltp >= entry + INDEX_TARGET:
            self.exit("TARGET")
        elif index_ltp <= entry - INDEX_SL:
            self.exit("SL")

    def exit(self, reason):
        self.fyers.sell_mkt(self.pos["symbol"], TOTAL_QTY, reason)
        send_telegram(f"🏁 EXIT [{reason}]")
        self.pos = None

# ================= MAIN =================
if __name__ == "__main__":
    send_telegram("🚀 STRATEGY STARTED")

    fyers = Fyers()
    ce, pe = get_itm_symbols(fyers)

    hist = fyers.client.history({
        "symbol": INDEX_SYMBOL,
        "resolution": "D",
        "date_format": "1",
        "range_from": (datetime.date.today() - datetime.timedelta(days=10)).strftime("%Y-%m-%d"),
        "range_to": (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d"),
        "cont_flag": "1"
    })

    last = hist["candles"][-1]
    cpr = compute_cpr({"high": last[2], "low": last[3], "close": last[4]})

    engine = ScenarioEngine(cpr)
    tm = TradeManager(fyers)

    candles = deque(maxlen=300)
    prefill_intraday_candles(fyers, candles)

    pending_signal = None
    signal_candle_time = None

    def on_tick(msg):
        global pending_signal, signal_candle_time

        if msg.get("symbol") != INDEX_SYMBOL:
            return

        ts = datetime.datetime.now()
        index_ltp = msg["ltp"]

        tm.on_tick(index_ltp, ts)

        bucket = ts.replace(minute=(ts.minute // 15) * 15, second=0, microsecond=0)

        if not candles or candles[-1]["time"] != bucket:
            if len(candles) >= 2:
                p, c = candles[-2], candles[-1]

                if p["time"].date() == c["time"].date():
                    closes = [x["close"] for x in candles[:-1]]
                    ema5 = ema(closes, EMA_FAST)
                    ema20 = ema(closes, EMA_SLOW)

                    if ema5 and ema20:
                        sig = engine.evaluate(p, c, ema5, ema20)
                        if sig:
                            pending_signal = sig
                            signal_candle_time = c["time"]

            candles.append({
                "time": bucket,
                "open": index_ltp,
                "high": index_ltp,
                "low": index_ltp,
                "close": index_ltp
            })

            if pending_signal and bucket > signal_candle_time:
                tm.enter(ce if pending_signal == "CALL" else pe, index_ltp)
                pending_signal = None
                signal_candle_time = None

        else:
            c = candles[-1]
            c["high"] = max(c["high"], index_ltp)
            c["low"] = min(c["low"], index_ltp)
            c["close"] = index_ltp

    def on_open():
        ws.subscribe(symbols=[INDEX_SYMBOL], data_type="SymbolUpdate")
        ws.keep_running()

    ws = data_ws.FyersDataSocket(
        access_token=fyers.auth,
        on_connect=on_open,
        on_message=on_tick,
        log_path=""
    )

    ws.connect()
