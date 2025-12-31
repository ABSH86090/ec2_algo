# =========================================================
# SENSEX CPR + EMA → ITM OPTIONS BUYING (INDEX SL / TARGET)
# ONE TRADE PER DAY | NO TRAILING | NO PARTIAL
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

# =========================================================
# CONFIG
# =========================================================
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
LOTS = 2
TOTAL_QTY = LOT_SIZE * LOTS

INDEX_TARGET = 300
INDEX_SL = 100

LOG_FILE = "sensex_index_sl_tp.log"

# =========================================================
# LOGGING
# =========================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler(sys.stdout)],
    force=True
)
logger = logging.getLogger(__name__)

# =========================================================
# TELEGRAM
# =========================================================
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

# =========================================================
# FYERS
# =========================================================
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

# =========================================================
# EMA
# =========================================================
def ema(values, period):
    if len(values) < period:
        return None
    sma = sum(values[:period]) / period
    e = sma
    k = 2 / (period + 1)
    for v in values[period:]:
        e = v * k + e * (1 - k)
    return e

# =========================================================
# CPR (UNCHANGED)
# =========================================================
def compute_cpr(prev):
    h, l, c = prev["high"], prev["low"], prev["close"]
    p = (h + l + c) / 3
    bc = (h + l) / 2
    tc = 2 * p - bc
    r1 = 2 * p - l
    s1 = 2 * p - h
    return {
        "P": p,
        "BC": min(bc, tc),
        "TC": max(bc, tc),
        "R1": r1,
        "S1": s1
    }

# =========================================================
# EXPIRY + SYMBOL HELPERS (ATM → ITM)
# =========================================================
SPECIAL_MARKET_HOLIDAYS = {
    datetime.date(2025, 10, 2),
    datetime.date(2025, 12, 25),
}

def is_last_thursday(d):
    return d.weekday() == 3 and (d + datetime.timedelta(days=7)).month != d.month

def get_next_expiry():
    today = datetime.date.today()
    days_to_thu = (3 - today.weekday()) % 7
    expiry = today + datetime.timedelta(days=days_to_thu)
    if expiry in SPECIAL_MARKET_HOLIDAYS:
        expiry -= datetime.timedelta(days=1)
    return expiry

def format_expiry_for_symbol(expiry_date):
    yy = expiry_date.strftime("%y")
    if is_last_thursday(expiry_date):
        return f"{yy}{expiry_date.strftime('%b').upper()}"
    m = expiry_date.month
    d = expiry_date.day
    if m in (10, 11, 12):
        m_token = {10: "O", 11: "N", 12: "D"}[m]
    else:
        m_token = str(m)
    return f"{yy}{m_token}{d:02d}"

def get_itm_symbols(fyers):
    q = fyers.client.quotes({"symbols": INDEX_SYMBOL})
    ltp = float(q["d"][0]["v"]["lp"])
    atm = round(ltp / 100) * 100

    expiry = get_next_expiry()
    exp = format_expiry_for_symbol(expiry)

    ce = f"BSE:SENSEX{exp}{atm - 500}CE"
    pe = f"BSE:SENSEX{exp}{atm + 500}PE"

    logger.info(f"[SYMBOLS] ATM={atm} CE={ce} PE={pe}")
    send_telegram(f"📌 ITM SYMBOLS\nCE={ce}\nPE={pe}")

    return ce, pe

# =========================================================
# SCENARIO ENGINE (ALL ORIGINAL SCENARIOS)
# =========================================================
class ScenarioEngine:
    def __init__(self, cpr):
        self.prev = deque(maxlen=2)
        self.cpr = cpr

    def evaluate(self, c, ema5, ema20):
        if not self.prev:
            return None

        p = self.prev[-1]

        # -------- PUT SCENARIOS --------
        if (
            p["close"] > self.cpr["BC"] and
            c["close"] < self.cpr["BC"] and
            c["close"] > self.cpr["S1"] and
            ema5 < ema20
        ):
            return "PUT"

        if (
            p["close"] > self.cpr["S1"] and
            c["close"] < self.cpr["S1"] and
            ema5 < ema20
        ):
            return "PUT"

        # -------- CALL SCENARIOS --------

        if (
            p["close"] < self.cpr["R1"] and
            c["close"] > self.cpr["R1"] and
            ema5 > ema20
        ):
            return "CALL"

        return None

# =========================================================
# TRADE MANAGER (INDEX SL / TARGET)
# =========================================================
class TradeManager:
    def __init__(self, fyers):
        self.fyers = fyers
        self.pos = None
        self.trade_date = None
        self.trade_taken_today = False

    def enter(self, symbol, index_entry):
        today = datetime.date.today()
        if self.trade_date != today:
            self.trade_date = today
            self.trade_taken_today = False

        if self.trade_taken_today or self.pos:
            return

        self.fyers.buy_mkt(symbol, TOTAL_QTY, "ENTRY")
        self.pos = {
            "symbol": symbol,
            "qty": TOTAL_QTY,
            "index_entry": index_entry
        }
        self.trade_taken_today = True

        send_telegram(
            f"🚀 ENTRY\n{symbol}\nIndex Entry: {index_entry}"
        )

    def on_index_tick(self, index_ltp):
        if not self.pos:
            return

        entry = self.pos["index_entry"]

        if index_ltp >= entry + INDEX_TARGET:
            self.exit("TARGET", index_ltp)
        elif index_ltp <= entry - INDEX_SL:
            self.exit("SL", index_ltp)

    def exit(self, reason, index_ltp):
        self.fyers.sell_mkt(self.pos["symbol"], self.pos["qty"], reason)
        send_telegram(
            f"🏁 EXIT [{reason}]\nIndex LTP: {index_ltp}"
        )
        self.pos = None

# =========================================================
# MAIN
# =========================================================
if __name__ == "__main__":
    logger.info("🚀 STRATEGY STARTED")
    send_telegram("🚀 SENSEX CPR INDEX SL/TP STRATEGY STARTED")

    fyers = Fyers()
    ce, pe = get_itm_symbols(fyers)

    r = fyers.client.history({
        "symbol": INDEX_SYMBOL,
        "resolution": "D",
        "date_format": "1",
        "range_from": (datetime.date.today() - datetime.timedelta(days=10)).strftime("%Y-%m-%d"),
        "range_to": (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d"),
        "cont_flag": "1"
    })

    last = r["candles"][-1]
    cpr = compute_cpr({
        "high": last[2],
        "low": last[3],
        "close": last[4]
    })

    scenario_engine = ScenarioEngine(cpr)
    trade_mgr = TradeManager(fyers)
    candles = deque(maxlen=100)

    def on_tick(msg):
        if msg.get("symbol") != INDEX_SYMBOL:
            return

        ltp = msg["ltp"]
        trade_mgr.on_index_tick(ltp)

        now = datetime.datetime.now()
        bucket = now.replace(minute=(now.minute // 15) * 15, second=0, microsecond=0)

        if not candles or candles[-1]["time"] != bucket:
            if candles:
                closed = candles[-1]
                scenario_engine.prev.append(closed)

                closes = [c["close"] for c in candles]
                if len(closes) >= EMA_SLOW:
                    ema5 = ema(closes, EMA_FAST)
                    ema20 = ema(closes, EMA_SLOW)
                    sig = scenario_engine.evaluate(closed, ema5, ema20)
                    if sig == "CALL":
                        trade_mgr.enter(ce, ltp)
                    elif sig == "PUT":
                        trade_mgr.enter(pe, ltp)

            candles.append({
                "time": bucket,
                "open": ltp,
                "high": ltp,
                "low": ltp,
                "close": ltp
            })
        else:
            c = candles[-1]
            c["high"] = max(c["high"], ltp)
            c["low"] = min(c["low"], ltp)
            c["close"] = ltp

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
