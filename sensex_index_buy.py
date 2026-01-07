# =========================================================
# SENSEX CPR + EMA → ITM OPTIONS BUYING (INDEX SL / TARGET)
# BACKTEST-ALIGNED | READY TO RUN
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
LOTS = 2
TOTAL_QTY = LOT_SIZE * LOTS

INDEX_TARGET = 300
INDEX_SL = 100

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
    """
    Reliable EMA warmup for INDEX:
    - Fetch multiple past days
    - Always log result
    """
    start = (datetime.date.today() - datetime.timedelta(days=days)).strftime("%Y-%m-%d")
    end = datetime.date.today().strftime("%Y-%m-%d")

    logger.info(
        f"EMA warmup fetch: {INDEX_SYMBOL} | {start} → {end}"
    )

    resp = fyers.client.history({
        "symbol": INDEX_SYMBOL,
        "resolution": "15",
        "date_format": "1",
        "range_from": start,
        "range_to": end,
        "cont_flag": "1"
    })

    candles_loaded = 0

    if resp.get("candles"):
        for c in resp["candles"]:
            ts = datetime.datetime.fromtimestamp(c[0])
            candles.append({
                "time": ts.replace(second=0, microsecond=0),
                "open": c[1],
                "high": c[2],
                "low": c[3],
                "close": c[4]
            })
            candles_loaded += 1

    closes = [x["close"] for x in candles]
    ema5 = ema(closes, EMA_FAST)
    ema20 = ema(closes, EMA_SLOW)

    # 🔥 ALWAYS LOG — even if None
    msg = (
        f"📊 EMA WARMUP STATUS\n"
        f"Candles Loaded: {candles_loaded}\n"
        f"EMA5 : {round(ema5,2) if ema5 else 'NOT READY'}\n"
        f"EMA20: {round(ema20,2) if ema20 else 'NOT READY'}"
    )

    logger.info(msg)
    send_telegram(msg)


# ================= EXPIRY HELPERS =================
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

# ================= SYMBOL SELECTION =================
def get_itm_symbols(fyers):
    q = fyers.client.quotes({"symbols": INDEX_SYMBOL})
    ltp = float(q["d"][0]["v"]["lp"])
    atm = round(ltp / 100) * 100

    expiry = get_next_expiry()
    exp = format_expiry(expiry)

    ce = f"BSE:SENSEX{exp}{atm - 500}CE"
    pe = f"BSE:SENSEX{exp}{atm + 500}PE"

    logger.info(f"[SYMBOLS] ATM={atm} EXPIRY={expiry} CE={ce} PE={pe}")
    send_telegram(f"📌 ITM SYMBOLS\nATM={atm}\nCE={ce}\nPE={pe}")

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

    def enter(self, symbol, index_entry):
        day = index_entry.date()
        if self.trade_day == day:
            return
        self.trade_day = day

        self.fyers.buy_mkt(symbol, TOTAL_QTY, "ENTRY")
        self.pos = {"symbol": symbol, "entry": index_entry}

        logger.info(f"ENTRY {symbol} @ Index {index_entry}")
        send_telegram(f"🚀 ENTRY\n{symbol}\nIndex Entry: {index_entry}")

    def on_tick(self, ltp):
        if not self.pos:
            return

        entry = self.pos["entry"]

        if ltp >= entry + INDEX_TARGET:
            self.exit("TARGET")
        elif ltp <= entry - INDEX_SL:
            self.exit("SL")

    def exit(self, reason):
        self.fyers.sell_mkt(self.pos["symbol"], TOTAL_QTY, reason)
        logger.info(f"EXIT {self.pos['symbol']} | {reason}")
        send_telegram(f"🏁 EXIT [{reason}]")
        self.pos = None

# ================= MAIN =================
if __name__ == "__main__":
    logger.info("🚀 STRATEGY STARTED")
    send_telegram("🚀 SENSEX CPR INDEX SL/TP STRATEGY STARTED")

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
    logger.info(f"CPR Levels: {cpr}")

    engine = ScenarioEngine(cpr)
    tm = TradeManager(fyers)
    candles = deque(maxlen=300)
    prefill_intraday_candles(fyers, candles)

    pending_signal = None

    def on_tick(msg):
        global pending_signal

        if msg.get("symbol") != INDEX_SYMBOL:
            return

        # --- SAFE TIMESTAMP EXTRACTION ---
        if "last_traded_time" in msg:
            ts = datetime.datetime.fromtimestamp(msg["last_traded_time"])
        elif "timestamp" in msg:
            ts = datetime.datetime.fromtimestamp(msg["timestamp"])
        else:
            # FYERS index ticks often miss timestamp → fallback
            ts = datetime.datetime.now()

        ltp = msg["ltp"]

        tm.on_tick(ltp)

        bucket = ts.replace(minute=(ts.minute // 15) * 15, second=0, microsecond=0)

        if not candles or candles[-1]["time"] != bucket:
            if len(candles) >= 2:
                p = candles[-2]
                c = candles[-1]

                closes = [x["close"] for x in candles[:-1]]
                ema5 = ema(closes, EMA_FAST)
                ema20 = ema(closes, EMA_SLOW)

                if ema5 is None or ema20 is None:
                    logger.debug("EMA not ready yet, skipping signal evaluation")
                else:
                    sig = engine.evaluate(p, c, ema5, ema20)
                    if sig:
                        pending_signal = sig
                        logger.info(f"SIGNAL {sig} at {c['time']}")

            candles.append({
                "time": bucket,
                "open": ltp,
                "high": ltp,
                "low": ltp,
                "close": ltp
            })

            if pending_signal:
                tm.enter(ce if pending_signal == "CALL" else pe, ts)
                pending_signal = None

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
