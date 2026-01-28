# =========================================================
# SENSEX CPR + EMA → SELL ATM + BUY HEDGE (4 strikes away)
# Entry:  Buy hedge FIRST → Sell ATM
# Exit:   Buy back sold ATM FIRST → Sell hedge
# Hard Exit: 14:59 sharp (same sequence, priority over SL/Target)
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

INDEX_SYMBOL      = "BSE:SENSEX-INDEX"
EMA_FAST          = 5
EMA_SLOW          = 20
LOT_SIZE          = 20
LOTS              = 1
TOTAL_QTY         = LOT_SIZE * LOTS

INDEX_TARGET      = 350
INDEX_SL          = 75
HARD_EXIT_TIME    = datetime.time(14, 59)

LOG_FILE = "sensex_atm_short_hedged.log"

# ================= LOGGING & TELEGRAM =================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler(sys.stdout)],
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
        except:
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
            "symbol": symbol, "qty": qty, "type": 2, "side": 1,
            "productType": "INTRADAY", "validity": "DAY", "orderTag": tag
        })

    def sell_mkt(self, symbol, qty, tag):
        return self.client.place_order({
            "symbol": symbol, "qty": qty, "type": 2, "side": -1,
            "productType": "INTRADAY", "validity": "DAY", "orderTag": tag
        })

# ================= INDICATORS =================
def ema(values, period):
    if len(values) < period: return None
    sma = sum(values[:period]) / period
    e = sma
    k = 2 / (period + 1)
    for v in values[period:]:
        e = v * k + e * (1 - k)
    return e

def compute_cpr(prev):
    h, l, c = prev["high"], prev["low"], prev["close"]
    p = (h + l + c) / 3
    bc = (h + l) / 2
    tc = 2 * p - bc
    return {
        "BC": min(bc, tc),
        "TC": max(bc, tc),
        "R1": 2 * p - l,
        "S1": 2 * p - h
    }

# ================= HISTORICAL WARMUP =================
def prefill_intraday_candles(fyers, candles, days=5):
    start = (datetime.date.today() - datetime.timedelta(days=days)).strftime("%Y-%m-%d")
    end = datetime.date.today().strftime("%Y-%m-%d")
    resp = fyers.client.history({
        "symbol": INDEX_SYMBOL, "resolution": "15", "date_format": "1",
        "range_from": start, "range_to": end, "cont_flag": "1"
    })
    if resp.get("candles"):
        for c in resp["candles"]:
            ts = datetime.datetime.fromtimestamp(c[0]).replace(second=0, microsecond=0)
            candles.append({"time": ts, "open": c[1], "high": c[2], "low": c[3], "close": c[4]})
    closes = [x["close"] for x in candles]
    send_telegram(f"EMA WARMUP | Candles: {len(candles)} | EMA{EMA_FAST}: {ema(closes, EMA_FAST)} | EMA{EMA_SLOW}: {ema(closes, EMA_SLOW)}")

# ================= EXPIRY & SYMBOLS =================
SPECIAL_MARKET_HOLIDAYS = {datetime.date(2026, 1, 26), datetime.date(2026, 1, 15)}

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
    if is_last_thursday(expiry):
        return f"{yy}{expiry.strftime('%b').upper()}"
    m = expiry.month
    d = expiry.day
    m_token = "O" if m == 10 else "N" if m == 11 else "D" if m == 12 else str(m)
    return f"{yy}{m_token}{d:02d}"

def get_atm_and_hedge(fyers, direction):
    q = fyers.client.quotes({"symbols": INDEX_SYMBOL})
    ltp = float(q["d"][0]["v"]["lp"])
    atm = round(ltp / 100) * 100
    exp = get_next_expiry()
    exp_str = format_expiry(exp)

    if direction == "CALL":  # Bullish - short ATM PE + hedge lower PE
        sold = f"BSE:SENSEX{exp_str}{atm}PE"
        hedge = f"BSE:SENSEX{exp_str}{atm - 400}PE"
    else:  # PUT - Bearish - short ATM CE + hedge higher CE
        sold = f"BSE:SENSEX{exp_str}{atm}CE"
        hedge = f"BSE:SENSEX{exp_str}{atm + 500}CE"

    msg = f"OPTIONS ({direction})\nLTP: {ltp:.0f} | ATM: {atm}\nExpiry: {exp} ({exp_str})\nSell: {sold}\nHedge: {hedge}"
    logger.info(msg)
    send_telegram(msg)
    return sold, hedge

# ================= SCENARIO ENGINE =================
class ScenarioEngine:
    def __init__(self, cpr):
        self.cpr = cpr

    def evaluate(self, prev, curr, ema5, ema20):
        if ema5 < ema20:
            if (prev["close"] > self.cpr["BC"] and curr["close"] < self.cpr["BC"] and curr["close"] > self.cpr["S1"]) or \
               (prev["close"] > self.cpr["S1"] and curr["close"] < self.cpr["S1"]):
                return "PUT"
        if ema5 > ema20:
            if prev["close"] < self.cpr["R1"] and curr["close"] > self.cpr["R1"]:
                return "CALL"
        return None

# ================= TRADE MANAGER =================
class TradeManager:
    def __init__(self, fyers):
        self.fyers = fyers
        self.pos = None
        self.trade_day = None

    def enter(self, direction, index_price):
        today = datetime.date.today()
        if self.trade_day == today:
            return

        sold, hedge = get_atm_and_hedge(self.fyers, direction)

        # ENTRY: Hedge first → Sold option
        self.fyers.buy_mkt(hedge, TOTAL_QTY, "HEDGEBUY")
        self.fyers.sell_mkt(sold, TOTAL_QTY, "ATMSHORT")

        self.trade_day = today
        self.pos = {
            "sold": sold,
            "hedge": hedge,
            "entry_index": index_price,
            "direction": direction
        }

        send_telegram(f"ENTRY {direction}\nBuy Hedge: {hedge}\nShort: {sold}\nIndex = {index_price:.0f}")

    def on_tick(self, index_ltp, now):
        if not self.pos:
            return

        # Hard exit has highest priority
        if now.time() >= HARD_EXIT_TIME:
            self.exit("HARDEXIT")
            return

        entry = self.pos["entry_index"]
        is_bullish = self.pos["direction"] == "CALL"

        if is_bullish:
            # Bullish position (short PUT) → profit when index rises
            if index_ltp >= entry + INDEX_TARGET:
                self.exit("TARGET")
            elif index_ltp <= entry - INDEX_SL:
                self.exit("SL")
        else:
            # Bearish position (short CALL) → profit when index falls
            if index_ltp <= entry - INDEX_TARGET:
                self.exit("TARGET")
            elif index_ltp >= entry + INDEX_SL:
                self.exit("SL")

    def exit(self, reason):
        if not self.pos:
            return

        sold = self.pos["sold"]
        hedge = self.pos["hedge"]

        # EXIT SEQUENCE: Buy back the sold (short) position first
        self.fyers.buy_mkt(sold, TOTAL_QTY, f"{reason}CLOSE")
        # Then close the hedge
        self.fyers.sell_mkt(hedge, TOTAL_QTY, f"{reason}HEDGE")

        send_telegram(f"EXIT [{reason}]\nBuyback {sold} first → then close {hedge}")
        self.pos = None

# ================= MAIN =================
if __name__ == "__main__":
    send_telegram("STRATEGY STARTED - ATM Short + Hedge")
    fyers = Fyers()

    hist = fyers.client.history({
        "symbol": INDEX_SYMBOL, "resolution": "D", "date_format": "1",
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

        ts = datetime.datetime.fromtimestamp(
            msg.get("last_traded_time", msg.get("timestamp", datetime.datetime.now().timestamp()))
        )
        index_ltp = msg["ltp"]

        tm.on_tick(index_ltp, ts)

        bucket = ts.replace(minute=(ts.minute // 15) * 15, second=0, microsecond=0)

        if not candles or candles[-1]["time"] != bucket:
            if len(candles) >= 2:
                p, c = candles[-2], candles[-1]
                if p["time"].date() == c["time"].date():
                    closes = [x["close"] for x in list(candles)[:-1]]
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
                tm.enter(pending_signal, index_ltp)
                pending_signal = None
                signal_candle_time = None
        else:
            c = candles[-1]
            c["high"] = max(c["high"], index_ltp)
            c["low"] = min(c["low"], index_ltp)
            c["close"] = index_ltp

    def on_open():
        ws.subscribe([INDEX_SYMBOL], "SymbolUpdate")
        ws.keep_running()

    ws = data_ws.FyersDataSocket(
        access_token=fyers.auth,
        on_connect=on_open,
        on_message=on_tick,
        log_path=""
    )
    ws.connect()
