# =========================================================
# SENSEX CPR + EMA → SELL ATM + BUY HEDGE (INDEX SL / TARGET)
# LIVE = BACKTEST ALIGNED (FIXED)
# =========================================================

import datetime
import os
import sys
import logging
import requests
import pandas as pd
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
LOTS = 5
TOTAL_QTY = LOT_SIZE * LOTS
QTY = TOTAL_QTY

INDEX_TARGET = 340
INDEX_SL = 75
HARD_EXIT_TIME = datetime.time(14, 50)

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

    def place_sl_sell(self, symbol, qty, trigger_price, limit, tag):
        return self.client.place_order({
            "symbol": symbol,
            "qty": qty,
            "type": 4,                  # SL-L
            "side": -1,                 # SELL
            "productType": "INTRADAY",
            "validity": "DAY",
            "stopPrice": trigger_price,
            "limitPrice": limit,
            "orderTag": tag
        })

    def cancel_order(self, order_id):
        return self.client.cancel_order({"id": order_id})

    def buy(self, symbol, qty, tag):
        return self.buy_mkt(symbol, qty, tag)
    
    def sell(self, symbol, qty, tag):
        return self.sell_mkt(symbol, qty, tag)



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

def get_resampled_cpr(fyers):
    today = datetime.date.today()
    start = (today - datetime.timedelta(days=7)).strftime("%Y-%m-%d")
    end = (today - datetime.timedelta(days=1)).strftime("%Y-%m-%d")

    resp = fyers.client.history({
        "symbol": INDEX_SYMBOL,
        "resolution": "15",
        "date_format": "1",
        "range_from": start,
        "range_to": end,
        "cont_flag": "1"
    })

    if not resp.get("candles"):
        raise Exception("No intraday candles for CPR")

    df = pd.DataFrame(
        resp["candles"],
        columns=["ts", "open", "high", "low", "close", "volume"]
    )

    df["time"] = (
        pd.to_datetime(df["ts"], unit="s", utc=True)
          .dt.tz_convert("Asia/Kolkata")
          .dt.tz_localize(None)
    )

    df.set_index("time", inplace=True)
    df = df.between_time("09:15", "15:30")

    # Last completed trading day
    daily = df.resample("1D").agg({
        "high": "max",
        "low": "min",
        "close": "last"
    }).dropna()

    prev_day = daily.iloc[-1]

    return compute_cpr({
        "high": prev_day["high"],
        "low": prev_day["low"],
        "close": prev_day["close"]
    })


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
    datetime.date(2026, 1, 15),
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

def get_sensex_option_symbols(fyers):
    q = fyers.client.quotes({"symbols": INDEX_SYMBOL})
    index_ltp = float(q["d"][0]["v"]["lp"])

    atm = round(index_ltp / 100) * 100

    expiry = get_next_expiry()
    exp_token = format_expiry(expiry)

    atm_ce = f"BSE:SENSEX{exp_token}{atm}CE"
    atm_pe = f"BSE:SENSEX{exp_token}{atm}PE"

    hedge_ce = f"BSE:SENSEX{exp_token}{atm + 1000}CE"
    hedge_pe = f"BSE:SENSEX{exp_token}{atm - 1000}PE"

    msg = (
        f"📌 SENSEX OPTION SELECTION\n"
        f"Index LTP : {index_ltp}\n"
        f"ATM       : {atm}\n"
        f"Expiry    : {expiry} ({exp_token})\n"
        f"CALL ATM  : {atm_ce}\n"
        f"PUT  ATM  : {atm_pe}\n"
        f"HEDGE CE : {hedge_ce}\n"
        f"HEDGE PE : {hedge_pe}"
    )

    logger.info(msg)
    send_telegram(msg)

    return atm_ce, atm_pe, hedge_ce, hedge_pe


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

    def enter(self, direction, index_price):
        if self.trade_day == datetime.date.today():
            return

        atm_ce, atm_pe, hedge_ce, hedge_pe = get_sensex_option_symbols(self.fyers)

        if direction == "CALL":
            # Bullish → Sell PE, Buy PE hedge
            hedge = hedge_pe
            main = atm_pe
        else:
            # Bearish → Sell CE, Buy CE hedge
            hedge = hedge_ce
            main = atm_ce

        # 1️⃣ Buy hedge FIRST
        hedge_resp = self.fyers.buy(hedge, QTY, "HEDGE")
        if hedge_resp is None:
            send_telegram("❌ ENTRY ABORTED: Hedge failed")
            return

        # 2️⃣ Sell ATM
        main_resp = self.fyers.sell(main, QTY, "SELL")
        if main_resp is None:
            send_telegram("⚠️ Hedge bought but ATM sell failed")

        self.pos = {
            "main": main,
            "hedge": hedge,
            "entry": index_price,
            "direction": direction
        }

        self.trade_day = datetime.date.today()
        send_telegram(
            f"🚀 ENTRY {direction}\n"
            f"Index={index_price}\n"
            f"SELL={main}\n"
            f"HEDGE={hedge}"
        )

    def on_tick(self, index_ltp, now):
        if not self.pos:
            return

        entry = self.pos["entry"]
        direction = self.pos["direction"]

        if now.time() >= HARD_EXIT_TIME:
            self.exit("TIME", index_ltp)
            return

        if direction == "CALL":
            if index_ltp >= entry + INDEX_TARGET:
                self.exit("TARGET", index_ltp)
            elif index_ltp <= entry - INDEX_SL:
                self.exit("SL", index_ltp)
        else:
            if index_ltp <= entry - INDEX_TARGET:
                self.exit("TARGET", index_ltp)
            elif index_ltp >= entry + INDEX_SL:
                self.exit("SL", index_ltp)

    def exit(self, reason, ltp):
        if not self.pos:
            return

        self.fyers.buy(self.pos["main"], QTY, f"EXIT{reason}")
        self.fyers.sell(self.pos["hedge"], QTY, f"EXIT{reason}")

        send_telegram(
            f"🏁 EXIT {reason}\n"
            f"Index={ltp}"
        )

        self.pos = None



# ================= MAIN =================
if __name__ == "__main__":
    send_telegram("🚀 STRATEGY STARTED")

    fyers = Fyers()

    hist = fyers.client.history({
        "symbol": INDEX_SYMBOL,
        "resolution": "D",
        "date_format": "1",
        "range_from": (datetime.date.today() - datetime.timedelta(days=10)).strftime("%Y-%m-%d"),
        "range_to": (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d"),
        "cont_flag": "1"
    })

    last = hist["candles"][-1]
    cpr = get_resampled_cpr(fyers)

    send_telegram(
        f"CPR (Intraday Derived)\n"
        f"BC={round(cpr['BC'],2)}\n"
        f"TC={round(cpr['TC'],2)}\n"
        f"S1={round(cpr['S1'],2)}\n"
        f"R1={round(cpr['R1'],2)}"
    )

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
        ws.subscribe(symbols=[INDEX_SYMBOL], data_type="SymbolUpdate")
        ws.keep_running()

    ws = data_ws.FyersDataSocket(
        access_token=fyers.auth,
        on_connect=on_open,
        on_message=on_tick,
        log_path=""
    )

    ws.connect()
