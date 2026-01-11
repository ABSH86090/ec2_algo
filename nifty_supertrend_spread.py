# =========================================================
# NIFTY CPR + SUPERTREND + EMA
# OPTION SELLING WITH HEDGE (INDEX SL / TARGET)
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

INDEX_SYMBOL = "NSE:NIFTY50-INDEX"

TIMEFRAME_MIN = 15
EMA_FAST = 5
EMA_SLOW = 20

ATR_PERIOD = 10
MULTIPLIER = 3.5

LOT_SIZE = 65
LOTS = 1
QTY = LOT_SIZE * LOTS

INDEX_SL = 50
INDEX_TARGET = 100
HARD_EXIT_TIME = datetime.time(14, 45)

LOG_FILE = "nifty_option_selling.log"

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

    def buy(self, symbol, qty, tag):
        logger.info(f"BUY {symbol}")
        return self.client.place_order({
            "symbol": symbol,
            "qty": qty,
            "type": 2,
            "side": 1,
            "productType": "INTRADAY",
            "validity": "DAY",
            "orderTag": tag
        })

    def sell(self, symbol, qty, tag):
        logger.info(f"SELL {symbol}")
        return self.client.place_order({
            "symbol": symbol,
            "qty": qty,
            "type": 2,
            "side": -1,
            "productType": "INTRADAY",
            "validity": "DAY",
            "orderTag": tag
        })

# ================= INDICATORS =================
def ema(values, period):
    if len(values) < period:
        return None
    k = 2 / (period + 1)
    e = sum(values[:period]) / period
    for v in values[period:]:
        e = v * k + e * (1 - k)
    return e


def atr(candles, period):
    tr = []
    for i in range(1, len(candles)):
        h, l = candles[i]["high"], candles[i]["low"]
        pc = candles[i - 1]["close"]
        tr.append(max(h - l, abs(h - pc), abs(l - pc)))

    atr_vals = [None] * period
    atr_val = sum(tr[:period]) / period
    atr_vals.append(atr_val)

    for t in tr[period:]:
        atr_val = (atr_val * (period - 1) + t) / period
        atr_vals.append(atr_val)

    return atr_vals


def supertrend(candles, period, multiplier):
    atr_vals = atr(candles, period)
    st = [None] * len(candles)
    final_ub = [None] * len(candles)
    final_lb = [None] * len(candles)
    trend = [None] * len(candles)

    for i in range(len(candles)):
        if atr_vals[i] is None:
            continue

        high, low, close = candles[i]["high"], candles[i]["low"], candles[i]["close"]
        hl2 = (high + low) / 2

        basic_ub = hl2 + multiplier * atr_vals[i]
        basic_lb = hl2 - multiplier * atr_vals[i]

        if i == 0 or final_ub[i - 1] is None:
            final_ub[i] = basic_ub
            final_lb[i] = basic_lb
            trend[i] = "BULLISH" if close > basic_ub else "BEARISH"
        else:
            final_ub[i] = basic_ub if (basic_ub < final_ub[i - 1] or close > final_ub[i - 1]) else final_ub[i - 1]
            final_lb[i] = basic_lb if (basic_lb > final_lb[i - 1] or close < final_lb[i - 1]) else final_lb[i - 1]

            if trend[i - 1] == "BULLISH" and close < final_lb[i - 1]:
                trend[i] = "BEARISH"
            elif trend[i - 1] == "BEARISH" and close > final_ub[i - 1]:
                trend[i] = "BULLISH"
            else:
                trend[i] = trend[i - 1]

        st[i] = final_lb[i] if trend[i] == "BULLISH" else final_ub[i]

    return st


def compute_cpr(prev_day):
    h = max(c["high"] for c in prev_day)
    l = min(c["low"] for c in prev_day)
    c = prev_day[-1]["close"]
    pivot = (h + l + c) / 3
    bc = (h + l) / 2
    tc = 2 * pivot - bc
    return min(bc, tc), max(bc, tc)

# ================= EXPIRY HELPERS (TUESDAY) =================
SPECIAL_MARKET_HOLIDAYS = {
    datetime.date(2025, 10, 2),
    datetime.date(2025, 12, 25),
}

def is_last_tuesday(d):
    return d.weekday() == 1 and (d + datetime.timedelta(days=7)).month != d.month


def get_next_tuesday_expiry():
    today = datetime.date.today()
    days_to_tue = (1 - today.weekday()) % 7
    expiry = today + datetime.timedelta(days=days_to_tue)

    if today.weekday() == 1 and datetime.datetime.now().time() >= datetime.time(15, 30):
        expiry += datetime.timedelta(days=7)

    if expiry in SPECIAL_MARKET_HOLIDAYS:
        expiry -= datetime.timedelta(days=1)

    return expiry


def format_nifty_expiry(expiry):
    yy = expiry.strftime("%y")

    if is_last_tuesday(expiry):
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
def get_nifty_option_symbols(fyers):
    q = fyers.client.quotes({"symbols": INDEX_SYMBOL})
    ltp = float(q["d"][0]["v"]["lp"])
    atm = round(ltp / 50) * 50

    expiry = get_next_tuesday_expiry()
    exp = format_nifty_expiry(expiry)

    atm_ce = f"NSE:NIFTY{exp}{atm}CE"
    atm_pe = f"NSE:NIFTY{exp}{atm}PE"
    hedge_ce = f"NSE:NIFTY{exp}{atm + 200}CE"
    hedge_pe = f"NSE:NIFTY{exp}{atm - 200}PE"

    logger.info(f"[NIFTY SYMBOLS] ATM={atm} EXP={expiry}")
    return atm_ce, atm_pe, hedge_ce, hedge_pe, atm

# ================= TRADE MANAGER =================
class TradeManager:
    def __init__(self, fyers):
        self.fyers = fyers
        self.pos = None
        self.trade_day = None

    def enter(self, bias, index_price):
        if self.trade_day == datetime.date.today():
            return

        atm_ce, atm_pe, hedge_ce, hedge_pe, atm = get_nifty_option_symbols(self.fyers)

        if bias == "BEARISH":
            self.fyers.buy(hedge_ce, QTY, "HEDGE_BUY")
            self.fyers.sell(atm_ce, QTY, "SELL_CALL")
            self.pos = {"main": atm_ce, "hedge": hedge_ce, "entry": index_price}
        else:
            self.fyers.buy(hedge_pe, QTY, "HEDGE_BUY")
            self.fyers.sell(atm_pe, QTY, "SELL_PUT")
            self.pos = {"main": atm_pe, "hedge": hedge_pe, "entry": index_price}

        self.trade_day = datetime.date.today()
        send_telegram(f"🚀 ENTRY {bias} @ {index_price}")

    def on_tick(self, ltp, now):
        if not self.pos:
            return

        entry = self.pos["entry"]

        if now.time() >= HARD_EXIT_TIME:
            self.exit("TIME EXIT")
            return

        if ltp >= entry + INDEX_SL:
            self.exit("SL")
        elif ltp <= entry - INDEX_TARGET:
            self.exit("TARGET")

    def exit(self, reason):
        self.fyers.buy(self.pos["main"], QTY, "EXIT_MAIN")
        self.fyers.sell(self.pos["hedge"], QTY, "EXIT_HEDGE")
        send_telegram(f"🏁 EXIT {reason}")
        self.pos = None

# ================= WARMUP =================
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

    for c in resp.get("candles", []):
        ts = datetime.datetime.fromtimestamp(c[0])
        candles.append({
            "time": ts.replace(second=0, microsecond=0),
            "open": c[1],
            "high": c[2],
            "low": c[3],
            "close": c[4]
        })

# ================= MAIN =================
if __name__ == "__main__":
    logger.info("🚀 NIFTY OPTION SELLING STRATEGY STARTED")
    send_telegram("🚀 NIFTY OPTION SELLING STRATEGY STARTED")

    fyers = Fyers()
    tm = TradeManager(fyers)

    # ---- CPR from previous day ----
    hist = fyers.client.history({
        "symbol": INDEX_SYMBOL,
        "resolution": "D",
        "date_format": "1",
        "range_from": (datetime.date.today() - datetime.timedelta(days=7)).strftime("%Y-%m-%d"),
        "range_to": (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d"),
        "cont_flag": "1"
    })

    last = hist["candles"][-1]
    prev_day = [{"high": last[2], "low": last[3], "close": last[4]}]
    BC, TC = compute_cpr(prev_day)

    candles = deque(maxlen=300)

    # 🔥 WARMUP
    prefill_intraday_candles(fyers, candles)

    # 🔍 WARMUP CHECK (AS REQUESTED)
    warmup_msg = (
        f"WARMUP CHECK | "
        f"Candles={len(candles)} | "
        f"EMA5={ema([c['close'] for c in candles], EMA_FAST)} | "
        f"EMA20={ema([c['close'] for c in candles], EMA_SLOW)} | "
        f"BC={BC:.2f} | "
        f"TC={TC:.2f} | "
        f"ST={supertrend(list(candles), ATR_PERIOD, MULTIPLIER)[-1]}"
    )

    logger.info(warmup_msg)
    send_telegram(f"📊 {warmup_msg}")

    pending_signal = None

    def on_tick(msg):
        global pending_signal

        if msg.get("symbol") != INDEX_SYMBOL:
            return

        ts = datetime.datetime.fromtimestamp(
            msg.get("last_traded_time", datetime.datetime.now().timestamp())
        )
        ltp = msg["ltp"]

        tm.on_tick(ltp, ts)

        bucket = ts.replace(minute=(ts.minute // 15) * 15, second=0, microsecond=0)

        if candles and candles[-1]["time"] == bucket:
            c = candles[-1]
            c["high"] = max(c["high"], ltp)
            c["low"] = min(c["low"], ltp)
            c["close"] = ltp
            return

        candles.append({
            "time": bucket,
            "open": ltp,
            "high": ltp,
            "low": ltp,
            "close": ltp
        })

        if len(candles) < 30:
            return

        closes = [x["close"] for x in candles]
        ema5 = ema(closes, EMA_FAST)
        ema20 = ema(closes, EMA_SLOW)
        st = supertrend(list(candles), ATR_PERIOD, MULTIPLIER)

        trend = "BULLISH" if candles[-1]["close"] > st[-1] else "BEARISH"

        if trend == "BEARISH" and ema5 < ema20 and candles[-1]["close"] < BC:
            pending_signal = "BEARISH"

        if trend == "BULLISH" and ema5 > ema20 and candles[-1]["close"] > TC:
            pending_signal = "BULLISH"

        if pending_signal:
            tm.enter(pending_signal, ltp)
            pending_signal = None

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
