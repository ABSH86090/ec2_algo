# =========================================================
# OPTION CPR 15M STRATEGY (ATM | WEEKLY | HEDGED)
# =========================================================

import datetime
import time
import os
import logging
import requests
from dotenv import load_dotenv
from fyers_apiv3 import fyersModel

# =========================================================
# CONFIG
# =========================================================
load_dotenv()

CLIENT_ID = os.getenv("FYERS_CLIENT_ID")
ACCESS_TOKEN = os.getenv("FYERS_ACCESS_TOKEN")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

LOT_SIZE = 65
TRADING_END = datetime.time(15, 0)

# =========================================================
# LOGGING
# =========================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

def send_telegram(msg):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": msg[:4000]},
            timeout=3
        )
    except Exception:
        pass

# =========================================================
# CPR
# =========================================================
def calculate_cpr(high, low, close):
    p = (high + low + close) / 3
    return {
        "P": p,
        "S1": 2 * p - high,
        "S2": p - (high - low)
    }

# =========================================================
# SYMBOL HELPERS (UNCHANGED)
# =========================================================
def is_last_tuesday(d):
    return d.weekday() == 1 and (d + datetime.timedelta(days=7)).month != d.month

def get_next_expiry():
    today = datetime.date.today()
    return today if today.weekday() == 1 else today + datetime.timedelta(days=(1 - today.weekday()) % 7)

def format_expiry(expiry):
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

def get_symbols(fyers):
    q = fyers.quotes({"symbols": "NSE:NIFTY50-INDEX"})
    spot = float(q["d"][0]["v"]["lp"])
    atm = round(spot / 50) * 50
    expiry = format_expiry(get_next_expiry())

    return {
        "CE": f"NSE:NIFTY{expiry}{atm}CE",
        "PE": f"NSE:NIFTY{expiry}{atm}PE",
        "HEDGE_CE": f"NSE:NIFTY{expiry}{atm + 300}CE",
        "HEDGE_PE": f"NSE:NIFTY{expiry}{atm - 300}PE",
        "ATM": atm,
        "EXPIRY": expiry
    }

# =========================================================
# STRATEGY CLASS
# =========================================================
class OptionCPR15M:
    def __init__(self, fyers, symbol, hedge):
        self.fyers = fyers
        self.symbol = symbol
        self.hedge = hedge

        self.trade_taken = False
        self.entry = None
        self.sl = None
        self.target = None
        self.last_candle_ts = None

        self.cpr, self.prev_low = self.compute_prevday_cpr()

        send_telegram(
            f"📌 CPR READY\n{self.symbol}\n"
            f"S1={self.cpr['S1']:.2f} | S2={self.cpr['S2']:.2f}\n"
            f"Prev Low={self.prev_low:.2f}"
        )

    # -----------------------------------------------------
    def compute_prevday_cpr(self):
        today = datetime.date.today()
        for i in range(1, 8):
            day = today - datetime.timedelta(days=i)
            r = self.fyers.history({
                "symbol": self.symbol,
                "resolution": "15",
                "date_format": "1",
                "range_from": day.strftime("%Y-%m-%d"),
                "range_to": day.strftime("%Y-%m-%d"),
                "cont_flag": "1"
            })
            candles = r.get("candles", [])
            if candles:
                high = max(c[2] for c in candles)
                low = min(c[3] for c in candles)
                close = candles[-1][4]
                return calculate_cpr(high, low, close), low
        raise Exception(f"No CPR data for {self.symbol}")

    # -----------------------------------------------------
    def latest_15m(self):
        today = datetime.date.today().strftime("%Y-%m-%d")
        r = self.fyers.history({
            "symbol": self.symbol,
            "resolution": "15",
            "date_format": "1",
            "range_from": today,
            "range_to": today,
            "cont_flag": "1"
        })
        candles = r.get("candles", [])
        return candles[-2] if len(candles) >= 2 else None

    # -----------------------------------------------------
    def evaluate(self):
        if datetime.datetime.now().time() < datetime.time(9, 30):
            return

        candle = self.latest_15m()
        if not candle:
            return

        ts, o, h, l, close = candle[0], candle[1], candle[2], candle[3], candle[4]

        # ---- prevent duplicate telegram per candle ----
        if self.last_candle_ts == ts:
            return
        self.last_candle_ts = ts

        status = "TAKEN" if self.trade_taken else "WAITING"

        send_telegram(
            f"📊 15M CANDLE\n{self.symbol}\n"
            f"O={o:.2f} H={h:.2f} L={l:.2f} C={close:.2f}\n"
            f"S1={self.cpr['S1']:.2f} | S2={self.cpr['S2']:.2f}\n"
            f"PrevLow={self.prev_low:.2f}\n"
            f"Status={status}"
        )

        if self.trade_taken:
            return

        if not (self.cpr["S2"] < close < self.cpr["S1"]):
            return

        if self.prev_low <= self.cpr["S1"]:
            return

        # ---------- ENTRY ----------
        self.entry = close
        self.sl = h
        rr_target = close - 2 * (h - close)
        self.target = max(rr_target, self.cpr["S2"])

        send_telegram(
            f"📉 ENTRY\n{self.symbol}\n"
            f"Entry={self.entry:.2f}\nSL={self.sl:.2f}\nTarget={self.target:.2f}"
        )

        self.fyers.place_order({
            "symbol": self.hedge,
            "qty": LOT_SIZE,
            "type": 2,
            "side": 1,
            "productType": "INTRADAY",
            "validity": "DAY",
            "orderTag": "HEDGE"
        })

        self.fyers.place_order({
            "symbol": self.symbol,
            "qty": LOT_SIZE,
            "type": 2,
            "side": -1,
            "productType": "INTRADAY",
            "validity": "DAY",
            "orderTag": "SELL"
        })

        self.trade_taken = True

    # -----------------------------------------------------
    def check_exit(self):
        if not self.trade_taken:
            return

        q = self.fyers.quotes({"symbols": self.symbol})
        ltp = q["d"][0]["v"]["lp"]

        if ltp >= self.sl:
            reason = "SL"
        elif ltp <= self.target:
            reason = "TARGET"
        else:
            return

        send_telegram(f"🛑 EXIT {reason}\n{self.symbol}\n@ {ltp:.2f}")

        self.fyers.place_order({
            "symbol": self.symbol,
            "qty": LOT_SIZE,
            "type": 2,
            "side": 1,
            "productType": "INTRADAY",
            "validity": "DAY",
            "orderTag": "EXIT"
        })

        self.fyers.place_order({
            "symbol": self.hedge,
            "qty": LOT_SIZE,
            "type": 2,
            "side": -1,
            "productType": "INTRADAY",
            "validity": "DAY",
            "orderTag": "EXIT_HEDGE"
        })

        self.trade_taken = True

# =========================================================
# MAIN
# =========================================================
if __name__ == "__main__":
    fyers = fyersModel.FyersModel(
        client_id=CLIENT_ID,
        token=ACCESS_TOKEN,
        is_async=False,
        log_path=""
    )

    symbols = get_symbols(fyers)

    ce_bot = OptionCPR15M(fyers, symbols["CE"], symbols["HEDGE_CE"])
    pe_bot = OptionCPR15M(fyers, symbols["PE"], symbols["HEDGE_PE"])

    while datetime.datetime.now().time() < TRADING_END:
        ce_bot.evaluate()
        pe_bot.evaluate()

        ce_bot.check_exit()
        pe_bot.check_exit()

        time.sleep(5)
