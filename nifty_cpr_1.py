import datetime
import logging
import os
import time
import re
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

LOT_SIZE = 150
TICK_SIZE = 0.05
S1_BUFFER = 0.5

LOG_FILE = "nifty_scenario2_15m.log"
MAX_CPR_LOOKBACK_DAYS = 10

TRADING_END_TIME = datetime.time(15, 0)

# =========================================================
# LOGGING
# =========================================================
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

def send_telegram(text):
    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        try:
            requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={"chat_id": TELEGRAM_CHAT_ID, "text": text[:4000]},
                timeout=3
            )
        except Exception:
            pass

class TelegramHandler(logging.Handler):
    def emit(self, record):
        send_telegram(self.format(record))

logger.addHandler(TelegramHandler())

# =========================================================
# UTILS
# =========================================================
def round_to_tick(price):
    return round(round(price / TICK_SIZE) * TICK_SIZE, 2)

# =========================================================
# CPR
# =========================================================
def calculate_cpr(h, l, c):
    p = (h + l + c) / 3
    bc = (h + l) / 2
    tc = 2 * p - bc
    bc, tc = min(bc, tc), max(bc, tc)
    return {
        "P": p,
        "BC": bc,
        "TC": tc,
        "R1": 2 * p - l,
        "S1": 2 * p - h,
        "S2": p - (h - l),
    }

def get_last_trading_day_candle(fyers, symbol):
    today = datetime.date.today()
    for i in range(1, MAX_CPR_LOOKBACK_DAYS + 1):
        d = today - datetime.timedelta(days=i)
        try:
            r = fyers.history({
                "symbol": symbol,
                "resolution": "D",
                "date_format": "1",
                "range_from": d.strftime("%Y-%m-%d"),
                "range_to": d.strftime("%Y-%m-%d"),
                "cont_flag": "1"
            })
            candles = r.get("candles", [])
            if candles:
                _, _, h, l, c, _ = candles[0]
                return calculate_cpr(h, l, c)
        except Exception:
            continue
    return None

# =========================================================
# NIFTY ATM SYMBOL (ORIGINAL LOGIC)
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
    resp = fyers.quotes({"symbols": "NSE:NIFTY50-INDEX"})
    ltp = float(resp["d"][0]["v"]["lp"])
    atm = round(ltp / 50) * 50
    expiry = format_nifty_expiry(get_next_nifty_expiry())
    return [
        f"NSE:NIFTY{expiry}{atm}CE",
        f"NSE:NIFTY{expiry}{atm}PE"
    ]

# =========================================================
# STRATEGY
# =========================================================
class Scenario2_15M:
    def __init__(self, fyers):
        self.fyers = fyers
        self.cpr = {}
        self.trade_taken = set()
        self.positions = {}

    def fetch_15m_candles(self, symbol):
        today = datetime.date.today().strftime("%Y-%m-%d")
        r = self.fyers.history({
            "symbol": symbol,
            "resolution": "15",
            "date_format": "1",
            "range_from": today,
            "range_to": today,
            "cont_flag": "1"
        })
        return r.get("candles", [])

    def evaluate(self, symbol):
        if symbol in self.trade_taken:
            return

        candles = self.fetch_15m_candles(symbol)
        if len(candles) < 3:
            return

        cpr = self.cpr[symbol]
        bc, s1, s2 = cpr["BC"], cpr["S1"], cpr["S2"]

        # ---- First candle ----
        first_close = candles[0][4]
        if not (bc < first_close < s1):
            return

        # ---- Green candle above S1 ----
        green_idx = None
        for i in range(1, len(candles)):
            o, h, l, c = candles[i][1:5]
            if c > o and c > s1 and l <= s1:
                if all(candles[j][4] > s1 for j in range(1, i)):
                    green_idx = i
                    break

        if green_idx is None:
            return

        # ---- First red candle below S1 ----
        for j in range(green_idx + 1, len(candles)):
            o, h, l, c = candles[j][1:5]
            if c < o and c <= s1 - S1_BUFFER:
                self.enter_trade(symbol, c, h, s2)
                break

    def enter_trade(self, symbol, entry, sl, target):
        self.fyers.place_order({
            "symbol": symbol,
            "qty": LOT_SIZE,
            "type": 2,
            "side": -1,
            "productType": "INTRADAY",
            "validity": "DAY",
            "orderTag": "S2ENTRY"
        })

        sl = round_to_tick(sl)
        sl_order = self.fyers.place_order({
            "symbol": symbol,
            "qty": LOT_SIZE,
            "type": 4,
            "side": 1,
            "stopPrice": sl,
            "limitPrice": sl + TICK_SIZE,
            "productType": "INTRADAY",
            "validity": "DAY",
            "orderTag": "S2SL"
        })

        self.positions[symbol] = sl_order.get("id")
        self.trade_taken.add(symbol)

        logger.info(
            f"[ENTRY] {symbol} SELL entry={entry} SL={sl} TARGET={target}"
        )

    def force_exit(self):
        for symbol, sl_id in list(self.positions.items()):
            self.fyers.place_order({
                "symbol": symbol,
                "qty": LOT_SIZE,
                "type": 2,
                "side": 1,
                "productType": "INTRADAY",
                "validity": "DAY",
                "orderTag": "TIMEEXIT"
            })
            self.fyers.cancel_order({"id": sl_id})
            logger.info(f"[TIME EXIT] {symbol} exited at 15:00")
            del self.positions[symbol]

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

    strategy = Scenario2_15M(fyers)
    symbols = get_atm_nifty_symbols(fyers)

    for s in symbols:
        strategy.cpr[s] = get_last_trading_day_candle(fyers, s)

    while True:
        now = datetime.datetime.now().time()

        for s in symbols:
            strategy.evaluate(s)

        if now >= TRADING_END_TIME:
            strategy.force_exit()
            break

        time.sleep(60)
