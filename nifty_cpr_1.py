import datetime
import logging
import os
import time
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

LOG_FILE = "nifty_s1_r1_15m.log"
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
logger.setLevel(logging.INFO)

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
        "R2": p + (h - l),
        "S1": 2 * p - h,
        "S2": p - (h - l),
    }

def get_last_trading_day_cpr(fyers, symbol):
    today = datetime.date.today()
    for i in range(1, MAX_CPR_LOOKBACK_DAYS + 1):
        d = today - datetime.timedelta(days=i)
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
            cpr = calculate_cpr(h, l, c)
            logger.info(
                f"[CPR INIT] {symbol} | "
                f"BC={round(cpr['BC'],2)} "
                f"TC={round(cpr['TC'],2)} "
                f"R1={round(cpr['R1'],2)} "
                f"R2={round(cpr['R2'],2)} "
                f"S1={round(cpr['S1'],2)} "
                f"S2={round(cpr['S2'],2)}"
            )
            return cpr
    logger.error(f"[CPR FAIL] {symbol}")
    return None

# =========================================================
# ATM NIFTY SYMBOL LOGIC
# =========================================================
def is_last_tuesday(d):
    return d.weekday() == 1 and (d + datetime.timedelta(days=7)).month != d.month

def get_next_nifty_expiry():
    today = datetime.date.today()
    return today if today.weekday() == 1 else today + datetime.timedelta(days=(1 - today.weekday()) % 7)

def format_nifty_expiry(expiry):
    yy = expiry.strftime("%y")
    if is_last_tuesday(expiry):
        return f"{yy}{expiry.strftime('%b').upper()}"
    m = {10: "O", 11: "N", 12: "D"}.get(expiry.month, f"{expiry.month:02d}")
    return f"{yy}{m}{expiry.day:02d}"

def get_atm_nifty_symbols(fyers):
    resp = fyers.quotes({"symbols": "NSE:NIFTY50-INDEX"})
    ltp = float(resp["d"][0]["v"]["lp"])
    atm = round(ltp / 50) * 50
    expiry = format_nifty_expiry(get_next_nifty_expiry())

    symbols = [
        f"NSE:NIFTY{expiry}{atm}CE",
        f"NSE:NIFTY{expiry}{atm}PE"
    ]

    logger.info(f"[SYMBOLS] ATM symbols selected: {symbols}")
    return symbols

# =========================================================
# STRATEGY
# =========================================================
class ScenarioS1R1_15M:
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

    def calc_sl_target(self, entry, high, s2=None, tc=None):
        sl = min(high + 1, entry + 16)
        sl = round_to_tick(sl)
        risk = sl - entry

        if s2 is not None:
            target = min(s2, entry - 2 * risk)
        else:
            target = tc

        return sl, round_to_tick(target)

    def evaluate(self, symbol):
        if symbol in self.trade_taken:
            return

        candles = self.fetch_15m_candles(symbol)
        if not candles:
            return

        cpr = self.cpr.get(symbol)
        if not cpr:
            return

        s1, s2, r1, r2, tc = cpr["S1"], cpr["S2"], cpr["R1"], cpr["R2"], cpr["TC"]

        # =========================
        # SCENARIO 1: S1 FAILURE
        # =========================
        if candles[0][4] > s1:
            green_idx = None
            for i, cd in enumerate(candles):
                o, h, l, c = cd[1:5]
                if c > o and c > s1 and l <= s1:
                    green_idx = i
                    break

            if green_idx is not None:
                for cd in candles[green_idx + 1:]:
                    o, h, l, c = cd[1:5]
                    if c < o and h >= s1 and s2 < c < s1:
                        sl, tgt = self.calc_sl_target(c, h, s2=s2)
                        self.enter_trade(symbol, c, sl, tgt, "S1FAIL")
                        return

        # =========================
        # SCENARIO 2: R1 REJECTION
        # =========================
        if any(cd[2] >= r2 for cd in candles):
            return

        for cd in candles:
            o, h, l, c = cd[1:5]
            if h >= r1 and c < r1 and c < o:
                sl, tgt = self.calc_sl_target(c, h, tc=tc)
                self.enter_trade(symbol, c, sl, tgt, "R1REJ")
                return

    def enter_trade(self, symbol, entry, sl, target, tag):
        logger.info(
            f"[ENTRY {tag}] {symbol} ENTRY={entry} SL={sl} TARGET={target}"
        )

        self.fyers.place_order({
            "symbol": symbol,
            "qty": LOT_SIZE,
            "type": 2,
            "side": -1,
            "productType": "INTRADAY",
            "validity": "DAY",
            "orderTag": tag
        })

        sl_id = self.fyers.place_order({
            "symbol": symbol,
            "qty": LOT_SIZE,
            "type": 4,
            "side": 1,
            "stopPrice": sl,
            "limitPrice": sl + TICK_SIZE,
            "productType": "INTRADAY",
            "validity": "DAY",
            "orderTag": f"{tag}_SL"
        }).get("id")

        self.positions[symbol] = sl_id
        self.trade_taken.add(symbol)

    def force_exit(self):
        for symbol, sl_id in list(self.positions.items()):
            logger.info(f"[TIME EXIT] {symbol}")
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
            del self.positions[symbol]

# =========================================================
# MAIN
# =========================================================
if __name__ == "__main__":
    logger.info("[STARTUP] S1 + R1 15m strategy started")

    fyers = fyersModel.FyersModel(
        client_id=CLIENT_ID,
        token=ACCESS_TOKEN,
        is_async=False,
        log_path=""
    )

    strategy = ScenarioS1R1_15M(fyers)

    symbols = get_atm_nifty_symbols(fyers)

    for s in symbols:
        strategy.cpr[s] = get_last_trading_day_cpr(fyers, s)

    while True:
        now = datetime.datetime.now().time()

        if now >= TRADING_END_TIME:
            strategy.force_exit()
            break

        for s in symbols:
            strategy.evaluate(s)

        

        time.sleep(60)
