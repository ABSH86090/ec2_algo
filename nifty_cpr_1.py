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

LOG_FILE = "nifty_scenario_s1_r1_15m.log"
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
            logger.info(f"[CPR INIT] {symbol} {cpr}")
            return cpr
    return None

# =========================================================
# STRATEGY
# =========================================================
class ScenarioS1R1_15M:
    def __init__(self, fyers):
        self.fyers = fyers
        self.cpr = {}
        self.trade_taken = set()
        self.positions = {}
        self.start_logged = set()

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
        if len(candles) < 1:
            return

        cpr = self.cpr.get(symbol)
        if not cpr:
            return

        s1, s2, r1, r2, tc = (
            cpr["S1"], cpr["S2"], cpr["R1"], cpr["R2"], cpr["TC"]
        )

        # =====================================================
        # SCENARIO 1 – S1 FAILURE
        # =====================================================
        first_close = candles[0][4]
        if first_close > s1:
            green_idx = None
            for i, cndl in enumerate(candles):
                o, h, l, c = cndl[1:5]
                if c > o and l <= s1:
                    green_idx = i
                    break

            if green_idx is not None:
                for j in range(green_idx + 1, len(candles)):
                    o, h, l, c = candles[j][1:5]
                    if c < o and c < s1:
                        sl, target = self.calc_sl_target(c, h, s2=s2)
                        self.enter_trade(symbol, c, sl, target, "S1FAIL")
                        return

        # =====================================================
        # SCENARIO 2 – R1 REJECTION
        # =====================================================
        touched_r2 = any(cndl[2] >= r2 for cndl in candles)
        if touched_r2:
            return

        for cndl in candles:
            o, h, l, c = cndl[1:5]
            if h >= r1 and c < r1 and c < o:
                sl, target = self.calc_sl_target(c, h, tc=tc)
                self.enter_trade(symbol, c, sl, target, "R1REJ")
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
    logger.info("[START] Scenario S1 + R1 15m started")

    fyers = fyersModel.FyersModel(
        client_id=CLIENT_ID,
        token=ACCESS_TOKEN,
        is_async=False,
        log_path=""
    )

    strategy = ScenarioS1R1_15M(fyers)

    symbols = []  # reuse your existing ATM symbol logic
    for s in symbols:
        strategy.cpr[s] = get_last_trading_day_cpr(fyers, s)

    while True:
        now = datetime.datetime.now().time()

        for s in symbols:
            strategy.evaluate(s)

        if now >= TRADING_END_TIME:
            strategy.force_exit()
            break

        time.sleep(60)
