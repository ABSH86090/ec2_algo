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

INDEX_SYMBOL = "NSE:NIFTY50-INDEX"

TRADING_END_TIME = datetime.time(15, 0)
MAX_CPR_LOOKBACK_DAYS = 10

MAX_RANGE_USAGE = 0.30   # 🔴 30% proximity filter

LOG_FILE = "nifty_cpr_multi_scenario_15m.log"

# =========================================================
# LOGGING
# =========================================================
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger()

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
        "R2": p + (h - l),
        "S2": p - (h - l),
    }

def get_last_trading_day_cpr(fyers):
    today = datetime.date.today()
    for i in range(1, MAX_CPR_LOOKBACK_DAYS + 1):
        d = today - datetime.timedelta(days=i)
        r = fyers.history({
            "symbol": INDEX_SYMBOL,
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
                f"[CPR] BC={cpr['BC']} TC={cpr['TC']} "
                f"R1={cpr['R1']} R2={cpr['R2']} "
                f"S1={cpr['S1']} S2={cpr['S2']}"
            )
            return cpr
    raise Exception("CPR not found")

# =========================================================
# OPTION SYMBOL HELPERS
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
    m_map = {10: "O", 11: "N", 12: "D"}
    return f"{yy}{m_map.get(expiry.month, expiry.strftime('%m'))}{expiry.day:02d}"

def get_option_symbols(index_ltp, side):
    atm = round(index_ltp / 50) * 50
    expiry = format_expiry(get_next_expiry())

    if side == "PUT":
        return (
            f"NSE:NIFTY{expiry}{atm - 100}PE",  # BUY OTM2
            f"NSE:NIFTY{expiry}{atm + 50}PE"    # SELL ITM1
        )
    else:
        return (
            f"NSE:NIFTY{expiry}{atm + 100}CE",  # BUY OTM2
            f"NSE:NIFTY{expiry}{atm - 50}CE"    # SELL ITM1
        )

# =========================================================
# BASE SCENARIO
# =========================================================
class CPRScenario:
    def __init__(self, name, fyers, cpr):
        self.name = name
        self.fyers = fyers
        self.cpr = cpr
        self.trade_taken = False
        self.active = False
        self.entry_high = None
        self.legs = {}

    def buy(self, symbol):
        return self.fyers.place_order({
            "symbol": symbol,
            "qty": LOT_SIZE,
            "type": 2,
            "side": 1,
            "productType": "INTRADAY",
            "validity": "DAY",
            "orderTag": self.name + "_BUY"
        })

    def sell(self, symbol):
        return self.fyers.place_order({
            "symbol": symbol,
            "qty": LOT_SIZE,
            "type": 2,
            "side": -1,
            "productType": "INTRADAY",
            "validity": "DAY",
            "orderTag": self.name + "_SELL"
        })

    def enter_spread(self, hedge, sell_leg):
        if self.trade_taken:
            return
        self.buy(hedge)
        time.sleep(0.5)
        self.sell(sell_leg)
        self.trade_taken = True
        self.active = True
        self.legs = {"hedge": hedge, "sell": sell_leg}
        logger.info(f"[{self.name}] ENTRY {hedge} / {sell_leg}")
        send_telegram(f"{self.name} ENTRY\n{hedge}\n{sell_leg}")

    def exit_spread(self):
        if not self.active:
            return
        self.buy(self.legs["sell"])
        time.sleep(0.5)
        self.sell(self.legs["hedge"])
        self.active = False
        logger.info(f"[{self.name}] EXIT")
        send_telegram(f"{self.name} EXIT")

# =========================================================
# SCENARIO 1 – PUT SPREAD
# =========================================================
class Scenario1(CPRScenario):
    def evaluate(self, candles, ltp):
        last = candles[-1]

        if self.trade_taken:
            if last[4] < self.cpr["R1"] or last[2] >= self.cpr["R2"]:
                self.exit_spread()
            return

        first = candles[0]
        if not (first[1] > self.cpr["TC"] and first[4] > self.cpr["TC"]):
            return

        for c in candles:
            if c[4] > self.cpr["R1"]:
                used = c[2] - self.cpr["R1"]
                total = self.cpr["R2"] - self.cpr["R1"]
                if total <= 0 or (used / total) >= MAX_RANGE_USAGE:
                    logger.info("[SCENARIO1] Skipped – too close to R2")
                    return
                hedge, sell = get_option_symbols(ltp, "PUT")
                self.enter_spread(hedge, sell)
                break

# =========================================================
# SCENARIO 2 – CALL SPREAD
# =========================================================
class Scenario2(CPRScenario):
    def evaluate(self, candles, ltp):
        last = candles[-1]

        if self.trade_taken:
            if last[4] > self.cpr["S1"] or last[3] <= self.cpr["S2"]:
                self.exit_spread()
            return

        first = candles[0]
        if not (first[1] < self.cpr["BC"] and first[4] < self.cpr["BC"]):
            return

        for c in candles:
            if c[4] < self.cpr["S1"]:
                used = self.cpr["S1"] - c[3]
                total = self.cpr["S1"] - self.cpr["S2"]
                if total <= 0 or (used / total) >= MAX_RANGE_USAGE:
                    logger.info("[SCENARIO2] Skipped – too close to S2")
                    return
                hedge, sell = get_option_symbols(ltp, "CALL")
                self.enter_spread(hedge, sell)
                break

# =========================================================
# SCENARIO 3 – MEAN REVERSION
# =========================================================
class Scenario3(CPRScenario):
    def evaluate(self, candles, ltp):
        last = candles[-1]

        if self.trade_taken:
            if last[4] > self.entry_high or last[3] <= self.cpr["S1"]:
                self.exit_spread()
            return

        first = candles[0]
        if not (first[1] > self.cpr["TC"] and first[4] > self.cpr["BC"]):
            return

        for c in candles[1:]:
            o, h, l, close = c[1:5]
            if close < o and close < self.cpr["BC"]:
                used = self.cpr["BC"] - l
                total = self.cpr["BC"] - self.cpr["S1"]
                if total <= 0 or (used / total) >= MAX_RANGE_USAGE:
                    logger.info("[SCENARIO3] Skipped – too close to S1")
                    return
                hedge, sell = get_option_symbols(ltp, "CALL")
                self.entry_high = h
                self.enter_spread(hedge, sell)
                break

# =========================================================
# DATA FETCH
# =========================================================
def fetch_15m_index_candles(fyers):
    today = datetime.date.today().strftime("%Y-%m-%d")
    r = fyers.history({
        "symbol": INDEX_SYMBOL,
        "resolution": "15",
        "date_format": "1",
        "range_from": today,
        "range_to": today,
        "cont_flag": "1"
    })
    return r.get("candles", [])

def get_index_ltp(fyers):
    r = fyers.quotes({"symbols": INDEX_SYMBOL})
    return float(r["d"][0]["v"]["lp"])

# =========================================================
# MAIN
# =========================================================
if __name__ == "__main__":
    logger.info("=== NIFTY CPR MULTI-SCENARIO STRATEGY STARTED ===")

    fyers = fyersModel.FyersModel(
        client_id=CLIENT_ID,
        token=ACCESS_TOKEN,
        is_async=False,
        log_path=""
    )

    cpr = get_last_trading_day_cpr(fyers)

    scenarios = [
        Scenario1("SCENARIO1_PUT", fyers, cpr),
        Scenario2("SCENARIO2_CALL", fyers, cpr),
        Scenario3("SCENARIO3_REVERSAL", fyers, cpr),
    ]

    while True:
        now = datetime.datetime.now().time()
        if now >= TRADING_END_TIME:
            for s in scenarios:
                s.exit_spread()
            break

        candles = fetch_15m_index_candles(fyers)
        if len(candles) < 2:
            time.sleep(30)
            continue

        ltp = get_index_ltp(fyers)

        for s in scenarios:
            s.evaluate(candles, ltp)

        time.sleep(60)
