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

LOT_SIZE = 150
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
# CPR CALCULATION (STANDARD)
# =========================================================
def calculate_cpr(high, low, close):
    p = (high + low + close) / 3
    bc = (high + low) / 2
    tc = 2 * p - bc
    return {
        "P": p,
        "BC": min(bc, tc),
        "TC": max(bc, tc),
        "S1": 2 * p - high,
        "S2": p - (high - low),
        "S3": low - 2 * (high - p)
    }

# =========================================================
# SYMBOL HELPERS
# =========================================================
def get_next_expiry():
    today = datetime.date.today()
    offset = (1 - today.weekday()) % 7
    return today + datetime.timedelta(days=offset)

def format_expiry(d):
    yy = d.strftime("%y")
    m = {10: "O", 11: "N", 12: "D"}.get(d.month, f"{d.month:02d}")
    return f"{yy}{m}{d.day:02d}"

def get_symbols(fyers):
    q = fyers.quotes({"symbols": "NSE:NIFTY50-INDEX"})
    spot = float(q["d"][0]["v"]["lp"])
    atm = round(spot / 50) * 50
    expiry = format_expiry(get_next_expiry())

    ce = atm + 100
    pe = atm - 100

    return {
        "SELL_CE": f"NSE:NIFTY{expiry}{ce}CE",
        "SELL_PE": f"NSE:NIFTY{expiry}{pe}PE",
        "HEDGE_CE": f"NSE:NIFTY{expiry}{ce + 300}CE",
        "HEDGE_PE": f"NSE:NIFTY{expiry}{pe - 300}PE",
        "CE_STRIKE": ce,
        "PE_STRIKE": pe,
        "EXPIRY": expiry
    }

# =========================================================
# STRATEGY CLASS
# =========================================================
class StrangleCPR3M:
    def __init__(self, fyers, symbols):
        self.fyers = fyers
        self.sym = symbols

        self.trade_taken = False
        self.scenario_locked = False
        self.scenario = None
        self.entry_premium = None

        self.cpr = self.compute_prevday_cpr()
        self.log_initial_state()

    # -----------------------------------------------------
    # CPR FROM PREVIOUS DAY (LOOKBACK SAFE)
    # -----------------------------------------------------
    def compute_prevday_cpr(self):
        today = datetime.date.today()

        def hist(symbol, day):
            r = self.fyers.history({
                "symbol": symbol,
                "resolution": "15",
                "date_format": "1",
                "range_from": day.strftime("%Y-%m-%d"),
                "range_to": day.strftime("%Y-%m-%d"),
                "cont_flag": "1"
            })
            return r.get("candles", [])

        for i in range(1, 8):
            day = today - datetime.timedelta(days=i)
            ce = hist(self.sym["SELL_CE"], day)
            pe = hist(self.sym["SELL_PE"], day)

            if ce and pe:
                highs, lows, closes = [], [], []
                for c1, c2 in zip(ce, pe):
                    highs.append(c1[2] + c2[2])
                    lows.append(c1[3] + c2[3])
                    closes.append(c1[4] + c2[4])

                return calculate_cpr(max(highs), min(lows), closes[-1])

        raise Exception("No CPR data found")

    # -----------------------------------------------------
    # LOG INITIAL STATE
    # -----------------------------------------------------
    def log_initial_state(self):
        q = self.fyers.quotes({
            "symbols": f"{self.sym['SELL_CE']},{self.sym['SELL_PE']}"
        })
        prices = {x["n"]: x["v"]["lp"] for x in q["d"]}
        ce_p = prices[self.sym["SELL_CE"]]
        pe_p = prices[self.sym["SELL_PE"]]

        msg = (
            f"🚀 STRATEGY STARTED\n\n"
            f"Expiry: {self.sym['EXPIRY']}\n"
            f"CE: {self.sym['CE_STRIKE']} ({ce_p:.2f})\n"
            f"PE: {self.sym['PE_STRIKE']} ({pe_p:.2f})\n"
            f"Combined: {ce_p + pe_p:.2f}\n\n"
            f"CPR LEVELS\n"
            f"P: {self.cpr['P']:.2f}\n"
            f"BC: {self.cpr['BC']:.2f}\n"
            f"S1: {self.cpr['S1']:.2f}\n"
            f"S2: {self.cpr['S2']:.2f}\n"
            f"S3: {self.cpr['S3']:.2f}"
        )

        logger.info(msg)
        send_telegram(msg)

    # -----------------------------------------------------
    # GET LATEST CLOSED 3M CANDLE (COMBINED)
    # -----------------------------------------------------
    def latest_3m(self):
        today = datetime.date.today().strftime("%Y-%m-%d")

        def last(symbol):
            r = self.fyers.history({
                "symbol": symbol,
                "resolution": "3",
                "date_format": "1",
                "range_from": today,
                "range_to": today,
                "cont_flag": "1"
            })
            c = r.get("candles", [])
            return c[-2] if len(c) >= 2 else None

        ce = last(self.sym["SELL_CE"])
        pe = last(self.sym["SELL_PE"])

        if not ce or not pe:
            return None

        return {
            "close": ce[4] + pe[4],
            "high": ce[2] + pe[2]
        }

    # -----------------------------------------------------
    # ENTRY EVALUATION
    # -----------------------------------------------------
    def evaluate(self):
        if self.trade_taken:
            return

        c = self.latest_3m()
        if not c:
            return

        close = c["close"]
        high = c["high"]

        # -------- Scenario 2 (early priority) --------
        if not self.scenario_locked:
            if self.is_early_candle(2):
                if self.cpr["S3"] < close < self.cpr["S2"]:
                    self.scenario = 2
                    self.scenario_locked = True
                    self.enter_trade(close)
                    return

        # -------- Scenario 1 --------
        if not self.scenario_locked:
            if self.is_first_candle():
                if self.cpr["S1"] < close < self.cpr["BC"]:
                    self.scenario = 1
                    self.scenario_locked = True
                    return

        if self.scenario == 1:
            if close < self.cpr["S1"] and high >= self.cpr["S1"]:
                self.enter_trade(close)

    def is_first_candle(self):
        now = datetime.datetime.now().time()
        return now <= datetime.time(9, 18)

    def is_early_candle(self, n):
        now = datetime.datetime.now().time()
        return now <= (datetime.datetime.combine(
            datetime.date.today(), datetime.time(9, 15)
        ) + datetime.timedelta(minutes=3 * n)).time()

    # -----------------------------------------------------
    # ENTER TRADE
    # -----------------------------------------------------
    def enter_trade(self, premium):
        logger.info(f"[ENTRY] Scenario {self.scenario} @ {premium:.2f}")
        send_telegram(f"📉 ENTRY Scenario {self.scenario}\nPremium={premium:.2f}")

        for tag, side, sym in [
            ("HEDGECE", 1, self.sym["HEDGE_CE"]),
            ("HEDGEPE", 1, self.sym["HEDGE_PE"]),
            ("SELLCE", -1, self.sym["SELL_CE"]),
            ("SELLPE", -1, self.sym["SELL_PE"]),
        ]:
            self.fyers.place_order({
                "symbol": sym,
                "qty": LOT_SIZE,
                "type": 2,
                "side": side,
                "productType": "INTRADAY",
                "validity": "DAY",
                "orderTag": tag
            })

        self.entry_premium = premium
        self.trade_taken = True

    # -----------------------------------------------------
    # EXIT LOGIC
    # -----------------------------------------------------
    def check_exit(self):
        if not self.trade_taken:
            return

        q = self.fyers.quotes({
            "symbols": f"{self.sym['SELL_CE']},{self.sym['SELL_PE']}"
        })
        prices = {x["n"]: x["v"]["lp"] for x in q["d"]}
        premium = prices[self.sym["SELL_CE"]] + prices[self.sym["SELL_PE"]]

        if self.scenario == 1:
            target = self.cpr["S2"] - 0.02 * self.cpr["S2"]
            sl = self.entry_premium + 15
        else:
            target = self.cpr["S3"] - 0.02 * self.cpr["S3"]
            sl = self.entry_premium + 10

        reason = None
        if premium <= target:
            reason = "TARGET"
        elif premium >= sl:
            reason = "SL"
        elif datetime.datetime.now().time() >= TRADING_END:
            reason = "TIME EXIT"

        if not reason:
            return

        logger.info(f"[EXIT] {reason} @ {premium:.2f}")
        send_telegram(f"🛑 EXIT {reason}\nPremium={premium:.2f}")

        exit_seq = (
            [("EXITCE", 1, self.sym["SELL_CE"]),
             ("EXITPE", 1, self.sym["SELL_PE"]),
             ("EXITHCE", -1, self.sym["HEDGE_CE"]),
             ("EXITHPE", -1, self.sym["HEDGE_PE"])]
            if reason == "SL"
            else
            [("EXITHCE", -1, self.sym["HEDGE_CE"]),
             ("EXITHPE", -1, self.sym["HEDGE_PE"]),
             ("EXITCE", 1, self.sym["SELL_CE"]),
             ("EXITPE", 1, self.sym["SELL_PE"])]
        )

        for tag, side, sym in exit_seq:
            self.fyers.place_order({
                "symbol": sym,
                "qty": LOT_SIZE,
                "type": 2,
                "side": side,
                "productType": "INTRADAY",
                "validity": "DAY",
                "orderTag": tag
            })

        self.trade_taken = False

# =========================================================
# MAIN LOOP
# =========================================================
if __name__ == "__main__":
    fyers = fyersModel.FyersModel(
        client_id=CLIENT_ID,
        token=ACCESS_TOKEN,
        is_async=False,
        log_path=""
    )

    symbols = get_symbols(fyers)
    strategy = StrangleCPR3M(fyers, symbols)

    while True:
        strategy.evaluate()
        strategy.check_exit()

        if datetime.datetime.now().time() >= TRADING_END:
            break

        time.sleep(30)
