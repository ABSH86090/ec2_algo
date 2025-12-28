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
# LOGGING + TELEGRAM
# =========================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
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
        except Exception:
            pass

# =========================================================
# CPR ON COMBINED STRANGLE
# =========================================================
def calculate_s1(high, low, close):
    p = (high + low + close) / 3
    return 2 * p - high

# =========================================================
# SYMBOL UTILS
# =========================================================
def is_last_tuesday(d):
    return d.weekday() == 1 and (d + datetime.timedelta(days=7)).month != d.month

def get_next_expiry():
    today = datetime.date.today()
    return today if today.weekday() == 1 else today + datetime.timedelta(days=(1 - today.weekday()) % 7)

def format_expiry(d):
    yy = d.strftime("%y")
    if is_last_tuesday(d):
        return f"{yy}{d.strftime('%b').upper()}"
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
        "HEDGE_CE": f"NSE:NIFTY{expiry}{ce+300}CE",
        "HEDGE_PE": f"NSE:NIFTY{expiry}{pe-300}PE",
    }

# =========================================================
# STRATEGY
# =========================================================
class StrangleS1Combined15M:
    def __init__(self, fyers, symbols):
        self.fyers = fyers
        self.sym = symbols
        self.traded = False
        self.sl = None

        self.s1 = self.compute_prevday_s1()
        logger.info(f"[CPR] Combined Strangle S1 = {self.s1:.2f}")
        send_telegram(f"📊 STRANGLE CPR S1 = {self.s1:.2f}")

    # -----------------------------------------------------
    # Build combined 15m candles for previous day
    # -----------------------------------------------------
    def compute_prevday_s1(self):
        today = datetime.date.today()
        prev = today - datetime.timedelta(days=1)

        def hist(symbol):
            r = self.fyers.history({
                "symbol": symbol,
                "resolution": "15",
                "date_format": "1",
                "range_from": prev.strftime("%Y-%m-%d"),
                "range_to": prev.strftime("%Y-%m-%d"),
                "cont_flag": "1"
            })
            return r.get("candles", [])

        ce_c = hist(self.sym["SELL_CE"])
        pe_c = hist(self.sym["SELL_PE"])

        if not ce_c or not pe_c:
            raise Exception("No history to compute combined CPR")

        combined = []
        for c1, c2 in zip(ce_c, pe_c):
            combined.append({
                "high": c1[2] + c2[2],
                "low":  c1[3] + c2[3],
                "close":c1[4] + c2[4],
            })

        day_high = max(c["high"] for c in combined)
        day_low  = min(c["low"] for c in combined)
        day_close = combined[-1]["close"]

        return calculate_s1(day_high, day_low, day_close)

    # -----------------------------------------------------
    # Fetch latest 15m combined candle (today)
    # -----------------------------------------------------
    def latest_combined_15m(self):
        today = datetime.date.today().strftime("%Y-%m-%d")

        def last(symbol):
            r = self.fyers.history({
                "symbol": symbol,
                "resolution": "15",
                "date_format": "1",
                "range_from": today,
                "range_to": today,
                "cont_flag": "1"
            })
            c = r.get("candles", [])
            return c[-1] if c else None

        ce = last(self.sym["SELL_CE"])
        pe = last(self.sym["SELL_PE"])
        if not ce or not pe:
            return None

        return {
            "close": ce[4] + pe[4],
            "high": ce[2] + pe[2]
        }

    # -----------------------------------------------------
    # ENTRY
    # -----------------------------------------------------
    def evaluate(self):
        if self.traded:
            return

        c = self.latest_combined_15m()
        if not c:
            return

        if c["close"] < self.s1:
            logger.info(f"[ENTRY] Combined Close={c['close']:.2f} < S1")
            send_telegram(
                f"📉 STRANGLE ENTRY\nClose={c['close']:.2f}\nSL={c['high']:.2f}"
            )

            # BUY HEDGES FIRST
            for tag, side in [
                ("HEDGECE", 1),
                ("HEDGEPE", 1),
                ("STRANGLECE", -1),
                ("STRANGLEPE", -1),
            ]:
                sym = (
                    self.sym["HEDGE_CE"] if tag=="HEDGECE" else
                    self.sym["HEDGE_PE"] if tag=="HEDGEPE" else
                    self.sym["SELL_CE"] if tag=="STRANGLECE" else
                    self.sym["SELL_PE"]
                )
                self.fyers.place_order({
                    "symbol": sym,
                    "qty": LOT_SIZE,
                    "type": 2,
                    "side": side,
                    "productType": "INTRADAY",
                    "validity": "DAY",
                    "orderTag": tag
                })

            self.sl = c["high"]
            self.traded = True

    # -----------------------------------------------------
    # SL / TIME EXIT (EXIT ALL LEGS)
    # -----------------------------------------------------
    def check_exit(self):
        if not self.traded:
            return

        q = self.fyers.quotes({
            "symbols": f"{self.sym['SELL_CE']},{self.sym['SELL_PE']}"
        })
        d = q.get("d", [])
        prices = {x["n"]: x["v"]["lp"] for x in d}

        premium = prices[self.sym["SELL_CE"]] + prices[self.sym["SELL_PE"]]

        if premium >= self.sl or datetime.datetime.now().time() >= TRADING_END:
            logger.info("[EXIT] SL or TIME hit → exiting all legs")
            send_telegram("🛑 STRANGLE EXIT (SL / TIME)")

            for tag, side, sym in [
                ("EXITCE", 1, self.sym["SELL_CE"]),
                ("EXITPE", 1, self.sym["SELL_PE"]),
                ("EXITHCE", -1, self.sym["HEDGE_CE"]),
                ("EXITHPE", -1, self.sym["HEDGE_PE"]),
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

            self.traded = False

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
    strategy = StrangleS1Combined15M(fyers, symbols)

    while True:
        strategy.evaluate()
        strategy.check_exit()

        if datetime.datetime.now().time() >= TRADING_END:
            break

        time.sleep(60)
