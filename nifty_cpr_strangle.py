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
# CPR
# =========================================================
def calculate_cpr_levels(high, low, close):
    p = (high + low + close) / 3
    return {
        "P": p,
        "S1": 2 * p - high,
        "S2": p - (high - low)
    }

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
class StrangleS1S2Combined15M:
    def __init__(self, fyers, symbols):
        self.fyers = fyers
        self.sym = symbols
        self.traded = False
        self.sl = None

        # 🔹 CPR INIT
        self.cpr = self.compute_prevday_cpr()
        self.s1 = self.cpr["S1"]
        self.s2 = self.cpr["S2"]

        cpr_msg = (
            f"📊 STRANGLE CPR INITIALIZED\n"
            f"S1 = {self.s1:.2f}\n"
            f"S2 = {self.s2:.2f}"
        )
        logger.info(cpr_msg)
        send_telegram(cpr_msg)

        # 🔹 Initial premium log
        self.log_initial_premium()

    # -----------------------------------------------------
    # Robust CPR with 7-day lookback
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

            ce_c = hist(self.sym["SELL_CE"], day)
            pe_c = hist(self.sym["SELL_PE"], day)

            if ce_c and pe_c:
                highs, lows, closes = [], [], []

                for c1, c2 in zip(ce_c, pe_c):
                    highs.append(c1[2] + c2[2])
                    lows.append(c1[3] + c2[3])
                    closes.append(c1[4] + c2[4])

                logger.info(f"[CPR] Using data from {day}")

                return calculate_cpr_levels(
                    max(highs),
                    min(lows),
                    closes[-1]
                )

        send_telegram("⚠️ CPR ERROR: No historical data found (7-day lookback)")
        raise Exception("No historical data found for CPR")

    # -----------------------------------------------------
    # Initial premium logger
    # -----------------------------------------------------
    def log_initial_premium(self):
        q = self.fyers.quotes({
            "symbols": f"{self.sym['SELL_CE']},{self.sym['SELL_PE']}"
        })

        prices = {x["n"]: x["v"]["lp"] for x in q.get("d", [])}
        ce = prices.get(self.sym["SELL_CE"])
        pe = prices.get(self.sym["SELL_PE"])

        if ce is None or pe is None:
            logger.warning("[INIT] Unable to fetch initial premiums")
            return

        total = ce + pe

        msg = (
            f"💰 STRANGLE PREMIUM (INIT)\n"
            f"CE = {ce:.2f}\n"
            f"PE = {pe:.2f}\n"
            f"TOTAL = {total:.2f}"
        )
        logger.info(msg)
        send_telegram(msg)

    # -----------------------------------------------------
    # Latest 15m combined candle
    # -----------------------------------------------------
    def latest_15m(self):
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

        c = self.latest_15m()
        if not c:
            return

        if c["close"] < self.s1:
            mid = self.s1 - 0.5 * (self.s1 - self.s2)

            if c["close"] < mid:
                logger.info(
                    f"[SKIP] Close {c['close']:.2f} too close to S2 (Mid={mid:.2f})"
                )
                return

            logger.info(
                f"[ENTRY] Close={c['close']:.2f} < S1 | SL={c['high']:.2f}"
            )

            send_telegram(
                f"📉 STRANGLE ENTRY\n"
                f"Close={c['close']:.2f}\n"
                f"S1={self.s1:.2f}\n"
                f"S2={self.s2:.2f}\n"
                f"SL={c['high']:.2f}"
            )

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

            self.sl = c["high"]
            self.traded = True

    # -----------------------------------------------------
    # EXIT
    # -----------------------------------------------------
    def check_exit(self):
        if not self.traded:
            return

        q = self.fyers.quotes({
            "symbols": f"{self.sym['SELL_CE']},{self.sym['SELL_PE']}"
        })
        prices = {x["n"]: x["v"]["lp"] for x in q.get("d", [])}
        premium = prices[self.sym["SELL_CE"]] + prices[self.sym["SELL_PE"]]

        if premium >= self.sl:
            reason = "SL HIT"
        elif premium <= self.s2:
            reason = "S2 HIT"
        elif datetime.datetime.now().time() >= TRADING_END:
            reason = "TIME EXIT"
        else:
            return

        logger.info(f"[EXIT] {reason} | Premium={premium:.2f}")
        send_telegram(f"🛑 STRANGLE EXIT\n{reason}")

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
    strategy = StrangleS1S2Combined15M(fyers, symbols)

    while True:
        strategy.evaluate()
        strategy.check_exit()

        if datetime.datetime.now().time() >= TRADING_END:
            break

        time.sleep(60)
