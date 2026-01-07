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

LOT_SIZE = 130
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
# CPR CALCULATION
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

        # -------- Scenario 3 state (ADDED ONLY) --------
        self.sc3_armed = False
        self.sc3_traded = False
        self.sc3_pending_green = False
        self.sc3_be_active = False

        self.cpr = self.compute_prevday_cpr()
        self.log_initial_state()

    # -------------------------------------------------
    def send_candle_telegram(self, msg):
        # prevent telegram flooding (1 msg per candle)
        now = datetime.datetime.now()
        if hasattr(self, "_last_tg_candle"):
            if (now - self._last_tg_candle).seconds < 170:
                return
        self._last_tg_candle = now
        send_telegram(msg)
    # -------------------------------------------------
    def compute_prevday_cpr(self):
        today = datetime.date.today()
    
        def hist_5s(symbol, day):
            r = self.fyers.history({
                "symbol": symbol,
                "resolution": "5S",
                "date_format": "1",
                "range_from": day.strftime("%Y-%m-%d"),
                "range_to": day.strftime("%Y-%m-%d"),
                "cont_flag": "1"
            })
            return r.get("candles", [])
    
        for i in range(1, 8):  # look back max 7 days
            day = today - datetime.timedelta(days=i)
    
            ce = hist_5s(self.sym["SELL_CE"], day)
            pe = hist_5s(self.sym["SELL_PE"], day)
    
            if not ce or not pe:
                continue
    
            # --- build strangle candles correctly ---
            strangle_candles = []
            for c1, c2 in zip(ce, pe):
                strangle_candles.append({
                    "open": c1[1] + c2[1],
                    "high": c1[2] + c2[2],
                    "low":  c1[3] + c2[3],
                    "close": c1[4] + c2[4]
                })
    
            if not strangle_candles:
                continue
    
            prev_open = strangle_candles[0]["open"]
            prev_high = max(c["high"] for c in strangle_candles)
            prev_low  = min(c["low"] for c in strangle_candles)
            prev_close = strangle_candles[-1]["close"]
    
            # --- apply 2% reduction on high ---
            adjusted_high = prev_high - (0.03 * prev_high)
    
            logger.info(
                f"[CPR BASE] "
                f"O={prev_open:.2f} "
                f"H={prev_high:.2f} "
                f"H_adj={adjusted_high:.2f} "
                f"L={prev_low:.2f} "
                f"C={prev_close:.2f}"
            )
    
            return calculate_cpr(
                high=adjusted_high,
                low=prev_low,
                close=prev_close
            )
    
        raise Exception("No CPR data found using 5S resolution")


    # -------------------------------------------------
    def log_initial_state(self):
        q = self.fyers.quotes({
            "symbols": f"{self.sym['SELL_CE']},{self.sym['SELL_PE']}"
        })
        prices = {x["n"]: x["v"]["lp"] for x in q["d"]}
        msg = (
            f"🚀 STRATEGY STARTED\n\n"
            f"Expiry: {self.sym['EXPIRY']}\n"
            f"CE: {self.sym['CE_STRIKE']} ({prices[self.sym['SELL_CE']]:.2f})\n"
            f"PE: {self.sym['PE_STRIKE']} ({prices[self.sym['SELL_PE']]:.2f})\n"
            f"P={self.cpr['P']:.2f} BC={self.cpr['BC']:.2f}\n"
            f"S1={self.cpr['S1']:.2f} S2={self.cpr['S2']:.2f} S3={self.cpr['S3']:.2f}"
        )
        logger.info(msg)
        send_telegram(msg)

    # -------------------------------------------------
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
            "open": ce[1] + pe[1],
            "high": ce[2] + pe[2],
            "close": ce[4] + pe[4]
        }

    # -------------------------------------------------
    def evaluate(self):
        if self.trade_taken:
            return

        c = self.latest_3m()
        if not c:
            return

        open_, high, close = c["open"], c["high"], c["close"]

        log_msg = (
            f"📊 3M Candle Closed\n"
            f"O={open_:.2f} H={high:.2f} C={close:.2f}\n"
            f"S1={self.cpr['S1']:.2f} BC={self.cpr['BC']:.2f} P={self.cpr['P']:.2f}\n"
            f"Scenario={self.scenario} Locked={self.scenario_locked}"
        )
        logger.info(log_msg)
        self.send_candle_telegram(log_msg)


        # ---------- Scenario 2 (UNCHANGED) ----------
        if not self.scenario_locked:
            if self.is_early_candle(2):
                if self.cpr["S3"] < close < self.cpr["S2"]:
                    self.scenario = 2
                    self.scenario_locked = True
                    self.enter_trade(close)
                    return

        # ---------- Scenario 1 (UNCHANGED) ----------
        if not self.scenario_locked:
            if self.is_first_candle():
                if self.cpr["S1"] < close < self.cpr["BC"]:
                    self.scenario = 1
                    self.scenario_locked = True
                    return

        if self.scenario == 1:
            if close < self.cpr["S1"] and high >= self.cpr["S1"]:
                self.enter_trade(close)
                return

        # ================= Scenario 3 (ADDED) =================
        if not self.sc3_armed and self.cpr["S1"] < close < self.cpr["BC"]:
            self.sc3_armed = True

        if self.sc3_armed and not self.sc3_traded:
            # Pattern 2
            if open_ > self.cpr["P"] and close < self.cpr["P"]:
                self.scenario = 3
                self.enter_trade(close)
                self.sc3_traded = True
                return

            # Pattern 1 (green)
            if close > open_ and high > self.cpr["P"] and close < self.cpr["P"]:
                self.sc3_pending_green = True
                return

            # Pattern 1 confirmation (red)
            if self.sc3_pending_green and close < self.cpr["P"] and close < open_:
                self.scenario = 3
                self.enter_trade(close)
                self.sc3_traded = True
                self.sc3_pending_green = False
                return

    def is_first_candle(self):
        return datetime.datetime.now().time() <= datetime.time(9, 18)

    def is_early_candle(self, n):
        return datetime.datetime.now().time() <= (
            datetime.datetime.combine(datetime.date.today(), datetime.time(9, 15))
            + datetime.timedelta(minutes=3 * n)
        ).time()

    # -------------------------------------------------
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
        self.sc3_be_active = False

    # -------------------------------------------------
    def check_exit(self):
        if not self.trade_taken:
            return

        q = self.fyers.quotes({
            "symbols": f"{self.sym['SELL_CE']},{self.sym['SELL_PE']}"
        })
        prices = {x["n"]: x["v"]["lp"] for x in q["d"]}
        premium = prices[self.sym["SELL_CE"]] + prices[self.sym["SELL_PE"]]

        reason = None

        # ---------- Scenario 3 exit (ADDED) ----------
        if self.scenario == 3:
            profit = self.entry_premium - premium

            if profit >= 15:
                self.sc3_be_active = True

            if self.sc3_be_active and premium >= self.entry_premium:
                reason = "SL"
            elif premium <= self.cpr["S1"]:
                reason = "TARGET"
            elif datetime.datetime.now().time() >= TRADING_END:
                reason = "TIME EXIT"

        # ---------- Original exit logic (UNCHANGED) ----------
        else:
            if self.scenario == 1:
                target = self.cpr["S2"] + 0.02 * self.cpr["S2"]
                sl = self.entry_premium + 15
            else:
                target = self.cpr["S3"] + 0.02 * self.cpr["S3"]
                sl = self.entry_premium + 10

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
    logger.info("========== SYMBOLS SELECTED ==========")
    logger.info(f"EXPIRY   : {symbols['EXPIRY']}")
    logger.info(f"SELL CE  : {symbols['SELL_CE']}")
    logger.info(f"SELL PE  : {symbols['SELL_PE']}")
    logger.info(f"HEDGE CE : {symbols['HEDGE_CE']}")
    logger.info(f"HEDGE PE : {symbols['HEDGE_PE']}")
    logger.info("======================================")
    
    print("\n========== SYMBOLS SELECTED ==========")
    print(f"EXPIRY   : {symbols['EXPIRY']}")
    print(f"SELL CE  : {symbols['SELL_CE']}")
    print(f"SELL PE  : {symbols['SELL_PE']}")
    print(f"HEDGE CE : {symbols['HEDGE_CE']}")
    print(f"HEDGE PE : {symbols['HEDGE_PE']}")
    print("======================================\n")
    strategy = StrangleCPR3M(fyers, symbols)

    while True:
        strategy.evaluate()
        strategy.check_exit()

        if datetime.datetime.now().time() >= TRADING_END:
            break

        time.sleep(30)
