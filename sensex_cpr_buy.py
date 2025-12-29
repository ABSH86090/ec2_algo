# =========================================================
# SENSEX INDEX → CPR + EMA → OPTIONS STRATEGY
# EXCHANGE-MANAGED STOP LOSS (SL-L)
# BUY ONLY ON 15-MIN CANDLE CLOSE
# =========================================================

import datetime
import time
import os
import sys
import logging
import requests
from collections import deque
from dotenv import load_dotenv
from fyers_apiv3 import fyersModel
from fyers_apiv3.FyersWebsocket import data_ws, order_ws

# =========================================================
# CONFIG
# =========================================================
load_dotenv()

CLIENT_ID = os.getenv("FYERS_CLIENT_ID")
ACCESS_TOKEN = os.getenv("FYERS_ACCESS_TOKEN")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

INDEX_SYMBOL = "BSE:SENSEX-INDEX"

TIMEFRAME_MIN = 15
EMA_FAST = 5
EMA_SLOW = 20

MAX_INDEX_CANDLE_SIZE = 200

LOT_SIZE = 20
LOTS = 2
TOTAL_QTY = LOT_SIZE * LOTS
PARTIAL_QTY = (TOTAL_QTY // 2 // LOT_SIZE) * LOT_SIZE

SL_POINTS = 30
TARGET_POINTS = 60

LOG_FILE = "sensex_exchange_sl_strategy.log"

# =========================================================
# LOGGING
# =========================================================
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

# =========================================================
# TELEGRAM
# =========================================================
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
# FYERS CLIENT
# =========================================================
class Fyers:
    def __init__(self):
        self.client = fyersModel.FyersModel(
            client_id=CLIENT_ID,
            token=ACCESS_TOKEN,
            is_async=False,
            log_path=""
        )
        self.auth = f"{CLIENT_ID}:{ACCESS_TOKEN}"

    def buy_mkt(self, symbol, qty, tag):
        return self.client.place_order({
            "symbol": symbol,
            "qty": qty,
            "type": 2,
            "side": 1,
            "productType": "INTRADAY",
            "validity": "DAY",
            "orderTag": tag
        })

    def sell_mkt(self, symbol, qty, tag):
        return self.client.place_order({
            "symbol": symbol,
            "qty": qty,
            "type": 2,
            "side": -1,
            "productType": "INTRADAY",
            "validity": "DAY",
            "orderTag": tag
        })

    def place_sl(self, symbol, qty, trigger, limit, tag):
        return self.client.place_order({
            "symbol": symbol,
            "qty": qty,
            "type": 4,                 # SL-L
            "side": -1,                # SELL to exit long
            "productType": "INTRADAY",
            "limitPrice": limit,
            "stopPrice": trigger,
            "validity": "DAY",
            "orderTag": tag
        })

    def modify_sl(self, order_id, qty, trigger, limit):
        return self.client.modify_order({
            "id": order_id,
            "qty": qty,
            "stopPrice": trigger,
            "limitPrice": limit
        })

    def cancel(self, order_id):
        return self.client.cancel_order({"id": order_id})

# =========================================================
# EMA
# =========================================================
def ema(values, period):
    if len(values) < period:
        return None
    sma = sum(values[:period]) / period
    e = sma
    k = 2 / (period + 1)
    for v in values[period:]:
        e = v * k + e * (1 - k)
    return e

# =========================================================
# CPR
# =========================================================
def compute_cpr(prev):
    h, l, c = prev["high"], prev["low"], prev["close"]
    p = (h + l + c) / 3
    bc = (h + l) / 2
    tc = p * 2 - bc
    r1 = 2 * p - l
    s1 = 2 * p - h
    return {"P": p, "BC": min(bc, tc), "TC": max(bc, tc), "R1": r1, "S1": s1}

# =========================================================
# ATM SYMBOL (YOUR LOGIC)
# =========================================================
SPECIAL_MARKET_HOLIDAYS = {
    datetime.date(2025, 10, 2),
    datetime.date(2025, 12, 25),
}

def is_last_thursday(d):
    return d.weekday() == 3 and (d + datetime.timedelta(days=7)).month != d.month

def get_next_expiry():
    today = datetime.date.today()
    weekday = today.weekday()
    days_to_thu = (3 - weekday) % 7
    candidate = today + datetime.timedelta(days=days_to_thu)
    return candidate - datetime.timedelta(days=1) if candidate in SPECIAL_MARKET_HOLIDAYS else candidate

def format_expiry_for_symbol(expiry_date):
    yy = expiry_date.strftime("%y")
    treat_as_monthly = (
        expiry_date.weekday() == 3 and is_last_thursday(expiry_date)
    ) or (
        expiry_date.weekday() == 2 and is_last_thursday(expiry_date + datetime.timedelta(days=1))
    )
    if treat_as_monthly:
        return f"{yy}{expiry_date.strftime('%b').upper()}"
    m = expiry_date.month
    d = expiry_date.day
    m_token = {10: "O", 11: "N", 12: "D"}.get(m, f"{m:02d}")
    return f"{yy}{m_token}{d:02d}"

def get_atm_symbols(fyers):
    resp = fyers.client.quotes({"symbols": INDEX_SYMBOL})
    ltp = float(resp["d"][0]["v"]["lp"])
    atm = round(ltp / 100) * 100
    expiry = get_next_expiry()
    exp = format_expiry_for_symbol(expiry)
    ce = f"BSE:SENSEX{exp}{atm}CE"
    pe = f"BSE:SENSEX{exp}{atm}PE"
    logger.info(f"[ATM] CE={ce} PE={pe}")
    send_telegram(f"📌 ATM SYMBOLS\nCE={ce}\nPE={pe}")
    return ce, pe

# =========================================================
# TRADE MANAGER (EXCHANGE SL)
# =========================================================
class TradeManager:
    def __init__(self, fyers):
        self.fyers = fyers
        self.pos = None

    def enter(self, symbol):
        if self.pos:
            return

        self.fyers.buy_mkt(symbol, TOTAL_QTY, "ENTRY")

        self.pos = {
            "symbol": symbol,
            "entry": None,
            "qty_left": TOTAL_QTY,
            "sl_id": None,
            "stage": 0,
            "sl_hit": False
        }

        send_telegram(f"🚀 ENTRY\n{symbol}\nQty={TOTAL_QTY}")

    def on_option_tick(self, ltp):
        if not self.pos:
            return

        # First tick → place SL
        if self.pos["entry"] is None:
            self.pos["entry"] = ltp
            sl = ltp - SL_POINTS
            resp = self.fyers.place_sl(
                self.pos["symbol"],
                self.pos["qty_left"],
                sl,
                sl - 0.5,
                "SL_INIT"
            )
            self.pos["sl_id"] = resp["id"]
            return

        entry = self.pos["entry"]
        qty = self.pos["qty_left"]
        pnl_points = ltp - entry
        mtm = pnl_points * qty

        # Partial exit
        if mtm >= 3000 and self.pos["stage"] == 0:
            self.fyers.sell_mkt(self.pos["symbol"], PARTIAL_QTY, "BOOK3000")
            self.pos["qty_left"] -= PARTIAL_QTY
            self.fyers.modify_sl(
                self.pos["sl_id"],
                self.pos["qty_left"],
                entry,
                entry - 0.5
            )
            self.pos["stage"] = 1
            send_telegram("✅ PARTIAL EXIT\nSL → COST")

        # Trail SL
        if mtm >= 5000 and self.pos["stage"] == 1:
            new_sl = entry + 30
            self.fyers.modify_sl(
                self.pos["sl_id"],
                self.pos["qty_left"],
                new_sl,
                new_sl - 0.5
            )
            self.pos["stage"] = 2
            send_telegram("🔒 SL → ENTRY + 30")

        # Target exit
        if pnl_points >= TARGET_POINTS and not self.pos["sl_hit"]:
            self.fyers.sell_mkt(self.pos["symbol"], self.pos["qty_left"], "TARGET")
            self.fyers.cancel(self.pos["sl_id"])
            send_telegram("🎯 TARGET HIT\nSL CANCELLED")
            self.pos = None

    def on_order_update(self, msg):
        if not self.pos:
            return

        if msg.get("id") == self.pos["sl_id"] and msg.get("status") == "FILLED":
            self.pos["sl_hit"] = True
            send_telegram("🛑 SL HIT BY EXCHANGE")
            self.pos = None

# =========================================================
# SCENARIO ENGINE (SAMPLE PUT)
# =========================================================
class ScenarioEngine:
    def __init__(self, cpr):
        self.prev = deque(maxlen=5)
        self.cpr = cpr

    def evaluate(self, c, ema5, ema20):
        if c["high"] - c["low"] > MAX_INDEX_CANDLE_SIZE:
            return None

        if (
            self.prev
            and self.prev[0]["close"] > self.cpr["BC"]
            and c["close"] < self.cpr["BC"]
            and c["close"] > self.cpr["S1"]
            and ema5 < ema20
        ):
            return "PUT"

        return None

# =========================================================
# MAIN
# =========================================================
if __name__ == "__main__":
    logger.info("🚀 STRATEGY STARTED")
    send_telegram("🚀 SENSEX EXCHANGE-SL STRATEGY STARTED")

    fyers = Fyers()
    ce, pe = get_atm_symbols(fyers)

    # ---- Fetch CPR ----
    hist = fyers.client.history({
        "symbol": INDEX_SYMBOL,
        "resolution": "15",
        "date_format": "1",
        "range_from": (datetime.date.today() - datetime.timedelta(days=5)).strftime("%Y-%m-%d"),
        "range_to": datetime.date.today().strftime("%Y-%m-%d"),
        "cont_flag": "1"
    })["candles"]

    candles_hist = [{
        "time": datetime.datetime.fromtimestamp(c[0]),
        "open": c[1],
        "high": c[2],
        "low": c[3],
        "close": c[4]
    } for c in hist]

    cpr = compute_cpr(candles_hist[-2])
    scenario_engine = ScenarioEngine(cpr)
    trade_mgr = TradeManager(fyers)

    candles = deque(maxlen=100)

    # =====================================================
    # WEBSOCKETS
    # =====================================================
    def on_tick(msg):
        if "symbol" not in msg or "ltp" not in msg:
            return

        now = datetime.datetime.now()

        # INDEX
        if msg["symbol"] == INDEX_SYMBOL:
            ltp = msg["ltp"]
            bucket = now.replace(minute=(now.minute // 15) * 15, second=0, microsecond=0)

            if not candles or candles[-1]["time"] != bucket:
                if candles:
                    closed = candles[-1]
                    scenario_engine.prev.append(closed)

                    closes = [c["close"] for c in candles]
                    if len(closes) >= EMA_SLOW:
                        ema5 = ema(closes, EMA_FAST)
                        ema20 = ema(closes, EMA_SLOW)
                        signal = scenario_engine.evaluate(closed, ema5, ema20)
                        if signal == "PUT":
                            trade_mgr.enter(pe)
                        elif signal == "CALL":
                            trade_mgr.enter(ce)

                candles.append({
                    "time": bucket,
                    "open": ltp,
                    "high": ltp,
                    "low": ltp,
                    "close": ltp
                })
            else:
                c = candles[-1]
                c["high"] = max(c["high"], ltp)
                c["low"] = min(c["low"], ltp)
                c["close"] = ltp

        # OPTION
        if trade_mgr.pos and msg["symbol"] == trade_mgr.pos["symbol"]:
            trade_mgr.on_option_tick(msg["ltp"])

    def on_order(msg):
        trade_mgr.on_order_update(msg)

    def on_open():
        ws_data.subscribe(symbols=[INDEX_SYMBOL, ce, pe], data_type="SymbolUpdate")
        ws_data.keep_running()

    ws_data = data_ws.FyersDataSocket(
        access_token=fyers.auth,
        on_connect=on_open,
        on_message=on_tick,
        log_path=""
    )

    ws_order = order_ws.FyersOrderSocket(
        access_token=fyers.auth,
        write_to_file=False,
        log_path="",
        on_connect=on_open,
        on_close=lambda m: logger.info(f"Order socket closed: {m}"),
        on_error=lambda m: logger.error(f"Order socket error: {m}"),
        on_orders=on_order
    )

    ws_order.connect()
    ws_data.connect()
