# =========================================================
# NIFTY CPR + SUPERTREND + EMA
# OPTION SELLING WITH HEDGE (INDEX SL / TARGET)
# LIVE | BACKTEST-ALIGNED | FULL TELEGRAM LOGGING
# =========================================================

import datetime
import os
import sys
import logging
import requests
from collections import deque
from dotenv import load_dotenv
from fyers_apiv3 import fyersModel
from fyers_apiv3.FyersWebsocket import data_ws

# ================= CONFIG =================
load_dotenv()

CLIENT_ID = os.getenv("FYERS_CLIENT_ID")
ACCESS_TOKEN = os.getenv("FYERS_ACCESS_TOKEN")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

INDEX_SYMBOL = "NSE:NIFTY50-INDEX"

EMA_FAST = 5
EMA_SLOW = 20
ATR_PERIOD = 10
MULTIPLIER = 3.5

LOT_SIZE = 65
LOTS = 1
QTY = LOT_SIZE * LOTS

INDEX_SL = 40
INDEX_TARGET = 120
HARD_EXIT_TIME = datetime.time(14, 45)
ACCEPTANCE_CUTOFF = datetime.time(11, 30)

# ================= LOGGING =================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
    force=True
)

# ================= TELEGRAM =================
def send_telegram(msg):
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": msg[:4000]},
            timeout=3
        )
    except Exception:
        pass  # silent fail - we don't want telegram errors to crash the script

# ================= FYERS =================
class Fyers:
    def __init__(self):
        self.client = fyersModel.FyersModel(
            client_id=CLIENT_ID,
            token=ACCESS_TOKEN,
            is_async=False,
            log_path=""
        )
        self.auth = f"{CLIENT_ID}:{ACCESS_TOKEN}"

    def safe_place_order(self, order_data, description):
        """Place order and alert on failure"""
        try:
            resp = self.client.place_order(order_data)
            if resp.get("s") != "ok":
                error_msg = resp.get("message", "Unknown error")
                send_telegram(
                    f"❌ ORDER FAILED: {description}\n"
                    f"Symbol: {order_data['symbol']}\n"
                    f"Side: {'BUY' if order_data['side']==1 else 'SELL'}\n"
                    f"Qty: {order_data['qty']}\n"
                    f"Error: {error_msg}\n"
                    f"Full response: {resp}"
                )
                logging.error(f"Order failed: {resp}")
                return None
            return resp
        except Exception as e:
            send_telegram(
                f"❌ EXCEPTION during {description}\n"
                f"Error: {str(e)}\n"
                f"Order data: {order_data}"
            )
            logging.exception("Order placement exception")
            return None

    def buy(self, symbol, qty, tag):
        order_data = {
            "symbol": symbol,
            "qty": qty,
            "type": 2,
            "side": 1,
            "productType": "INTRADAY",
            "validity": "DAY",
            "orderTag": tag
        }
        return self.safe_place_order(order_data, f"BUY {tag} {symbol}")

    def sell(self, symbol, qty, tag):
        order_data = {
            "symbol": symbol,
            "qty": qty,
            "type": 2,
            "side": -1,
            "productType": "INTRADAY",
            "validity": "DAY",
            "orderTag": tag
        }
        return self.safe_place_order(order_data, f"SELL {tag} {symbol}")

# ================= INDICATORS =================
def ema(values, period):
    if len(values) < period:
        return None
    k = 2 / (period + 1)
    e = sum(values[:period]) / period
    for v in values[period:]:
        e = v * k + e * (1 - k)
    return e

def atr(candles, period):
    tr = []
    for i in range(1, len(candles)):
        h, l = candles[i]["high"], candles[i]["low"]
        pc = candles[i - 1]["close"]
        tr.append(max(h - l, abs(h - pc), abs(l - pc)))
    atr_val = sum(tr[:period]) / period if tr else 0
    atrs = [None] * period + [atr_val]
    for t in tr[period:]:
        atr_val = (atr_val * (period - 1) + t) / period
        atrs.append(atr_val)
    return atrs

def supertrend(candles, period, multiplier):
    atrs = atr(candles, period)
    st, ub, lb, trend = [None]*len(candles), [None]*len(candles), [None]*len(candles), [None]*len(candles)

    for i in range(len(candles)):
        if atrs[i] is None:
            continue
        h, l, c = candles[i]["high"], candles[i]["low"], candles[i]["close"]
        hl2 = (h + l) / 2
        b_ub = hl2 + multiplier * atrs[i]
        b_lb = hl2 - multiplier * atrs[i]

        if i == 0 or ub[i-1] is None:
            ub[i], lb[i] = b_ub, b_lb
            trend[i] = "BULLISH" if c > b_ub else "BEARISH"
        else:
            ub[i] = b_ub if (b_ub < ub[i-1] or c > ub[i-1]) else ub[i-1]
            lb[i] = b_lb if (b_lb > lb[i-1] or c < lb[i-1]) else lb[i-1]
            trend[i] = (
                "BEARISH" if trend[i-1]=="BULLISH" and c < lb[i-1]
                else "BULLISH" if trend[i-1]=="BEARISH" and c > ub[i-1]
                else trend[i-1]
            )
        st[i] = lb[i] if trend[i]=="BULLISH" else ub[i]
    return st

# ================= CPR =================
def compute_cpr(prev):
    h, l, c = prev["high"], prev["low"], prev["close"]
    pivot = (h + l + c) / 3
    bc = (h + l) / 2
    tc = 2 * pivot - bc
    return min(bc, tc), max(bc, tc)

# ================= EXPIRY HELPERS =================
SPECIAL_MARKET_HOLIDAYS = {
    datetime.date(2026, 1, 26),
    datetime.date(2026, 3, 3),
    datetime.date(2026, 3, 26),
    datetime.date(2026, 3, 31)
}

def is_last_tuesday(d):
    return d.weekday() == 1 and (d + datetime.timedelta(days=7)).month != d.month

def get_next_tuesday_expiry():
    today = datetime.date.today()
    days = (1 - today.weekday()) % 7
    expiry = today + datetime.timedelta(days=days)

    if today.weekday() == 1 and datetime.datetime.now().time() >= datetime.time(15, 30):
        expiry += datetime.timedelta(days=7)

    if expiry in SPECIAL_MARKET_HOLIDAYS:
        expiry -= datetime.timedelta(days=1)

    return expiry

def format_nifty_expiry(expiry):
    yy = expiry.strftime("%y")
    if is_last_tuesday(expiry):
        return f"{yy}{expiry.strftime('%b').upper()}"

    m, d = expiry.month, expiry.day
    m_token = {10: "O", 11: "N", 12: "D"}.get(m, str(m))
    return f"{yy}{m_token}{d:02d}"

# ================= SYMBOL SELECTION =================
def get_nifty_option_symbols(fyers):
    ltp = float(fyers.client.quotes({"symbols": INDEX_SYMBOL})["d"][0]["v"]["lp"])
    atm = round(ltp / 50) * 50
    exp = format_nifty_expiry(get_next_tuesday_expiry())

    return (
        f"NSE:NIFTY{exp}{atm}CE",
        f"NSE:NIFTY{exp}{atm}PE",
        f"NSE:NIFTY{exp}{atm + 200}CE",
        f"NSE:NIFTY{exp}{atm - 200}PE",
    )

# ================= WARMUP =================
def prefill_intraday_candles(fyers, candles, days=5):
    r = fyers.client.history({
        "symbol": INDEX_SYMBOL,
        "resolution": "15",
        "date_format": "1",
        "range_from": (datetime.date.today()-datetime.timedelta(days=days)).strftime("%Y-%m-%d"),
        "range_to": datetime.date.today().strftime("%Y-%m-%d"),
        "cont_flag": "1"
    })

    for c in r.get("candles", []):
        ts = datetime.datetime.fromtimestamp(c[0]).replace(second=0, microsecond=0)
        candles.append({"time": ts, "open": c[1], "high": c[2], "low": c[3], "close": c[4]})

# ================= TRADE MANAGER =================
class TradeManager:
    def __init__(self, fyers):
        self.fyers = fyers
        self.pos = None
        self.trade_day = None

    def enter(self, bias, ltp):
        if self.trade_day == datetime.date.today():
            return

        atm_ce, atm_pe, hedge_ce, hedge_pe = get_nifty_option_symbols(self.fyers)

        if bias == "BEARISH":
            # Hedge first (buy), then main (sell)
            hedge_resp = self.fyers.buy(hedge_ce, QTY, "HEDGE")
            if hedge_resp is None:
                send_telegram("⚠️ ENTRY ABORTED: Hedge buy failed")
                return

            main_resp = self.fyers.sell(atm_ce, QTY, "SELL_CE")
            if main_resp is None:
                send_telegram("⚠️ PARTIAL ENTRY: Hedge bought but SELL_CE failed!")
                # You may want to square off hedge here in production

            self.pos = {"main": atm_ce, "hedge": hedge_ce, "entry": ltp}
        else:
            hedge_resp = self.fyers.buy(hedge_pe, QTY, "HEDGE")
            if hedge_resp is None:
                send_telegram("⚠️ ENTRY ABORTED: Hedge buy failed")
                return

            main_resp = self.fyers.sell(atm_pe, QTY, "SELL_PE")
            if main_resp is None:
                send_telegram("⚠️ PARTIAL ENTRY: Hedge bought but SELL_PE failed!")

            self.pos = {"main": atm_pe, "hedge": hedge_pe, "entry": ltp}

        self.trade_day = datetime.date.today()
        send_telegram(f"🚀 ENTRY {bias} @ {ltp}")

    def on_tick(self, ltp, ts):
        if not self.pos:
            return

        e = self.pos["entry"]

        if ts.time() >= HARD_EXIT_TIME:
            self.exit("TIME", ltp)
        elif ltp >= e + INDEX_SL:
            self.exit("SL", ltp)
        elif ltp <= e - INDEX_TARGET:
            self.exit("TARGET", ltp)

    def exit(self, reason, ltp):
        if not self.pos:
            return

        main_resp = self.fyers.buy(self.pos["main"], QTY, f"EXIT_{reason}_MAIN")
        hedge_resp = self.fyers.sell(self.pos["hedge"], QTY, f"EXIT_{reason}_HEDGE")

        send_telegram(f"🏁 EXIT {reason} @ {ltp}")

        if main_resp is None or hedge_resp is None:
            send_telegram(
                f"⚠️ EXIT WARNING: Some orders may have failed!\n"
                f"Reason: {reason} | LTP: {ltp}\n"
                f"Check Fyers dashboard immediately!"
            )

        self.pos = None

# ================= MAIN =================
if __name__ == "__main__":
    send_telegram("🚀 NIFTY STRATEGY STARTED - with order failure alerts")

    fyers = Fyers()
    tm = TradeManager(fyers)

    # ---- CPR ----
    hist = fyers.client.history({
        "symbol": INDEX_SYMBOL,
        "resolution": "D",
        "date_format": "1",
        "range_from": (datetime.date.today()-datetime.timedelta(days=7)).strftime("%Y-%m-%d"),
        "range_to": (datetime.date.today()-datetime.timedelta(days=1)).strftime("%Y-%m-%d"),
        "cont_flag": "1"
    })
    last = hist["candles"][-1]
    BC, TC = compute_cpr({"high": last[2], "low": last[3], "close": last[4]})

    # ---- Warmup ----
    candles = deque(maxlen=300)
    prefill_intraday_candles(fyers, candles)

    closes = [c["close"] for c in candles]
    st = supertrend(list(candles), ATR_PERIOD, MULTIPLIER)

    send_telegram(
        f"WARMUP DONE | Candles={len(candles)} | "
        f"EMA5={ema(closes,5):.2f} EMA20={ema(closes,20):.2f} "
        f"ST={st[-1]:.2f} | BC={BC:.2f} TC={TC:.2f}"
    )

    bearish_acceptance = False
    bullish_acceptance = False
    current_day = None

    def on_tick(msg):
        global bearish_acceptance, bullish_acceptance, current_day

        if msg.get("symbol") != INDEX_SYMBOL:
            return

        ts = datetime.datetime.fromtimestamp(
            msg.get("last_traded_time", msg.get("timestamp", datetime.datetime.now().timestamp()))
        )
        ltp = msg["ltp"]
        tm.on_tick(ltp, ts)

        bucket = ts.replace(minute=(ts.minute//15)*15, second=0, microsecond=0)
        prev = None

        if candles and candles[-1]["time"] == bucket:
            c = candles[-1]
            c["high"] = max(c["high"], ltp)
            c["low"] = min(c["low"], ltp)
            c["close"] = ltp
            return
        else:
            prev = candles[-1] if candles else None
            candles.append({"time": bucket, "open": ltp, "high": ltp, "low": ltp, "close": ltp})

        if not prev or len(candles) < 30:
            return

        if current_day != prev["time"].date():
            bearish_acceptance = bullish_acceptance = False
            current_day = prev["time"].date()
            send_telegram("🔄 New trading day reset")

        closes = [c["close"] for c in candles]
        ema5, ema20 = ema(closes, 5), ema(closes, 20)
        st = supertrend(list(candles), ATR_PERIOD, MULTIPLIER)

        send_telegram(
            f"CANDLE {prev['time'].time()} | "
            f"O={prev['open']} H={prev['high']} L={prev['low']} C={prev['close']} | "
            f"BC={BC:.2f} TC={TC:.2f} | "
            f"EMA5={ema5:.1f} EMA20={ema20:.1f} ST={st[-2]:.1f} | "
            f"BA={bearish_acceptance} BU={bullish_acceptance}"
        )

        # ---- ACCEPTANCE ----
        if not bearish_acceptance and prev["time"].time() <= ACCEPTANCE_CUTOFF and prev["close"] > prev["open"] and prev["close"] < BC:
            bearish_acceptance = True
            send_telegram("🔴 BEARISH ACCEPTANCE")

        elif not bullish_acceptance and prev["time"].time() <= ACCEPTANCE_CUTOFF and prev["close"] < prev["open"] and prev["close"] > TC:
            bullish_acceptance = True
            send_telegram("🟢 BULLISH ACCEPTANCE")

        # ---- TRIGGERS ----
        elif bearish_acceptance and prev["close"] < BC and prev["close"] < st[-2] and ema5 < ema20:
            send_telegram("🔥 BEARISH TRIGGER")
            tm.enter("BEARISH", ltp)
            bearish_acceptance = False

        elif bullish_acceptance and prev["close"] > TC and prev["close"] > st[-2] and ema5 > ema20:
            send_telegram("🚀 BULLISH TRIGGER")
            tm.enter("BULLISH", ltp)
            bullish_acceptance = False

    ws = data_ws.FyersDataSocket(
        access_token=fyers.auth,
        on_connect=lambda: ws.subscribe(symbols=[INDEX_SYMBOL], data_type="SymbolUpdate"),
        on_message=on_tick,
        log_path=""
    )
    ws.connect()
