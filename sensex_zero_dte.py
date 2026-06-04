# =========================================================
# SENSEX ASYMMETRIC DTE0 SHORT STRANGLE
# Entry  : 9:16 AM on expiry day (Thursday) only
#          Sell OTM2 CE + Sell OTM3 PE — 1 lot each
# SL     : 20% above sell price per leg (fixed SL-L order)
# Exit   : SL hit per leg | 15:10 hard exit
# =========================================================

import datetime
import os
import sys
import logging
import requests
from dotenv import load_dotenv
from fyers_apiv3 import fyersModel
from fyers_apiv3.FyersWebsocket import data_ws

# ================= CONFIG =================
load_dotenv()

CLIENT_ID          = os.getenv("FYERS_CLIENT_ID")
ACCESS_TOKEN       = os.getenv("FYERS_ACCESS_TOKEN")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID")

INDEX_SYMBOL  = "BSE:SENSEX-INDEX"
LOT_SIZE      = 20
LOTS          = 4               # number of lots per leg
QTY           = LOT_SIZE * LOTS # total qty per leg = 80

SL_PCT        = 0.20            # 20% above sell price
STRIKE_STEP   = 100             # SENSEX strike interval
CE_OTM        = 2               # OTM2 for CE leg
PE_OTM        = 3               # OTM3 for PE leg

TICK_SIZE      = 0.05               # SENSEX option minimum price movement

ENTRY_TIME     = datetime.time(9, 16, 0)
HARD_EXIT_TIME = datetime.time(15, 10, 0)
LOG_FILE       = "sensex_dte0_asymmetric_strangle.log"

SPECIAL_MARKET_HOLIDAYS = {
    datetime.date(2026, 1, 26),
    datetime.date(2026, 1, 15),
}

# ================= LOGGING =================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout),
    ],
    force=True,
)
logger = logging.getLogger(__name__)

# ================= HELPERS =================
def round_tick(price):
    """Round price to the nearest valid SENSEX option tick (0.05)."""
    return round(round(price / TICK_SIZE) * TICK_SIZE, 2)

# ================= TELEGRAM =================
def send_telegram(msg):
    logger.info(msg)
    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        try:
            requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={"chat_id": TELEGRAM_CHAT_ID, "text": msg[:4000]},
                timeout=3,
            )
        except Exception:
            pass

# ================= FYERS CLIENT =================
class Fyers:
    def __init__(self):
        self.client = fyersModel.FyersModel(
            client_id=CLIENT_ID,
            token=ACCESS_TOKEN,
            is_async=False,
            log_path="",
        )
        self.auth = f"{CLIENT_ID}:{ACCESS_TOKEN}"

    def sell_mkt(self, symbol, qty, tag):
        resp = self.client.place_order({
            "symbol": symbol, "qty": qty,
            "type": 2, "side": -1,
            "productType": "INTRADAY", "validity": "DAY",
            "orderTag": tag,
        })
        logger.info(f"SELL_MKT {symbol} qty={qty} → {resp}")
        return resp

    def buy_mkt(self, symbol, qty, tag):
        resp = self.client.place_order({
            "symbol": symbol, "qty": qty,
            "type": 2, "side": 1,
            "productType": "INTRADAY", "validity": "DAY",
            "orderTag": tag,
        })
        logger.info(f"BUY_MKT {symbol} qty={qty} → {resp}")
        return resp

    def place_sl_buy(self, symbol, qty, trigger, tag):
        """SL-Limit BUY to cover a short when premium rises to `trigger`."""
        stop  = round_tick(trigger)
        limit = round_tick(trigger * 1.05)
        resp = self.client.place_order({
            "symbol": symbol, "qty": qty,
            "type": 4, "side": 1,
            "productType": "INTRADAY", "validity": "DAY",
            "stopPrice": stop,
            "limitPrice": limit,
            "orderTag": tag,
        })
        logger.info(f"SL_BUY {symbol} stop={stop} limit={limit} → {resp}")
        return resp

    def cancel_order(self, order_id):
        resp = self.client.cancel_order({"id": order_id})
        logger.info(f"CANCEL {order_id} → {resp}")
        return resp

    def get_ltp(self, symbol):
        q = self.client.quotes({"symbols": symbol})
        return float(q["d"][0]["v"]["lp"])

    def get_order_status(self, order_id):
        """Fyers status: 1=Cancelled, 2=Traded, 4=Transit, 5=Rejected, 6=Pending."""
        ob = self.client.orderbook()
        for o in ob.get("orderBook", []):
            if o["id"] == order_id:
                return o["status"]
        return None

# ================= EXPIRY HELPERS =================
def is_last_thursday(d):
    return d.weekday() == 3 and (d + datetime.timedelta(days=7)).month != d.month

def get_weekly_expiry():
    """
    This week's expiry: normally Thursday.
    Shifts to Wednesday when Thursday is a market holiday.
    """
    today = datetime.date.today()
    days_to_thursday = (3 - today.weekday()) % 7
    thursday = today + datetime.timedelta(days=days_to_thursday)
    if thursday in SPECIAL_MARKET_HOLIDAYS:
        return thursday - datetime.timedelta(days=1)   # Wednesday
    return thursday

def is_dte0():
    """True when today is this week's expiry day (Thursday, or Wednesday if Thursday is holiday)."""
    return datetime.date.today() == get_weekly_expiry()

def format_expiry(expiry):
    yy = expiry.strftime("%y")
    if is_last_thursday(expiry):
        return f"{yy}{expiry.strftime('%b').upper()}"
    m_token = {10: "O", 11: "N", 12: "D"}.get(expiry.month, str(expiry.month))
    return f"{yy}{m_token}{expiry.day:02d}"

def build_entry_symbols(index_ltp):
    atm       = round(index_ltp / STRIKE_STEP) * STRIKE_STEP
    expiry    = get_weekly_expiry()
    exp_token = format_expiry(expiry)

    ce_strike = atm + CE_OTM * STRIKE_STEP   # OTM2
    pe_strike = atm - PE_OTM * STRIKE_STEP   # OTM3

    ce_sym = f"BSE:SENSEX{exp_token}{ce_strike}CE"
    pe_sym = f"BSE:SENSEX{exp_token}{pe_strike}PE"

    send_telegram(
        f"📌 SYMBOL SELECTION\n"
        f"Index={index_ltp}  ATM={atm}  Expiry={expiry} ({exp_token})\n"
        f"CE (OTM{CE_OTM}): {ce_sym}\n"
        f"PE (OTM{PE_OTM}): {pe_sym}"
    )
    return ce_sym, pe_sym

# ================= TRADE MANAGER =================
class TradeManager:
    def __init__(self, fyers_obj):
        self.fyers      = fyers_obj
        self.entry_done = False
        self.trade_date = None
        self.all_exited = False
        self.legs       = {}   # "CE" / "PE"

    # ---- Entry --------------------------------------------------------
    def enter(self, index_ltp):
        if self.entry_done or self.trade_date == datetime.date.today():
            return

        if not is_dte0():
            send_telegram("⏭️ No trade today — not an expiry day (DTE0 required). Skipping.")
            self.entry_done = True   # suppress repeated skips on every index tick
            return

        ce_sym, pe_sym = build_entry_symbols(index_ltp)

        ce_ltp = self.fyers.get_ltp(ce_sym)
        pe_ltp = self.fyers.get_ltp(pe_sym)

        # Sell CE (OTM2)
        ce_resp = self.fyers.sell_mkt(ce_sym, QTY, "CESELL")
        if not ce_resp or ce_resp.get("s") != "ok":
            send_telegram(f"❌ CE SELL FAILED: {ce_resp}")
            return

        # Sell PE (OTM3) — roll back CE on failure
        pe_resp = self.fyers.sell_mkt(pe_sym, QTY, "PESELL")
        if not pe_resp or pe_resp.get("s") != "ok":
            send_telegram(f"❌ PE SELL FAILED — rolling back CE: {pe_resp}")
            self.fyers.buy_mkt(ce_sym, QTY, "CEROLLBACK")
            return

        # SL = 20% above sell price, rounded to nearest tick
        ce_sl = round_tick(ce_ltp * (1 + SL_PCT))
        pe_sl = round_tick(pe_ltp * (1 + SL_PCT))

        ce_sl_resp = self.fyers.place_sl_buy(ce_sym, QTY, ce_sl, "CESL")
        pe_sl_resp = self.fyers.place_sl_buy(pe_sym, QTY, pe_sl, "PESL")

        now = datetime.datetime.now()
        self.legs = {
            "CE": {
                "symbol":      ce_sym,
                "entry_price": ce_ltp,
                "sl_price":    ce_sl,
                "sl_order_id": (ce_sl_resp or {}).get("id", ""),
                "current_ltp": ce_ltp,
                "active":      True,
                "last_sl_chk": now,
            },
            "PE": {
                "symbol":      pe_sym,
                "entry_price": pe_ltp,
                "sl_price":    pe_sl,
                "sl_order_id": (pe_sl_resp or {}).get("id", ""),
                "current_ltp": pe_ltp,
                "active":      True,
                "last_sl_chk": now,
            },
        }

        self.entry_done = True
        self.trade_date = datetime.date.today()

        send_telegram(
            f"🚀 ENTRY DONE\n"
            f"CE (OTM{CE_OTM}): {ce_sym}  sell≈{ce_ltp:.1f}  SL={ce_sl:.1f}\n"
            f"PE (OTM{PE_OTM}): {pe_sym}  sell≈{pe_ltp:.1f}  SL={pe_sl:.1f}"
        )

        ws.subscribe(symbols=[ce_sym, pe_sym], data_type="SymbolUpdate")

    # ---- Per-tick processing ------------------------------------------
    def on_option_tick(self, symbol, current_ltp):
        for leg_name, leg in self.legs.items():
            if leg["symbol"] == symbol and leg["active"]:
                leg["current_ltp"] = current_ltp
                self._detect_sl_fill(leg_name, leg, current_ltp)
                break

    def _detect_sl_fill(self, leg_name, leg, current_ltp):
        """Poll order status when LTP is at or above the SL trigger (throttled to 5s)."""
        if current_ltp < leg["sl_price"]:
            return

        now = datetime.datetime.now()
        if (now - leg["last_sl_chk"]).total_seconds() < 5:
            return
        leg["last_sl_chk"] = now

        status = self.fyers.get_order_status(leg["sl_order_id"])
        if status == 2:   # Traded → SL executed by broker
            leg["active"] = False
            send_telegram(
                f"⛔ SL HIT {leg_name}: ltp={current_ltp:.1f}  sl={leg['sl_price']:.1f}"
            )
            if all(not l["active"] for l in self.legs.values()):
                self.all_exited = True
                send_telegram("✅ Both legs exited. Strategy complete.")

    # ---- Hard exit ----------------------------------------------------
    def exit_all(self, reason):
        if self.all_exited:
            return
        for leg_name, leg in self.legs.items():
            if not leg["active"]:
                continue
            if leg["sl_order_id"]:
                self.fyers.cancel_order(leg["sl_order_id"])
            self.fyers.buy_mkt(leg["symbol"], QTY, f"EXIT{reason}{leg_name}")
            leg["active"] = False
            send_telegram(f"🏁 {leg_name} CLOSED ({reason})")
        self.all_exited = True
        send_telegram(f"✅ All legs closed. Reason: {reason}")

# ================= WEBSOCKET CALLBACKS =================
fyers_obj = None
tm        = None
ws        = None

def on_tick(msg):
    if not msg:
        return

    symbol = msg.get("symbol")
    ltp    = msg.get("ltp")
    if not symbol or ltp is None:
        return

    now = datetime.datetime.now()

    if tm.entry_done and not tm.all_exited and now.time() >= HARD_EXIT_TIME:
        tm.exit_all("TIME")
        return

    if not tm.entry_done and symbol == INDEX_SYMBOL and now.time() >= ENTRY_TIME:
        logger.info(f"Entry triggered at {now.time()}, index_ltp={ltp}")
        tm.enter(float(ltp))
        return

    if tm.entry_done and not tm.all_exited:
        tm.on_option_tick(symbol, float(ltp))

def on_open():
    ws.subscribe(symbols=[INDEX_SYMBOL], data_type="SymbolUpdate")
    ws.keep_running()

def on_error(msg):
    logger.error(f"WS Error: {msg}")
    send_telegram(f"❌ WS Error: {msg}")

def on_close(msg):
    logger.info(f"WS Closed: {msg}")

# ================= MAIN =================
if __name__ == "__main__":
    send_telegram("🚀 SENSEX DTE0 ASYMMETRIC STRANGLE STARTED")

    fyers_obj = Fyers()
    tm        = TradeManager(fyers_obj)

    ws = data_ws.FyersDataSocket(
        access_token=fyers_obj.auth,
        on_connect=on_open,
        on_message=on_tick,
        on_error=on_error,
        on_close=on_close,
        log_path="",
    )

    ws.connect()
