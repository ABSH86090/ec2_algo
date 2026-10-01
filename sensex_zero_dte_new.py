# =========================================================
# SENSEX DTE0 SHORT STRANGLE  (OTM4, 200% SL, step-trailing SL)
# Entry  : 9:16 AM on expiry day only
#          Sell OTM4 CE + Sell OTM4 PE — LOTS lots each
# SL     : 200% of sell price per leg (SL = 2 x entry), SL-L buy order
# Trail  : For every 40% fall in premium (measured on entry price),
#          SL is moved down by 40% of entry price.
#          e.g. entry 100 → SL 200
#               LTP ≤ 60  → SL 160
#               LTP ≤ 20  → SL 120
#          SL only ever moves down, never up.
# Reentry: None
# Exit   : SL hit per leg | 15:30 hard exit
# =========================================================

import datetime
import os
import sys
import time
import threading
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

SL_MULT         = 2.00          # SL = 200% of entry price (2 x entry)
STRIKE_STEP     = 100           # SENSEX strike interval
CE_OTM          = 4             # OTM4 for CE leg
PE_OTM          = 4             # OTM4 for PE leg

# Trailing SL: for every TRAIL_TRIGGER_PCT fall in premium (of entry price),
# move SL down by TRAIL_SL_PCT of entry price.
TRAIL_TRIGGER_PCT = 0.40
TRAIL_SL_PCT      = 0.40
TRAIL_RETRY_SECS  = 2           # min gap between trail-modify attempts per leg

SL_BUY_LIMIT_BUFFER = 0.05      # SL-buy limit = trigger * (1 + 5%)
TICK_SIZE           = 0.05      # SENSEX option minimum price movement

ENTRY_TIME     = datetime.time(9, 16, 0)
HARD_EXIT_TIME = datetime.time(15, 30, 0)
LOG_FILE       = "sensex_dte0_otm4_trailing_strangle.log"

SPECIAL_MARKET_HOLIDAYS = {
    datetime.date(2026, 1, 26),
    datetime.date(2026, 3, 3),
    datetime.date(2026, 3, 26),
    datetime.date(2026, 3, 31),
    datetime.date(2026, 4, 14),
    datetime.date(2026, 5, 1),
    datetime.date(2026, 5, 28),
    datetime.date(2026, 6, 26),
    datetime.date(2026, 9, 14),
    datetime.date(2026, 10, 2),
    datetime.date(2026, 11, 24),
    datetime.date(2026, 12, 25),
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
        limit = round_tick(trigger * (1 + SL_BUY_LIMIT_BUFFER))
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

    def modify_sl_buy(self, order_id, qty, trigger):
        """Move an existing SL-Limit BUY to a new trigger (used for trailing)."""
        stop  = round_tick(trigger)
        limit = round_tick(trigger * (1 + SL_BUY_LIMIT_BUFFER))
        resp = self.client.modify_order({
            "id": order_id,
            "type": 4,
            "qty": qty,
            "stopPrice": stop,
            "limitPrice": limit,
        })
        logger.info(f"MODIFY_SL {order_id} stop={stop} limit={limit} → {resp}")
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
    """True when today is this week's expiry day."""
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

    ce_strike = atm + CE_OTM * STRIKE_STEP
    pe_strike = atm - PE_OTM * STRIKE_STEP

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
# Per-leg states:
#   OPEN   -> short is live, SL-buy order working (and being trailed)
#   CLOSED -> leg finished (SL hit or hard exit)
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
            self.entry_done = True
            self.all_exited = True
            return

        ce_sym, pe_sym = build_entry_symbols(index_ltp)

        ce_ltp = self.fyers.get_ltp(ce_sym)
        pe_ltp = self.fyers.get_ltp(pe_sym)

        # Sell CE
        ce_resp = self.fyers.sell_mkt(ce_sym, QTY, "CESELL")
        if not ce_resp or ce_resp.get("s") != "ok":
            send_telegram(f"❌ CE SELL FAILED: {ce_resp}")
            return

        # Sell PE — roll back CE on failure
        pe_resp = self.fyers.sell_mkt(pe_sym, QTY, "PESELL")
        if not pe_resp or pe_resp.get("s") != "ok":
            send_telegram(f"❌ PE SELL FAILED — rolling back CE: {pe_resp}")
            self.fyers.buy_mkt(ce_sym, QTY, "CEROLLBACK")
            return

        # SL = 200% of sell price, rounded to nearest tick
        ce_sl = round_tick(ce_ltp * SL_MULT)
        pe_sl = round_tick(pe_ltp * SL_MULT)

        ce_sl_resp = self.fyers.place_sl_buy(ce_sym, QTY, ce_sl, "CESL")
        pe_sl_resp = self.fyers.place_sl_buy(pe_sym, QTY, pe_sl, "PESL")

        for name, r in (("CE", ce_sl_resp), ("PE", pe_sl_resp)):
            if not r or r.get("s") != "ok":
                send_telegram(f"🚨 {name} SL ORDER FAILED — position unprotected! {r}")

        now = datetime.datetime.now()
        self.legs = {
            "CE": self._new_leg(ce_sym, ce_ltp, ce_sl, ce_sl_resp, now),
            "PE": self._new_leg(pe_sym, pe_ltp, pe_sl, pe_sl_resp, now),
        }

        self.entry_done = True
        self.trade_date = datetime.date.today()

        send_telegram(
            f"🚀 ENTRY DONE\n"
            f"CE (OTM{CE_OTM}): {ce_sym}  sell≈{ce_ltp:.1f}  SL={ce_sl:.1f}\n"
            f"PE (OTM{PE_OTM}): {pe_sym}  sell≈{pe_ltp:.1f}  SL={pe_sl:.1f}\n"
            f"Trail: every {TRAIL_TRIGGER_PCT:.0%} fall → SL down {TRAIL_SL_PCT:.0%} of entry"
        )

        ws.subscribe(symbols=[ce_sym, pe_sym], data_type="SymbolUpdate")

    @staticmethod
    def _new_leg(symbol, entry, sl, sl_resp, now):
        return {
            "symbol":         symbol,
            "entry_price":    entry,
            "initial_sl":     sl,
            "sl_price":       sl,
            "sl_order_id":    (sl_resp or {}).get("id", ""),
            "trail_steps":    0,
            "current_ltp":    entry,
            "state":          "OPEN",
            "last_chk":       now,
            "last_trail_try": now - datetime.timedelta(seconds=TRAIL_RETRY_SECS),
        }

    # ---- Per-tick processing ------------------------------------------
    def on_option_tick(self, symbol, current_ltp):
        for leg_name, leg in self.legs.items():
            if leg["symbol"] != symbol:
                continue
            leg["current_ltp"] = current_ltp
            if leg["state"] == "OPEN":
                self._maybe_trail_sl(leg_name, leg, current_ltp)
                self._detect_sl_fill(leg_name, leg, current_ltp)
            break

    def _throttled(self, leg, seconds=5):
        now = datetime.datetime.now()
        if (now - leg["last_chk"]).total_seconds() < seconds:
            return False
        leg["last_chk"] = now
        return True

    # ---- Trailing SL ---------------------------------------------------
    def _maybe_trail_sl(self, leg_name, leg, current_ltp):
        """
        For every TRAIL_TRIGGER_PCT (of entry) the premium has fallen,
        SL = initial SL - steps * TRAIL_SL_PCT * entry.  SL never moves up.
        """
        if not leg["sl_order_id"]:
            return

        entry     = leg["entry_price"]
        step_move = entry * TRAIL_TRIGGER_PCT
        drop      = entry - current_ltp
        if drop <= 0 or step_move <= 0:
            return

        steps = int((drop + 1e-9) // step_move)
        if steps <= leg["trail_steps"]:
            return

        new_sl = round_tick(leg["initial_sl"] - steps * entry * TRAIL_SL_PCT)
        if new_sl >= leg["sl_price"] or new_sl <= current_ltp:
            return

        now = datetime.datetime.now()
        if (now - leg["last_trail_try"]).total_seconds() < TRAIL_RETRY_SECS:
            return
        leg["last_trail_try"] = now

        resp = self.fyers.modify_sl_buy(leg["sl_order_id"], QTY, new_sl)
        if resp and resp.get("s") == "ok":
            old_sl = leg["sl_price"]
            leg["sl_price"]    = new_sl
            leg["trail_steps"] = steps
            send_telegram(
                f"📉 {leg_name} SL TRAILED (step {steps}): ltp={current_ltp:.1f}  "
                f"SL {old_sl:.1f} → {new_sl:.1f}"
            )
        else:
            send_telegram(f"⚠️ {leg_name} SL TRAIL MODIFY FAILED (will retry): {resp}")

    # ---- SL fill detection --------------------------------------------
    def _detect_sl_fill(self, leg_name, leg, current_ltp):
        """Poll order status when LTP is at or above the SL trigger (throttled to 5s)."""
        if current_ltp < leg["sl_price"]:
            return
        if not self._throttled(leg):
            return

        status = self.fyers.get_order_status(leg["sl_order_id"])
        if status == 2:   # Traded → SL executed by broker
            leg["state"] = "CLOSED"
            send_telegram(
                f"⛔ SL HIT {leg_name}: ltp={current_ltp:.1f}  sl={leg['sl_price']:.1f}  "
                f"entry={leg['entry_price']:.1f}"
            )
            self._check_all_closed()

    def _check_all_closed(self):
        if self.legs and all(l["state"] == "CLOSED" for l in self.legs.values()):
            self.all_exited = True
            send_telegram("✅ Both legs fully closed. Strategy complete.")

    # ---- Hard exit ----------------------------------------------------
    def exit_all(self, reason):
        if self.all_exited:
            return
        for leg_name, leg in self.legs.items():
            if leg["state"] != "OPEN":
                continue

            # Don't double-cover if the SL filled just before exit time
            if leg["sl_order_id"] and self.fyers.get_order_status(leg["sl_order_id"]) == 2:
                leg["state"] = "CLOSED"
                send_telegram(f"🏁 {leg_name} already closed by SL before {reason} exit")
                continue

            if leg["sl_order_id"]:
                self.fyers.cancel_order(leg["sl_order_id"])
            self.fyers.buy_mkt(leg["symbol"], QTY, f"EXIT{reason}{leg_name}")
            leg["state"] = "CLOSED"
            send_telegram(f"🏁 {leg_name} CLOSED ({reason})")

        self.all_exited = True
        send_telegram(f"✅ All legs closed. Reason: {reason}")

# ================= WEBSOCKET CALLBACKS =================
fyers_obj = None
tm        = None
ws        = None
tm_lock   = threading.Lock()

def on_tick(msg):
    if not msg:
        return

    symbol = msg.get("symbol")
    ltp    = msg.get("ltp")
    if not symbol or ltp is None:
        return

    now = datetime.datetime.now()

    with tm_lock:
        if tm.entry_done and not tm.all_exited and now.time() >= HARD_EXIT_TIME:
            tm.exit_all("TIME")
            return

        if not tm.entry_done and symbol == INDEX_SYMBOL and now.time() >= ENTRY_TIME:
            logger.info(f"Entry triggered at {now.time()}, index_ltp={ltp}")
            tm.enter(float(ltp))
            return

        if tm.entry_done and not tm.all_exited:
            tm.on_option_tick(symbol, float(ltp))

def exit_timer():
    """Fires the hard exit even if no ticks arrive at that moment."""
    while True:
        with tm_lock:
            if tm.entry_done and tm.all_exited:
                return
            if tm.entry_done and datetime.datetime.now().time() >= HARD_EXIT_TIME:
                tm.exit_all("TIME")
                return
        time.sleep(1)

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
    send_telegram("🚀 SENSEX DTE0 OTM4 STRANGLE STARTED (200% SL, trailing, no reentry)")

    fyers_obj = Fyers()
    tm        = TradeManager(fyers_obj)

    threading.Thread(target=exit_timer, daemon=True).start()

    ws = data_ws.FyersDataSocket(
        access_token=fyers_obj.auth,
        on_connect=on_open,
        on_message=on_tick,
        on_error=on_error,
        on_close=on_close,
        log_path="",
    )

    ws.connect()
