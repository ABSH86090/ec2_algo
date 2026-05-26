# =========================================================
# SENSEX OTM3 SHORT STRANGLE WITH TRAILING SL
# Entry  : 9:16 AM — Sell OTM3 CE + OTM3 PE (no trade on Thursdays)
# SL     : 30% above sell price per leg (broker SL-L order)
# Trail  : every 10 pts of favorable move → shift SL down 10 pts
#          (cancel old SL order, place fresh SL order)
# Exit   : SL hit per leg | combined MTM loss ≥ ₹5000 | 15:25 hard exit
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
LOTS          = 4
QTY           = LOT_SIZE * LOTS

SL_PCT           = 0.30   # 30% initial stop-loss above sell price
TRAIL_TRIGGER    = 10     # pts of favorable move required to trigger trail
TRAIL_STEP       = 10     # pts to move SL down on each trail
STRIKE_STEP      = 100    # SENSEX option strike interval
OTM_DISTANCE     = 3      # OTM3 = 3 strikes away from ATM
COMBINED_SL_LIMIT = 5000  # exit all if combined MTM loss reaches this (₹)

ENTRY_TIME     = datetime.time(9, 16, 0)
HARD_EXIT_TIME = datetime.time(15, 10, 0)
LOG_FILE       = "otm3_strangle.log"

# Add known market holidays here
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
    datetime.date(2026, 12, 25)
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
        """SL-Limit BUY to cover a short position when premium rises to `trigger`."""
        limit = round(trigger * 1.05, 1)   # 5% buffer above trigger to ensure fill
        resp = self.client.place_order({
            "symbol": symbol, "qty": qty,
            "type": 4, "side": 1,           # SL-L, BUY side
            "productType": "INTRADAY", "validity": "DAY",
            "stopPrice": round(trigger, 1),
            "limitPrice": limit,
            "orderTag": tag,
        })
        logger.info(f"SL_BUY {symbol} trigger={trigger} limit={limit} → {resp}")
        return resp

    def cancel_order(self, order_id):
        resp = self.client.cancel_order({"id": order_id})
        logger.info(f"CANCEL {order_id} → {resp}")
        return resp

    def get_ltp(self, symbol):
        q = self.client.quotes({"symbols": symbol})
        return float(q["d"][0]["v"]["lp"])

    def get_order_status(self, order_id):
        """Returns Fyers status code for the order, or None if not found.
        Fyers status: 1=Cancelled, 2=Traded, 4=Transit, 5=Rejected, 6=Pending."""
        ob = self.client.orderbook()
        for o in ob.get("orderBook", []):
            if o["id"] == order_id:
                return o["status"]
        return None

# ================= EXPIRY HELPERS =================
def is_last_thursday(d):
    return d.weekday() == 3 and (d + datetime.timedelta(days=7)).month != d.month

def get_next_expiry():
    today = datetime.date.today()
    days_ahead = (3 - today.weekday()) % 7
    expiry = today + datetime.timedelta(days=days_ahead)
    if expiry in SPECIAL_MARKET_HOLIDAYS:
        expiry -= datetime.timedelta(days=1)
    return expiry

def format_expiry(expiry):
    yy = expiry.strftime("%y")
    if is_last_thursday(expiry):
        return f"{yy}{expiry.strftime('%b').upper()}"
    m_token = {10: "O", 11: "N", 12: "D"}.get(expiry.month, str(expiry.month))
    return f"{yy}{m_token}{expiry.day:02d}"

def build_otm3_symbols(index_ltp):
    atm = round(index_ltp / STRIKE_STEP) * STRIKE_STEP
    expiry    = get_next_expiry()
    exp_token = format_expiry(expiry)

    ce_strike = atm + OTM_DISTANCE * STRIKE_STEP
    pe_strike = atm - OTM_DISTANCE * STRIKE_STEP

    ce_sym = f"BSE:SENSEX{exp_token}{ce_strike}CE"
    pe_sym = f"BSE:SENSEX{exp_token}{pe_strike}PE"

    send_telegram(
        f"📌 OTM3 SELECTION\n"
        f"Index={index_ltp}  ATM={atm}  Expiry={expiry} ({exp_token})\n"
        f"CE: {ce_sym}\n"
        f"PE: {pe_sym}"
    )
    return ce_sym, pe_sym

# ================= TRADE MANAGER =================
class TradeManager:
    """
    Manages both legs of the short strangle.

    Each leg dict:
        symbol       – option symbol string
        entry_price  – sell price (used for reference; SL calculated from LTP)
        sl_price     – current SL trigger price (rises with losses, trails down with wins)
        sl_order_id  – active broker SL order ID
        trail_ref    – price level from which next 10-pt trail is measured
        current_ltp  – latest known LTP (used for combined MTM calculation)
        active       – False once the leg is closed
        last_sl_chk  – datetime of last orderbook status poll (throttle)
    """

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

        if datetime.date.today().weekday() == 3:   # 3 = Thursday
            send_telegram("⏭️ No trade today — Thursday (expiry day). Skipping.")
            return

        ce_sym, pe_sym = build_otm3_symbols(index_ltp)

        # Fetch LTPs just before selling (best approximation of fill price)
        ce_ltp = self.fyers.get_ltp(ce_sym)
        pe_ltp = self.fyers.get_ltp(pe_sym)

        # Sell CE
        ce_resp = self.fyers.sell_mkt(ce_sym, QTY, "OTM3CESELL")
        if not ce_resp or ce_resp.get("s") != "ok":
            send_telegram(f"❌ CE SELL FAILED: {ce_resp}")
            return

        # Sell PE (roll back CE if this fails)
        pe_resp = self.fyers.sell_mkt(pe_sym, QTY, "OTM3PESELL")
        if not pe_resp or pe_resp.get("s") != "ok":
            send_telegram(f"❌ PE SELL FAILED — rolling back CE: {pe_resp}")
            self.fyers.buy_mkt(ce_sym, QTY, "CE_ROLLBACK")
            return

        # SL = 30% above sell price
        ce_sl = round(ce_ltp * (1 + SL_PCT), 1)
        pe_sl = round(pe_ltp * (1 + SL_PCT), 1)

        ce_sl_resp = self.fyers.place_sl_buy(ce_sym, QTY, ce_sl, "CESL")
        pe_sl_resp = self.fyers.place_sl_buy(pe_sym, QTY, pe_sl, "PESL")

        now = datetime.datetime.now()
        self.legs = {
            "CE": {
                "symbol":      ce_sym,
                "entry_price": ce_ltp,
                "sl_price":    ce_sl,
                "sl_order_id": (ce_sl_resp or {}).get("id", ""),
                "trail_ref":   ce_ltp,   # trail measured from entry price
                "current_ltp": ce_ltp,
                "active":      True,
                "last_sl_chk": now,
            },
            "PE": {
                "symbol":      pe_sym,
                "entry_price": pe_ltp,
                "sl_price":    pe_sl,
                "sl_order_id": (pe_sl_resp or {}).get("id", ""),
                "trail_ref":   pe_ltp,
                "current_ltp": pe_ltp,
                "active":      True,
                "last_sl_chk": now,
            },
        }

        self.entry_done = True
        self.trade_date = datetime.date.today()

        send_telegram(
            f"🚀 ENTRY DONE\n"
            f"CE: {ce_sym} sell≈{ce_ltp:.1f}  SL={ce_sl:.1f}\n"
            f"PE: {pe_sym} sell≈{pe_ltp:.1f}  SL={pe_sl:.1f}"
        )

        # Extend websocket subscription to option symbols
        ws.subscribe(symbols=[ce_sym, pe_sym], data_type="SymbolUpdate")

    # ---- Per-tick processing ------------------------------------------
    def on_option_tick(self, symbol, current_ltp):
        for leg_name, leg in self.legs.items():
            if leg["symbol"] == symbol and leg["active"]:
                leg["current_ltp"] = current_ltp
                self._detect_sl_fill(leg_name, leg, current_ltp)
                if leg["active"]:
                    self._trail(leg_name, leg, current_ltp)
                break

        self._check_combined_loss()

    def _check_combined_loss(self):
        """Exit all if total MTM loss across active legs reaches COMBINED_SL_LIMIT."""
        combined_loss = sum(
            (leg["current_ltp"] - leg["entry_price"]) * QTY
            for leg in self.legs.values()
            if leg["active"]
        )
        if combined_loss >= COMBINED_SL_LIMIT:
            send_telegram(
                f"🚨 COMBINED SL HIT: MTM loss = ₹{combined_loss:.0f} "
                f"(limit ₹{COMBINED_SL_LIMIT}) — exiting all legs."
            )
            self.exit_all("COMBINED_SL")

    def _detect_sl_fill(self, leg_name, leg, current_ltp):
        """
        Poll order status when LTP is at or above the SL trigger.
        Throttled to once per 5 seconds to avoid excessive API calls.
        """
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

    def _trail(self, leg_name, leg, current_ltp):
        """
        Trail the SL in TRAIL_STEP increments whenever the premium
        falls TRAIL_TRIGGER pts from the last reference level.

        Since we are SHORT options, "favorable" = price falling.
        Each 10-pt drop → SL moves down 10 pts → cancel old SL, place new one.
        """
        steps = 0
        while current_ltp <= leg["trail_ref"] - TRAIL_TRIGGER:
            leg["trail_ref"] -= TRAIL_STEP
            leg["sl_price"]  -= TRAIL_STEP
            steps += 1

        if steps == 0:
            return

        # Safety: verify the existing SL order is still pending before touching it
        status = self.fyers.get_order_status(leg["sl_order_id"])
        if status not in (4, 6):   # Not in transit/pending → already filled or cancelled
            leg["active"] = False
            send_telegram(
                f"⚠️ {leg_name} SL order no longer pending (status={status}) — "
                f"marking leg inactive to avoid accidental new position."
            )
            return

        # Cancel old SL order
        if leg["sl_order_id"]:
            self.fyers.cancel_order(leg["sl_order_id"])

        # Place new (lower) SL order
        resp = self.fyers.place_sl_buy(
            leg["symbol"], QTY, leg["sl_price"], f"{leg_name}_SL_TRAIL"
        )
        leg["sl_order_id"] = (resp or {}).get("id", "")

        send_telegram(
            f"🔄 TRAIL {leg_name} ×{steps}: "
            f"ltp={current_ltp:.1f}  new SL={leg['sl_price']:.1f}  "
            f"ref={leg['trail_ref']:.0f}"
        )

    # ---- Hard exit ----------------------------------------------------
    def exit_all(self, reason):
        if self.all_exited:
            return
        for leg_name, leg in self.legs.items():
            if not leg["active"]:
                continue
            if leg["sl_order_id"]:
                self.fyers.cancel_order(leg["sl_order_id"])
            self.fyers.buy_mkt(leg["symbol"], QTY, f"EXIT_{reason}_{leg_name}")
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

    # Hard exit check
    if tm.entry_done and not tm.all_exited and now.time() >= HARD_EXIT_TIME:
        tm.exit_all("TIME")
        return

    # Entry trigger — first INDEX tick at or after 9:16
    if not tm.entry_done and symbol == INDEX_SYMBOL and now.time() >= ENTRY_TIME:
        logger.info(f"Entry triggered at {now.time()}, index_ltp={ltp}")
        tm.enter(float(ltp))
        return

    # Option tick → SL fill detection + trailing
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
    send_telegram("🚀 OTM3 STRANGLE STRATEGY STARTED")

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
