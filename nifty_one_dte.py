"""
NIFTY OTM4 STRANGLE SELL — DTE1 ONLY
=======================================
Sell OTM4 CE + OTM4 PE at 9:16 AM on the day before expiry (DTE1).

DTE1 is normally Monday (Tuesday is expiry).
If Monday is a market holiday, Friday becomes DTE1.
If Tuesday is a market holiday, expiry shifts to Monday — Friday becomes DTE1.

Per-leg SL  = 70% of each leg's individual entry price (exchange SL-M order).
Trailing    = every 5% move in our favour, cancel old SL-M and place new one 5% lower.
Force-exit  = 3:10 PM — cancel any open SL orders, then market-buy to close.
"""

import datetime
import logging
import os
import sys
import time

import requests
from dotenv import load_dotenv
from fyers_apiv3 import fyersModel
from fyers_apiv3.FyersWebsocket import data_ws

# =========================================================
# CONFIG
# =========================================================
load_dotenv()

CLIENT_ID          = os.getenv("FYERS_CLIENT_ID")
ACCESS_TOKEN       = os.getenv("FYERS_ACCESS_TOKEN")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID")

LOT_SIZE        = 75           # NIFTY lot size — verify with NSE before running
NUM_LOTS        = 5            # Number of lots to trade per leg
STRIKE_STEP     = 50           # NIFTY strike spacing in points
OTM4_STRIKES    = 4            # Both legs: ATM ± 4×50 = ATM ± 200

SL_PCT           = 0.70        # Initial SL at 70% above entry  (price × 1.70)
TRAIL_TRIGGER_PCT = 0.05       # Trail fires when price drops 5% from trail reference
TRAIL_STEP_PCT    = 0.05       # SL is reduced by 5% of current SL on each trail

TICK_SIZE = 0.05               # NIFTY options minimum price movement


def round_price(price):
    """Round price to the nearest NIFTY options tick (0.05)."""
    return round(round(price / TICK_SIZE) * TICK_SIZE, 2)

STATUS_CHECK_SECS = 30         # Seconds between exchange order status polls

ENTRY_TIME  = datetime.time(9, 16)
TRADING_END = datetime.time(15, 10)   # 3:10 PM force-exit

LOG_FILE = "nifty_otm_strangle_dte1.log"

# =========================================================
# LOGGING
# =========================================================
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


def send_telegram(msg):
    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        try:
            requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={"chat_id": TELEGRAM_CHAT_ID, "text": msg[:4000]},
                timeout=3,
            )
        except Exception:
            pass


# =========================================================
# SYMBOL / DATE UTILS
# =========================================================
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


def is_last_tuesday(d, holidays=SPECIAL_MARKET_HOLIDAYS):
    is_tuesday   = d.weekday() == 1
    is_last_week = (d + datetime.timedelta(days=7)).month != d.month
    if is_tuesday and is_last_week:
        return not (d in holidays)
    if d.weekday() == 0:
        next_day = d + datetime.timedelta(days=1)
        is_last_week_tuesday = (next_day + datetime.timedelta(days=7)).month != next_day.month
        if next_day in holidays and is_last_week_tuesday:
            return True
    return False


def get_expiry_for_this_week():
    """
    Return this week's expiry date.
    Normal expiry = Tuesday. If Tuesday is a holiday, expiry = Monday.
    If called on Tuesday after 3:30 PM, returns NEXT Tuesday's expiry.
    """
    today  = datetime.date.today()
    days   = (1 - today.weekday()) % 7
    expiry = today + datetime.timedelta(days=days)

    if today.weekday() == 1 and datetime.datetime.now().time() >= datetime.time(15, 30):
        expiry += datetime.timedelta(days=7)

    if expiry in SPECIAL_MARKET_HOLIDAYS:
        expiry -= datetime.timedelta(days=1)   # Tuesday holiday → Monday expiry

    return expiry


def get_dte1_date():
    """
    Return the DTE1 date for this week's expiry.

    Tuesday expiry (normal):
      DTE1 = Monday. If Monday is holiday → DTE1 = Friday.

    Monday expiry (Tuesday was holiday):
      DTE1 = Friday.
    """
    expiry = get_expiry_for_this_week()

    if expiry.weekday() == 1:           # Normal Tuesday expiry
        monday = expiry - datetime.timedelta(days=1)
        if monday in SPECIAL_MARKET_HOLIDAYS:
            return monday - datetime.timedelta(days=3)  # Friday
        return monday
    else:                               # Monday expiry
        return expiry - datetime.timedelta(days=3)      # Friday


def is_dte1_today():
    return datetime.date.today() == get_dte1_date()


def format_expiry(expiry):
    yy = expiry.strftime("%y")
    if is_last_tuesday(expiry):
        return f"{yy}{expiry.strftime('%b').upper()}"
    m, d  = expiry.month, expiry.day
    m_tok = {10: "O", 11: "N", 12: "D"}.get(m, str(m))
    return f"{yy}{m_tok}{d:02d}"


# =========================================================
# MARKET DATA HELPERS
# =========================================================
def get_nifty_spot(fyers_client):
    r = fyers_client.quotes({"symbols": "NSE:NIFTY50-INDEX"})
    for item in r.get("d", []):
        lp = item.get("v", {}).get("lp")
        if lp is not None:
            return float(lp)
    raise RuntimeError(f"Could not fetch NIFTY50 spot price. Response: {r}")


def get_atm(spot):
    return round(spot / STRIKE_STEP) * STRIKE_STEP


def get_option_ltps(fyers_client, ce_sym, pe_sym):
    r = fyers_client.quotes({"symbols": f"{ce_sym},{pe_sym}"})
    ltps = {}
    for item in r.get("d", []):
        lp = item.get("v", {}).get("lp")
        if lp is not None:
            ltps[item["n"]] = float(lp)
    ce_ltp = ltps.get(ce_sym)
    pe_ltp = ltps.get(pe_sym)
    if ce_ltp is None or pe_ltp is None:
        raise RuntimeError(
            f"Option LTP fetch failed — CE={ce_sym}: {ce_ltp}, PE={pe_sym}: {pe_ltp}\n"
            f"Full response: {ltps}"
        )
    return ce_ltp, pe_ltp


def build_strangle_symbols(atm):
    expiry    = get_expiry_for_this_week()
    exp_str   = format_expiry(expiry)
    offset    = OTM4_STRIKES * STRIKE_STEP    # 200 pts
    ce_strike = atm + offset
    pe_strike = atm - offset
    ce_sym    = f"NSE:NIFTY{exp_str}{ce_strike}CE"
    pe_sym    = f"NSE:NIFTY{exp_str}{pe_strike}PE"

    logger.info(
        f"[SYMBOLS] ATM={atm} | CE (OTM4 +{offset})={ce_sym} "
        f"| PE (OTM4 -{offset})={pe_sym}"
    )
    send_telegram(
        f"📌 STRANGLE SYMBOLS (DTE1)\n"
        f"ATM       : {atm}\n"
        f"Expiry    : {expiry}\n"
        f"CE (OTM4) : {ce_sym}  [{ce_strike}]\n"
        f"PE (OTM4) : {pe_sym}  [{pe_strike}]"
    )
    return ce_sym, pe_sym


# =========================================================
# FYERS CLIENT
# =========================================================
class FyersClient:
    def __init__(self):
        self.client = fyersModel.FyersModel(
            client_id=CLIENT_ID,
            token=ACCESS_TOKEN,
            is_async=False,
            log_path="",
        )
        self.auth = f"{CLIENT_ID}:{ACCESS_TOKEN}"

    def sell_market(self, symbol, tag):
        resp = self.client.place_order({
            "symbol":      symbol,
            "qty":         LOT_SIZE * NUM_LOTS,
            "type":        2,
            "side":        -1,
            "productType": "INTRADAY",
            "validity":    "DAY",
            "orderTag":    tag,
        })
        logger.info(f"[ORDER SELL] {symbol} qty={LOT_SIZE * NUM_LOTS} tag={tag} resp={resp}")
        return resp

    def buy_market(self, symbol, tag):
        resp = self.client.place_order({
            "symbol":      symbol,
            "qty":         LOT_SIZE * NUM_LOTS,
            "type":        2,
            "side":        1,
            "productType": "INTRADAY",
            "validity":    "DAY",
            "orderTag":    tag,
        })
        logger.info(f"[ORDER BUY]  {symbol} qty={LOT_SIZE * NUM_LOTS} tag={tag} resp={resp}")
        return resp

    def place_sl_market(self, symbol, trigger_price, tag):
        """SL-M buy order to cover the short when price hits trigger_price."""
        resp = self.client.place_order({
            "symbol":      symbol,
            "qty":         LOT_SIZE * NUM_LOTS,
            "type":        3,           # SL-M
            "side":        1,           # buy to cover short
            "productType": "INTRADAY",
            "validity":    "DAY",
            "stopPrice":   round(trigger_price, 2),
            "orderTag":    tag,
        })
        order_id = resp.get("id")
        logger.info(
            f"[SLM ORDER] {symbol} trigger={trigger_price:.2f} "
            f"tag={tag} id={order_id} resp={resp}"
        )
        return order_id

    def cancel_order(self, order_id):
        resp = self.client.cancel_order({"id": str(order_id)})
        logger.info(f"[CANCEL] id={order_id} resp={resp}")
        return resp

    def get_order_status(self, order_id):
        """
        Return Fyers order status code for the given order ID.
        Codes: 2 = Traded/Filled, 1 = Cancelled, 6 = Pending. None = not found.
        """
        try:
            ob = self.client.orderbook()
            for order in ob.get("orderBook", []):
                if str(order.get("id")) == str(order_id):
                    return order.get("status")
        except Exception as e:
            logger.warning(f"[STATUS CHECK] Error querying order {order_id}: {e}")
        return None


# =========================================================
# STRANGLE ENGINE
# =========================================================
class StrangleEngine:
    """
    Manages the OTM4 CE + OTM4 PE short strangle with trailing SL.

    Flow per leg:
      1. Sell at entry → immediately place exchange SL-M buy order
      2. On each tick: if price dropped TRAIL_TRIGGER_PCT from trail reference
         → cancel old SL-M order → place new SL-M order TRAIL_STEP_PCT lower
      3. Periodic poll: detect if exchange SL-M order was already filled
      4. EOD 3:10 PM: cancel any pending SL-M orders, market-buy to close
    """

    def __init__(self, fyers, ce_sym, pe_sym):
        self.fyers  = fyers
        self.ce_sym = ce_sym
        self.pe_sym = pe_sym

        self.ce_entry = None
        self.pe_entry = None
        self.ce_sl    = None
        self.pe_sl    = None

        self.ce_open  = False
        self.pe_open  = False

        self._ce_sl_oid    = None   # exchange SL-M order ID for CE
        self._pe_sl_oid    = None
        self._ce_trail_ref = None   # price from which next 5% move is measured
        self._pe_trail_ref = None

        self._last_status_check = datetime.datetime.min

    # ----------------------------------------------------------
    def _cancel_sl_gracefully(self, order_id, leg):
        """Cancel a SL-M order, silently ignoring errors (e.g. already filled)."""
        if not order_id:
            return
        try:
            self.fyers.cancel_order(order_id)
        except Exception as e:
            logger.warning(f"[{leg} SL CANCEL] id={order_id} — {e} (may already be filled)")

    # ----------------------------------------------------------
    def enter_trade(self, ce_ltp, pe_ltp):
        self.ce_entry = ce_ltp
        self.pe_entry = pe_ltp
        self.ce_sl    = round_price(ce_ltp * (1 + SL_PCT))
        self.pe_sl    = round_price(pe_ltp * (1 + SL_PCT))

        logger.info(
            f"[ENTRY] CE entry≈{ce_ltp:.2f}  SL={self.ce_sl:.2f}  (+{int(SL_PCT*100)}%)"
        )
        logger.info(
            f"[ENTRY] PE entry≈{pe_ltp:.2f}  SL={self.pe_sl:.2f}  (+{int(SL_PCT*100)}%)"
        )
        send_telegram(
            f"📉 OTM4 STRANGLE ENTRY  {datetime.datetime.now().strftime('%H:%M:%S')}\n"
            f"Lots per leg : {NUM_LOTS}  ({LOT_SIZE * NUM_LOTS} qty)\n"
            f"\nCE (OTM4) : {self.ce_sym}\n"
            f"  Entry ≈ {ce_ltp:.2f}  |  SL = {self.ce_sl:.2f}  ({int(SL_PCT*100)}% above)\n"
            f"\nPE (OTM4) : {self.pe_sym}\n"
            f"  Entry ≈ {pe_ltp:.2f}  |  SL = {self.pe_sl:.2f}  ({int(SL_PCT*100)}% above)\n"
            f"\nTrail : every {int(TRAIL_TRIGGER_PCT*100)}% move → SL drops {int(TRAIL_STEP_PCT*100)}%"
        )

        # Place sell orders first
        self.fyers.sell_market(self.ce_sym, "D1STRCE")
        self.fyers.sell_market(self.pe_sym, "D1STRPE")

        self.ce_open = True
        self.pe_open = True

        # Trail reference starts at entry price
        self._ce_trail_ref = ce_ltp
        self._pe_trail_ref = pe_ltp

        # Place exchange SL-M orders
        self._ce_sl_oid = self.fyers.place_sl_market(self.ce_sym, self.ce_sl, "D1CESL")
        self._pe_sl_oid = self.fyers.place_sl_market(self.pe_sym, self.pe_sl, "D1PESL")

    # ----------------------------------------------------------
    def _exit_ce(self, reason):
        if not self.ce_open:
            return
        self._cancel_sl_gracefully(self._ce_sl_oid, "CE")
        self._ce_sl_oid = None
        logger.info(f"[CE EXIT] {reason}")
        send_telegram(f"🛑 CE EXIT\n{reason}\nSymbol: {self.ce_sym}")
        self.fyers.buy_market(self.ce_sym, "D1CEXIT")
        self.ce_open = False

    def _exit_pe(self, reason):
        if not self.pe_open:
            return
        self._cancel_sl_gracefully(self._pe_sl_oid, "PE")
        self._pe_sl_oid = None
        logger.info(f"[PE EXIT] {reason}")
        send_telegram(f"🛑 PE EXIT\n{reason}\nSymbol: {self.pe_sym}")
        self.fyers.buy_market(self.pe_sym, "D1PEXIT")
        self.pe_open = False

    def exit_all(self, reason):
        """Do a final exchange status check, then exit any open leg."""
        self._update_position_status()
        self._exit_ce(reason)
        self._exit_pe(reason)

    # ----------------------------------------------------------
    def _update_position_status(self):
        """
        Poll the orderbook to detect if an exchange SL-M order was filled.
        Updates ce_open / pe_open so EOD exit doesn't double-buy a closed position.
        """
        if self.ce_open and self._ce_sl_oid:
            status = self.fyers.get_order_status(self._ce_sl_oid)
            if status == 2:     # filled by exchange
                logger.info("[CE] Exchange SL-M filled — marking CE closed")
                send_telegram(
                    f"✅ CE SL filled by exchange\n"
                    f"Symbol : {self.ce_sym}\n"
                    f"SL was : {self.ce_sl:.2f}"
                )
                self.ce_open    = False
                self._ce_sl_oid = None

        if self.pe_open and self._pe_sl_oid:
            status = self.fyers.get_order_status(self._pe_sl_oid)
            if status == 2:
                logger.info("[PE] Exchange SL-M filled — marking PE closed")
                send_telegram(
                    f"✅ PE SL filled by exchange\n"
                    f"Symbol : {self.pe_sym}\n"
                    f"SL was : {self.pe_sl:.2f}"
                )
                self.pe_open    = False
                self._pe_sl_oid = None

    # ----------------------------------------------------------
    def _trail_check(self, leg, ltp):
        """
        If LTP has dropped TRAIL_TRIGGER_PCT from the trail reference:
          1. Cancel existing SL-M order
          2. Place new SL-M order at current_sl × (1 - TRAIL_STEP_PCT)
          3. Update trail reference to current LTP
        """
        trail_ref = self._ce_trail_ref if leg == "ce" else self._pe_trail_ref
        sl_price  = self.ce_sl         if leg == "ce" else self.pe_sl
        sl_oid    = self._ce_sl_oid    if leg == "ce" else self._pe_sl_oid
        sym       = self.ce_sym        if leg == "ce" else self.pe_sym
        tag       = "D1CESL"           if leg == "ce" else "D1PESL"

        trigger_level = round(trail_ref * (1 - TRAIL_TRIGGER_PCT), 2)

        if ltp > trigger_level:
            return      # price has not moved enough in our favour yet

        new_sl = round_price(sl_price * (1 - TRAIL_STEP_PCT))

        logger.info(
            f"[{leg.upper()} TRAIL] LTP={ltp:.2f} <= trigger={trigger_level:.2f} "
            f"| Old SL={sl_price:.2f} → New SL={new_sl:.2f} "
            f"| trail_ref {trail_ref:.2f} → {ltp:.2f}"
        )
        send_telegram(
            f"📐 {leg.upper()} SL TRAILED\n"
            f"LTP        : {ltp:.2f}\n"
            f"Trail ref  : {trail_ref:.2f} → {ltp:.2f}\n"
            f"Old SL     : {sl_price:.2f}\n"
            f"New SL     : {new_sl:.2f}"
        )

        # Cancel old SL-M order
        self._cancel_sl_gracefully(sl_oid, leg.upper())

        # Place new SL-M order at the lower SL
        new_oid = self.fyers.place_sl_market(sym, new_sl, tag)

        # Persist updated state
        if leg == "ce":
            self.ce_sl          = new_sl
            self._ce_sl_oid     = new_oid
            self._ce_trail_ref  = ltp
        else:
            self.pe_sl          = new_sl
            self._pe_sl_oid     = new_oid
            self._pe_trail_ref  = ltp

    # ----------------------------------------------------------
    def on_tick(self, ce_ltp=None, pe_ltp=None):
        """Called on every websocket tick."""
        now = datetime.datetime.now()

        # EOD force-exit
        if now.time() >= TRADING_END:
            if self.ce_open or self.pe_open:
                self.exit_all("EOD 3:10 PM force-exit")
            return

        # Periodic exchange order status poll (every STATUS_CHECK_SECS seconds)
        if (now - self._last_status_check).total_seconds() >= STATUS_CHECK_SECS:
            self._update_position_status()
            self._last_status_check = now

        # Trail check for each open leg
        if ce_ltp is not None and self.ce_open:
            self._trail_check("ce", ce_ltp)

        if pe_ltp is not None and self.pe_open:
            self._trail_check("pe", pe_ltp)

    @property
    def all_closed(self):
        return not self.ce_open and not self.pe_open


# =========================================================
# MAIN
# =========================================================
if __name__ == "__main__":
    logger.info("[BOOT] NIFTY OTM4 STRANGLE DTE1 STRATEGY STARTED")
    send_telegram("🚀 NIFTY OTM4 STRANGLE DTE1 STRATEGY STARTED")

    # ── DTE1 gate ──
    if not is_dte1_today():
        dte1   = get_dte1_date()
        expiry = get_expiry_for_this_week()
        logger.info(
            f"[ABORT] Today ({datetime.date.today()}) is NOT DTE1. "
            f"DTE1={dte1}  Expiry={expiry}. Exiting."
        )
        send_telegram(
            f"⛔ NOT DTE1 — strategy will not run today.\n"
            f"DTE1 date : {dte1}\n"
            f"Expiry    : {expiry}"
        )
        sys.exit(0)

    expiry_date = get_expiry_for_this_week()
    dte1_date   = get_dte1_date()
    logger.info(f"[DTE1] Confirmed DTE1={dte1_date}  Expiry={expiry_date}")
    send_telegram(f"✅ DTE1 confirmed\nToday  : {dte1_date}\nExpiry : {expiry_date}")

    # ── Guard: don't run if already past exit time ──
    if datetime.datetime.now().time() >= TRADING_END:
        logger.info("[ABORT] Already past 3:10 PM trading end. Exiting.")
        sys.exit(0)

    fyers = FyersClient()

    # ── Wait until 9:16 AM ──
    logger.info(f"[WAIT] Waiting until {ENTRY_TIME} ...")
    while datetime.datetime.now().time() < ENTRY_TIME:
        time.sleep(1)

    # ── Determine ATM from live NIFTY spot ──
    spot = get_nifty_spot(fyers.client)
    atm  = get_atm(spot)
    logger.info(f"[ATM] NIFTY spot={spot:.2f} → ATM={atm}")
    send_telegram(f"📊 NIFTY Spot = {spot:.2f}  →  ATM = {atm}")

    # ── Build option symbols ──
    ce_sym, pe_sym = build_strangle_symbols(atm)

    # ── Fetch pre-entry LTPs ──
    ce_ltp, pe_ltp = get_option_ltps(fyers.client, ce_sym, pe_sym)
    logger.info(f"[PRE-ENTRY LTP] CE={ce_ltp:.2f}  PE={pe_ltp:.2f}")

    # ── Enter trade and place initial SL-M orders ──
    engine = StrangleEngine(fyers, ce_sym, pe_sym)
    engine.enter_trade(ce_ltp, pe_ltp)

    # ── Websocket for live tick monitoring ──
    last_ltp = {}

    def on_tick(msg):
        if "symbol" not in msg or "ltp" not in msg:
            return
        sym = msg["symbol"]
        if sym not in (ce_sym, pe_sym):
            return
        last_ltp[sym] = float(msg["ltp"])
        engine.on_tick(
            ce_ltp=last_ltp.get(ce_sym),
            pe_ltp=last_ltp.get(pe_sym),
        )

    def on_open():
        ws.subscribe(symbols=[ce_sym, pe_sym], data_type="SymbolUpdate")
        ws.keep_running()

    ws = data_ws.FyersDataSocket(
        access_token=fyers.auth,
        on_connect=on_open,
        on_message=on_tick,
        log_path="",
    )
    ws.connect()
