"""
NIFTY OTM STRANGLE SELL — DTE0 ONLY
======================================
Sell OTM2 CE + OTM3 PE at 9:16 AM on expiry day (Tuesday, or Monday if Tuesday
is a market holiday).

Per-leg SL = 100% of each leg's individual entry price (legs exit independently).
Force-exit at 3:10 PM for any remaining open positions.
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

LOT_SIZE        = 65           # NIFTY lot size — verify with NSE before running
NUM_LOTS        = 4            # Number of lots to trade per leg
STRIKE_STEP     = 50           # NIFTY strike spacing in points
OTM2_CE_STRIKES = 2            # CE leg : ATM + 2×50 = ATM+100
OTM3_PE_STRIKES = 3            # PE leg : ATM − 3×50 = ATM−150

ENTRY_TIME  = datetime.time(9, 16)
TRADING_END = datetime.time(15, 10)   # 3:10 PM force-exit

TICK_SIZE = 0.05               # NIFTY options minimum price movement

LOG_FILE = "nifty_otm_strangle_dte0.log"


def round_price(price):
    """Round price to the nearest NIFTY options tick (0.05)."""
    return round(round(price / TICK_SIZE) * TICK_SIZE, 2)

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


def get_expiry_for_today():
    """
    Return this week's expiry date.
    Normal expiry = Tuesday. If Tuesday is a holiday, expiry = Monday.
    If called on Tuesday after 3:30 PM, returns NEXT Tuesday's expiry.
    """
    today  = datetime.date.today()
    days   = (1 - today.weekday()) % 7     # days until the coming Tuesday
    expiry = today + datetime.timedelta(days=days)

    # After market close on expiry day, roll to next week
    if today.weekday() == 1 and datetime.datetime.now().time() >= datetime.time(15, 30):
        expiry += datetime.timedelta(days=7)

    # Tuesday is a holiday → move expiry to Monday
    if expiry in SPECIAL_MARKET_HOLIDAYS:
        expiry -= datetime.timedelta(days=1)

    return expiry


def is_dte0_today():
    """True if today is the expiry date (DTE0)."""
    return datetime.date.today() == get_expiry_for_today()


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
    """Return live NIFTY50 index price."""
    r = fyers_client.quotes({"symbols": "NSE:NIFTY50-INDEX"})
    for item in r.get("d", []):
        lp = item.get("v", {}).get("lp")
        if lp is not None:
            return float(lp)
    raise RuntimeError(f"Could not fetch NIFTY50 spot price. Response: {r}")


def get_atm(spot):
    return round(spot / STRIKE_STEP) * STRIKE_STEP


def get_option_ltps(fyers_client, ce_sym, pe_sym):
    """Return (ce_ltp, pe_ltp) from the quotes API."""
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
    """Build OTM2 CE and OTM3 PE Fyers symbol strings for today's expiry."""
    expiry     = get_expiry_for_today()
    exp_str    = format_expiry(expiry)
    ce_strike  = atm + OTM2_CE_STRIKES * STRIKE_STEP   # e.g. ATM + 100
    pe_strike  = atm - OTM3_PE_STRIKES * STRIKE_STEP   # e.g. ATM - 150
    ce_sym     = f"NSE:NIFTY{exp_str}{ce_strike}CE"
    pe_sym     = f"NSE:NIFTY{exp_str}{pe_strike}PE"

    logger.info(
        f"[SYMBOLS] ATM={atm} | CE (OTM2, +{OTM2_CE_STRIKES*STRIKE_STEP})={ce_sym} "
        f"| PE (OTM3, -{OTM3_PE_STRIKES*STRIKE_STEP})={pe_sym}"
    )
    send_telegram(
        f"📌 STRANGLE SYMBOLS\n"
        f"ATM       : {atm}\n"
        f"CE (OTM2) : {ce_sym}  [{ce_strike}]\n"
        f"PE (OTM3) : {pe_sym}  [{pe_strike}]"
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
        logger.info(f"[ORDER SELL] {symbol} tag={tag} resp={resp}")
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
        logger.info(f"[ORDER BUY] {symbol} tag={tag} resp={resp}")
        return resp


# =========================================================
# STRANGLE ENGINE
# =========================================================
class StrangleEngine:
    """
    Holds the short strangle position and manages per-leg SL.

    CE and PE are independent: either can hit SL while the other stays open.
    SL for each leg = 100% of its entry LTP (i.e. price doubles → exit).
    """

    def __init__(self, fyers, ce_sym, pe_sym):
        self.fyers  = fyers
        self.ce_sym = ce_sym
        self.pe_sym = pe_sym

        self.ce_entry = None
        self.pe_entry = None
        self.ce_sl    = None    # ce_entry × 2
        self.pe_sl    = None    # pe_entry × 2

        self.ce_open  = False
        self.pe_open  = False

    # ----------------------------------------------------------
    def enter_trade(self, ce_ltp, pe_ltp):
        self.ce_entry = ce_ltp
        self.pe_entry = pe_ltp
        self.ce_sl    = round_price(ce_ltp * 2)
        self.pe_sl    = round_price(pe_ltp * 2)

        logger.info(
            f"[ENTRY] CE entry≈{ce_ltp:.2f}  SL={self.ce_sl:.2f}  (+100%)"
        )
        logger.info(
            f"[ENTRY] PE entry≈{pe_ltp:.2f}  SL={self.pe_sl:.2f}  (+100%)"
        )
        send_telegram(
            f"📉 OTM STRANGLE ENTRY  {datetime.datetime.now().strftime('%H:%M:%S')}\n"
            f"\nCE (OTM2) : {self.ce_sym}\n"
            f"  Entry ≈ {ce_ltp:.2f}  |  SL = {self.ce_sl:.2f}  (100% above)\n"
            f"\nPE (OTM3) : {self.pe_sym}\n"
            f"  Entry ≈ {pe_ltp:.2f}  |  SL = {self.pe_sl:.2f}  (100% above)"
        )

        # Sell CE first, then PE
        self.fyers.sell_market(self.ce_sym, "STRCE")
        self.fyers.sell_market(self.pe_sym, "STRPE")

        self.ce_open = True
        self.pe_open = True

    # ----------------------------------------------------------
    def _exit_ce(self, reason):
        if not self.ce_open:
            return
        logger.info(f"[CE EXIT] {reason}")
        send_telegram(f"🛑 CE EXIT\n{reason}\nSymbol: {self.ce_sym}")
        self.fyers.buy_market(self.ce_sym, "STRCEXIT")
        self.ce_open = False

    def _exit_pe(self, reason):
        if not self.pe_open:
            return
        logger.info(f"[PE EXIT] {reason}")
        send_telegram(f"🛑 PE EXIT\n{reason}\nSymbol: {self.pe_sym}")
        self.fyers.buy_market(self.pe_sym, "STRPEXIT")
        self.pe_open = False

    def exit_all(self, reason):
        self._exit_ce(reason)
        self._exit_pe(reason)

    # ----------------------------------------------------------
    def on_tick(self, ce_ltp=None, pe_ltp=None):
        """
        Called on every websocket tick.
        Checks EOD exit first, then per-leg SL independently.
        """
        now = datetime.datetime.now()

        if now.time() >= TRADING_END:
            if self.ce_open or self.pe_open:
                self.exit_all("EOD 3:10 PM force-exit")
            return

        if ce_ltp is not None and self.ce_open:
            if ce_ltp >= self.ce_sl:
                self._exit_ce(
                    f"SL hit — CE live {ce_ltp:.2f} >= SL {self.ce_sl:.2f} "
                    f"(entry {self.ce_entry:.2f})"
                )

        if pe_ltp is not None and self.pe_open:
            if pe_ltp >= self.pe_sl:
                self._exit_pe(
                    f"SL hit — PE live {pe_ltp:.2f} >= SL {self.pe_sl:.2f} "
                    f"(entry {self.pe_entry:.2f})"
                )

    @property
    def all_closed(self):
        return not self.ce_open and not self.pe_open


# =========================================================
# MAIN
# =========================================================
if __name__ == "__main__":
    logger.info("[BOOT] NIFTY OTM STRANGLE DTE0 STRATEGY STARTED")
    send_telegram("🚀 NIFTY OTM STRANGLE DTE0 STRATEGY STARTED")

    # ── DTE0 gate: only run on expiry day ──
    if not is_dte0_today():
        expiry = get_expiry_for_today()
        logger.info(
            f"[ABORT] Today ({datetime.date.today()}) is NOT an expiry day. "
            f"Next expiry: {expiry}. Exiting."
        )
        send_telegram(
            f"⛔ NOT DTE0 — strategy will not run today.\n"
            f"Next expiry: {expiry}"
        )
        sys.exit(0)

    expiry_date = get_expiry_for_today()
    logger.info(f"[DTE0] Confirmed expiry day: {expiry_date}")
    send_telegram(f"✅ DTE0 confirmed — expiry {expiry_date}")

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

    # ── Fetch pre-entry LTPs (used as entry price reference for SL) ──
    ce_ltp, pe_ltp = get_option_ltps(fyers.client, ce_sym, pe_sym)
    logger.info(f"[PRE-ENTRY LTP] CE={ce_ltp:.2f}  PE={pe_ltp:.2f}")

    # ── Place sells and initialise engine ──
    engine = StrangleEngine(fyers, ce_sym, pe_sym)
    engine.enter_trade(ce_ltp, pe_ltp)

    # ── Websocket for live SL / EOD monitoring ──
    last_ltp = {}   # {symbol: ltp}

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
