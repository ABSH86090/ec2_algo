"""
NIFTY ATM 50%-RETRACEMENT SELL STRATEGY  (market-order version)
===================================================================
First 30-minute candle (9:15-9:45) on the ATM strike, checked independently
for CE and PE (same ATM strike for both).

━━━ SETUP (once, at 9:45 AM) ━━━
  1. ATM strike = round(NIFTY50 30-min candle (9:15-9:45) CLOSE / 50) * 50
  2. For EACH of ATM-CE and ATM-PE, independently:
       - Pull that option's own 9:15-9:45 candle.
       - If it is NOT a red candle (close >= open)  → no trade on that leg.
       - If it IS red:
           high, low   = candle high/low
           length      = high - low
           mid         = low + 0.5 * length            (50% retracement level)
           sl_price     = high                          (stop, above entry)
           sl_distance  = high - mid
           target_price = mid - 2 * sl_distance          (2:1 reward:risk, below entry)

━━━ ENTRY ━━━
  No resting limit order. The live option price is tracked on every tick;
  the moment live LTP rises to >= mid, a MARKET sell order is fired
  immediately (entry price = that live tick, effectively at/near mid).

━━━ EXIT (only three ways, once in a position) ━━━
  1. Target hit : live LTP <= target_price
  2. SL hit     : live LTP >= sl_price
  3. Safety net : 3:14 PM — force market-exit if still in a position;
                  if the entry level was never reached, the leg is simply
                  abandoned for the day (no order was ever placed).

Only ONE trade per leg (CE / PE) per day.
Qty per order = 1 (LOT_SIZE below — adjust to your broker's real lot qty
if the API expects total share count instead of "1").
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

LOT_SIZE    = 65     # qty sent to the API — adjust if your broker needs the
                     # actual share count for 1 lot instead of "1".
STRIKE_STEP = 50

FIRST_CANDLE_START = datetime.time(9, 15)
SETUP_WAIT_TIME    = datetime.time(9, 45)   # wait for the 30-min candle to close
SAFETY_TIME        = datetime.time(15, 14)  # EOD safety net

HISTORY_RESOLUTION = "30"

LOG_FILE = "nifty_atm_retracement_sell.log"

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
# EXPIRY / SYMBOL UTILS
# =========================================================
SPECIAL_MARKET_HOLIDAYS = {
    datetime.date(2026, 1, 26), datetime.date(2026, 3, 3), datetime.date(2026, 3, 26),
    datetime.date(2026, 3, 31), datetime.date(2026, 4, 14), datetime.date(2026, 5, 1),
    datetime.date(2026, 5, 28), datetime.date(2026, 6, 26), datetime.date(2026, 9, 14),
    datetime.date(2026, 10, 2), datetime.date(2026, 11, 24), datetime.date(2026, 12, 25),
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


def get_next_expiry():
    today = datetime.date.today()
    days  = (1 - today.weekday()) % 7
    expiry = today + datetime.timedelta(days=days)
    if today.weekday() == 1 and datetime.datetime.now().time() >= datetime.time(15, 30):
        expiry += datetime.timedelta(days=7)
    if expiry in SPECIAL_MARKET_HOLIDAYS:
        expiry -= datetime.timedelta(days=1)
    return expiry


def format_expiry(expiry):
    yy = expiry.strftime("%y")
    if is_last_tuesday(expiry):
        return f"{yy}{expiry.strftime('%b').upper()}"
    m, d  = expiry.month, expiry.day
    m_tok = {10: "O", 11: "N", 12: "D"}.get(m, str(m))
    return f"{yy}{m_tok}{d:02d}"


def get_first_30min_candle(fyers_client, symbol):
    """Fetch the 9:15-9:45 30-min candle for `symbol` (today)."""
    today = datetime.date.today()
    r = fyers_client.history({
        "symbol":      symbol,
        "resolution":  HISTORY_RESOLUTION,
        "date_format": "1",
        "range_from":  today.strftime("%Y-%m-%d"),
        "range_to":    today.strftime("%Y-%m-%d"),
        "cont_flag":   "1",
    })
    for c in r.get("candles", []):
        ts = datetime.datetime.fromtimestamp(c[0])
        if ts.time() == FIRST_CANDLE_START:
            return {"time": ts, "open": c[1], "high": c[2], "low": c[3], "close": c[4]}
    raise RuntimeError(f"9:15 30-min candle not found for {symbol} — check market is open")


def get_atm_from_first_candle(fyers_client):
    candle = get_first_30min_candle(fyers_client, "NSE:NIFTY50-INDEX")
    atm = round(candle["close"] / STRIKE_STEP) * STRIKE_STEP
    logger.info(f"[ATM] 30-min candle (9:15-9:45) close={candle['close']:.2f} → ATM={atm}")
    send_telegram(f"📊 30-MIN CANDLE (9:15-9:45)\nClose={candle['close']:.2f} → ATM Strike={atm}")
    return atm


def build_symbol(strike, opt_type):
    expiry = format_expiry(get_next_expiry())
    return f"NSE:NIFTY{expiry}{strike}{opt_type}"


# =========================================================
# FYERS CLIENT
# =========================================================
class FyersClient:
    def __init__(self):
        self.client = fyersModel.FyersModel(
            client_id=CLIENT_ID,
            token=ACCESS_TOKEN,
            is_async=False,
            log_path=""
        )
        self.auth = f"{CLIENT_ID}:{ACCESS_TOKEN}"

    def sell_market(self, symbol, tag):
        return self.client.place_order({
            "symbol":      symbol,
            "qty":         LOT_SIZE,
            "type":        2,          # 2 = Market order
            "side":        -1,
            "productType": "INTRADAY",
            "validity":    "DAY",
            "orderTag":    tag,
        })

    def buy_market(self, symbol, tag):
        return self.client.place_order({
            "symbol":      symbol,
            "qty":         LOT_SIZE,
            "type":        2,
            "side":        1,
            "productType": "INTRADAY",
            "validity":    "DAY",
            "orderTag":    tag,
        })


# =========================================================
# OPTION LEG STRATEGY  (one instance per leg — CE and PE run independently)
# =========================================================
class OptionLegStrategy:
    """
    states: "waiting"     (tracking live price, not yet at the entry level)
            "in_position" (sold at market, monitoring SL/target)
            "done"        (exited, or abandoned at EOD without ever entering)
    """

    def __init__(self, fyers, symbol, label, high, low, mid, sl_price, target_price):
        self.fyers  = fyers
        self.symbol = symbol
        self.label  = label   # "CE" or "PE"

        self.high          = high
        self.low           = low
        self.mid           = mid
        self.sl_price      = sl_price
        self.target_price  = target_price

        self.state        = "waiting"
        self.entry_price  = None

        logger.info(
            f"[{self.label} SETUP] symbol={self.symbol} H={high:.2f} L={low:.2f} "
            f"entry_level={mid:.2f} sl={sl_price:.2f} target={target_price:.2f}"
        )
        send_telegram(
            f"📌 {self.label} SETUP — {self.symbol}\n"
            f"Candle: H={high:.2f} L={low:.2f}\n"
            f"Entry level (50%) = {mid:.2f}  (tracking live price to sell at market)\n"
            f"SL     = {sl_price:.2f}\n"
            f"Target = {target_price:.2f}  (2x SL distance)"
        )

    def _enter(self, ltp):
        logger.info(
            f"[{self.label} ENTRY] symbol={self.symbol} live_price={ltp:.2f} "
            f">= entry_level={self.mid:.2f} — selling at market"
        )
        send_telegram(
            f"📉 {self.label} ENTRY — {self.symbol}\n"
            f"Live price {ltp:.2f} reached entry level {self.mid:.2f} — SOLD at market\n"
            f"SL     = {self.sl_price:.2f}\n"
            f"Target = {self.target_price:.2f}"
        )
        tag = f"ATM50{self.label}"
        self.fyers.sell_market(self.symbol, f"{tag}SELL")
        self.entry_price = ltp
        self.state = "in_position"

    def _exit(self, reason):
        logger.info(f"[{self.label} EXIT] symbol={self.symbol} {reason}")
        send_telegram(f"🛑 {self.label} EXIT — {self.symbol}\nReason: {reason}")
        tag = f"ATM50{self.label}"
        self.fyers.buy_market(self.symbol, f"{tag}BUY")
        self.state = "done"

    def on_tick(self, ltp, now_dt):
        # ── EOD safety net ──
        if now_dt.time() >= SAFETY_TIME:
            if self.state == "in_position":
                self._exit("3:14 PM safety-net force-exit")
            elif self.state == "waiting":
                logger.info(f"[{self.label} SAFETY] entry level never reached — no trade")
                send_telegram(f"🕒 {self.label} SAFETY NET — entry level never reached, no trade ({self.symbol})")
                self.state = "done"
            return

        if self.state == "waiting":
            if ltp >= self.mid:
                self._enter(ltp)
            return

        if self.state == "in_position":
            if ltp <= self.target_price:
                self._exit(f"TARGET HIT — live price {ltp:.2f} <= target {self.target_price:.2f}")
            elif ltp >= self.sl_price:
                self._exit(f"SL HIT — live price {ltp:.2f} >= SL {self.sl_price:.2f}")


# =========================================================
# MAIN
# =========================================================
if __name__ == "__main__":
    logger.info("[BOOT] NIFTY ATM 50%-RETRACEMENT SELL STRATEGY STARTED")
    send_telegram("🚀 NIFTY ATM 50%-RETRACEMENT SELL STRATEGY STARTED")

    fyers = FyersClient()

    # ── Step 1: wait for the first 30-min candle (9:15-9:45) to close ──
    logger.info(f"[WAIT] Waiting until {SETUP_WAIT_TIME} for the 30-min candle to close...")
    while datetime.datetime.now().time() < SETUP_WAIT_TIME:
        time.sleep(1)

    # ── Step 2: ATM strike from NIFTY's own first 30-min candle ──
    atm = get_atm_from_first_candle(fyers.client)
    ce_symbol = build_symbol(atm, "CE")
    pe_symbol = build_symbol(atm, "PE")
    logger.info(f"[SYMBOLS] ATM={atm} CE={ce_symbol} PE={pe_symbol}")
    send_telegram(f"📌 ATM={atm}\nCE: {ce_symbol}\nPE: {pe_symbol}")

    # ── Step 3: evaluate each leg's own first 30-min candle ──
    engines = {}
    for label, symbol in [("CE", ce_symbol), ("PE", pe_symbol)]:
        candle = get_first_30min_candle(fyers.client, symbol)
        is_red = candle["close"] < candle["open"]
        logger.info(
            f"[{label} CANDLE] O={candle['open']:.2f} H={candle['high']:.2f} "
            f"L={candle['low']:.2f} C={candle['close']:.2f} red={is_red}"
        )
        if not is_red:
            logger.info(f"[{label}] first candle not red — no trade on this leg")
            send_telegram(f"⚪ {label} — first 30-min candle not red, skipping this leg")
            continue

        high, low = candle["high"], candle["low"]
        length     = high - low
        mid        = low + 0.5 * length
        sl_price   = high
        sl_dist    = high - mid
        target     = mid - 2 * sl_dist

        engines[symbol] = OptionLegStrategy(fyers, symbol, label, high, low, mid, sl_price, target)

    if not engines:
        logger.info("[DONE] Neither CE nor PE had a red first candle — nothing to trade today.")
        send_telegram("⚪ Neither CE nor PE had a red first candle — no trades today.")
        sys.exit(0)

    # ── Step 4: websocket — track live LTP for entry / SL / target / safety-net ──
    SUBSCRIBED_SYMBOLS = list(engines.keys())

    def on_tick(msg):
        if "symbol" not in msg or "ltp" not in msg:
            return
        sym = msg["symbol"]
        if sym not in engines:
            return
        engines[sym].on_tick(msg["ltp"], datetime.datetime.now())

    def on_open():
        ws.subscribe(symbols=SUBSCRIBED_SYMBOLS, data_type="SymbolUpdate")
        ws.keep_running()

    ws = data_ws.FyersDataSocket(
        access_token=fyers.auth,
        on_connect=on_open,
        on_message=on_tick,
        log_path=""
    )
    ws.connect()
