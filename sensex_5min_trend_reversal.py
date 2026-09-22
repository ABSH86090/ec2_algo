# =========================================================
# SENSEX ITM1 CE/PE  +  20% RALLY / RETEST BREAKOUT
# 5-MIN TIMEFRAME, LIVE (WEBSOCKET)
# =========================================================
#
# STRATEGY SUMMARY
# -----------------
# 1. At 09:16 AM, capture Sensex spot LTP. Round to nearest strike (step 100)
#    to get ATM, then derive:
#       ITM1 CE strike = ATM - 100   (1 strike ITM for a call)
#       ITM1 PE strike = ATM + 100   (1 strike ITM for a put)
#    These two option symbols are tracked independently for the rest of
#    the day. No new strikes are picked later in the day.
#
# 2. On each leg's OWN 5-min candle series (built from that option's own
#    premium ticks), find an "identifying candle":
#       - Every closed candle since the last reset is a base candidate.
#       - A candidate QUALIFIES once some LATER candle's HIGH reaches
#         >= 120% of the candidate's low (a +20% rally off that low). A
#         candle can never confirm itself off its own high/low range --
#         only a subsequent candle's high counts.
#       - When more than one candidate qualifies at once (e.g. a fast
#         move that jumps past several +20% thresholds within one
#         candle), the one with the LOWEST LOW is chosen as the
#         identifying candle -- the true start of that swing/trend, not
#         whichever candle happens to be latest.
#
# 3. Once confirmed, wait for a RETEST:
#       - The first GREEN candle (close > open) whose LOW falls within
#         -10% / +5% of the identifying candle's low triggers an entry --
#         PROVIDED the candle immediately before it is RED (close < open).
#         A green candle in the band that is preceded by another green
#         candle does NOT trigger; the engine keeps waiting.
#       - If price breaks back BELOW the identifying low before any such
#         green candle forms, the identifying candle is invalidated and
#         the search restarts from that new (lower) candle.
#
# 4. ENTRY: buy the option at the trigger (green) candle's CLOSE.
#       Initial SL = trigger candle's low - 2 points.
#       No new entries are taken after 3:00 PM.
#
# 5. TRAILING SL: after entry, every subsequent GREEN candle whose LOW is
#    higher than the LOW of the last green candle formed (the trigger
#    candle counts as the first "last green candle") shifts SL up to
#    this candle's low, and this candle becomes the new "last green
#    candle" for the next comparison.
#
# 6. EXIT / RESULT:
#       - SL hit while it has been trailed at least once  -> WIN
#       - SL hit at the ORIGINAL (never-shifted) level    -> LOSS
#       - Any open position at 3:00 PM is force-closed (TIME exit),
#         scored the same win/loss way based on whether it had trailed,
#         and any pending orders for that leg are cancelled.
#
# 7. PER-LEG TRADE LIMITS (CE and PE tracked independently):
#       - Max 2 LOSS trades per leg  -> leg stops trading for the day.
#       - Max 3 trades total per leg -> leg stops trading for the day.
#       (whichever limit is hit first stops the leg; 3 is a hard ceiling,
#       not "3 more on top of losses").
#    After a trade closes, if the leg is still allowed to trade, it
#    resets and starts searching for a brand-new identifying candle.
#
# 8. The SENSEX INDEX is used ONLY ONCE, at 09:16, to compute ATM and
#    derive the ITM1 CE/PE strikes. Everything else is computed purely
#    from each option contract's own premium data.
# =========================================================

import datetime
import time as time_module
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

INDEX_SYMBOL = "BSE:SENSEX-INDEX"

TIMEFRAME_MIN = 5          # candle timeframe for pattern detection
STRIKE_STEP = 100          # Sensex option strike interval
ITM_OFFSET = 100           # 1 strike ITM = 100 points

DECISION_TIME = datetime.time(9, 16, 0)     # when ITM1 strikes are locked in
NO_NEW_TRADES_TIME = datetime.time(15, 0)   # no new entries taken from this time onward
HARD_EXIT_TIME = datetime.time(15, 0)       # any open position force-closed at this time

RALLY_PCT = 0.20            # +20% rally from a candle's low confirms it
RETEST_BAND_LOWER_PCT = 0.10   # retest trigger band: up to 10% BELOW identifying low
RETEST_BAND_UPPER_PCT = 0.05   # retest trigger band: up to 5% ABOVE identifying low
SL_BUFFER = 2                # points below trigger/trailing low for SL

MAX_LOSSES_PER_LEG = 2
MAX_TRADES_PER_LEG = 3

LOT_SIZE = 20
LOTS = 1
QTY = LOT_SIZE * LOTS

LOG_FILE = "sensex_itm1_retest_breakout.log"

# ================= LOGGING =================
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


# ================= TELEGRAM =================
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


# ================= FYERS =================
def sanitize_order_tag(tag):
    """Fyers order tags must be plain alphanumeric. Strip anything else
    (spaces, emoji, punctuation, etc.) so a bad tag never reaches the
    order API."""
    cleaned = "".join(ch for ch in str(tag) if ch.isalnum())
    return cleaned[:20] if cleaned else "TRADE"


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
            "orderTag": sanitize_order_tag(tag)
        })

    def sell_mkt(self, symbol, qty, tag):
        return self.client.place_order({
            "symbol": symbol,
            "qty": qty,
            "type": 2,
            "side": -1,
            "productType": "INTRADAY",
            "validity": "DAY",
            "orderTag": sanitize_order_tag(tag)
        })

    def buy(self, symbol, qty, tag):
        return self.buy_mkt(symbol, qty, tag)

    def sell(self, symbol, qty, tag):
        return self.sell_mkt(symbol, qty, tag)

    def get_pending_orders(self, symbol=None):
        """Fyers order status codes: 1=Cancelled, 2=Traded, 5=Rejected,
        6=Pending. Anything not in the finished set (1, 2, 5) is treated
        as still-open/pending."""
        try:
            resp = self.client.orderbook()
        except Exception as e:
            logger.warning(f"orderbook() failed: {e}")
            return []

        orders = (resp or {}).get("orderBook", []) or []
        finished_statuses = (1, 2, 5)
        pending = [o for o in orders if o.get("status") not in finished_statuses]
        if symbol:
            pending = [o for o in pending if o.get("symbol") == symbol]
        return pending

    def cancel_pending_orders(self, symbol=None):
        """Cancel every pending order (optionally filtered to one symbol)."""
        pending = self.get_pending_orders(symbol)
        for o in pending:
            order_id = o.get("id")
            try:
                self.client.cancel_order({"id": order_id})
                logger.info(f"Cancelled pending order {order_id} for {o.get('symbol')}")
            except Exception as e:
                logger.warning(f"Failed to cancel order {order_id}: {e}")
        return pending


# ================= EXPIRY HELPERS =================

SPECIAL_MARKET_HOLIDAYS = {
    datetime.date(2026, 1, 26), datetime.date(2026, 3, 3), datetime.date(2026, 3, 26),
    datetime.date(2026, 3, 31), datetime.date(2026, 4, 14), datetime.date(2026, 5, 1),
    datetime.date(2026, 5, 28), datetime.date(2026, 6, 26), datetime.date(2026, 9, 14),
    datetime.date(2026, 10, 2), datetime.date(2026, 11, 24), datetime.date(2026, 12, 25),
}


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


# ================= ITM1 STRIKE SELECTION =================
def get_itm1_symbols(fyers):
    """Called once, at/after DECISION_TIME, using the current spot LTP."""
    q = fyers.client.quotes({"symbols": INDEX_SYMBOL})
    index_ltp = float(q["d"][0]["v"]["lp"])

    atm = round(index_ltp / STRIKE_STEP) * STRIKE_STEP

    itm1_ce_strike = atm - ITM_OFFSET   # ITM for a call = strike below spot
    itm1_pe_strike = atm + ITM_OFFSET   # ITM for a put  = strike above spot

    expiry = get_next_expiry()
    exp_token = format_expiry(expiry)

    itm1_ce = f"BSE:SENSEX{exp_token}{itm1_ce_strike}CE"
    itm1_pe = f"BSE:SENSEX{exp_token}{itm1_pe_strike}PE"

    msg = (
        f"📌 ITM1 STRIKE SELECTION (09:16 spot)\n"
        f"Index LTP : {index_ltp}\n"
        f"ATM       : {atm}\n"
        f"Expiry    : {expiry} ({exp_token})\n"
        f"ITM1 CE   : {itm1_ce}\n"
        f"ITM1 PE   : {itm1_pe}"
    )
    logger.info(msg)
    send_telegram(msg)

    return itm1_ce, itm1_pe, index_ltp


# ================= 20% RALLY / RETEST ENGINE =================
class RetestEngine:
    """
    Runs on ONE instrument's own 5-min candles (CE premium or PE premium).

    State machine: SEARCH_BASE -> SEARCH_TRIGGER -> (external trade) -> reset

    SEARCH_BASE:
        Every closed candle since the last reset is kept as a pending
        base candidate (`pending_candidates`, oldest to newest). On each
        new candle, all pending candidates are checked against this
        candle's HIGH: a candidate QUALIFIES once
        candle.high >= candidate.low * (1 + RALLY_PCT). When more than
        one candidate qualifies (e.g. a fast move that jumps past
        several thresholds in one candle), the one with the LOWEST LOW
        is chosen as the "identifying candle" -- i.e. the true start of
        that swing/trend, not whichever happens to be most recent.

    SEARCH_TRIGGER:
        Waits for the first GREEN candle whose LOW falls within
        +/- RETEST_BAND_PCT of the identifying candle's low.
        If price breaks back below the identifying low first, the
        identifying candle is invalidated and the search restarts from
        that new candle (base-candidate list reset to just it).
    """

    def __init__(self, label):
        self.label = label
        self.candles = deque(maxlen=300)
        self.candle_index = -1

        self.state = "SEARCH_BASE"
        self.pending_candidates = []   # base candidates not yet confirmed, oldest -> newest
        self.identified_candle = None

        self.leg_active = True     # False once loss/trade caps, or 3pm cutoff, are hit
        self.trades_taken = 0
        self.losses = 0
        self.busy = False          # True while a trade from this leg is open

    def start_new_search(self):
        """Call after a trade closes (and the leg is still allowed to trade)."""
        self.state = "SEARCH_BASE"
        self.pending_candidates = []
        self.identified_candle = None
        self.busy = False

    def on_new_candle(self, candle):
        """candle = the candle that JUST closed.
        Returns a trigger signal dict {trigger_candle, identified_candle}
        if a valid retest entry happens on this candle, else None."""

        self.candle_index += 1
        self.candles.append(candle)

        if not self.leg_active or self.busy:
            return None

        if self.state == "SEARCH_BASE":
            # Find every PRIOR pending candidate whose +20% rally target
            # has been reached by this candle's high, and lock onto the
            # one with the LOWEST LOW among them -- the true start of the
            # swing/trend. A candle can only confirm an EARLIER candle's
            # low; it can never confirm itself off its own high/low range
            # -- "a later candle rallies off an earlier low" is the whole
            # point, not one volatile candle's own intrabar swing.
            qualifying = [
                cand for cand in self.pending_candidates
                if candle["high"] >= cand["low"] * (1 + RALLY_PCT)
            ]

            confirmed = min(qualifying, key=lambda c: c["low"]) if qualifying else None

            self.pending_candidates.append(candle)

            if confirmed is not None:
                self.identified_candle = confirmed
                self.state = "SEARCH_TRIGGER"
                self.pending_candidates = []
                logger.info(
                    f"[{self.label}] IDENTIFYING CANDLE CONFIRMED (lowest low of qualifying set) -> "
                    f"low={self.identified_candle['low']} ({self.identified_candle['time']}); "
                    f"+{RALLY_PCT*100:.0f}% rally hit by candle high={candle['high']} ({candle['time']})"
                )

        elif self.state == "SEARCH_TRIGGER":
            id_low = self.identified_candle["low"]
            lower_band = id_low * (1 - RETEST_BAND_LOWER_PCT)
            upper_band = id_low * (1 + RETEST_BAND_UPPER_PCT)

            # invalidation: price broke below the LOWER BAND (not just below
            # the bare identifying low -- the band intentionally allows up
            # to 10% below it as still-valid retest territory).
            if candle["low"] < lower_band:
                logger.info(
                    f"[{self.label}] price broke below the retest band (low={candle['low']}, "
                    f"band floor={round(lower_band,2)}) @ {candle['time']}; "
                    f"restarting base search from this candle."
                )
                self.state = "SEARCH_BASE"
                self.identified_candle = None
                self.pending_candidates = [candle]
                return None

            is_green = candle["close"] > candle["open"]

            prev_candle = self.candles[-2] if len(self.candles) >= 2 else None
            prev_is_red = prev_candle is not None and prev_candle["close"] < prev_candle["open"]

            if is_green and prev_is_red and lower_band <= candle["low"] <= upper_band:
                logger.info(
                    f"[{self.label}] RETEST TRIGGER @ low={candle['low']} close={candle['close']} "
                    f"({candle['time']}) -- within -{RETEST_BAND_LOWER_PCT*100:.0f}%/"
                    f"+{RETEST_BAND_UPPER_PCT*100:.0f}% of identifying low {round(id_low,2)}; "
                    f"prior candle red @ {prev_candle['time']}"
                )
                self.busy = True
                return {
                    "trigger_candle": candle,
                    "identified_candle": self.identified_candle
                }

        return None

    def record_trade_result(self, is_loss):
        self.trades_taken += 1
        if is_loss:
            self.losses += 1

        if self.losses >= MAX_LOSSES_PER_LEG or self.trades_taken >= MAX_TRADES_PER_LEG:
            self.leg_active = False
            logger.info(
                f"[{self.label}] leg DONE for the day -- trades={self.trades_taken}, "
                f"losses={self.losses} (caps: {MAX_TRADES_PER_LEG} trades / {MAX_LOSSES_PER_LEG} losses)"
            )
        else:
            self.start_new_search()
            logger.info(
                f"[{self.label}] leg continues -- trades={self.trades_taken}, losses={self.losses}; "
                f"searching for next identifying candle."
            )


# ================= 5-MIN CANDLE BUILDER =================
class CandleBuilder:
    def __init__(self, timeframe_min):
        self.timeframe_min = timeframe_min
        self.current = None

    def on_tick(self, ltp, ts):
        bucket = ts.replace(
            minute=(ts.minute // self.timeframe_min) * self.timeframe_min,
            second=0, microsecond=0
        )

        closed_candle = None

        if self.current is None:
            self.current = {"time": bucket, "open": ltp, "high": ltp, "low": ltp, "close": ltp}
        elif self.current["time"] != bucket:
            closed_candle = dict(self.current)
            self.current = {"time": bucket, "open": ltp, "high": ltp, "low": ltp, "close": ltp}
        else:
            self.current["high"] = max(self.current["high"], ltp)
            self.current["low"] = min(self.current["low"], ltp)
            self.current["close"] = ltp

        return closed_candle


# ================= TRADE MANAGER (single leg: CE or PE) =================
class LegTradeManager:
    """
    Handles order placement, trailing-SL bookkeeping, and win/loss
    classification for one leg. Tick-level LTP is used to detect SL
    hits (fast); candle closes are used to evaluate trailing-SL shifts
    (per the "every green candle that forms above the high of the
    reference candle" rule).
    """

    def __init__(self, fyers, symbol, label, engine):
        self.fyers = fyers
        self.symbol = symbol
        self.label = label
        self.engine = engine
        self.pos = None
        self.hard_exit_done = False   # ensures the 3pm flatten/cancel runs exactly once

    def enter(self, signal):
        trigger = signal["trigger_candle"]
        entry_price = trigger["close"]
        sl = trigger["low"] - SL_BUFFER

        if sl >= entry_price:
            logger.warning(f"[{self.label}] invalid SL >= entry, skipping entry")
            self.engine.busy = False
            return False

        resp = self.fyers.buy(self.symbol, QTY, f"{self.label}ENTRY")
        if resp is None:
            send_telegram(f"❌ [{self.label}] ENTRY FAILED")
            self.engine.busy = False
            return False

        self.pos = {
            "entry": entry_price,
            "sl": sl,
            "original_sl": sl,
            "reference_low": trigger["low"],   # trigger candle counts as the first "last green candle"
            "trailed": False
        }

        send_telegram(
            f"🚀 ENTRY {self.label}\n"
            f"Symbol={self.symbol}\n"
            f"Entry={entry_price}  Initial SL={round(sl,2)}\n"
            f"Identifying low={signal['identified_candle']['low']}  "
            f"Trigger candle low={trigger['low']} @ {trigger['time']}"
        )
        return True

    def on_new_candle(self, candle):
        """Evaluate trailing-SL shift on each closed candle while in a position.
        Rule: every GREEN candle whose LOW is higher than the LOW of the
        last green candle formed shifts SL up to this candle's low, and
        this candle becomes the new "last green candle" for the next
        comparison -- whether or not it triggered a shift."""
        if not self.pos:
            return

        is_green = candle["close"] > candle["open"]
        if not is_green:
            return

        if candle["low"] > self.pos["reference_low"]:
            new_sl = candle["low"] - SL_BUFFER
            if new_sl > self.pos["sl"]:
                self.pos["sl"] = new_sl
                self.pos["trailed"] = True
                logger.info(
                    f"[{self.label}] SL TRAILED -> {round(new_sl,2)} "
                    f"(green candle low={candle['low']} above last green candle's low "
                    f"{self.pos['reference_low']}, @ {candle['time']})"
                )
                send_telegram(f"🔧 [{self.label}] SL trailed to {round(new_sl,2)}")

        self.pos["reference_low"] = candle["low"]

    def on_tick(self, ltp, now):
        # 3:00 PM cutoff: force-close any open position, cancel any pending
        # orders, and stop this leg from taking any further trades. Runs
        # exactly once regardless of whether a position happens to be open.
        if not self.hard_exit_done and now.time() >= HARD_EXIT_TIME:
            self.hard_exit_done = True
            self.engine.leg_active = False   # no new trades after 3pm

            if self.pos:
                self.exit("TIME", ltp)
            else:
                cancelled = self.fyers.cancel_pending_orders(self.symbol)
                logger.info(
                    f"[{self.label}] 3:00 PM reached, no open position; "
                    f"cancelled {len(cancelled)} pending order(s)."
                )
                send_telegram(f"⏹️ [{self.label}] 3:00 PM cutoff -- no open position, pending orders cancelled.")
            return

        if not self.pos:
            return

        if ltp <= self.pos["sl"]:
            self.exit("SL", ltp)

    def exit(self, reason, ltp):
        if not self.pos:
            return

        is_loss = not self.pos["trailed"]
        result = "LOSS" if is_loss else "WIN"

        self.fyers.sell(self.symbol, QTY, f"{self.label}EXIT{reason}")

        cancelled = []
        if reason == "TIME":
            cancelled = self.fyers.cancel_pending_orders(self.symbol)

        send_telegram(
            f"🏁 EXIT {self.label} {reason}\n"
            f"LTP={ltp}  Result={result}  (trailed={self.pos['trailed']})"
            + (f"\nCancelled {len(cancelled)} pending order(s)." if reason == "TIME" else "")
        )
        self.pos = None

        self.engine.record_trade_result(is_loss=is_loss)


# ================= MAIN =================
if __name__ == "__main__":
    send_telegram("🚀 ITM1 20% RALLY / RETEST STRATEGY STARTED")

    fyers = Fyers()

    # ---- Wait for decision time (09:16) then lock ITM1 strikes ----
    while datetime.datetime.now().time() < DECISION_TIME:
        time_module.sleep(1)

    itm1_ce, itm1_pe, spot_at_decision = get_itm1_symbols(fyers)

    ce_engine = RetestEngine("CE")
    pe_engine = RetestEngine("PE")

    ce_tm = LegTradeManager(fyers, itm1_ce, "CE", ce_engine)
    pe_tm = LegTradeManager(fyers, itm1_pe, "PE", pe_engine)

    ce_builder = CandleBuilder(TIMEFRAME_MIN)
    pe_builder = CandleBuilder(TIMEFRAME_MIN)

    def on_tick(msg):
        symbol = msg.get("symbol")
        ts = datetime.datetime.fromtimestamp(
            msg.get("last_traded_time", msg.get("timestamp", datetime.datetime.now().timestamp()))
        )
        ltp = msg["ltp"]

        if symbol == itm1_ce:
            ce_tm.on_tick(ltp, ts)
            closed = ce_builder.on_tick(ltp, ts)
            if closed:
                if ce_tm.pos:
                    ce_tm.on_new_candle(closed)
                elif ce_engine.leg_active:
                    signal = ce_engine.on_new_candle(closed)
                    if signal:
                        if not ce_tm.enter(signal):
                            ce_engine.busy = False

        elif symbol == itm1_pe:
            pe_tm.on_tick(ltp, ts)
            closed = pe_builder.on_tick(ltp, ts)
            if closed:
                if pe_tm.pos:
                    pe_tm.on_new_candle(closed)
                elif pe_engine.leg_active:
                    signal = pe_engine.on_new_candle(closed)
                    if signal:
                        if not pe_tm.enter(signal):
                            pe_engine.busy = False

    def on_open():
        ws.subscribe(symbols=[itm1_ce, itm1_pe], data_type="SymbolUpdate")
        ws.keep_running()

    ws = data_ws.FyersDataSocket(
        access_token=fyers.auth,
        on_connect=on_open,
        on_message=on_tick,
        log_path=""
    )

    ws.connect()
