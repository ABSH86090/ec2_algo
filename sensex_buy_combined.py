import datetime
import logging
import os
import csv
from collections import defaultdict, deque

from dotenv import load_dotenv
from fyers_apiv3 import fyersModel
from fyers_apiv3.FyersWebsocket import order_ws, data_ws

# ---------------- CONFIG ----------------
load_dotenv()

CLIENT_ID = os.getenv("FYERS_CLIENT_ID")
ACCESS_TOKEN = os.getenv("FYERS_ACCESS_TOKEN")
LOT_SIZE = 20  # option lot size
TICK_SIZE = 0.05  # tick size

TRADING_START = datetime.time(9, 45)
TRADING_END = datetime.time(15, 0)

# --- Strategy selection ---
# Map each symbol to strategy id: 1 = W-pattern (existing), 2 = EMA5–VWMA20 pullback (Sensex)
SYMBOL_STRATEGY = {
    "BSE:SENSEX2590481000CE": 2,
    "BSE:SENSEX2590481000PE": 2,
}

# --- Strategy 1 (W-pattern) config ---
REQUIRE_W_CONTEXT = True
MIN_BELOW_CANDLES = 3
MAX_PATTERN_AGE = 25

# --- Strategy 2 (EMA5–VWMA20 pullback) config ---
STRAT2_MIN_RED = 1           # at least this many red pullback candles
STRAT2_MAX_RED = 6           # cap the pullback length
STRAT2_PROX_MODE = "percent" # 'percent' or 'points'
STRAT2_PROX_VALUE = 0.0015   # 0.15% if 'percent'; else points if 'points'
STRAT2_SL_OFFSET = 3.0       # SL = low(entry) + 3
STRAT2_BULLISH_MAX_GAP = 0.01  # entry candle close must be within 1% of high

STRAT2_TAG_ENTRY = "SENSEXSTRAT2ENTRY"
STRAT2_TAG_SL = "SENSEXSTRAT2SL"

# Reset logging handlers
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

logging.basicConfig(
    filename="nifty_breakout_wpattern.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# ---------------- UTILS ----------------
def round_to_tick(price, tick_size=TICK_SIZE):
    """Round a price to the nearest tick size (default = 0.05)."""
    return round(round(price / tick_size) * tick_size, 2)

# ---------------- JOURNAL LOGGING ----------------
def log_trade(symbol, side, entry, sl, target, exit_price, pnl, file="trades.csv"):
    file_exists = os.path.isfile(file)
    with open(file, "a", newline="") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["date","symbol","side","entry","sl","target","exit","pnl"])
        writer.writerow([
            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            symbol, side, entry, sl, target, exit_price, pnl
        ])

# ---------------- FYERS CLIENT ----------------
class FyersClient:
    def __init__(self, client_id: str, access_token: str, lot_size: int = 20):
        self.client_id = client_id
        self.access_token = access_token
        self.auth_token = f"{client_id}:{access_token}"
        self.lot_size = lot_size
        self.client = fyersModel.FyersModel(
            client_id=client_id,
            token=access_token,
            is_async=False,
            log_path=""
        )
        self.order_callbacks = []
        self.trade_callbacks = []

    def _log_order_resp(self, action, resp):
        logging.info(f"{action} Response: {resp}")
        print(f"{action} Response: {resp}")
        return resp

    def place_limit_buy(self, symbol: str, qty: int, price: float, tag: str = ""):
        price = round_to_tick(price)
        data = {
            "symbol": symbol,
            "qty": qty,
            "type": 1,
            "side": 1,  # BUY
            "productType": "INTRADAY",
            "limitPrice": price,
            "stopPrice": 0,
            "validity": "DAY",
            "disclosedQty": 0,
            "offlineOrder": False,
            "orderTag": tag
        }
        return self._log_order_resp("Limit Buy", self.client.place_order(data))

    def place_stoploss_sell(self, symbol: str, qty: int, stop_price: float, tag: str = ""):
        stop_price = round_to_tick(stop_price)
        data = {
            "symbol": symbol,
            "qty": qty,
            "type": 3,  # SL-M
            "side": -1,  # SELL
            "productType": "INTRADAY",
            "limitPrice": 0,
            "stopPrice": stop_price,
            "validity": "DAY",
            "disclosedQty": 0,
            "offlineOrder": False,
            "orderTag": tag
        }
        return self._log_order_resp("SL Sell", self.client.place_order(data))

    def place_market_sell(self, symbol: str, qty: int, tag: str = ""):
        data = {
            "symbol": symbol,
            "qty": qty,
            "type": 2,  # MARKET
            "side": -1,  # SELL
            "productType": "INTRADAY",
            "limitPrice": 0,
            "stopPrice": 0,
            "validity": "DAY",
            "disclosedQty": 0,
            "offlineOrder": False,
            "orderTag": tag
        }
        return self._log_order_resp("Market Sell", self.client.place_order(data))

    def cancel_order(self, order_id: str):
        return self._log_order_resp("Cancel Order", self.client.cancel_order({"id": order_id}))

    def start_order_socket(self):
        def on_order(msg):
            logging.info(f"Order update: {msg}")
            for cb in self.order_callbacks:
                cb(msg)

        def on_trade(msg):
            logging.info(f"Trade update: {msg}")
            for cb in self.trade_callbacks:
                cb(msg)

        def on_open():
            fyers.subscribe(data_type="OnOrders,OnTrades")
            fyers.keep_running()

        fyers = order_ws.FyersOrderSocket(
            access_token=self.auth_token,
            write_to_file=False,
            log_path="",
            on_connect=on_open,
            on_close=lambda m: logging.info(f"Order socket closed: {m}"),
            on_error=lambda m: logging.error(f"Order socket error: {m}"),
            on_orders=on_order,
            on_trades=on_trade
        )
        fyers.connect()

    def register_order_callback(self, cb):
        self.order_callbacks.append(cb)

    def register_trade_callback(self, cb):
        self.trade_callbacks.append(cb)

    def subscribe_market_data(self, instrument_ids, on_candle_callback):
        candle_buffers = defaultdict(lambda: None)

        def on_message(tick):
            try:
                if "symbol" not in tick or "ltp" not in tick:
                    return
                symbol = tick["symbol"]
                ltp = float(tick["ltp"])
                ts = int(tick["last_traded_time"])
                cum_vol = int(tick.get("vol_traded_today", 0))  # cumulative volume
            except Exception as e:
                logging.error(f"Bad tick: {tick}, {e}")
                return

            dt = datetime.datetime.fromtimestamp(ts)
            bucket_minute = (dt.minute // 3) * 3
            candle_time = dt.replace(second=0, microsecond=0, minute=bucket_minute)

            c = candle_buffers[symbol]
            if c is None or c["time"] != candle_time:
                if c is not None:
                    on_candle_callback(symbol, c)
                candle_buffers[symbol] = {
                    "time": candle_time,
                    "open": ltp,
                    "high": ltp,
                    "low": ltp,
                    "close": ltp,
                    "volume": 0,
                    "start_cum_vol": cum_vol
                }
            else:
                c["high"] = max(c["high"], ltp)
                c["low"] = min(c["low"], ltp)
                c["close"] = ltp
                c["volume"] = max(0, cum_vol - c["start_cum_vol"])

        def on_open():
            fyers.subscribe(symbols=instrument_ids, data_type="SymbolUpdate")
            fyers.keep_running()

        fyers = data_ws.FyersDataSocket(
            access_token=self.auth_token,
            log_path="",
            on_connect=on_open,
            on_close=lambda m: logging.info(f"Data socket closed: {m}"),
            on_error=lambda m: logging.error(f"Data socket error: {m}"),
            on_message=on_message
        )
        fyers.connect()

# ---------------- STRATEGY ENGINE ----------------
class NiftyBuyStrategy:
    def __init__(self, fyers_client: FyersClient, lot_size: int = 20):
        self.fyers = fyers_client
        self.lot_size = lot_size
        self.candles = defaultdict(lambda: deque(maxlen=200))
        self.positions = {}       # Active positions per symbol
        self.pending_orders = {}  # Pending entry orders per symbol
        self.w_state = defaultdict(lambda: {
            "phase": 0, "age": 0, "below_cnt": 0,
            "greens_under_cnt": 0, "reds_pullback_cnt": 0
        })

    # --- history prefill ---
    def prefill_history(self, symbols, days_back=1):
        for symbol in symbols:
            try:
                to_date = datetime.datetime.now().strftime("%Y-%m-%d")
                from_date = (datetime.datetime.now() - datetime.timedelta(days=days_back)).strftime("%Y-%m-%d")

                params = {
                    "symbol": symbol,
                    "resolution": "3",
                    "date_format": "1",
                    "range_from": from_date,
                    "range_to": to_date,
                    "cont_flag": "1"
                }
                hist = self.fyers.client.history(params)

                if hist.get("candles"):
                    for c in hist["candles"]:
                        ts = datetime.datetime.fromtimestamp(c[0])
                        candle = {
                            "time": ts,
                            "open": c[1],
                            "high": c[2],
                            "low": c[3],
                            "close": c[4],
                            "volume": c[5],
                        }
                        self.candles[symbol].append(candle)

                    closes = [c["close"] for c in self.candles[symbol]]
                    volumes = [c["volume"] for c in self.candles[symbol]]
                    ema5 = self.compute_ema(closes, 5)
                    vwma20 = self.compute_vwma(closes, volumes, 20)
                    if ema5 and vwma20:
                        logging.info(f"[Init] {symbol} EMA5={ema5:.2f}, VWMA20={vwma20:.2f}")
                        print(f"Init {symbol} EMA5={ema5:.2f}, VWMA20={vwma20:.2f}")
            except Exception as e:
                logging.error(f"History fetch fail {symbol}: {e}")

    # --- utils ---
    def compute_ema(self, prices, period):
        if len(prices) < period:
            return None
        ema = prices[0]
        k = 2/(period+1)
        for p in prices[1:]:
            ema = p*k + ema*(1-k)
        return ema

    def compute_ema_series(self, prices, period):
        series = []
        if not prices:
            return series
        ema = prices[0]
        k = 2/(period+1)
        for p in prices:
            ema = p*k + ema*(1-k)
            series.append(ema)
        return series

    def compute_vwma(self, closes, volumes, period):
        if len(closes) < period:
            return None
        return sum([c*v for c, v in zip(closes[-period:], volumes[-period:])]) / sum(volumes[-period:])

    def compute_vwma_series(self, closes, volumes, period):
        vw = []
        num = 0.0
        den = 0.0
        from collections import deque as dq
        q = dq()
        for c, v in zip(closes, volumes):
            q.append((c, v))
            num += c*v
            den += v
            if len(q) > period:
                oc, ov = q.popleft()
                num -= oc*ov
                den -= ov
            vw.append(num/den if den > 0 else None)
        return vw

    def is_green(self, c): return c["close"] > c["open"]
    def is_red(self, c): return c["close"] < c["open"]
    def is_below_both(self, close, ema5, vwma): return close < vwma and close < ema5

    # --- W-pattern updater (Strategy 1) ---
    def update_w_pattern(self, symbol, candles, ema5, vwma):
        st = self.w_state[symbol]
        st["age"] += 1
        c = candles[-1]
        close = c["close"]
        vwma_over_ema = vwma > ema5

        if st["age"] > MAX_PATTERN_AGE:
            st.update({"phase": 0, "age": 0, "below_cnt": 0,
                       "greens_under_cnt": 0, "reds_pullback_cnt": 0})
            return

        if st["phase"] == 0:
            if vwma_over_ema and self.is_below_both(close, ema5, vwma):
                st["below_cnt"] += 1
                if self.is_green(c) and st["below_cnt"] >= MIN_BELOW_CANDLES:
                    st["phase"] = 1; st["age"]=0; st["greens_under_cnt"]=1
        elif st["phase"] == 1:
            if vwma_over_ema and self.is_green(c) and close < vwma:
                st["greens_under_cnt"] += 1
            elif vwma_over_ema and not self.is_green(c) and self.is_below_both(close, ema5, vwma):
                st["phase"] = 2; st["age"]=0; st["reds_pullback_cnt"]=1
        elif st["phase"] == 2:
            if vwma_over_ema and not self.is_green(c) and self.is_below_both(close, ema5, vwma):
                st["reds_pullback_cnt"] += 1
            elif close > vwma and close > ema5:
                st["phase"] = 3

    # --- Strategy 2: EMA5–VWMA20 pullback detector ---
    def strat2_entry_ready(self, candles):
        """
        Returns dict with entry params if the *last* candle qualifies for strategy 2; else None.
        Conditions:
          - A green 'anchor' candle earlier closed above both EMA5 & VWMA20
          - Then 1..STRAT2_MAX_RED red candles whose close is 'very close' to VWMA20
          - Current last candle is green, **close within 1% of high**, and its EMA5 & VWMA > last red's
          - Entry at last candle close; SL = low(last green) + 3; Target = entry + 2.5*(entry - SL)
        """
        if len(candles) < 25:
            return None

        closes = [c["close"] for c in candles]
        volumes = [c["volume"] for c in candles]
        ema5_series = self.compute_ema_series(closes, 5)
        vwma20_series = self.compute_vwma_series(closes, volumes, 20)

        i = len(candles) - 1  # last candle index
        last = candles[i]
        ema_i = ema5_series[i]
        vwma_i = vwma20_series[i]
        if vwma_i is None:
            return None

        # Entry candle must be green and "bullish" (close within 1% of high)
        if not self.is_green(last):
            return None
        if last["close"] < last["high"] * (1 - STRAT2_BULLISH_MAX_GAP):
            return None  # not close enough to the high

        # Find the immediate previous red block (1..max) contiguous ending at i-1
        j = i - 1
        red_indices = []
        while j >= 0 and len(red_indices) < STRAT2_MAX_RED:
            c = candles[j]
            if self.is_red(c):
                vw = vwma20_series[j]
                if vw is None:
                    return None
                c_close = c["close"]
                if STRAT2_PROX_MODE == "percent":
                    near = abs(c_close - vw) <= STRAT2_PROX_VALUE * vw
                else:
                    near = abs(c_close - vw) <= STRAT2_PROX_VALUE
                if near:
                    red_indices.append(j)
                    j -= 1
                    continue
            break

        if len(red_indices) < STRAT2_MIN_RED:
            return None

        last_red_idx = red_indices[0]  # closest to i (the last red)
        # Before the red block, we need an anchor green above both
        anchor_idx = last_red_idx - 1
        found_anchor = None
        while anchor_idx >= 0:
            c = candles[anchor_idx]
            if self.is_green(c):
                e = ema5_series[anchor_idx]
                v = vwma20_series[anchor_idx]
                if e is not None and v is not None and c["close"] > e and c["close"] > v:
                    found_anchor = anchor_idx
                    break
            if last_red_idx - anchor_idx > 30:  # sanity cap
                break
            anchor_idx -= 1

        if found_anchor is None:
            return None

        # Current green must have EMA5 & VWMA > those of last red candle
        ema_last_red = ema5_series[last_red_idx]
        vwma_last_red = vwma20_series[last_red_idx]
        if ema_i <= ema_last_red or vwma_i <= vwma_last_red:
            return None

        entry = round_to_tick(last["close"])
        sl = round_to_tick(last["low"] + STRAT2_SL_OFFSET)  # low + 3
        if sl >= entry:
            return None  # invalid R

        R = entry - sl
        target = round_to_tick(entry + 2.5 * R)

        return {
            "entry_index": i,
            "entry_price": entry,
            "sl": sl,
            "target": target,
            "anchor_index": found_anchor,
            "last_red_index": last_red_idx
        }

    # --- candle handler ---
    def on_candle(self, symbol, candle):
        self.candles[symbol].append(candle)
        candles = list(self.candles[symbol])

        closes = [c["close"] for c in candles]
        volumes = [c["volume"] for c in candles]
        ema5 = self.compute_ema(closes, 5)
        vwma20 = self.compute_vwma(closes, volumes, 20)

        if ema5 is None or vwma20 is None:
            return

        now = datetime.datetime.now().time()
        if not (TRADING_START <= now <= TRADING_END):
            return

        strategy = SYMBOL_STRATEGY.get(symbol, 1)  # default to Strategy 1

        pos = self.positions.get(symbol)
        pending = self.pending_orders.get(symbol)

        # ---------------- STRATEGY 2 (Sensex pullback) ----------------
        if strategy == 2:
            if pos is None and pending is None:
                sig = self.strat2_entry_ready(candles)
                if sig:
                    entry_price = sig["entry_price"]
                    sl_price = sig["sl"]
                    target_price = sig["target"]

                    resp = self.fyers.place_limit_buy(symbol, self.lot_size, entry_price, tag=STRAT2_TAG_ENTRY)
                    self.pending_orders[symbol] = {
                        "entry": entry_price, "sl": sl_price,
                        "target": target_price, "resp": resp, "strategy": 2
                    }
                    logging.info(f"[STRAT2] ENTRY ORDER PLACED {symbol} @ {entry_price} "
                                 f"(SL {sl_price}, TGT {target_price})")

            elif pos is not None and pos.get("strategy") == 2:
                if candle["close"] >= pos["target"]:
                    exit_price = round_to_tick(pos["target"])
                    pnl = (exit_price - pos["entry"]) * self.lot_size
                    log_trade(symbol, "BUY", pos["entry"], pos["sl"], pos["target"], exit_price, pnl)
                    if pos["sl_order"] and "id" in pos["sl_order"]:
                        self.fyers.cancel_order(pos["sl_order"]["id"])
                    self.fyers.place_market_sell(symbol, self.lot_size, tag="STRAT2TARGETEXIT")
                    logging.info(f"[STRAT2] EXIT TARGET {symbol} @ {exit_price} PnL={pnl}")
                    self.positions[symbol] = None

            return  # done for strategy 2

        # ---------------- STRATEGY 1 (existing W-pattern) ----------------
        self.update_w_pattern(symbol, candles, ema5, vwma20)
        w_phase = self.w_state[symbol]["phase"]

        if pos is None and pending is None:
            if (not REQUIRE_W_CONTEXT) or (w_phase == 3):
                if candle["close"] > vwma20 and candle["close"] > ema5:
                    entry_price = round_to_tick(vwma20)
                    sl_price = round_to_tick(candle["low"] - 2)
                    risk = entry_price - sl_price
                    if risk > 0.2 * entry_price:
                        return
                    target_price = round_to_tick(entry_price + 2*risk)

                    resp = self.fyers.place_limit_buy(symbol, self.lot_size, entry_price, tag="NIFTYBUYENTRY")
                    self.pending_orders[symbol] = {
                        "entry": entry_price, "sl": sl_price,
                        "target": target_price, "resp": resp, "strategy": 1
                    }
                    logging.info(f"ENTRY ORDER PLACED {symbol} @ {entry_price}")

        elif pos is not None and pos.get("strategy") == 1:
            if candle["close"] >= pos["target"]:
                exit_price = round_to_tick(pos["target"])
                pnl = (exit_price - pos["entry"]) * self.lot_size
                log_trade(symbol, "BUY", pos["entry"], pos["sl"], pos["target"], exit_price, pnl)
                if pos["sl_order"] and "id" in pos["sl_order"]:
                    self.fyers.cancel_order(pos["sl_order"]["id"])
                self.fyers.place_market_sell(symbol, self.lot_size, tag="TARGETEXIT")
                logging.info(f"EXIT TARGET {symbol} @ {exit_price} PnL={pnl}")
                self.positions[symbol] = None

    # --- trade handler ---
    def on_trade(self, msg):
        if not msg.get("trades"): return
        t = msg["trades"]
        side = t.get("side")
        symbol = t.get("symbol")
        tag = t.get("orderTag")

        # Strategy 1 entry fill
        if side == 1 and tag == "NIFTYBUYENTRY":
            pending = self.pending_orders.pop(symbol, None)
            if pending:
                self.positions[symbol] = {
                    "entry": pending["entry"],
                    "sl": pending["sl"],
                    "target": pending["target"],
                    "sl_order": None,
                    "strategy": 1
                }
                sl_price = round_to_tick(pending["sl"])
                sl_resp = self.fyers.place_stoploss_sell(symbol, self.lot_size, sl_price, tag="NIFTYSL")
                self.positions[symbol]["sl_order"] = sl_resp
                logging.info(f"ENTRY FILLED {symbol} (STRAT1), SL placed @ {sl_price}")

        # Strategy 1 SL hit
        elif side == -1 and tag == "NIFTYSL":
            pos = self.positions.get(symbol)
            if pos and pos.get("strategy") == 1:
                exit_price = round_to_tick(pos["sl"])
                pnl = (exit_price - pos["entry"]) * self.lot_size
                log_trade(symbol, "BUY", pos["entry"], pos["sl"], pos["target"], exit_price, pnl)
                logging.info(f"SL HIT {symbol} (STRAT1) @ {exit_price} PnL={pnl}")
                self.positions[symbol] = None

        # Strategy 2 entry fill
        elif side == 1 and tag == STRAT2_TAG_ENTRY:
            pending = self.pending_orders.pop(symbol, None)
            if pending:
                self.positions[symbol] = {
                    "entry": pending["entry"],
                    "sl": pending["sl"],
                    "target": pending["target"],
                    "sl_order": None,
                    "strategy": 2
                }
                sl_price = round_to_tick(pending["sl"])
                sl_resp = self.fyers.place_stoploss_sell(symbol, self.lot_size, sl_price, tag=STRAT2_TAG_SL)
                self.positions[symbol]["sl_order"] = sl_resp
                logging.info(f"[STRAT2] ENTRY FILLED {symbol}, SL placed @ {sl_price}")

        # Strategy 2 SL hit
        elif side == -1 and tag == STRAT2_TAG_SL:
            pos = self.positions.get(symbol)
            if pos and pos.get("strategy") == 2:
                exit_price = round_to_tick(pos["sl"])
                pnl = (exit_price - pos["entry"]) * self.lot_size
                log_trade(symbol, "BUY", pos["entry"], pos["sl"], pos["target"], exit_price, pnl)
                logging.info(f"[STRAT2] SL HIT {symbol} @ {exit_price} PnL={pnl}")
                self.positions[symbol] = None

# ---------------- MAIN ----------------
if __name__ == "__main__":
    fyers_client = FyersClient(CLIENT_ID, ACCESS_TOKEN, LOT_SIZE)
    engine = NiftyBuyStrategy(fyers_client, LOT_SIZE)

    # Sensex options -> Strategy 2
    option_symbols = ["BSE:SENSEX2590481000CE","BSE:SENSEX2590481000PE"]

    engine.prefill_history(option_symbols, days_back=1)

    fyers_client.register_trade_callback(engine.on_trade)
    fyers_client.subscribe_market_data(option_symbols, engine.on_candle)
    fyers_client.start_order_socket()
