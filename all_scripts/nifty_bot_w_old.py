import datetime
import logging
import os
import csv
from collections import defaultdict, deque

from dotenv import load_dotenv
from fyers_apiv3 import fyersModel
from fyers_apiv3.FyersWebsocket import order_ws, data_ws

from get_token import ensure_token

# ---------------- CONFIG ----------------
# ACCESS_TOKEN = ensure_token()
load_dotenv()

CLIENT_ID = os.getenv("FYERS_CLIENT_ID")
ACCESS_TOKEN = os.getenv("FYERS_ACCESS_TOKEN")
LOT_SIZE = 75  # official NIFTY option lot size

TRADING_START = datetime.time(10, 0)
TRADING_END = datetime.time(15, 0)

# --- W-pattern config ---
REQUIRE_W_CONTEXT = True
MIN_BELOW_CANDLES = 3
MAX_PATTERN_AGE = 25

# Reset logging handlers
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

logging.basicConfig(
    filename="nifty_breakout_wpattern.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

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
    def __init__(self, client_id: str, access_token: str, lot_size: int = 75):
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

    def place_limit_buy(self, symbol: str, qty: int, price: float, tag: str = ""):
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
        return self.client.place_order(data)

    def place_stoploss_sell(self, symbol: str, qty: int, stop_price: float, tag: str = ""):
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
        return self.client.place_order(data)

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
        return self.client.place_order(data)

    def cancel_order(self, order_id: str):
        return self.client.cancel_order({"id": order_id})

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
                volume = int(tick.get("last_traded_qty", 0))
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
                    "volume": volume,
                }
            else:
                c["high"] = max(c["high"], ltp)
                c["low"] = min(c["low"], ltp)
                c["close"] = ltp
                c["volume"] += volume

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
    def __init__(self, fyers_client: FyersClient, lot_size: int = 75):
        self.fyers = fyers_client
        self.lot_size = lot_size
        self.candles = defaultdict(lambda: deque(maxlen=200))
        self.position = None
        self.entry_order = None
        self.sl_order = None
        self.target_price = None

        # W-pattern state
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
                else:
                    logging.warning(f"No history for {symbol}, response={hist}")
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

    def compute_vwma(self, closes, volumes, period):
        if len(closes) < period:
            return None
        return sum([c*v for c, v in zip(closes[-period:], volumes[-period:])]) / sum(volumes[-period:])

    def is_green(self, c): return c["close"] >= c["open"]
    def is_below_both(self, close, ema5, vwma): return close < vwma and close < ema5

    # --- W-pattern updater ---
    def update_w_pattern(self, symbol, candles, ema5, vwma):
        st = self.w_state[symbol]
        st["age"] += 1
        c = candles[-1]
        close = c["close"]
        vwma_over_ema = vwma > ema5

        if st["age"] > MAX_PATTERN_AGE:
            st.update({"phase": 0, "age": 0, "below_cnt": 0,
                       "greens_under_cnt": 0, "reds_pullback_cnt": 0})
            logging.info(f"WSTATE {symbol} -> RESET (stale)")
            return

        if st["phase"] == 0:
            if vwma_over_ema and self.is_below_both(close, ema5, vwma):
                st["below_cnt"] += 1
                if self.is_green(c) and close < vwma and st["below_cnt"] >= MIN_BELOW_CANDLES:
                    st["phase"] = 1; st["age"]=0; st["greens_under_cnt"]=1
                    logging.info(f"WSTATE {symbol} -> PHASE1")
            else:
                st.update({"phase":0,"age":0,"below_cnt":0,"greens_under_cnt":0,"reds_pullback_cnt":0})

        elif st["phase"] == 1:
            if vwma_over_ema and self.is_green(c) and close < vwma:
                st["greens_under_cnt"] += 1
            elif vwma_over_ema and not self.is_green(c) and self.is_below_both(close, ema5, vwma):
                st["phase"] = 2; st["age"]=0; st["reds_pullback_cnt"]=1
                logging.info(f"WSTATE {symbol} -> PHASE2")
            else:
                st.update({"phase":0,"age":0,"below_cnt":0,"greens_under_cnt":0,"reds_pullback_cnt":0})

        elif st["phase"] == 2:
            if vwma_over_ema and not self.is_green(c) and self.is_below_both(close, ema5, vwma):
                st["reds_pullback_cnt"] += 1
            elif close > vwma and close > ema5:
                st["phase"] = 3
                logging.info(f"WSTATE {symbol} -> PHASE3 (breakout)")
            else:
                st.update({"phase":0,"age":0,"below_cnt":0,"greens_under_cnt":0,"reds_pullback_cnt":0})

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

        logging.info(f"[Candle] {symbol} {candle['time']} EMA5={ema5:.2f}, VWMA20={vwma20:.2f}, Close={candle['close']}")

        now = datetime.datetime.now().time()
        if not (TRADING_START <= now <= TRADING_END):
            return

        self.update_w_pattern(symbol, candles, ema5, vwma20)
        w_phase = self.w_state[symbol]["phase"]

        if self.position is None:
            if (not REQUIRE_W_CONTEXT) or (w_phase == 3):
                if candle["close"] > vwma20 and candle["close"] > ema5:
                    entry_price = vwma20
                    sl_price = candle["low"] - 2
                    risk = entry_price - sl_price

                    if risk > 0.2 * entry_price:
                        logging.info(f"SKIP {symbol}: SL={risk:.2f} >20% of premium {entry_price:.2f}")
                        self.w_state[symbol]["phase"] = 2; self.w_state[symbol]["age"]=0
                        return

                    self.target_price = entry_price + 2*risk
                    resp = self.fyers.place_limit_buy(symbol, self.lot_size, entry_price, tag="NIFTY_BUY_ENTRY")
                    self.entry_order = resp
                    self.position = {"symbol": symbol, "entry": entry_price, "sl": sl_price}
                    logging.info(f"ENTRY {symbol} (W) Entry={entry_price}, SL={sl_price}, Target={self.target_price}")

                    self.w_state[symbol] = {"phase":0,"age":0,"below_cnt":0,"greens_under_cnt":0,"reds_pullback_cnt":0}

        # --- target exit ---
        if self.position and candle["close"] >= self.target_price:
            exit_price = self.target_price
            pnl = (exit_price - self.position["entry"]) * self.lot_size
            log_trade(self.position["symbol"], "BUY",
                      self.position["entry"], self.position["sl"], self.target_price,
                      exit_price, pnl)

            if self.sl_order and "id" in self.sl_order:
                self.fyers.cancel_order(self.sl_order["id"])
            self.fyers.place_market_sell(self.position["symbol"], self.lot_size, tag="TARGET_EXIT")
            logging.info(f"EXIT TARGET {symbol} @ {exit_price} PnL={pnl}")
            self.position=None; self.sl_order=None

    # --- trade handler ---
    def on_trade(self, msg):
        if not msg.get("trades"): return
        t = msg["trades"]
        side = t.get("side")
        symbol = t.get("symbol")

        if self.position and side==1:  # BUY filled
            sl_price = self.position["sl"]
            self.sl_order = self.fyers.place_stoploss_sell(symbol, self.lot_size, sl_price, tag="SL_ORDER")
            logging.info(f"SL placed {symbol} @ {sl_price}")

        elif self.position and side==-1 and t.get("orderTag")=="SL_ORDER":
            exit_price = self.position["sl"]
            pnl = (exit_price - self.position["entry"]) * self.lot_size
            log_trade(self.position["symbol"], "BUY",
                      self.position["entry"], self.position["sl"], self.target_price,
                      exit_price, pnl)
            logging.info(f"SL HIT {symbol} @ {exit_price} PnL={pnl}")
            self.position=None; self.sl_order=None

# ---------------- MAIN ----------------
if __name__ == "__main__":
    fyers_client = FyersClient(CLIENT_ID, ACCESS_TOKEN, LOT_SIZE)
    engine = NiftyBuyStrategy(fyers_client, LOT_SIZE)

    option_symbols = ["NSE:NIFTY25SEP22500CE"]  # replace with actual symbol

    # Prefill history for EMA/VWMA
    engine.prefill_history(option_symbols, days_back=1)

    fyers_client.register_trade_callback(engine.on_trade)
    fyers_client.subscribe_market_data(option_symbols, engine.on_candle)
    fyers_client.start_order_socket()
