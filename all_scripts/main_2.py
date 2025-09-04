import datetime
import logging
from collections import defaultdict, deque
from statistics import mean
from fyers_apiv3 import fyersModel
from fyers_apiv3.FyersWebsocket import order_ws, data_ws
import datetime

# ---------------- CONFIG ----------------
ACCESS_TOKEN = ""
CLIENT_ID = ""
secret_key = ""
LOT_SIZE = 20
MAX_SLS_PER_DAY = 3
TRADING_START = datetime.time(9, 15)
TRADING_END = datetime.time(15, 0)

# Setup logging
logging.basicConfig(
    filename="strategy.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

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

    def place_limit_sell(self, symbol: str, qty: int, price: float, tag: str = ""):
        data = {
            "symbol": symbol,
            "qty": qty,
            "type": 1,      # LIMIT
            "side": -1,     # SELL
            "productType": "INTRADAY",
            "limitPrice": price,
            "stopPrice": 0,
            "validity": "DAY",
            "disclosedQty": 0,
            "offlineOrder": False,
            "orderTag": tag
        }
        return self.client.place_order(data)

    def place_stoploss_buy(self, symbol: str, qty: int, stop_price: float, tag: str = ""):
        data = {
            "symbol": symbol,
            "qty": qty,
            "type": 3,      # SL-M
            "side": 1,      # BUY
            "productType": "INTRADAY",
            "limitPrice": 0,
            "stopPrice": stop_price,
            "validity": "DAY",
            "disclosedQty": 0,
            "offlineOrder": False,
            "orderTag": tag
        }
        return self.client.place_order(data)

    def place_market_buy(self, symbol: str, qty: int, tag: str = ""):
        data = {
            "symbol": symbol,
            "qty": qty,
            "type": 2,      # MARKET
            "side": 1,      # BUY
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
        data = {"id": order_id}
        return self.client.cancel_order(data)

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

        def on_message(message):
            try:
                tick = message["d"][0]["v"]
                symbol = message["d"][0]["n"]
                ltp = float(tick["lp"])
                ts = int(tick["tt"])  # epoch ms
                volume = int(tick.get("v", 0))
            except Exception as e:
                logging.error(f"Bad tick: {message}, {e}")
                return

            dt = datetime.datetime.fromtimestamp(ts/1000)
            bucket_minute = (dt.minute // 3) * 3
            candle_open_time = dt.replace(second=0, microsecond=0, minute=bucket_minute)

            c = candle_buffers[symbol]
            if c is None or c["time"] != candle_open_time:
                if c is not None:
                    on_candle_callback(symbol, c)
                candle_buffers[symbol] = {
                    "time": candle_open_time,
                    "open": ltp,
                    "high": ltp,
                    "low": ltp,
                    "close": ltp,
                    "volume": volume
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
class StrategyEngine:
    def __init__(self, fyers_client: FyersClient, lot_size: int = 20):
        self.fyers = fyers_client
        self.lot_size = lot_size
        self.position = None
        self.sl_count = 0
        self.candles = defaultdict(lambda: deque(maxlen=50))

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

    def detect_pattern(self, candles):
        if len(candles) < 5:
            return False
        last5 = list(candles)[-5:]
        def is_red(c): return c["close"] <= c["open"]
        def is_green(c): return c["close"] > c["open"]
        if not (is_red(last5[0]) and is_green(last5[1]) and is_red(last5[2]) and is_green(last5[3]) and is_red(last5[4])):
            return False
        lowest4 = min(c["close"] for c in last5[:4])
        return last5[4]["close"] < lowest4

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

        if self.sl_count >= MAX_SLS_PER_DAY:
            logging.info("Max SLs reached. No more trades today.")
            return

        if self.detect_pattern(candles) and ema5 < vwma20 and self.position is None:
            breakdown = candles[-1]
            entry_price = breakdown["close"]
            sl_price = breakdown["high"] + 2

            resp = self.fyers.place_limit_sell(symbol, self.lot_size, entry_price, tag="ENTRY")
            sl_resp = self.fyers.place_stoploss_buy(symbol, self.lot_size, sl_price, tag="SL")

            self.position = {"symbol": symbol, "sl_order": sl_resp}

            print(f"ENTRY: {symbol} @ {entry_price}, SL: {sl_price}")
            logging.info(f"Entry placed: {resp}, SL: {sl_resp}")

        if self.position and candle["close"] > ema5 and candle["close"] > vwma20:
            sl_order_id = self.position["sl_order"].get("id") if self.position["sl_order"] else None
            if sl_order_id:
                self.fyers.cancel_order(sl_order_id)
                logging.info(f"SL order cancelled: {sl_order_id}")

            resp = self.fyers.place_market_buy(self.position["symbol"], self.lot_size, tag="EXIT_COND")
            print(f"EXIT: {self.position['symbol']} by condition")
            logging.info(f"Exit placed: {resp}")
            self.position = None

    def on_trade(self, msg):
        if self.position and msg.get("trades") and msg["trades"]["side"] == 1:
            print(f"EXIT: {self.position['symbol']} by SL")
            logging.info(f"SL hit trade: {msg}")
            self.sl_count += 1
            self.position = None

    def prefill_history(self, symbols, days_back=1):
        """
        Fetch historical 3-min candles for initialization and
        compute EMA/VWMA immediately.
        """
        for symbol in symbols:
            try:
                to_date = datetime.datetime.now().strftime("%Y-%m-%d")
                from_date = (datetime.datetime.now() - datetime.timedelta(days=days_back)).strftime("%Y-%m-%d")

                params = {
                    "symbol": symbol,
                    "resolution": "3",   # 3-min candles
                    "date_format": "1",
                    "range_from": from_date,
                    "range_to": to_date,
                    "cont_flag": "1"
                }
                hist = self.fyers.client.history(params)

                if hist.get("candles"):
                    for c in hist["candles"]:
                        ts = datetime.datetime.fromtimestamp(c[0])  # epoch in seconds
                        candle = {
                            "time": ts,
                            "open": c[1],
                            "high": c[2],
                            "low": c[3],
                            "close": c[4],
                            "volume": c[5],
                        }
                        self.candles[symbol].append(candle)

                    logging.info(f"Prefilled {len(self.candles[symbol])} candles for {symbol}")

                    # ✅ Compute EMA5 & VWMA20 immediately
                    closes = [c["close"] for c in self.candles[symbol]]
                    volumes = [c["volume"] for c in self.candles[symbol]]

                    ema5 = self.compute_ema(closes, 5)
                    vwma20 = self.compute_vwma(closes, volumes, 20)

                    if ema5 and vwma20:
                        logging.info(f"Init {symbol} EMA5={ema5:.2f}, VWMA20={vwma20:.2f}")
                        print(f"Init {symbol} EMA5={ema5:.2f}, VWMA20={vwma20:.2f}")
                else:
                    logging.warning(f"No history for {symbol}, response={hist}")
            except Exception as e:
                logging.error(f"Failed to fetch history for {symbol}: {e}")



# ---------------- MAIN ----------------
if __name__ == "__main__":
    fyers_client = FyersClient(CLIENT_ID, ACCESS_TOKEN, LOT_SIZE)
    engine = StrategyEngine(fyers_client, LOT_SIZE)
    option_symbols = ["BSE:SENSEX2590480100CE", "BSE:SENSEX2590480100PE"]

    # ✅ Pre-fill EMA/VWMA before live ticks
    engine.prefill_history(option_symbols, days_back=1)

    fyers_client.register_trade_callback(engine.on_trade)

    
    fyers_client.subscribe_market_data(option_symbols, engine.on_candle)
    fyers_client.start_order_socket()
