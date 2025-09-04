import datetime
import logging
from get_token import ensure_token
from collections import defaultdict, deque
import os
from dotenv import load_dotenv
from fyers_apiv3 import fyersModel
from fyers_apiv3.FyersWebsocket import order_ws, data_ws

ACCESS_TOKEN = ensure_token()
load_dotenv()

# ---------------- CONFIG ----------------
CLIENT_ID = os.getenv("FYERS_CLIENT_ID")
secret_key = os.getenv("FYERS_SECRET_KEY")
LOT_SIZE = 75   # ✅ Official NIFTY lot size
TRADING_START = datetime.time(9, 15)
TRADING_END = datetime.time(15, 30)

# Reset logging handlers (important if rerun in same session)
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

# Setup logging (separate log for NIFTY strategy)
logging.basicConfig(
    filename="nifty_strategy.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

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
        self.trade_callbacks = []
        self.order_callbacks = []

    def place_limit_buy(self, symbol: str, qty: int, price: float, tag: str = ""):
        """Place a LIMIT BUY order"""
        return self.client.place_order({
            "symbol": symbol,
            "qty": qty,
            "type": 1,      # LIMIT
            "side": 1,      # BUY
            "productType": "INTRADAY",
            "limitPrice": price,
            "stopPrice": 0,
            "validity": "DAY",
            "disclosedQty": 0,
            "offlineOrder": False,
            "orderTag": tag
        })

    def place_market_sell(self, symbol: str, qty: int, tag: str = ""):
        return self.client.place_order({
            "symbol": symbol,
            "qty": qty,
            "type": 2,      # MARKET
            "side": -1,     # SELL
            "productType": "INTRADAY",
            "limitPrice": 0,
            "stopPrice": 0,
            "validity": "DAY",
            "disclosedQty": 0,
            "offlineOrder": False,
            "orderTag": tag
        })

    def place_stoploss_sell(self, symbol: str, qty: int, stop_price: float, tag: str = ""):
        """Place a stop-loss sell (SL-M) order"""
        return self.client.place_order({
            "symbol": symbol,
            "qty": qty,
            "type": 3,      # SL-M
            "side": -1,     # SELL
            "productType": "INTRADAY",
            "limitPrice": 0,
            "stopPrice": stop_price,
            "validity": "DAY",
            "disclosedQty": 0,
            "offlineOrder": False,
            "orderTag": tag
        })

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

    def register_trade_callback(self, cb):
        self.trade_callbacks.append(cb)

    def register_order_callback(self, cb):
        self.order_callbacks.append(cb)

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
                logging.error(f"Bad tick format: {tick}, {e}")
                return

            dt = datetime.datetime.fromtimestamp(ts)
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
class NiftyBuyStrategy:
    def __init__(self, fyers_client: FyersClient, lot_size: int = 75):
        self.fyers = fyers_client
        self.lot_size = lot_size
        self.position = None
        self.pending_entry = None
        self.candles = defaultdict(lambda: deque(maxlen=200))

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

    def detect_buy_signal(self, candles, ema5, vwma20):
        if len(candles) < 2:
            return None
        now = datetime.datetime.now().time()
        if now < datetime.time(10, 0):
            return None

        prev_candle = candles[-2]
        breakout_candle = candles[-1]

        if prev_candle["close"] < vwma20 and vwma20 > ema5:
            if breakout_candle["close"] > vwma20 and breakout_candle["close"] > ema5:
                entry = vwma20
                sl = breakout_candle["low"] - 2
                target = entry + 2*(entry - sl)
                return {"entry": entry, "sl": sl, "target": target}
        return None

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

        if self.position is None and self.pending_entry is None:
            signal = self.detect_buy_signal(candles, ema5, vwma20)
            if signal:
                order_resp = self.fyers.place_limit_buy(symbol, self.lot_size, signal["entry"], tag="NIFTY_BUY_ENTRY")
                self.pending_entry = {"symbol": symbol, "signal": signal, "order_resp": order_resp}
                logging.info(f"[ENTRY ORDER PLACED] {symbol} LIMIT {signal['entry']}, SL={signal['sl']}, Target={signal['target']}")
                print(f"[ENTRY ORDER PLACED] {symbol} LIMIT {signal['entry']}")

        if self.position:
            if candle["high"] >= self.position["target"]:
                sl_order_id = self.position["sl_order"].get("id") if self.position["sl_order"] else None
                if sl_order_id:
                    cancel_resp = self.fyers.cancel_order(sl_order_id)
                    logging.info(f"SL order cancelled before target exit: {cancel_resp}")
                exit_resp = self.fyers.place_market_sell(self.position["symbol"], self.lot_size, tag="EXIT_TARGET")
                logging.info(f"[EXIT TARGET] {self.position['symbol']} Target hit. Exit resp: {exit_resp}")
                print(f"[EXIT TARGET] {self.position['symbol']} Target hit")
                self.position = None

    def on_trade(self, msg):
        trades = msg.get("trades")
        if not trades:
            return

        side = trades.get("side")
        order_tag = trades.get("orderTag")
        symbol = trades.get("symbol")

        # Entry fill confirmation
        if side == 1 and order_tag == "NIFTY_BUY_ENTRY" and self.pending_entry:
            signal = self.pending_entry["signal"]
            sl_resp = self.fyers.place_stoploss_sell(symbol, self.lot_size, signal["sl"], tag="NIFTY_BUY_SL")
            self.position = {"symbol": symbol, "sl_order": sl_resp, "target": signal["target"]}
            self.pending_entry = None
            logging.info(f"[ENTRY FILLED] {symbol}, SL={signal['sl']}, Target={signal['target']}")
            print(f"[ENTRY FILLED] {symbol}")

        # SL exit
        if side == -1 and order_tag == "NIFTY_BUY_SL" and self.position:
            logging.info(f"[EXIT SL] {self.position['symbol']} SL hit")
            print(f"[EXIT SL] {self.position['symbol']}")
            self.position = None

# ---------------- MAIN ----------------
if __name__ == "__main__":
    fyers_client = FyersClient(CLIENT_ID, ACCESS_TOKEN, LOT_SIZE)
    engine = NiftyBuyStrategy(fyers_client, LOT_SIZE)

    # Example NIFTY option symbols (update dynamically in live trading)
    option_symbols = ["NSE:NIFTY2590224400CE", "NSE:NIFTY2590224450PE"]

    fyers_client.register_trade_callback(engine.on_trade)
    fyers_client.subscribe_market_data(option_symbols, engine.on_candle)
    fyers_client.start_order_socket()
