import datetime
import logging
import csv
import os
from collections import defaultdict, deque
from dotenv import load_dotenv
from fyers_apiv3 import fyersModel
from fyers_apiv3.FyersWebsocket import order_ws, data_ws

# ---------------- CONFIG ----------------
load_dotenv()

CLIENT_ID = os.getenv("FYERS_CLIENT_ID")
ACCESS_TOKEN = os.getenv("FYERS_ACCESS_TOKEN")  # Format: APPID-XXXXX:token
LOT_SIZE = 20
MAX_SLS_PER_DAY = 3
TRADING_START = datetime.time(9, 15)
TRADING_END = datetime.time(15, 0)
JOURNAL_FILE = "sensex_trades.csv"

# Ensure journal file exists with headers
if not os.path.exists(JOURNAL_FILE):
    with open(JOURNAL_FILE, mode="w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "timestamp", "symbol", "action", "strategy",
            "entry_price", "sl_price", "exit_price", "remarks"
        ])

# Reset logging handlers
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

logging.basicConfig(
    filename="strategy.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# ---------------- JOURNAL FUNCTION ----------------
def log_to_journal(symbol, action, strategy=None, entry=None, sl=None, exit=None, remarks=""):
    with open(JOURNAL_FILE, mode="a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            symbol, action, strategy, entry, sl, exit, remarks
        ])

# ---------------- EXPIRY & SYMBOL UTILS ----------------
def get_next_expiry():
    """Return expiry date (Thursday) for SENSEX options."""
    today = datetime.date.today()
    weekday = today.weekday()  # Monday=0 ... Sunday=6

    if weekday == 3:  # Thursday
        expiry = today
    else:
        days_to_thu = (3 - weekday) % 7
        expiry = today + datetime.timedelta(days=days_to_thu)
    return expiry

def format_expiry(expiry_date):
    """Format expiry date based on month rule (YYMDD if month<10, YYMMDD otherwise)."""
    yy = expiry_date.strftime("%y")
    m = expiry_date.month
    d = expiry_date.day
    if m < 10:
        return f"{yy}{m}{d:02d}"   # YYMDD
    else:
        return f"{yy}{m:02d}{d:02d}"  # YYMMDD

def get_atm_symbols(fyers_client):
    """Fetch SENSEX spot, round to ATM strike, and build CE/PE option symbols."""
    data = {"symbols": "BSE:SENSEX-INDEX"}
    resp = fyers_client.client.quotes(data)
    if not resp.get("d"):
        raise Exception(f"Failed to fetch SENSEX spot: {resp}")

    ltp = float(resp["d"][0]["v"]["lp"])  # last traded price
    atm_strike = round(ltp / 100) * 100   # nearest 100

    expiry = get_next_expiry()
    expiry_str = format_expiry(expiry)

    ce_symbol = f"BSE:SENSEX{expiry_str}{atm_strike}CE"
    pe_symbol = f"BSE:SENSEX{expiry_str}{atm_strike}PE"

    print(f"[ATM SYMBOLS] CE={ce_symbol}, PE={pe_symbol}")
    logging.info(f"[ATM SYMBOLS] CE={ce_symbol}, PE={pe_symbol}")
    return [ce_symbol, pe_symbol]

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

    def place_limit_sell(self, symbol: str, qty: int, price: float, tag: str = ""):
        data = {
            "symbol": symbol, "qty": qty, "type": 1, "side": -1,
            "productType": "INTRADAY", "limitPrice": price, "stopPrice": 0,
            "validity": "DAY", "disclosedQty": 0, "offlineOrder": False, "orderTag": tag
        }
        return self._log_order_resp("Limit Sell", self.client.place_order(data))

    def place_stoploss_buy(self, symbol: str, qty: int, stop_price: float, tag: str = ""):
        data = {
            "symbol": symbol, "qty": qty, "type": 3, "side": 1,
            "productType": "INTRADAY", "limitPrice": 0, "stopPrice": stop_price,
            "validity": "DAY", "disclosedQty": 0, "offlineOrder": False, "orderTag": tag
        }
        return self._log_order_resp("SL Buy", self.client.place_order(data))

    def place_market_buy(self, symbol: str, qty: int, tag: str = ""):
        data = {
            "symbol": symbol, "qty": qty, "type": 2, "side": 1,
            "productType": "INTRADAY", "limitPrice": 0, "stopPrice": 0,
            "validity": "DAY", "disclosedQty": 0, "offlineOrder": False, "orderTag": tag
        }
        return self._log_order_resp("Market Buy", self.client.place_order(data))

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
            write_to_file=False, log_path="",
            on_connect=on_open,
            on_close=lambda m: logging.info(f"Order socket closed: {m}"),
            on_error=lambda m: logging.error(f"Order socket error: {m}"),
            on_orders=on_order,
            on_trades=on_trade
        )
        fyers.connect()

    def subscribe_market_data(self, instrument_ids, on_candle_callback):
        """Subscribe to live market data and build 3-min candles."""
        candle_buffers = defaultdict(lambda: None)

        def on_message(tick):
            try:
                if "symbol" not in tick or "ltp" not in tick:
                    return
                symbol = tick["symbol"]
                ltp = float(tick["ltp"])
                ts = int(tick["last_traded_time"])
                cum_vol = int(tick.get("vol_traded_today", 0))
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

    def register_order_callback(self, cb): self.order_callbacks.append(cb)
    def register_trade_callback(self, cb): self.trade_callbacks.append(cb)

# ---------------- STRATEGY ENGINE ----------------
class StrategyEngine:
    def __init__(self, fyers_client: FyersClient, lot_size: int = 20):
        self.fyers = fyers_client
        self.lot_size = lot_size
        self.positions = {}
        self.sl_count = 0
        self.candles = defaultdict(lambda: deque(maxlen=200))

    def prefill_history(self, symbols, days_back=1):
        """Prefill candles with historical data for warm start."""
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
                logging.info(f"Fetched history for {symbol}")
            except Exception as e:
                logging.error(f"Failed to fetch history for {symbol}: {e}")

        
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
    
    def detect_new_short_strategy(self, candles, ema5, vwma20):
        if len(candles) < 3:
            return None
        c1, c2, c3 = candles[-3], candles[-2], candles[-1]
        is_green = lambda c: c["close"] > c["open"]
        is_red = lambda c: c["close"] < c["open"]

        if (is_green(c1) and is_green(c2) and
            c1["close"] > c1["open"] and c2["close"] > c2["open"] and
            vwma20 > ema5 and
            is_red(c3) and
            c3["high"] >= vwma20 and
            c3["close"] < ema5 and c3["close"] < vwma20):
            return {"entry": c3["close"], "sl": c3["high"] + 5}
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

        if self.sl_count >= MAX_SLS_PER_DAY:
            logging.info("Max SLs reached. No more trades today.")
            return

        # Current position for this symbol
        position = self.positions.get(symbol)

        # -------- STRATEGY 1 --------
        # [same logic as before ...]
        # ENTRY strategy examples with journal logging:
        if position is None:
            if self.detect_pattern(candles) and ema5 < vwma20:
                breakdown = candles[-1]
                entry_price = breakdown["close"]
                sl_price = breakdown["high"] + 2

                resp = self.fyers.place_limit_sell(symbol, self.lot_size, entry_price, tag="STRAT1ENTRY")
                sl_resp = self.fyers.place_stoploss_buy(symbol, self.lot_size, sl_price, tag="STRAT1SL")

                self.fyers.place_limit_sell(symbol, self.lot_size, entry_price, "STRAT1ENTRY")
                sl_resp = self.fyers.place_stoploss_buy(symbol, self.lot_size, sl_price, "STRAT1SL")
                self.positions[symbol] = {"sl_order": sl_resp, "strategy": "strat1"}
                logging.info(f"[STRAT1 ENTRY] {symbol} @ {entry_price}, SL: {sl_price}")
                print(f"[STRAT1 ENTRY] {symbol} @ {entry_price}, SL: {sl_price}")
                log_to_journal(symbol, "ENTRY", "strat1", entry=entry_price, sl=sl_price)

        # -------- STRATEGY 2 --------
        if position is None:
            strat2 = self.detect_new_short_strategy(candles, ema5, vwma20)
            if strat2:
                resp = self.fyers.place_limit_sell(symbol, self.lot_size, strat2["entry"], tag="STRAT2ENTRY")
                sl_resp = self.fyers.place_stoploss_buy(symbol, self.lot_size, strat2["sl"], tag="STRAT2SL")

                self.fyers.place_limit_sell(symbol, self.lot_size, strat2["entry"], "STRAT2ENTRY")
                sl_resp = self.fyers.place_stoploss_buy(symbol, self.lot_size, strat2["sl"], "STRAT2SL")
                self.positions[symbol] = {"sl_order": sl_resp, "strategy": "strat2"}
                logging.info(f"[STRAT2 ENTRY] {symbol} @ {strat2['entry']}, SL: {strat2['sl']}")
                print(f"[STRAT2 ENTRY] {symbol} @ {strat2['entry']}, SL: {strat2['sl']}")
                log_to_journal(symbol, "ENTRY", "strat2", entry=strat2["entry"], sl=strat2["sl"])

        # -------- EXIT CONDITIONS (only for this symbol) --------
        position = self.positions.get(symbol)
        # EXIT conditions with journal logging
        if position:
            if candle["close"] > ema5 and candle["close"] > vwma20 and candle["close"] > candle["open"]:
                sl_order_id = position["sl_order"].get("id") if position["sl_order"] else None
                if sl_order_id:
                    cancel_resp = self.fyers.cancel_order(sl_order_id)
                    logging.info(f"SL order cancel attempt: {cancel_resp}")

                    # ✅ Only if cancel succeeded, exit at market
                    if cancel_resp.get("s") == "ok":
                        resp = self.fyers.place_market_buy(symbol, self.lot_size, tag="EXITCOND")
                        logging.info(f"[EXIT COND] {symbol} {position['strategy']} exited via market, Close={candle['close']}")
                        print(f"[EXIT COND] {symbol} {position['strategy']} exited")
                        resp = self.fyers.place_market_buy(symbol, self.lot_size, "EXITCOND")
                        log_to_journal(symbol, "EXIT", position["strategy"], exit=candle["close"], remarks="Exit condition met")
                        self.positions[symbol] = None
                    else:
                        logging.warning(f"[EXIT SKIPPED] {symbol} - SL may have already triggered. Cancel failed: {cancel_resp}")
                else:
                    logging.warning(f"[EXIT SKIPPED] {symbol} - No SL order ID found.")

    def on_trade(self, msg):
        if not msg.get("trades"):
            return

        symbol = msg["trades"].get("symbol")
        side = msg["trades"].get("side")  # 1 = BUY, -1 = SELL

        side = msg["trades"].get("side")
        position = self.positions.get(symbol)
        if position and side == 1:  # SL buy executed
            logging.info(f"[EXIT SL] {symbol} {position['strategy']} SL hit, Trade={msg}")
            print(f"[EXIT SL] {symbol} {position['strategy']} SL hit")
            log_to_journal(symbol, "SL_HIT", position["strategy"], remarks=str(msg))
            self.sl_count += 1
            self.positions[symbol] = None

    # ... compute_ema, compute_vwma, detect_pattern, detect_new_short_strategy, on_candle, on_trade ...
    # (unchanged from the version I gave earlier)

# ---------------- MAIN ----------------
if __name__ == "__main__":
    fyers_client = FyersClient(CLIENT_ID, ACCESS_TOKEN, LOT_SIZE)
    engine = StrategyEngine(fyers_client, LOT_SIZE)

    # Wait until 9:15 AM
    while datetime.datetime.now().time() < TRADING_START:
        pass

    option_symbols = get_atm_symbols(fyers_client)

    # ✅ Prefill 1 day of historical 3-min candles
    engine.prefill_history(option_symbols, days_back=1)

    fyers_client.register_trade_callback(engine.on_trade)
    fyers_client.subscribe_market_data(option_symbols, engine.on_candle)
    fyers_client.start_order_socket()
