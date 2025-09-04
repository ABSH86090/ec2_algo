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
        if len(prices) < period: return None
        ema = prices[0]; k = 2/(period+1)
        for p in prices[1:]:
            ema = p*k + ema*(1-k)
        return ema

    def compute_vwma(self, closes, volumes, period):
        if len(closes) < period: return None
        return sum([c*v for c, v in zip(closes[-period:], volumes[-period:])]) / sum(volumes[-period:])

    def detect_pattern(self, candles):
        
        """
        Pattern (last 6 candles):
        R, R, G, R, G, R
        Constraint:
        close[2nd red] < close[1st red]
        Additional filter (built-in here):
        For each of these 6 candles, EMA5 < VWMA20 when computed on data up to that candle.
        """
        n = len(candles)
        if n < 6:
            return False

        # Color checks on the last 6 candles
        last6 = list(candles)[-6:]
        def is_red(c):   return c["close"] <= c["open"]
        def is_green(c): return c["close"] > c["open"]

        c0, c1, c2, c3, c4, c5 = last6  # oldest -> newest
        if not (is_red(c0) and is_red(c1) and is_green(c2) and is_red(c3) and is_green(c4) and is_red(c5)):
            return False

        # 2nd red close < 1st red close
        if not (c1["close"] < c0["close"]):
            return False

        # Ensure EMA5 < VWMA20 at EACH step of these 6 candles.
        # We compute indicators using all data up to that candle (inclusive).
        # Need enough history for VWMA20; if not available at any step, fail.
        closes = [c["close"] for c in candles]
        volumes = [c["volume"] for c in candles]

        start_idx = n - 6  # index of c0 in the full series
        for i in range(start_idx, n):  # iterate c0..c5 (inclusive)
            ema5  = self.compute_ema(closes[:i+1], 5)
            vwma20 = self.compute_vwma(closes[:i+1], volumes[:i+1], 20)
            if ema5 is None or vwma20 is None:
                return False  # not enough history to validate the rule
            if not (ema5 < vwma20):
                return False

        return True

    def detect_new_short_strategy(self, candles, ema5, vwma20):
        if len(candles) < 3: return None
        if ema5 is None or vwma20 is None:
            return None
        if not (ema5 < vwma20 and vwma20 >= ema5 * 1.02):
            return None
        c1, c2, c3 = candles[-3], candles[-2], candles[-1]
        is_green = lambda c: c["close"] > c["open"]
        is_red = lambda c: c["close"] < c["open"]
        if (is_green(c1) and is_green(c2) and
            vwma20 > ema5 and
            is_red(c3) and c3["high"] >= vwma20 and
            c3["close"] < ema5 and c3["close"] < vwma20):
            entry = c3["close"]
            sl    = c3["high"] + 5

            if (sl - entry) > 60:
                logging.info(f"[STRAT2] Skipped due to wide SL: {sl-entry:.2f} pts")
                return None
            return {"entry": c3["close"], "sl": c3["high"] + 5}
        return None

    def reconcile_position(self, symbol):
        """Check broker positions to ensure internal state is correct."""
        try:
            resp = self.fyers.client.positions()
            if resp.get("s") == "ok":
                net_positions = resp.get("netPositions", [])
                open_qty = 0
                for pos in net_positions:
                    if pos["symbol"] == symbol:
                        open_qty = pos.get("netQty", 0)
                        break
                if open_qty == 0:
                    self.positions[symbol] = None
                    logging.warning(f"[RECONCILE] {symbol} - No broker position, resetting local state.")
        except Exception as e:
            logging.error(f"[RECONCILE FAILED] {symbol} - {e}")

    def on_candle(self, symbol, candle):
        self.candles[symbol].append(candle)
        candles = list(self.candles[symbol])
        closes = [c["close"] for c in candles]
        volumes = [c["volume"] for c in candles]
        ema5 = self.compute_ema(closes, 5)
        vwma20 = self.compute_vwma(closes, volumes, 20)
        if ema5 is None or vwma20 is None: return

        logging.info(
            f"[Candle] {symbol} {candle['time']} EMA5={ema5:.2f}, "
            f"VWMA20={vwma20:.2f}, Close={candle['close']}"
        )

        now = datetime.datetime.now().time()
        if not (TRADING_START <= now <= TRADING_END): return
        if self.sl_count >= MAX_SLS_PER_DAY: return

        position = self.positions.get(symbol)

        # STRAT1
        if position is None and self.detect_pattern(candles) and ema5 < vwma20:
            entry_price = candles[-1]["close"]
            sl_price = candles[-1]["high"] + 2
            if (sl_price - entry_price) > 60:
                logging.info(f"[STRAT1] Skipped due to wide SL: {sl_price-entry_price:.2f} pts")
            else:
                self.fyers.place_limit_sell(symbol, self.lot_size, entry_price, "STRAT1ENTRY")
                sl_resp = self.fyers.place_stoploss_buy(symbol, self.lot_size, sl_price, "STRAT1SL")
                # Store entry & SL for later comparison/journaling
                self.positions[symbol] = {
                    "sl_order": sl_resp,
                    "strategy": "strat1",
                    "entry_price": entry_price,
                    "sl_price": sl_price
                }
                log_to_journal(symbol, "ENTRY", "strat1", entry=entry_price, sl=sl_price)

        # STRAT2
        if position is None:
            strat2 = self.detect_new_short_strategy(candles, ema5, vwma20)
            if strat2:
                self.fyers.place_limit_sell(symbol, self.lot_size, strat2["entry"], "STRAT2ENTRY")
                sl_resp = self.fyers.place_stoploss_buy(symbol, self.lot_size, strat2["sl"], "STRAT2SL")
                self.positions[symbol] = {
                    "sl_order": sl_resp,
                    "strategy": "strat2",
                    "entry_price": strat2["entry"],
                    "sl_price": strat2["sl"]
                }
                log_to_journal(symbol, "ENTRY", "strat2", entry=strat2["entry"], sl=strat2["sl"])

        # EXIT conditions
        position = self.positions.get(symbol)
        if position and candle["close"] > ema5 and candle["close"] > vwma20 and candle["close"] > candle["open"]:
            sl_order_id = position["sl_order"].get("id") if position["sl_order"] else None
            exit_price = candle["close"]
            if sl_order_id:
                cancel_resp = self.fyers.cancel_order(sl_order_id)
                if cancel_resp.get("s") == "ok":
                    self.fyers.place_market_buy(symbol, self.lot_size, "EXITCOND")
                    # --- Treat exit as SL if exit_price > entry_price (loss on short) ---
                    entry_price = position.get("entry_price")
                    sl_price = position.get("sl_price")
                    if entry_price is not None and exit_price > entry_price:
                        # Count as SL hit
                        self.sl_count += 1
                        log_to_journal(
                            symbol, "SL_HIT", position["strategy"],
                            entry=entry_price, sl=sl_price, exit=exit_price,
                            remarks="Exit condition; Exit>Entry so treated as SL"
                        )
                    else:
                        log_to_journal(
                            symbol, "EXIT", position["strategy"],
                            entry=entry_price, sl=sl_price, exit=exit_price,
                            remarks="Exit condition met"
                        )
                    self.positions[symbol] = None
                else:
                    logging.warning(f"[EXIT SKIPPED] {symbol} - Cancel failed: {cancel_resp}")
                    # ✅ Reconcile with broker
                    self.reconcile_position(symbol)

    def on_trade(self, msg):
        if not msg.get("trades"): return
        symbol = msg["trades"].get("symbol")
        side = msg["trades"].get("side")
        position = self.positions.get(symbol)
        if position and side == 1:  # SL Buy executed
            # Keep existing SL logging; include stored entry/sl for completeness
            log_to_journal(
                symbol, "SL_HIT", position["strategy"],
                entry=position.get("entry_price"), sl=position.get("sl_price"),
                remarks=str(msg)
            )
            self.sl_count += 1
            self.positions[symbol] = None

# ---------------- MAIN ----------------
if __name__ == "__main__":
    fyers_client = FyersClient(CLIENT_ID, ACCESS_TOKEN, LOT_SIZE)
    engine = StrategyEngine(fyers_client, LOT_SIZE)

    # Wait until 9:15 AM
    while datetime.datetime.now().time() < TRADING_START:
        pass

    option_symbols = get_atm_symbols(fyers_client)
    engine.prefill_history(option_symbols, days_back=1)
    fyers_client.register_trade_callback(engine.on_trade)
    fyers_client.subscribe_market_data(option_symbols, engine.on_candle)
    fyers_client.start_order_socket()
