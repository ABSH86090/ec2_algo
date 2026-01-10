import os
import datetime
import time
import pandas as pd
from dotenv import load_dotenv
from fyers_apiv3 import fyersModel
from fyers_apiv3.FyersSocket import FyersSocket
import math
import threading
import logging

# ======================================================
# SETUP LOGGING
# ======================================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ======================================================
# LOAD ENV
# ======================================================
load_dotenv()
CLIENT_ID = os.getenv("FYERS_CLIENT_ID")
ACCESS_TOKEN = os.getenv("FYERS_ACCESS_TOKEN")
if not CLIENT_ID or not ACCESS_TOKEN:
    raise Exception("FYERS credentials not found")

# ======================================================
# CONFIG
# ======================================================
TIMEFRAME = "5"  # 5-minute candles
RR_MULTIPLIER = 1.5
MAX_HOLD_MINUTES = 150  # 30 * 5 min
MARKET_OPEN = datetime.time(9, 15)
FIRST_CANDLE_CLOSE = datetime.time(9, 20)
MARKET_CLOSE = datetime.time(15, 30)
EXPIRY_FORMAT = "26JAN"  # Adjust to current/nearest expiry, e.g., "26JAN" for January 2026 monthly
POSITION_SIZE = 1  # Multiplier for lot size; set to 1 for full lot

# ======================================================
# LOT SIZES (UPDATED FOR ALL NIFTY 50 AS OF JAN 2026)
# ======================================================
LOT_SIZE = {
    "ADANIENT": 309,
    "ADANIPORTS": 475,
    "APOLLOHOSP": 125,
    "ASIANPAINT": 250,
    "AXISBANK": 625,
    "BAJAJ-AUTO": 75,
    "BAJFINANCE": 750,
    "BAJAJFINSV": 250,
    "BEL": 1425,
    "BHARTIARTL": 475,
    "CIPLA": 375,
    "COALINDIA": 1350,
    "DRREDDY": 625,
    "EICHERMOT": 100,
    "ETERNAL": 2425,
    "GRASIM": 250,
    "HCLTECH": 350,
    "HDFCBANK": 550,
    "HDFCLIFE": 1100,
    "HINDALCO": 700,
    "HINDUNILVR": 300,
    "ICICIBANK": 700,
    "ITC": 1600,
    "INFY": 400,
    "INDIGO": 150,
    "JSWSTEEL": 675,
    "JIOFIN": 2350,
    "KOTAKBANK": 400,
    "LT": 175,
    "M&M": 200,
    "MARUTI": 50,
    "MAXHEALTH": 525,
    "NTPC": 1500,
    "NESTLEIND": 500,
    "ONGC": 2250,
    "POWERGRID": 1900,
    "RELIANCE": 500,
    "SBILIFE": 375,
    "SHRIRAMFIN": 825,
    "SBIN": 750,
    "SUNPHARMA": 350,
    "TCS": 175,
    "TATACONSUM": 550,
    "TMPV": 800,
    "TATASTEEL": 5500,
    "TECHM": 600,
    "TITAN": 175,
    "TRENT": 100,
    "ULTRACEMCO": 50,
    "WIPRO": 3000
}

# ======================================================
# STEP SIZES (PRIMARY FOR NEAR-ATM AS OF JAN 2026)
# ======================================================
STEP_SIZE = {
    "ADANIENT": 20,
    "ADANIPORTS": 20,
    "APOLLOHOSP": 50,
    "ASIANPAINT": 20,
    "AXISBANK": 10,
    "BAJAJ-AUTO": 100,
    "BAJFINANCE": 10,
    "BAJAJFINSV": 20,
    "BEL": 5,
    "BHARTIARTL": 20,
    "CIPLA": 10,
    "COALINDIA": 2.5,
    "DRREDDY": 10,
    "EICHERMOT": 50,
    "ETERNAL": 5,
    "GRASIM": 20,
    "HCLTECH": 20,
    "HDFCBANK": 5,
    "HDFCLIFE": 10,
    "HINDALCO": 10,
    "HINDUNILVR": 20,
    "ICICIBANK": 10,
    "ITC": 2.5,
    "INFY": 20,
    "INDIGO": 50,
    "JSWSTEEL": 10,
    "JIOFIN": 2.5,
    "KOTAKBANK": 20,
    "LT": 20,
    "M&M": 50,
    "MARUTI": 100,
    "MAXHEALTH": 10,
    "NTPC": 2.5,
    "NESTLEIND": 10,
    "ONGC": 1,
    "POWERGRID": 2.5,
    "RELIANCE": 10,
    "SBILIFE": 20,
    "SHRIRAMFIN": 10,
    "SBIN": 5,
    "SUNPHARMA": 10,
    "TCS": 20,
    "TATACONSUM": 10,
    "TMPV": 5,
    "TATASTEEL": 1,
    "TECHM": 20,
    "TITAN": 20,
    "TRENT": 50,
    "ULTRACEMCO": 100,
    "WIPRO": 2.5
}

SYMBOLS = [f"NSE:{s}-EQ" for s in LOT_SIZE.keys()]

# ======================================================
# FYERS CLIENT
# ======================================================
fyers = fyersModel.FyersModel(
    client_id=CLIENT_ID,
    token=ACCESS_TOKEN,
    is_async=False,
    log_path=""
)

# ======================================================
# HELPER FUNCTIONS
# ======================================================
def get_current_date():
    return datetime.datetime.now().date()

def is_trading_day():
    today = datetime.datetime.now().weekday()
    return today < 5  # Monday to Friday

def get_prev_trading_day():
    today = get_current_date()
    prev_day = today - datetime.timedelta(days=1)
    while prev_day.weekday() >= 5:  # Skip weekends
        prev_day -= datetime.timedelta(days=1)
    return prev_day

def compute_cpr(day):
    h, l, c = day["high"], day["low"], day["close"]
    p = (h + l + c) / 3
    r1 = round(2 * p - l, 2)
    s1 = round(2 * p - h, 2)
    return r1, s1

def get_ohlc(symbol, date):
    resp = fyers.history({
        "symbol": symbol,
        "resolution": "D",
        "date_format": "1",
        "range_from": date.strftime("%Y-%m-%d"),
        "range_to": date.strftime("%Y-%m-%d"),
        "cont_flag": "1"
    })
    if resp.get("s") == "ok" and resp.get("candles"):
        candle = resp["candles"][0]
        return {"open": candle[1], "high": candle[2], "low": candle[3], "close": candle[4]}
    return None

def get_first_candle(symbol):
    today = get_current_date()
    resp = fyers.history({
        "symbol": symbol,
        "resolution": TIMEFRAME,
        "date_format": "1",
        "range_from": today.strftime("%Y-%m-%d"),
        "range_to": today.strftime("%Y-%m-%d"),
        "cont_flag": "1"
    })
    if resp.get("s") == "ok" and resp.get("candles"):
        df = pd.DataFrame(resp["candles"], columns=["ts", "open", "high", "low", "close", "volume"])
        df["time"] = pd.to_datetime(df["ts"], unit="s", utc=True).dt.tz_convert("Asia/Kolkata").dt.time
        first_candle = df[df["time"] == MARKET_OPEN].iloc[0] if not df[df["time"] == MARKET_OPEN].empty else None
        return first_candle
    return None

def get_nearest_itm_strike(underlying_price, step, option_type):
    if option_type == "CE":  # ITM Call: strike <= underlying
        strike = math.floor(underlying_price / step) * step
    elif option_type == "PE":  # ITM Put: strike >= underlying
        strike = math.ceil(underlying_price / step) * step
    else:
        raise ValueError("Invalid option_type")
    return strike

def construct_option_symbol(stock, strike, option_type):
    return f"NSE:{stock}{EXPIRY_FORMAT}{int(strike)}{option_type}"

def place_order(symbol, qty, side=1):  # side=1 buy, -1 sell
    order = {
        "symbol": symbol,
        "qty": qty,
        "type": 2,  # Market order
        "side": side,
        "productType": "INTRADAY",
        "validity": "DAY",
        "limitPrice": 0,
        "stopPrice": 0,
        "disclosedQty": 0,
        "offlineOrder": False
    }
    resp = fyers.place_order(order)
    if resp.get("s") == "ok":
        logger.info(f"Order placed: {symbol} Qty: {qty} Side: {'Buy' if side==1 else 'Sell'}")
        return resp["data"]["orderDetails"]["orderNumber"]
    else:
        logger.error(f"Order failed: {resp}")
        return None

def get_ltp(symbol):
    resp = fyers.quotes({"symbols": symbol})
    if resp.get("s") == "ok":
        return resp["d"][0]["v"]["lp"]
    return None

# ======================================================
# WEBSOCKET FOR REAL-TIME LTP MONITORING
# ======================================================
def monitor_ltp(stock_symbol, sl, target, direction, option_symbol, qty, stop_event):
    fs = FyersSocket(access_token=ACCESS_TOKEN, logs=True)
    fs.websocket_data = lambda data: on_ws_data(data, sl, target, direction, option_symbol, qty, stop_event)
    fs.subscribe(symbol=stock_symbol, data_type="symbolData")
    fs.keep_running()

def on_ws_data(data, sl, target, direction, option_symbol, qty, stop_event):
    if stop_event.is_set():
        return
    try:
        ltp = data[0]['ltp']  # Assuming data format
        high = data[0].get('high_price_of_the_day', ltp)
        low = data[0].get('low_price_of_the_day', ltp)
    except:
        ltp = get_ltp(data[0]['symbol'])
        high = ltp
        low = ltp
    
    exited = False
    if direction == "BUY":
        if low <= sl:
            place_order(option_symbol, qty, side=-1)  # Sell to exit
            logger.info(f"SL hit for {option_symbol} at {sl}")
            exited = True
        elif high >= target:
            place_order(option_symbol, qty, side=-1)
            logger.info(f"Target hit for {option_symbol} at {target}")
            exited = True
    else:  # SELL (but since we buy PE, direction for monitoring is short-like)
        if high >= sl:
            place_order(option_symbol, qty, side=-1)
            logger.info(f"SL hit for {option_symbol} at {sl}")
            exited = True
        elif low <= target:
            place_order(option_symbol, qty, side=-1)
            logger.info(f"Target hit for {option_symbol} at {target}")
            exited = True
    
    if exited:
        stop_event.set()

# ======================================================
# MAIN LIVE TRADING LOOP
# ======================================================
def run_strategy():
    if not is_trading_day():
        logger.info("Not a trading day. Exiting.")
        return
    
    prev_day = get_prev_trading_day()
    
    for symbol in SYMBOLS:
        stock = symbol.split(":")[1].replace("-EQ", "")
        lot = LOT_SIZE.get(stock, 1) * POSITION_SIZE
        step = STEP_SIZE.get(stock, 5)  # Default to 5 if missing
        
        # Get previous day OHLC
        prev_ohlc = get_ohlc(symbol, prev_day)
        if not prev_ohlc:
            logger.warning(f"No prev OHLC for {stock}")
            continue
        R1, S1 = compute_cpr(prev_ohlc)
        
        # Wait for first candle close (9:20)
        while datetime.datetime.now().time() < FIRST_CANDLE_CLOSE:
            time.sleep(10)
        
        first_candle = get_first_candle(symbol)
        if first_candle is None:
            logger.warning(f"No first candle for {stock}")
            continue
        
        entry_price = None
        sl = None
        target = None
        direction = None
        option_type = None
        
        # Scenario 1: R1 Breakout - Buy ITM CE
        if first_candle["open"] < R1 and first_candle["close"] > R1:
            entry_price = get_ltp(symbol)  # Current stock LTP for strike calc
            if not entry_price:
                continue
            sl = first_candle["low"]
            risk = entry_price - sl
            if risk <= 0:
                continue
            target = entry_price + RR_MULTIPLIER * risk
            direction = "BUY"
            option_type = "CE"
        
        # Scenario 2: S1 Breakdown - Buy ITM PE
        elif first_candle["open"] > S1 and first_candle["close"] < S1:
            entry_price = get_ltp(symbol)
            if not entry_price:
                continue
            sl = first_candle["high"]
            risk = sl - entry_price
            if risk <= 0:
                continue
            target = entry_price - RR_MULTIPLIER * risk
            direction = "SELL"  # But buying PE, monitoring as short
            option_type = "PE"
        
        else:
            continue
        
        # Calculate strike and symbol
        strike = get_nearest_itm_strike(entry_price, step, option_type)
        option_symbol = construct_option_symbol(stock, strike, option_type)
        
        # Place buy order for option
        order_id = place_order(option_symbol, lot, side=1)
        if not order_id:
            continue
        
        # Start monitoring thread
        stop_event = threading.Event()
        monitor_thread = threading.Thread(target=monitor_ltp, args=(symbol, sl, target, direction, option_symbol, lot, stop_event))
        monitor_thread.start()
        
        # Time exit logic
        start_time = datetime.datetime.now()
        while not stop_event.is_set():
            if (datetime.datetime.now() - start_time).total_seconds() > MAX_HOLD_MINUTES * 60 or datetime.datetime.now().time() >= MARKET_CLOSE:
                place_order(option_symbol, lot, side=-1)
                logger.info(f"Time/EOD exit for {option_symbol}")
                stop_event.set()
            time.sleep(10)  # Poll every 10s if WS fails
        
        monitor_thread.join()

if __name__ == "__main__":
    run_strategy()
