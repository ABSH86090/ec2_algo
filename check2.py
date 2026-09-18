import datetime
import os
from dotenv import load_dotenv
from fyers_apiv3 import fyersModel

load_dotenv()
fyers = fyersModel.FyersModel(
    client_id=os.getenv("FYERS_CLIENT_ID"),
    token=os.getenv("FYERS_ACCESS_TOKEN"),
    is_async=False,
    log_path=""
)

TARGET_DATE = datetime.date(2026, 9, 18)
LOOKBACK_DAYS = 7          # extra prior days fetched only to seed EMA5/EMA20
START_TIME = datetime.time(9, 15)
END_TIME   = datetime.time(15, 15)
EXPIRY     = "26918"       # NIFTY weekly expiry code for 18-Sep-2026 — verify/edit if wrong


def get_candles(symbol, from_date, to_date):
    r = fyers.history({
        "symbol": symbol,
        "resolution": "5",
        "date_format": "1",
        "range_from": from_date.strftime("%Y-%m-%d"),
        "range_to": to_date.strftime("%Y-%m-%d"),
        "cont_flag": "1",
    })
    candles = []
    for c in r.get("candles", []):
        dt = datetime.datetime.fromtimestamp(c[0])
        if START_TIME <= dt.time() <= END_TIME:
            candles.append({"time": dt, "open": c[1], "close": c[4]})
    candles.sort(key=lambda x: x["time"])
    return candles


def compute_ema(closes, period):
    if len(closes) < period:
        return None
    k = 2 / (period + 1)
    ema = sum(closes[:period]) / period
    for close in closes[period:]:
        ema = round(close * k + ema * (1 - k), 2)
    return ema


from_date = TARGET_DATE - datetime.timedelta(days=LOOKBACK_DAYS)

# ── Step 1: spot on target date's 9:15 candle close -> ATM -> ITM1 CE/PE ──
spot_candles = get_candles("NSE:NIFTY50-INDEX", TARGET_DATE, TARGET_DATE)
spot = spot_candles[0]["close"]
atm = round(spot / 50) * 50
ce_strike = atm - 50
pe_strike = atm + 50

ce_symbol = f"NSE:NIFTY{EXPIRY}{ce_strike}CE"
pe_symbol = f"NSE:NIFTY{EXPIRY}{pe_strike}PE"
print(f"Spot(9:15 close)={spot}  ATM={atm}  ITM1 CE={ce_strike} ({ce_symbol})  ITM1 PE={pe_strike} ({pe_symbol})\n")

# ── Step 2: for each strike, pull [lookback ... target_date], seed EMA on
#            prior days, then print only target_date's rows ──
for label, symbol in [("CE", ce_symbol), ("PE", pe_symbol)]:
    print(f"--- {label} {symbol} ---")
    all_candles = get_candles(symbol, from_date, TARGET_DATE)

    closes = []
    for c in all_candles:
        closes.append(c["close"])
        if c["time"].date() != TARGET_DATE:
            continue   # prior day -> only used to seed EMA, don't print
        ema5 = compute_ema(closes, 5)
        ema20 = compute_ema(closes, 20)
        print(f"{c['time'].strftime('%H:%M')}  open={c['open']:.2f}  close={c['close']:.2f}  "
              f"ema5={ema5 if ema5 is None else round(ema5,2)}  "
              f"ema20={ema20 if ema20 is None else round(ema20,2)}")
    print()
