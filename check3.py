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

TARGET_DATE   = datetime.date(2026, 9, 17)
LOOKBACK_DAYS = 7          # extra prior days fetched only to seed EMA5/EMA20
START_TIME = datetime.time(9, 15)
END_TIME   = datetime.time(15, 15)

# Same weekly-expiry holiday list as the main strategy script — extend if needed.
SPECIAL_MARKET_HOLIDAYS = {
    datetime.date(2026, 1, 26), datetime.date(2026, 3, 3), datetime.date(2026, 3, 26),
    datetime.date(2026, 3, 31), datetime.date(2026, 4, 14), datetime.date(2026, 5, 1),
    datetime.date(2026, 5, 28), datetime.date(2026, 6, 26), datetime.date(2026, 9, 14),
    datetime.date(2026, 10, 2), datetime.date(2026, 11, 24), datetime.date(2026, 12, 25),
}


def is_last_tuesday(d):
    is_tuesday   = d.weekday() == 1
    is_last_week = (d + datetime.timedelta(days=7)).month != d.month
    if is_tuesday and is_last_week:
        return d not in SPECIAL_MARKET_HOLIDAYS
    if d.weekday() == 0:
        next_day = d + datetime.timedelta(days=1)
        is_last_week_tuesday = (next_day + datetime.timedelta(days=7)).month != next_day.month
        if next_day in SPECIAL_MARKET_HOLIDAYS and is_last_week_tuesday:
            return True
    return False


def get_next_expiry_for(ref_date):
    """The weekly expiry (next Tuesday, inclusive) applicable when trading on ref_date."""
    days   = (1 - ref_date.weekday()) % 7
    expiry = ref_date + datetime.timedelta(days=days)
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
            candles.append({"time": dt, "open": c[1], "high": c[2], "low": c[3], "close": c[4]})
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
expiry_code = format_expiry(get_next_expiry_for(TARGET_DATE))

# ── Step 1: spot on target date's 9:15 candle close -> ATM -> ITM1 CE/PE ──
spot_candles = get_candles("NSE:NIFTY50-INDEX", TARGET_DATE, TARGET_DATE)
spot = spot_candles[0]["close"]
atm = round(spot / 50) * 50
ce_strike = atm - 50
pe_strike = atm + 50

ce_symbol = f"NSE:NIFTY2692223400CE"
pe_symbol = f"NSE:NIFTY2692223400PE"
print(f"Expiry code={expiry_code}  Spot(9:15 close)={spot}  ATM={atm}  "
      f"ITM1 CE={ce_strike} ({ce_symbol})  ITM1 PE={pe_strike} ({pe_symbol})\n")

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
        print(f"{c['time'].strftime('%H:%M')}  open={c['open']:.2f}  high={c['high']:.2f}  "
              f"low={c['low']:.2f}  close={c['close']:.2f}  "
              f"ema5={ema5 if ema5 is None else round(ema5,2)}  "
              f"ema20={ema20 if ema20 is None else round(ema20,2)}")
    print()
