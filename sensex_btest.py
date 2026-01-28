import os
import datetime
import pandas as pd
from dotenv import load_dotenv
from fyers_apiv3 import fyersModel

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
SYMBOL = "BSE:SENSEX-INDEX"
TIMEFRAME = "15"

EMA_FAST = 5
EMA_SLOW = 20

TARGET_POINTS = 340
SL_POINTS = 70

HARD_EXIT_TIME = datetime.time(14, 50)  # 🔒 2:45 PM hard exit

END_DATE = datetime.date.today() - datetime.timedelta(days=0)
START_DATE = END_DATE - datetime.timedelta(days=365)

CHUNK_DAYS = 7

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
# HELPERS
# ======================================================
def ema(series, period):
    return series.ewm(span=period, adjust=False).mean()

def compute_cpr(day):
    h, l, c = day["high"], day["low"], day["close"]
    p = (h + l + c) / 3
    bc = (h + l) / 2
    tc = 2 * p - bc
    r1 = 2 * p - l
    s1 = 2 * p - h
    return {
        "BC": min(bc, tc),
        "TC": max(bc, tc),
        "R1": r1,
        "S1": s1
    }

# ======================================================
# SCENARIO ENGINE
# ======================================================
def evaluate_scenarios(prev_candle, candle, cpr, ema5, ema20):

    if (
        prev_candle["close"] > cpr["BC"]
        and candle["close"] < cpr["BC"]
        and candle["close"] > cpr["S1"]
        and ema5 < ema20
    ):
        return "S1_BC_BREAKDOWN", "PUT"

    if (
        prev_candle["close"] > cpr["S1"]
        and candle["close"] < cpr["S1"]
        and ema5 < ema20
    ):
        return "S2_S1_BREAKDOWN", "PUT"

    if (
        prev_candle["close"] < cpr["R1"]
        and candle["close"] > cpr["R1"]
        and ema5 > ema20
    ):
        return "S5_R1_BREAKOUT", "CALL"

    return None, None

# ======================================================
# FETCH DATA
# ======================================================
all_candles = []
current = START_DATE

while current <= END_DATE:
    chunk_end = min(current + datetime.timedelta(days=CHUNK_DAYS), END_DATE)

    resp = fyers.history({
        "symbol": SYMBOL,
        "resolution": TIMEFRAME,
        "date_format": "1",
        "range_from": current.strftime("%Y-%m-%d"),
        "range_to": chunk_end.strftime("%Y-%m-%d"),
        "cont_flag": "1"
    })

    if resp.get("s") == "ok" and resp.get("candles"):
        all_candles.extend(resp["candles"])

    current = chunk_end + datetime.timedelta(days=1)

df = pd.DataFrame(all_candles, columns=["ts", "open", "high", "low", "close", "volume"])

# ======================================================
# TIMEZONE FIX
# ======================================================
df["time"] = (
    pd.to_datetime(df["ts"], unit="s", utc=True)
      .dt.tz_convert("Asia/Kolkata")
      .dt.tz_localize(None)
)

df.set_index("time", inplace=True)
df.sort_index(inplace=True)
df = df.between_time("09:15", "15:30")
df = df[~df.index.duplicated(keep="first")]

# ======================================================
# DAILY OHLC
# ======================================================
daily_ohlc = df.resample("1D").agg({
    "open": "first",
    "high": "max",
    "low": "min",
    "close": "last"
}).dropna()

# ======================================================
# INDICATORS
# ======================================================
df["ema5"] = ema(df["close"], EMA_FAST)
df["ema20"] = ema(df["close"], EMA_SLOW)

# ======================================================
# BACKTEST
# ======================================================
trades = []
traded_days = set()

for i in range(2, len(df) - 2):

    candle = df.iloc[i]
    prev_candle = df.iloc[i - 1]
    trade_day = candle.name.date()

    # one trade per day
    if trade_day in traded_days:
        continue

    # skip first candle trigger
    if prev_candle.name.date() != candle.name.date():
        continue

    prev_days = daily_ohlc.index[daily_ohlc.index < candle.name.normalize()]
    if prev_days.empty:
        continue

    cpr = compute_cpr(daily_ohlc.loc[prev_days[-1]])

    scenario, trade_type = evaluate_scenarios(
        prev_candle,
        candle,
        cpr,
        candle["ema5"],
        candle["ema20"]
    )

    if scenario is None:
        continue

    # ================= ENTRY =================
    entry_candle = df.iloc[i + 1]
    entry = entry_candle["open"]

    if trade_type == "PUT":
        sl = entry + SL_POINTS
        target = entry - TARGET_POINTS
    else:
        sl = entry - SL_POINTS
        target = entry + TARGET_POINTS

    exit_price = None
    exit_time = None
    reason = None

    # ================= EXIT LOGIC =================
    for j in range(i + 2, len(df)):
        c = df.iloc[j]

        if c.name.date() != trade_day:
            break

        # 🔒 HARD EXIT AT 2:45 PM
        if c.name.time() >= HARD_EXIT_TIME:
            exit_price = c["close"]
            exit_time = c.name
            reason = "2:45_EXIT"
            break

        h, l = c["high"], c["low"]

        if trade_type == "PUT":
            if h >= sl:
                exit_price = sl
                exit_time = c.name
                reason = "SL"
                break
            if l <= target:
                exit_price = target
                exit_time = c.name
                reason = "TARGET"
                break
        else:
            if l <= sl:
                exit_price = sl
                exit_time = c.name
                reason = "SL"
                break
            if h >= target:
                exit_price = target
                exit_time = c.name
                reason = "TARGET"
                break

    # ================= SAFETY EOD EXIT =================
    if exit_price is None:
        last_candle = df[df.index.date == trade_day].iloc[-1]
        exit_price = last_candle["close"]
        exit_time = last_candle.name
        reason = "EOD"

    trades.append({
        "Scenario": scenario,
        "Type": trade_type,
        "Entry Time": entry_candle.name,
        "Entry": round(entry, 2),
        "Exit Time": exit_time,
        "Exit": round(exit_price, 2),
        "PnL (pts)": round(
            entry - exit_price if trade_type == "PUT" else exit_price - entry, 2
        ),
        "Exit Reason": reason
    })

    traded_days.add(trade_day)

# ======================================================
# RESULTS
# ======================================================
results = pd.DataFrame(trades)

print("\n===== BACKTEST RESULTS (2:45 PM HARD EXIT) =====\n")
print(results)

if not results.empty:
    print("\nTotal Trading Days :", len(results))
    print("Win Rate           :", round((results['PnL (pts)'] > 0).mean() * 100, 2), "%")
    print("Total PnL          :", results['PnL (pts)'].sum())
    print("Avg PnL / Day      :", round(results['PnL (pts)'].mean(), 2))
    print("\nExit Reason Breakdown:")
    print(results["Exit Reason"].value_counts())
