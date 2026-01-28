import datetime
import os
from collections import defaultdict
from dotenv import load_dotenv
from fyers_apiv3 import fyersModel

# ================= CONFIG =================
load_dotenv()

CLIENT_ID = os.getenv("FYERS_CLIENT_ID")
ACCESS_TOKEN = os.getenv("FYERS_ACCESS_TOKEN")

SYMBOL = "NSE:NIFTY50-INDEX"
RESOLUTION = "15"

ATR_PERIOD = 10
MULTIPLIER = 3.5

EMA_FAST = 5
EMA_SLOW = 20

START_DATE = datetime.date(2025, 1, 15)
END_DATE   = datetime.date(2026, 1, 27)
TG_POINTS = 140
SL_POINTS = 30

ENTRY_CUTOFF = datetime.time(14, 00)

MAX_CHUNK_DAYS = 90
LOOKBACK_DAYS = 2   # previous trading days for EMA + ST warmup

# ================= FYERS =================
fyers = fyersModel.FyersModel(
    client_id=CLIENT_ID,
    token=ACCESS_TOKEN,
    is_async=False,
    log_path=""
)

# ================= SAFE CHUNKED FETCH =================
def fetch_intraday(symbol, resolution, start_date, end_date):
    all_candles = []
    cur = start_date

    while cur <= end_date:
        to = min(cur + datetime.timedelta(days=MAX_CHUNK_DAYS), end_date)

        resp = fyers.history({
            "symbol": symbol,
            "resolution": resolution,
            "date_format": "1",
            "range_from": cur.strftime("%Y-%m-%d"),
            "range_to": to.strftime("%Y-%m-%d"),
            "cont_flag": "1"
        })

        if resp.get("s") != "ok" or "candles" not in resp:
            raise RuntimeError(f"FYERS history failed: {resp}")

        all_candles.extend(resp["candles"])
        cur = to + datetime.timedelta(days=1)

    return all_candles

raw = fetch_intraday(SYMBOL, RESOLUTION, START_DATE, END_DATE)

# ================= BUILD CANDLES + TRADING DAYS =================
days = defaultdict(list)

for c in raw:
    ts = datetime.datetime.fromtimestamp(c[0])
    days[ts.date()].append({
        "time": ts,
        "open": c[1],
        "high": c[2],
        "low": c[3],
        "close": c[4]
    })

trading_days = sorted(days.keys())

# ================= INDICATORS =================
def ema(values, period):
    out = [None] * len(values)
    k = 2 / (period + 1)
    out[period - 1] = sum(values[:period]) / period
    for i in range(period, len(values)):
        out[i] = values[i] * k + out[i - 1] * (1 - k)
    return out


def atr(candles, period):
    tr = []
    for i in range(1, len(candles)):
        h, l = candles[i]["high"], candles[i]["low"]
        pc = candles[i - 1]["close"]
        tr.append(max(h - l, abs(h - pc), abs(l - pc)))

    atr_vals = [None] * period
    atr_val = sum(tr[:period]) / period
    atr_vals.append(atr_val)

    for t in tr[period:]:
        atr_val = (atr_val * (period - 1) + t) / period
        atr_vals.append(atr_val)

    return atr_vals


# ================= FIXED SUPERTREND =================
def supertrend(candles, period, multiplier):
    atr_vals = atr(candles, period)

    st = [None] * len(candles)
    final_ub = [None] * len(candles)
    final_lb = [None] * len(candles)
    trend = [None] * len(candles)

    for i in range(len(candles)):
        if atr_vals[i] is None:
            continue

        high = candles[i]["high"]
        low = candles[i]["low"]
        close = candles[i]["close"]

        hl2 = (high + low) / 2
        basic_ub = hl2 + multiplier * atr_vals[i]
        basic_lb = hl2 - multiplier * atr_vals[i]

        if i == 0 or final_ub[i - 1] is None:
            final_ub[i] = basic_ub
            final_lb[i] = basic_lb
            trend[i] = "BULLISH" if close > basic_ub else "BEARISH"
        else:
            final_ub[i] = (
                basic_ub if basic_ub < final_ub[i - 1] or close > final_ub[i - 1]
                else final_ub[i - 1]
            )
            final_lb[i] = (
                basic_lb if basic_lb > final_lb[i - 1] or close < final_lb[i - 1]
                else final_lb[i - 1]
            )

            if trend[i - 1] == "BULLISH" and close < final_lb[i - 1]:
                trend[i] = "BEARISH"
            elif trend[i - 1] == "BEARISH" and close > final_ub[i - 1]:
                trend[i] = "BULLISH"
            else:
                trend[i] = trend[i - 1]

        st[i] = final_lb[i] if trend[i] == "BULLISH" else final_ub[i]

    return st


def compute_cpr_levels(prev_day):
    h = max(c["high"] for c in prev_day)
    l = min(c["low"] for c in prev_day)
    c = prev_day[-1]["close"]

    pivot = (h + l + c) / 3
    bc = (h + l) / 2
    tc = 2 * pivot - bc

    return min(bc, tc), max(bc, tc)


def build_combined(idx):
    start = max(0, idx - LOOKBACK_DAYS)
    combo = []
    for d in trading_days[start:idx + 1]:
        combo.extend(days[d])
    return combo

# ================= BACKTEST =================
trades = []

for i, day in enumerate(trading_days):
    if i == 0:
        continue

    today = days[day]
    prev_day = days[trading_days[i - 1]]

    BC, TC = compute_cpr_levels(prev_day)

    combined = build_combined(i)

    ema_closes = [c["close"] for c in combined]
    ema5_all = ema(ema_closes, EMA_FAST)
    ema20_all = ema(ema_closes, EMA_SLOW)

    ema5_today = ema5_all[-len(today):]
    ema20_today = ema20_all[-len(today):]

    st_today = supertrend(combined, ATR_PERIOD, MULTIPLIER)[-len(today):]

    # 🔑 FIRST CANDLE TREND DERIVED CORRECTLY
    first_trend = "BULLISH" if today[0]["close"] > st_today[0] else "BEARISH"

    seen_opposite = False
    trade = None

    for idx in range(1, len(today)):
        c = today[idx]
        if c["time"].time() >= ENTRY_CUTOFF:
            break

        close = c["close"]
        st_val = st_today[idx]
        green = close > c["open"]
        red = close < c["open"]

        # ===== PUT =====
        if first_trend == "BEARISH":
            if green and close < st_val:
                seen_opposite = True
            if (
                seen_opposite and red
                and close < st_val
                and close < BC
                and close < ema5_today[idx]
                and close < ema20_today[idx]
            ):
                trade = ("PUT", idx)
                break

        # ===== CALL (uses TC) =====
        if first_trend == "BULLISH":
            if red and close > st_val:
                seen_opposite = True
            if (
                seen_opposite and green
                and close > st_val
                and close > TC
                and close > ema5_today[idx]
                and close > ema20_today[idx]
            ):
                trade = ("CALL", idx)
                break

    if not trade:
        continue

    direction, entry_idx = trade
    entry_candle = today[entry_idx]
    entry_price = entry_candle["close"]

    if direction == "CALL":
        sl = entry_price - SL_POINTS
        target = entry_price + TG_POINTS
    else:  # PUT
        sl = entry_price + SL_POINTS
        target = entry_price - TG_POINTS

    exit_time, exit_price, result = None, None, "EOD"

    for c in today[entry_idx + 1:]:
        if direction == "CALL":
            if c["low"] <= sl:
                exit_price, result = sl, "SL"
                exit_time = c["time"]
                break
            if c["high"] >= target:
                exit_price, result = target, "TARGET"
                exit_time = c["time"]
                break
        else:
            if c["high"] >= sl:
                exit_price, result = sl, "SL"
                exit_time = c["time"]
                break
            if c["low"] <= target:
                exit_price, result = target, "TARGET"
                exit_time = c["time"]
                break

    if exit_time is None:
        last = today[-1]
        exit_time = last["time"]
        exit_price = last["close"]

    pts = exit_price - entry_price if direction == "CALL" else entry_price - exit_price

    trades.append({
        "date": day,
        "direction": direction,
        "entry_time": entry_candle["time"],
        "entry_price": round(entry_price, 2),
        "exit_time": exit_time,
        "exit_price": round(exit_price, 2),
        "result": result,
        "points": round(pts, 2)
    })

# ================= REPORT =================
print("\nDAY WISE TRADE REPORT")
print("Date       Dir  EntryTime  Entry   ExitTime   Exit    Result   Pts")
print("-" * 80)

for t in trades:
    print(
        f"{t['date']}  {t['direction']:4} "
        f"{t['entry_time'].time()}  {t['entry_price']:7}  "
        f"{t['exit_time'].time()}  {t['exit_price']:7}  "
        f"{t['result']:7}  {t['points']:7}"
    )

wins = sum(1 for t in trades if t["result"] == "TARGET")
losses = sum(1 for t in trades if t["result"] == "SL")
total_points = round(sum(t["points"] for t in trades), 2)

print("\nSUMMARY")
print("--------------------------------")
print(f"Trades       : {len(trades)}")
print(f"Wins         : {wins}")
print(f"Losses       : {losses}")
print(f"Win %        : {round((wins / len(trades)) * 100, 2) if trades else 0}")
print(f"Total Points : {total_points}")
