"""
OPTIONS BACKTEST — 15-Min Candle Close > EMA5 + EMA5 < 50% of EMA20
======================================================================
Strategy:
  - Instrument  : CE or PE option (configurable)
  - Timeframe   : 15-minute candles
  - Entry Signal: BOTH conditions must be true on the same candle:
                    1. Candle CLOSES above EMA5
                    2. EMA5 < 50% of EMA20  (i.e. EMA5 / EMA20 < 0.50)
  - Entry Price : Next candle's OPEN (trade placed at next open after signal)
  - Target      : +50% of entry price
  - Stop Loss   : -30% of entry price
  - One trade at a time (no re-entry while position is open)
  - Re-entry    : Allowed after previous trade is closed (next signal)

Output:
  - Per-trade log with entry/exit details
  - Equity curve summary
  - Win rate, avg R:R, max drawdown

Usage:
  Set DATE_FROM / DATE_TO range, choose SYMBOL (CE or PE), run.
"""

import datetime
import os
from dotenv import load_dotenv
from fyers_apiv3 import fyersModel

# ===================== CONFIG =====================
load_dotenv()

CLIENT_ID    = os.getenv("FYERS_CLIENT_ID")
ACCESS_TOKEN = os.getenv("FYERS_ACCESS_TOKEN")

# ── Change these to your desired symbol & date range ──
SYMBOL       = "NSE:NIFTY2660223900CE"   # CE or PE symbol to backtest
DATE_FROM    = datetime.date(2026, 5, 26)  # backtest start (inclusive)
DATE_TO      = datetime.date(2026, 5, 27) # backtest end   (inclusive)

EMA_PERIOD   = 5
EMA20_PERIOD = 20
EMA5_TO_EMA20_RATIO = 0.90   # EMA5 must be LESS than this fraction of EMA20
WARMUP_DAYS  = 30             # calendar days fetched BEFORE DATE_FROM to seed EMAs
                              # (covers ~20 trading days — enough for EMA20 to stabilise)
TARGET_PCT   = 0.30   # 50% profit target
SL_PCT       = 0.30   # 30% stop loss
LOT_SIZE     = 65     # NIFTY lot size (change if needed)
NUM_LOTS     = 1      # number of lots per trade

# ===================== FYERS ======================
fyers = fyersModel.FyersModel(
    client_id=CLIENT_ID,
    token=ACCESS_TOKEN,
    is_async=False,
    log_path=""
)

# ===================== FETCH ======================
def fetch_candles(symbol, date, resolution="15"):
    """Fetch OHLC candles for a given symbol, date, and resolution (minutes)."""
    resp = fyers.history({
        "symbol":      symbol,
        "resolution":  resolution,
        "date_format": "1",
        "range_from":  date.strftime("%Y-%m-%d"),
        "range_to":    date.strftime("%Y-%m-%d"),
        "cont_flag":   "1",
    })
    if resp.get("s") != "ok" or not resp.get("candles"):
        return {}
    result = {}
    for c in resp["candles"]:
        ts = datetime.datetime.fromtimestamp(c[0])
        result[ts] = {
            "open":  c[1],
            "high":  c[2],
            "low":   c[3],
            "close": c[4],
            "volume": c[5] if len(c) > 5 else 0,
        }
    return result

def get_trading_days(date_from, date_to):
    """Return all weekdays (Mon–Fri) between date_from and date_to inclusive."""
    days = []
    d = date_from
    while d <= date_to:
        if d.weekday() < 5:  # 0=Mon … 4=Fri
            days.append(d)
        d += datetime.timedelta(days=1)
    return days

# ===================== EMA ========================
def compute_ema_series(closes, period):
    """
    Returns list of EMA values aligned to closes[].
    Values are None until the first full period is complete.
    """
    emas = [None] * len(closes)
    if len(closes) < period:
        return emas
    k = 2 / (period + 1)
    emas[period - 1] = sum(closes[:period]) / period
    for i in range(period, len(closes)):
        emas[i] = closes[i] * k + emas[i - 1] * (1 - k)
    return emas

# ===================== BACKTEST ===================
def run_backtest(symbol, date_from, date_to):
    trading_days = get_trading_days(date_from, date_to)
    print(f"\nBacktest: {symbol}")
    print(f"Period  : {date_from} → {date_to}  ({len(trading_days)} trading days)")
    print(f"Strategy: 15-min close > EMA{EMA_PERIOD}  AND  EMA{EMA_PERIOD} < {EMA5_TO_EMA20_RATIO*100:.0f}% of EMA{EMA20_PERIOD} | Target +{TARGET_PCT*100:.0f}% | SL -{SL_PCT*100:.0f}%")
    print(f"Lot Size: {LOT_SIZE} × {NUM_LOTS} lot(s)")
    print()

    # ── Warm-up: fetch WARMUP_DAYS calendar days before DATE_FROM to seed EMAs ──
    warmup_start = date_from - datetime.timedelta(days=WARMUP_DAYS)
    warmup_days  = get_trading_days(warmup_start, date_from - datetime.timedelta(days=1))

    warmup_candles = []
    print(f"Fetching warm-up candles ({warmup_start} → {date_from - datetime.timedelta(days=1)})...")
    for day in warmup_days:
        day_data = fetch_candles(symbol, day, resolution="15")
        if not day_data:
            continue
        for ts in sorted(day_data):
            warmup_candles.append({"time": ts, **day_data[ts]})
    print(f"  {len(warmup_candles)} warm-up candles fetched ({len(warmup_days)} days attempted)")

    # ── Collect backtest candles (DATE_FROM → DATE_TO) ──
    all_candles  = []   # only the tradeable candles
    skipped_days = []
    print(f"Fetching candles...")
    for day in trading_days:
        day_data = fetch_candles(symbol, day, resolution="15")
        if not day_data:
            skipped_days.append(day)
            print(f"  {day}  ← no data (holiday / non-trading / expired strike)")
            continue
        for ts in sorted(day_data):
            all_candles.append({"time": ts, **day_data[ts]})
        print(f"  {day}  {len(day_data):>3} candles fetched")

    trades = []

    if not all_candles:
        print("\nNo candles fetched. Check symbol, dates, and Fyers credentials.")
        return

    print(f"\nWarm-up candles : {len(warmup_candles)}")
    print(f"Backtest candles: {len(all_candles)}")

    # ── Compute EMA5 and EMA20 over warmup + backtest candles combined ──
    combined     = warmup_candles + all_candles
    combined_cls = [c["close"] for c in combined]
    emas5_all    = compute_ema_series(combined_cls, EMA_PERIOD)
    emas20_all   = compute_ema_series(combined_cls, EMA20_PERIOD)

    # Slice off the warmup portion — keep only backtest candle EMAs
    offset = len(warmup_candles)
    emas5  = emas5_all[offset:]
    emas20 = emas20_all[offset:]

    # ── Annotate backtest candles with both EMAs ──
    for i, c in enumerate(all_candles):
        c["ema5"]  = round(emas5[i],  2) if emas5[i]  is not None else None
        c["ema20"] = round(emas20[i], 2) if emas20[i] is not None else None
        c["ema"]   = c["ema5"]   # keep legacy key for compatibility

    # ── Strategy Execution ──
    position    = None   # None or dict with trade details
    equity      = 0.0    # running P&L in ₹

    print()
    print("=" * 90)
    print(f"  {'#':<4}  {'Entry Time':<17}  {'Entry':>8}  {'Target':>8}  {'SL':>8}  "
          f"{'Exit Time':<17}  {'Exit':>8}  {'P&L ₹':>10}  Result")
    print(f"  {'─' * 84}")

    def close_position(pos, exit_candle, result, exit_price):
        """Helper: record a closed trade and print it."""
        nonlocal equity
        pnl = round((exit_price - pos["entry_price"]) * LOT_SIZE * NUM_LOTS, 2)
        equity = round(equity + pnl, 2)
        trades.append({
            **pos,
            "exit_time":  exit_candle["time"],
            "exit_price": round(exit_price, 2),
            "pnl":        pnl,
            "result":     result,
            "equity":     equity,
        })
        icon = "✅" if result == "TARGET" else ("📋" if result == "EOD_CLOSE" else "❌")
        print(
            f"  {len(trades):<4}"
            f"  {pos['entry_time'].strftime('%d-%b %H:%M'):<17}"
            f"  {pos['entry_price']:>8.2f}"
            f"  {pos['target']:>8.2f}"
            f"  {pos['sl']:>8.2f}"
            f"  {exit_candle['time'].strftime('%d-%b %H:%M'):<17}"
            f"  {exit_price:>8.2f}"
            f"  {pnl:>+10.2f}"
            f"  {icon} {result}"
        )

    for i, candle in enumerate(all_candles):
        ema5  = candle["ema5"]
        ema20 = candle["ema20"]

        is_last_candle_of_day = (
            i + 1 == len(all_candles)
            or all_candles[i + 1]["time"].date() != candle["time"].date()
        )

        # ── Check exit for open position ──
        if position is not None:
            entry     = position["entry_price"]
            target_px = position["target"]
            sl_px     = position["sl"]

            # Force-close at EOD if position was entered today
            if is_last_candle_of_day:
                # Check target/SL first — they take priority over EOD close
                hit_target = candle["high"] >= target_px
                hit_sl     = candle["low"]  <= sl_px
                if hit_sl and hit_target:
                    close_position(position, candle, "SL", sl_px)
                elif hit_target:
                    close_position(position, candle, "TARGET", target_px)
                elif hit_sl:
                    close_position(position, candle, "SL", sl_px)
                else:
                    close_position(position, candle, "EOD_CLOSE", candle["close"])
                position = None
                continue

            # Intra-day: check if target or SL hit
            hit_target = candle["high"] >= target_px
            hit_sl     = candle["low"]  <= sl_px

            if hit_target or hit_sl:
                # Both hit same candle → conservative: SL wins
                if hit_sl and hit_target:
                    close_position(position, candle, "SL", sl_px)
                elif hit_target:
                    close_position(position, candle, "TARGET", target_px)
                else:
                    close_position(position, candle, "SL", sl_px)
                position = None
                continue

        # ── Check entry signal ──
        ema5_filter = (
            ema20 is not None
            and ema5 < (EMA5_TO_EMA20_RATIO * ema20)
        )
        next_is_same_day = (
            i + 1 < len(all_candles)
            and all_candles[i + 1]["time"].date() == candle["time"].date()
        )
        if (
            position is None
            and ema5 is not None
            and candle["close"] > ema5
            and ema5_filter
            and next_is_same_day
            and not is_last_candle_of_day
        ):
            next_candle = all_candles[i + 1]
            entry_price = next_candle["open"]
            target_px   = round(entry_price * (1 + TARGET_PCT), 2)
            sl_px       = round(entry_price * (1 - SL_PCT),     2)

            position = {
                "entry_time":        next_candle["time"],
                "entry_price":       entry_price,
                "target":            target_px,
                "sl":                sl_px,
                "signal_time":       candle["time"],
                "signal_close":      candle["close"],
                "signal_ema5":       ema5,
                "signal_ema20":      ema20,
                "ema5_pct_of_ema20": round(ema5 / ema20 * 100, 1),
            }

    # ── Safety net ──
    if position is not None:
        last = all_candles[-1]
        close_position(position, last, "EOD_CLOSE", last["close"])
        position = None

    print(f"  {'─' * 84}")

    # ===================== SUMMARY ====================
    if not trades:
        print("\n  No trades taken in this period.")
        return

    winners   = [t for t in trades if t["result"] == "TARGET"]
    losers    = [t for t in trades if t["result"] == "SL"]
    eod       = [t for t in trades if t["result"] == "EOD_CLOSE"]
    total     = len(trades)
    win_rate  = len(winners) / total * 100

    total_pnl    = sum(t["pnl"] for t in trades)
    avg_win      = sum(t["pnl"] for t in winners) / len(winners) if winners else 0
    avg_loss     = sum(t["pnl"] for t in losers)  / len(losers)  if losers  else 0

    equity_curve = [0] + [t["equity"] for t in trades]
    peak = equity_curve[0]
    max_dd = 0
    for val in equity_curve:
        if val > peak:
            peak = val
        dd = peak - val
        if dd > max_dd:
            max_dd = dd

    print()
    print("  BACKTEST SUMMARY")
    print(f"  {'─' * 45}")
    print(f"  Symbol          : {symbol}")
    print(f"  Period          : {date_from} → {date_to}")
    print(f"  Total Trades    : {total}")
    print(f"  Winners (Target): {len(winners)}")
    print(f"  Losers (SL)     : {len(losers)}")
    print(f"  EOD Close       : {len(eod)}")
    print(f"  Win Rate        : {win_rate:.1f}%")
    print(f"  {'─' * 45}")
    print(f"  Avg Win  ₹      : {avg_win:>+10.2f}")
    print(f"  Avg Loss ₹      : {avg_loss:>+10.2f}")
    if avg_loss != 0:
        print(f"  Reward:Risk     : {abs(avg_win / avg_loss):.2f}x")
    print(f"  {'─' * 45}")
    print(f"  Net P&L  ₹      : {total_pnl:>+10.2f}")
    print(f"  Max Drawdown ₹  : {max_dd:>10.2f}")
    print(f"  {'─' * 45}")
    print()

    # ── Per-trade detail dump ──
    print("  TRADE DETAIL")
    print(f"  {'─' * 45}")
    for t in trades:
        print(
            f"  [{t['result']:<10}]  "
            f"Entry {t['entry_time'].strftime('%d-%b %H:%M')} @ {t['entry_price']:.2f}  |  "
            f"Exit {t['exit_time'].strftime('%d-%b %H:%M')} @ {t['exit_price']:.2f}  |  "
            f"P&L {t['pnl']:>+8.2f}  |  "
            f"EMA5={t['signal_ema5']:.2f}  EMA20={t['signal_ema20']:.2f}  "
            f"(EMA5 was {t['ema5_pct_of_ema20']:.1f}% of EMA20)"
        )
    print(f"  {'─' * 45}\n")


# ===================== ENTRY ======================
if __name__ == "__main__":
    run_backtest(SYMBOL, DATE_FROM, DATE_TO)
