import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime

# ---------------- Load trades ----------------
@st.cache_data
def load_trades():
    try:
        df = pd.read_csv("dummy_trades.csv", parse_dates=["date"])
        return df
    except FileNotFoundError:
        return pd.DataFrame(columns=["date","symbol","side","entry","sl","target","exit","pnl"])

df = load_trades()

st.set_page_config(page_title="Trading Journal", layout="wide")
st.markdown("<h1 style='color:white;'>📊 Trading Journal Dashboard</h1>", unsafe_allow_html=True)

if df.empty:
    st.warning("⚠️ No trades found yet. Run your bot and log trades to trades.csv.")
else:
    # Extract year & month
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month

    years = sorted(df["year"].unique())
    selected_year = st.selectbox("Select Year", years, index=len(years)-1)

    months = sorted(df[df["year"]==selected_year]["month"].unique())
    selected_month = st.selectbox("Select Month", months, index=len(months)-1)

    df_filtered = df[(df["year"]==selected_year) & (df["month"]==selected_month)]

    # ---------------- KPIs at the top ----------------
    if not df_filtered.empty:
        total_trades = len(df_filtered)
        wins = df_filtered[df_filtered["pnl"] > 0]
        losses = df_filtered[df_filtered["pnl"] <= 0]
        win_rate = round(len(wins)/total_trades*100, 2) if total_trades > 0 else 0
        avg_win = wins["pnl"].mean() if not wins.empty else 0
        avg_loss = losses["pnl"].mean() if not losses.empty else 0
        profit_factor = round(wins["pnl"].sum() / abs(losses["pnl"].sum()), 2) if not losses.empty else float("inf")
        net_pnl = df_filtered["pnl"].sum()

        st.markdown("## 📌 Performance Summary")

        # KPI Cards (Dark Theme)
        kpi_values = {
            "📈 Total Trades": total_trades,
            "🏆 Win Rate %": f"{win_rate}%",
            "💰 Avg Win": f"{avg_win:.2f}",
            "📉 Avg Loss": f"{avg_loss:.2f}",
            "⚖️ Profit Factor": profit_factor,
            "💵 Net PnL": f"{net_pnl:.2f}"
        }

        cols = st.columns(len(kpi_values))
        for i, (title, val) in enumerate(kpi_values.items()):
            with cols[i]:
                st.markdown(
                    f"""
                    <div style="background-color:#1e1e1e; padding:16px; border-radius:12px;
                                text-align:center; box-shadow: 0px 0px 8px rgba(255,255,255,0.1);">
                        <h4 style="color:grey; font-size:14px;">{title}</h4>
                        <h2 style="color:white; font-size:20px; margin-top:5px;">{val}</h2>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

    # ---------------- Trade Table ----------------
    st.subheader(f"📅 Trade Log - {selected_month}/{selected_year}")
    st.dataframe(df_filtered.style.set_properties(**{
        'background-color': '#1e1e1e',
        'color': 'white',
        'border-color': 'grey'
    }))

    # ---------------- Daily Equity Curve ----------------
    if not df_filtered.empty:
        st.subheader("📈 Daily Equity Curve")
        df_filtered["date_only"] = df_filtered["date"].dt.date
        daily_pnl = df_filtered.groupby("date_only")["pnl"].sum().reset_index()
        daily_pnl["cum_pnl"] = daily_pnl["pnl"].cumsum()

        plt.style.use("dark_background")
        fig, ax = plt.subplots()
        ax.plot(daily_pnl["date_only"], daily_pnl["cum_pnl"], marker="o", color="#FF9800", linewidth=2)
        ax.set_xlabel("Date", color="white")
        ax.set_ylabel("Cumulative PnL", color="white")
        ax.set_title("Daily Cumulative PnL", color="white")
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%d-%b"))
        ax.tick_params(colors="white")
        ax.set_facecolor("#111")
        fig.patch.set_facecolor("#111")
        ax.grid(alpha=0.2, color="grey", linestyle="--")
        plt.xticks(rotation=45)
        st.pyplot(fig)

    # ---------------- Monthly Equity Curve ----------------
    st.subheader("📊 Monthly Equity Curve")
    df["year_month"] = df["date"].dt.to_period("M").astype(str)
    monthly_pnl = df.groupby("year_month")["pnl"].sum().reset_index()
    monthly_pnl["cum_pnl"] = monthly_pnl["pnl"].cumsum()

    fig, ax = plt.subplots()
    ax.bar(monthly_pnl["year_month"], monthly_pnl["pnl"], color="#03A9F4", label="Monthly PnL")
    ax.plot(monthly_pnl["year_month"], monthly_pnl["cum_pnl"], color="#FF9800", marker="o", linewidth=2, label="Cumulative PnL")
    ax.set_xlabel("Month", color="white")
    ax.set_ylabel("PnL", color="white")
    ax.set_title("Monthly Performance", color="white")
    ax.legend(facecolor="#111", labelcolor="white")
    ax.tick_params(colors="white")
    ax.set_facecolor("#111")
    fig.patch.set_facecolor("#111")
    ax.grid(alpha=0.2, color="grey", linestyle="--")
    plt.xticks(rotation=45, color="white")
    st.pyplot(fig)

    # ---------------- Win vs Loss Count ----------------
    st.subheader("✅ Win vs ❌ Loss Count")
    fig, ax = plt.subplots()
    df_filtered["result"] = df_filtered["pnl"].apply(lambda x: "Win" if x>0 else "Loss")
    df_filtered["result"].value_counts().plot(kind="bar", ax=ax, color=["#4CAF50","#F44336"])
    ax.set_ylabel("Number of Trades", color="white")
    ax.set_title("Win vs Loss Distribution", color="white")
    ax.tick_params(colors="white")
    ax.set_facecolor("#111")
    fig.patch.set_facecolor("#111")
    ax.grid(alpha=0.2, color="grey", linestyle="--", axis="y")
    plt.xticks(rotation=0, color="white")
    st.pyplot(fig)
