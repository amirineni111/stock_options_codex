from datetime import datetime

import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh

from options_screening.config import get_settings
from options_screening.market_hours import is_regular_market_hours
from options_screening.refresh import format_refresh_interval, refresh_interval_to_ms
from options_screening.scanner import ScanRequest, run_scan
from options_screening.storage import Storage
from options_screening.universe import load_sp500_tickers


st.set_page_config(page_title="Options Screener", layout="wide")


def _init_state() -> None:
    if "last_scan_at" not in st.session_state:
        st.session_state.last_scan_at = None
    if "auto_refresh" not in st.session_state:
        st.session_state.auto_refresh = False
    if "last_auto_refresh_count" not in st.session_state:
        st.session_state.last_auto_refresh_count = None
    if "last_auto_refresh_key" not in st.session_state:
        st.session_state.last_auto_refresh_key = None


def _render_metric_row(df: pd.DataFrame) -> None:
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Ranked Ideas", len(df))
    col2.metric("Tickers", df["underlying"].nunique() if not df.empty else 0)
    col3.metric("Avg Score", f"{df['score'].mean():.1f}" if not df.empty else "0.0")
    col4.metric("Last Scan", st.session_state.last_scan_at or "Not run")


def _format_results(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    columns = [
        "rank",
        "underlying",
        "contract_type",
        "contract_ticker",
        "expiration_date",
        "strike_price",
        "last_price",
        "mid_price",
        "spread_pct",
        "delta",
        "implied_volatility",
        "open_interest",
        "volume",
        "days_to_expiration",
        "max_contracts_by_risk",
        "premium_at_risk",
        "breakeven",
        "score",
        "score_liquidity",
        "score_spread",
        "score_delta",
        "score_expiration",
        "score_iv",
        "reason",
        "as_of",
    ]
    available = [col for col in columns if col in df.columns]
    return df[available].copy()


def main() -> None:
    _init_state()
    settings = get_settings()
    storage = Storage(settings.db_path)
    storage.initialize()

    st.title("Local Options Screening Dashboard")
    st.caption("Decision-support screener for conservative swing-trade call and put ideas. No broker execution.")

    with st.sidebar:
        st.header("Settings")
        key_status = "Loaded" if settings.polygon_api_key else "Missing"
        st.metric("Polygon API Key", key_status)
        st.caption("Set POLYGON_API_KEY in .env before scanning live data.")

        fixed_risk = st.number_input("Fixed dollar max risk", min_value=25.0, max_value=10000.0, value=250.0, step=25.0)
        min_volume = st.number_input("Minimum volume", min_value=0, max_value=100000, value=50, step=10)
        min_open_interest = st.number_input("Minimum open interest", min_value=0, max_value=100000, value=250, step=25)
        max_spread_pct = st.slider("Maximum bid-ask spread %", 1.0, 50.0, 12.0, 0.5)
        min_dte, max_dte = st.slider("Days to expiration", 1, 180, (21, 75))
        min_delta_abs, max_delta_abs = st.slider("Absolute delta range", 0.05, 0.95, (0.25, 0.65), 0.01)
        min_iv, max_iv = st.slider("Implied volatility range", 0.01, 3.0, (0.05, 1.2), 0.01)
        max_contracts = st.number_input("Max contracts per ticker", min_value=5, max_value=250, value=50, step=5)
        ticker_limit = st.number_input("Ticker scan limit", min_value=1, max_value=503, value=50, step=5)
        st.subheader("Auto Refresh")
        st.session_state.auto_refresh = st.checkbox("Auto-refresh during market hours", value=st.session_state.auto_refresh)
        refresh_unit = st.selectbox("Refresh unit", ["minutes", "seconds"], index=0)
        default_interval = 15 if refresh_unit == "minutes" else 60
        max_interval = 1440 if refresh_unit == "minutes" else 86400
        refresh_interval = st.number_input(
            "Refresh every",
            min_value=1,
            max_value=max_interval,
            value=default_interval,
            step=1,
            disabled=not st.session_state.auto_refresh,
        )
        refresh_interval_ms = refresh_interval_to_ms(float(refresh_interval), refresh_unit)
        refresh_label = format_refresh_interval(float(refresh_interval), refresh_unit)
        if st.session_state.auto_refresh and refresh_interval_ms < 60 * 1000:
            st.warning("Very short refresh intervals can quickly consume API quota.")
        st.caption("Shorter refreshes rerun the dashboard more often; they do not make delayed market data real-time.")

    tickers, universe_note = load_sp500_tickers()
    if universe_note:
        st.warning(universe_note)

    selected_tickers = tickers[: int(ticker_limit)]
    if not settings.polygon_api_key:
        st.error("Add POLYGON_API_KEY to .env, then restart Streamlit or rerun the app.")

    tabs = st.tabs(["Ranked Calls", "Ranked Puts", "Ticker Detail", "Rejected", "Scan Logs", "Settings"])

    scan_request = ScanRequest(
        tickers=selected_tickers,
        fixed_risk=float(fixed_risk),
        min_volume=int(min_volume),
        min_open_interest=int(min_open_interest),
        max_spread_pct=float(max_spread_pct),
        min_days_to_expiration=int(min_dte),
        max_days_to_expiration=int(max_dte),
        min_abs_delta=float(min_delta_abs),
        max_abs_delta=float(max_delta_abs),
        min_iv=float(min_iv),
        max_iv=float(max_iv),
        max_contracts_per_ticker=int(max_contracts),
    )

    run_col, info_col = st.columns([1, 4])
    with run_col:
        run_now = st.button("Run Scan", type="primary", disabled=not bool(settings.polygon_api_key))
    with info_col:
        st.write(f"Universe: S&P 500 first {len(selected_tickers)} tickers. Refresh target: {refresh_label} during market hours.")

    auto_count = None
    if st.session_state.auto_refresh:
        auto_refresh_key = f"auto_refresh_counter_{refresh_interval_ms}"
        if st.session_state.last_auto_refresh_key != auto_refresh_key:
            st.session_state.last_auto_refresh_count = None
            st.session_state.last_auto_refresh_key = auto_refresh_key
        auto_count = st_autorefresh(interval=refresh_interval_ms, key=auto_refresh_key)
        if not is_regular_market_hours():
            st.info(f"Auto-refresh is enabled every {refresh_label} and waiting for regular US market hours.")

    auto_due = (
        st.session_state.auto_refresh
        and bool(settings.polygon_api_key)
        and is_regular_market_hours()
        and auto_count is not None
        and auto_count != st.session_state.last_auto_refresh_count
    )

    if run_now or auto_due:
        with st.spinner("Scanning Polygon option chains..."):
            summary = run_scan(settings, storage, scan_request)
        st.session_state.last_scan_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if auto_due:
            st.session_state.last_auto_refresh_count = auto_count
        st.success(f"Scan complete: {summary.accepted} accepted, {summary.rejected} rejected, {summary.errors} errors.")

    latest = storage.load_latest_results()
    rejected = storage.load_latest_rejections()
    logs = storage.load_scan_logs()

    calls = latest[latest["contract_type"] == "call"].copy() if not latest.empty else latest
    puts = latest[latest["contract_type"] == "put"].copy() if not latest.empty else latest

    with tabs[0]:
        _render_metric_row(calls)
        st.dataframe(_format_results(calls), use_container_width=True, hide_index=True)
        if not calls.empty:
            st.download_button("Export Calls CSV", calls.to_csv(index=False), "ranked_calls.csv", "text/csv")

    with tabs[1]:
        _render_metric_row(puts)
        st.dataframe(_format_results(puts), use_container_width=True, hide_index=True)
        if not puts.empty:
            st.download_button("Export Puts CSV", puts.to_csv(index=False), "ranked_puts.csv", "text/csv")

    with tabs[2]:
        ticker = st.selectbox("Ticker", selected_tickers)
        detail = latest[latest["underlying"] == ticker].copy() if not latest.empty else latest
        st.dataframe(_format_results(detail), use_container_width=True, hide_index=True)

    with tabs[3]:
        st.dataframe(rejected, use_container_width=True, hide_index=True)

    with tabs[4]:
        st.dataframe(logs, use_container_width=True, hide_index=True)

    with tabs[5]:
        st.json(
            {
                **scan_request.model_dump(),
                "auto_refresh_enabled": st.session_state.auto_refresh,
                "refresh_interval": refresh_label,
                "refresh_interval_ms": refresh_interval_ms,
            }
        )
        st.caption("These settings are used for the next manual or auto-refresh scan. Keep Streamlit open during market hours.")


if __name__ == "__main__":
    main()
