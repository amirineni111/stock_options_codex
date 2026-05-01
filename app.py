from datetime import datetime
import json
import os
from pathlib import Path
import shutil
import stat
from zoneinfo import ZoneInfo

import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh

from options_screening.config import get_settings
from options_screening.intraday import IntradayScanRequest, run_intraday_scan
from options_screening.market_hours import is_regular_market_hours
from options_screening.refresh import format_refresh_interval, refresh_interval_to_ms
from options_screening.scanner import ScanRequest, run_scan
from options_screening.storage import Storage
from options_screening.universe import load_sp100_tickers, load_sp500_tickers


st.set_page_config(page_title="Options Screener", layout="wide")

EASTERN_TZ = ZoneInfo("America/New_York")
APP_PREFERENCES_PATH = Path("data/app_preferences.json")
DEFAULT_PREFERENCES = {
    "fixed_risk": 250.0,
    "min_volume": 50,
    "min_open_interest": 250,
    "max_spread_pct": 12.0,
    "allow_missing_spread": False,
    "days_to_expiration": [21, 75],
    "absolute_delta_range": [0.25, 0.65],
    "implied_volatility_range": [0.05, 1.2],
    "max_contracts_per_ticker": 50,
    "ticker_limit": 50,
    "ticker_source": "S&P 500",
    "custom_tickers": "",
    "use_trend_context": True,
    "require_trend_alignment": False,
    "check_earnings": False,
    "avoid_earnings_before_expiration": False,
    "ignore_missing_spread_for_signal": True,
    "auto_refresh_enabled": False,
    "refresh_unit": "minutes",
    "refresh_interval": 15,
    "intraday_mode": "Both",
    "intraday_universe": "S&P 100",
    "intraday_custom_tickers": "",
    "intraday_min_price": 5.0,
    "intraday_max_price": 1000.0,
    "intraday_min_relative_volume": 0.05,
    "intraday_min_day_change_pct": 0.5,
    "intraday_max_spread_pct": 1.0,
    "intraday_include_shorts": True,
    "intraday_use_rsi_confirmation": True,
    "intraday_use_trend_confirmation": True,
    "intraday_auto_refresh_enabled": False,
    "intraday_refresh_interval": 15,
}
RESULT_COLUMN_GUIDE = [
    ("rank", "Position after sorting by total score. 1 is the highest-ranked contract in the latest scan.", "1"),
    ("underlying", "Stock or ETF ticker that the option is based on.", "PEP"),
    ("contract_type", "Call is bullish exposure; put is bearish exposure.", "call"),
    ("contract_ticker", "Full option contract symbol from Polygon/OCC.", "O:PEP260618C00155000"),
    ("expiration_date", "Date the option expires. After this date, time value is gone.", "2026-06-18"),
    ("strike_price", "Price where the option starts to have intrinsic value at expiration.", "155.00"),
    ("last_price", "Most recent reported option price per share. One contract is this value times 100.", "5.45 = about $545"),
    ("mid_price", "Estimated fair quote midpoint. Uses bid/ask midpoint when available, otherwise last price.", "5.45"),
    ("spread_pct", "Bid-ask spread as a percent of mid price. Lower is better; blank means bid/ask was unavailable.", "8.0%"),
    ("delta", "Approximate option price move for a $1 move in the underlying. Calls are positive; puts are negative.", "0.54"),
    ("implied_volatility", "Market-implied expected volatility. Higher IV usually means more expensive option premium.", "0.22 = 22%"),
    ("open_interest", "Number of existing open contracts. Higher usually means better liquidity.", "2,570"),
    ("volume", "Contracts traded today. Higher usually means more active trading.", "44"),
    ("days_to_expiration", "Calendar days left until expiration.", "52"),
    ("max_contracts_by_risk", "How many contracts fit inside your fixed-dollar max risk setting.", "1"),
    ("premium_at_risk", "Estimated dollars at risk for max_contracts_by_risk contracts.", "545.00"),
    ("breakeven", "Expiration breakeven. Calls: strike + premium. Puts: strike - premium.", "160.45"),
    ("trade_signal", "Rule-based decision label. It is a candidate/watch/avoid signal, not a guaranteed trade.", "BUY_CALL_CANDIDATE"),
    ("signal_reason", "Plain-English reason for the signal, including warnings that downgraded the setup.", "bid/ask spread unavailable"),
    ("underlying_last_price", "Latest underlying stock price used for trend and scenario checks.", "154.20"),
    ("sma20", "20-day simple moving average of the underlying stock.", "151.80"),
    ("sma50", "50-day simple moving average of the underlying stock.", "148.40"),
    ("trend_signal", "Bullish when price is above SMA20 and SMA20 is above SMA50; bearish is the reverse.", "bullish"),
    ("trend_aligned", "True when calls are in a bullish trend or puts are in a bearish trend.", "True"),
    ("earnings_date", "Next earnings date found before the max expiration window, if earnings check is enabled.", "2026-05-01"),
    ("earnings_warning", "Earnings status. Earnings before expiration can add event risk and IV crush risk.", "before expiration"),
    ("breakeven_distance_pct", "How far the underlying must move to reach expiration breakeven.", "4.05%"),
    ("expected_move_pct", "Rough expected move to expiration from IV: IV x square root of DTE/365.", "8.20%"),
    ("expected_move_to_breakeven_ok", "True when expiration breakeven is within the rough IV expected move.", "True"),
    ("favorable_2pct_value", "Estimated total value if the underlying moves 2% in the favorable direction today.", "652.00"),
    ("favorable_2pct_pnl", "Estimated profit/loss for that favorable 2% move, using max_contracts_by_risk.", "107.00"),
    ("adverse_2pct_value", "Estimated total value if the underlying moves 2% against the trade today.", "440.00"),
    ("adverse_2pct_pnl", "Estimated profit/loss for that adverse 2% move, using max_contracts_by_risk.", "-105.00"),
    ("decision_checklist", "Plain-English checklist summarizing trend, spread, expected move, and earnings risk.", "trend ok; verify bid/ask"),
    ("score", "Total ranking score from liquidity, spread, delta, expiration, and IV components. Higher ranks first.", "69.26"),
    ("score_liquidity", "Score from volume and open interest. Max is 25.", "25.00"),
    ("score_spread", "Score from tight bid-ask spread. Max is 25; missing bid/ask gets 0.", "0.00"),
    ("score_delta", "Score for delta being near the center of your selected delta range. Max is 20.", "19.49"),
    ("score_expiration", "Score for DTE being near the center of your selected expiration range. Max is 15.", "10.62"),
    ("score_iv", "Score for IV within your selected IV range. Lower IV in range scores better. Max is 15.", "14.15"),
    ("reason", "Why the contract was accepted, including warnings such as missing bid/ask spread.", "Accepted...verify quote"),
    ("as_of", "When the option snapshot was parsed, shown in Eastern Time.", "2026-04-27 10:22:26 EDT"),
]
INTRADAY_COLUMN_GUIDE = [
    ("rsi14", "Momentum oscillator from 0 to 100. Above 50 supports bullish momentum; below 50 supports bearish momentum. Extreme readings can favor mean reversion.", "Bullish momentum: 50-70. Oversold: below 30. Overbought: above 70."),
    ("ema9", "Fast intraday exponential moving average. It reacts quicker than EMA20 and helps show short-term direction.", "Bullish momentum prefers price >= EMA9 >= EMA20."),
    ("ema20", "Slower intraday exponential moving average. It gives the fast EMA a trend baseline.", "Bearish momentum prefers price <= EMA9 <= EMA20."),
    ("macd", "Difference between EMA12 and EMA26. Positive means short-term price trend is above the slower trend; negative means below.", "MACD above signal is bullish; below signal is bearish."),
    ("macd_signal", "EMA9 of MACD. Use it as the comparison line for MACD.", "MACD crossing above signal supports bullish momentum."),
    ("macd_histogram", "MACD minus MACD signal. This is the quickest MACD read: positive favors bullish momentum, negative favors bearish momentum.", "Rising positive histogram means momentum is strengthening."),
    ("vwap", "Volume-weighted average price for the current intraday session. Price above VWAP favors bullish acceptance; below VWAP favors bearish acceptance.", "Longs prefer price above VWAP; shorts prefer price below VWAP."),
    ("signal_reason", "Plain-English summary of why the row became a candidate, watch, or avoid signal.", "May mention RSI, EMA/MACD/VWAP, spread, or volume."),
]


def _init_state(preferences: dict) -> None:
    if "last_scan_at" not in st.session_state:
        st.session_state.last_scan_at = None
    if "auto_refresh" not in st.session_state:
        st.session_state.auto_refresh = bool(preferences["auto_refresh_enabled"])
    if "last_auto_refresh_count" not in st.session_state:
        st.session_state.last_auto_refresh_count = None
    if "last_auto_refresh_key" not in st.session_state:
        st.session_state.last_auto_refresh_key = None
    if "intraday_last_scan_at" not in st.session_state:
        st.session_state.intraday_last_scan_at = None
    if "intraday_auto_refresh" not in st.session_state:
        st.session_state.intraday_auto_refresh = bool(preferences["intraday_auto_refresh_enabled"])
    if "intraday_last_auto_refresh_count" not in st.session_state:
        st.session_state.intraday_last_auto_refresh_count = None


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
        "trade_signal",
        "signal_reason",
        "decision_checklist",
        "trend_signal",
        "trend_aligned",
        "underlying_last_price",
        "sma20",
        "sma50",
        "earnings_date",
        "earnings_warning",
        "breakeven_distance_pct",
        "expected_move_pct",
        "expected_move_to_breakeven_ok",
        "favorable_2pct_value",
        "favorable_2pct_pnl",
        "adverse_2pct_value",
        "adverse_2pct_pnl",
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


def _result_column_config() -> dict:
    return {
        column: st.column_config.Column(label=column, help=f"{meaning} Example: {example}")
        for column, meaning, example in RESULT_COLUMN_GUIDE
    }


def _render_result_column_guide() -> None:
    guide = pd.DataFrame(
        [{"Column": column, "Meaning": meaning, "Example": example} for column, meaning, example in RESULT_COLUMN_GUIDE]
    )
    with st.expander("Column guide and examples"):
        st.dataframe(guide, use_container_width=True, hide_index=True)


def _render_results_table(df: pd.DataFrame) -> None:
    _render_result_column_guide()
    st.dataframe(
        _format_results(df),
        use_container_width=True,
        hide_index=True,
        column_config=_result_column_config(),
    )


def _filter_by_underlying(df: pd.DataFrame, selected_underlyings) -> pd.DataFrame:
    if df.empty or not selected_underlyings or "underlying" not in df.columns:
        return df
    return df[df["underlying"].isin(selected_underlyings)].copy()


def _render_underlying_filter(df: pd.DataFrame, key: str):
    if df.empty or "underlying" not in df.columns:
        return []
    options = sorted(df["underlying"].dropna().unique().tolist())
    return st.multiselect("Filter underlying", options=options, default=[], key=key, placeholder="All underlyings")


def _render_signal_filter(df: pd.DataFrame, key: str):
    if df.empty or "trade_signal" not in df.columns:
        return []
    options = sorted(df["trade_signal"].dropna().unique().tolist())
    return st.multiselect("Filter signal", options=options, default=[], key=key, placeholder="All signals")


def _filter_by_signal(df: pd.DataFrame, selected_signals) -> pd.DataFrame:
    if df.empty or not selected_signals or "trade_signal" not in df.columns:
        return df
    return df[df["trade_signal"].isin(selected_signals)].copy()


def _render_intraday_table(df: pd.DataFrame) -> None:
    columns = [
        "rank",
        "ticker",
        "last_price",
        "day_change_pct",
        "volume",
        "relative_volume",
        "open",
        "high",
        "low",
        "prev_close",
        "minute_price",
        "rsi14",
        "ema9",
        "ema20",
        "macd",
        "macd_signal",
        "macd_histogram",
        "vwap",
        "spread_pct",
        "signal_mode",
        "momentum_score",
        "mean_reversion_score",
        "total_score",
        "trade_signal",
        "signal_reason",
        "risk_notes",
        "as_of",
    ]
    available = [col for col in columns if col in df.columns]
    _render_intraday_column_guide()
    st.dataframe(
        df[available].copy() if not df.empty else df,
        use_container_width=True,
        hide_index=True,
        column_config=_intraday_column_config(),
    )


def _intraday_column_config() -> dict:
    return {
        column: st.column_config.Column(label=column, help=meaning)
        for column, meaning, _ in INTRADAY_COLUMN_GUIDE
    }


def _render_intraday_column_guide() -> None:
    guide = pd.DataFrame(
        [{"Column": column, "How to read it": meaning, "Example": example} for column, meaning, example in INTRADAY_COLUMN_GUIDE]
    )
    with st.expander("Indicator guide"):
        st.dataframe(guide, use_container_width=True, hide_index=True)
        st.caption("MACD quick read: compare MACD to signal, then use histogram for strength. Histogram above 0 favors bullish momentum; below 0 favors bearish momentum.")


def _filter_intraday_results(df: pd.DataFrame, tickers, signals, modes, min_score: float) -> pd.DataFrame:
    if df.empty:
        return df
    filtered = df.copy()
    if tickers and "ticker" in filtered.columns:
        filtered = filtered[filtered["ticker"].isin(tickers)]
    if signals and "trade_signal" in filtered.columns:
        filtered = filtered[filtered["trade_signal"].isin(signals)]
    if modes and "signal_mode" in filtered.columns:
        filtered = filtered[filtered["signal_mode"].isin(modes)]
    if "total_score" in filtered.columns:
        filtered = filtered[filtered["total_score"].fillna(0) >= min_score]
    return filtered


def _render_intraday_watchlist(storage: Storage, latest: pd.DataFrame) -> None:
    st.subheader("Intraday Watchlist")
    watchlist = storage.load_intraday_watchlist()
    if latest.empty:
        st.info("Run an intraday scan first, then add stocks to the watchlist.")
    else:
        choices = latest["ticker"].dropna().tolist()
        with st.form("add_intraday_watch"):
            ticker = st.selectbox("Ticker", choices)
            selected_row = latest[latest["ticker"] == ticker].iloc[0]
            col1, col2, col3 = st.columns(3)
            entry_price = col1.number_input("Entry price", min_value=0.0, value=float(selected_row.get("last_price") or 0.0), step=0.01)
            target_price = col2.number_input("Target price", min_value=0.0, value=0.0, step=0.01)
            stop_price = col3.number_input("Stop price", min_value=0.0, value=0.0, step=0.01)
            notes = st.text_area("Notes")
            submitted = st.form_submit_button("Add to Intraday Watchlist")
            if submitted:
                storage.add_intraday_watch(
                    ticker=ticker,
                    signal=selected_row.get("trade_signal"),
                    entry_price=float(entry_price) if entry_price else None,
                    target_price=float(target_price) if target_price else None,
                    stop_price=float(stop_price) if stop_price else None,
                    notes=notes,
                )
                st.success("Added to intraday watchlist.")
                st.rerun()

    if watchlist.empty:
        st.dataframe(watchlist, use_container_width=True, hide_index=True)
        return

    st.dataframe(watchlist, use_container_width=True, hide_index=True)
    open_ids = watchlist[watchlist["status"] == "watching"]["id"].tolist()
    if open_ids:
        close_id = st.selectbox("Close intraday watch item", open_ids)
        if st.button("Mark Intraday Item Closed"):
            storage.close_intraday_watch(int(close_id))
            st.success("Intraday watch item closed.")
            st.rerun()


def _render_intraday_page(settings, storage: Storage, preferences: dict) -> None:
    st.title("Intraday Stock Screener")
    st.caption("Decision-support screener for intraday S&P 100 or custom stock ideas. No broker execution.")

    with st.sidebar:
        st.header("Intraday Settings")
        key_status = "Loaded" if settings.polygon_api_key else "Missing"
        st.metric("Polygon API Key", key_status)
        mode_options = ["Both", "Momentum", "Mean Reversion"]
        intraday_mode = st.selectbox(
            "Signal mode",
            mode_options,
            index=mode_options.index(preferences["intraday_mode"]) if preferences["intraday_mode"] in mode_options else 0,
        )
        universe_options = ["S&P 100", "Custom"]
        intraday_universe = st.radio(
            "Universe",
            universe_options,
            index=universe_options.index(preferences["intraday_universe"]) if preferences["intraday_universe"] in universe_options else 0,
            horizontal=True,
        )
        intraday_custom_tickers = st.text_area(
            "Custom tickers",
            value=str(preferences["intraday_custom_tickers"]),
            placeholder="AAPL, MSFT, NVDA, SPY, QQQ",
            disabled=intraday_universe != "Custom",
            key="intraday_custom_tickers_input",
        )
        min_price = st.number_input("Min price", min_value=0.0, max_value=10000.0, value=_bounded_number(preferences["intraday_min_price"], 0.0, 10000.0, 5.0), step=1.0)
        max_price = st.number_input("Max price", min_value=1.0, max_value=10000.0, value=_bounded_number(preferences["intraday_max_price"], 1.0, 10000.0, 1000.0), step=5.0)
        min_relative_volume = st.number_input("Min relative volume", min_value=0.0, max_value=10.0, value=_bounded_number(preferences["intraday_min_relative_volume"], 0.0, 10.0, 0.05), step=0.01)
        min_day_change_pct = st.number_input("Min day change %", min_value=0.0, max_value=20.0, value=_bounded_number(preferences["intraday_min_day_change_pct"], 0.0, 20.0, 0.5), step=0.1)
        max_spread_pct = st.number_input("Max spread %", min_value=0.01, max_value=20.0, value=_bounded_number(preferences["intraday_max_spread_pct"], 0.01, 20.0, 1.0), step=0.1)
        include_shorts = st.checkbox("Include short candidates", value=bool(preferences["intraday_include_shorts"]))
        use_rsi_confirmation = st.checkbox("Use RSI confirmation", value=bool(preferences["intraday_use_rsi_confirmation"]))
        use_trend_confirmation = st.checkbox("Use EMA/MACD/VWAP confirmation", value=bool(preferences["intraday_use_trend_confirmation"]))
        st.subheader("Auto Refresh")
        st.session_state.intraday_auto_refresh = st.checkbox(
            "Auto-refresh during market hours",
            value=st.session_state.intraday_auto_refresh,
            key="intraday_auto_refresh_checkbox",
        )
        intraday_refresh_interval = st.number_input(
            "Refresh every minutes",
            min_value=1,
            max_value=1440,
            value=_bounded_number(preferences["intraday_refresh_interval"], 1, 1440, 15),
            step=1,
            disabled=not st.session_state.intraday_auto_refresh,
        )

    if intraday_universe == "Custom":
        selected_tickers = _parse_custom_tickers(intraday_custom_tickers)
        if not selected_tickers:
            st.warning("Add at least one custom ticker to run a custom intraday scan.")
    else:
        selected_tickers, universe_note = load_sp100_tickers()
        if universe_note:
            st.warning(universe_note)

    try:
        _save_app_preferences(
            {
                "intraday_mode": intraday_mode,
                "intraday_universe": intraday_universe,
                "intraday_custom_tickers": intraday_custom_tickers,
                "intraday_min_price": float(min_price),
                "intraday_max_price": float(max_price),
                "intraday_min_relative_volume": float(min_relative_volume),
                "intraday_min_day_change_pct": float(min_day_change_pct),
                "intraday_max_spread_pct": float(max_spread_pct),
                "intraday_include_shorts": bool(include_shorts),
                "intraday_use_rsi_confirmation": bool(use_rsi_confirmation),
                "intraday_use_trend_confirmation": bool(use_trend_confirmation),
                "intraday_auto_refresh_enabled": bool(st.session_state.intraday_auto_refresh),
                "intraday_refresh_interval": int(intraday_refresh_interval),
            }
        )
    except OSError as exc:
        st.warning(f"Could not save intraday settings: {exc}")

    if not settings.polygon_api_key:
        st.error("Add POLYGON_API_KEY to .env, then restart Streamlit or rerun the app.")

    request = IntradayScanRequest(
        tickers=selected_tickers,
        mode=intraday_mode,
        min_price=float(min_price),
        max_price=float(max_price),
        min_relative_volume=float(min_relative_volume),
        min_day_change_pct=float(min_day_change_pct),
        max_spread_pct=float(max_spread_pct),
        include_shorts=bool(include_shorts),
        use_rsi_confirmation=bool(use_rsi_confirmation),
        use_trend_confirmation=bool(use_trend_confirmation),
    )

    run_col, info_col = st.columns([1, 4])
    with run_col:
        run_now = st.button("Run Intraday Scan", type="primary", disabled=not bool(settings.polygon_api_key) or not selected_tickers)
    with info_col:
        st.write(f"Universe: {intraday_universe}, {len(selected_tickers)} tickers. Refresh target: {int(intraday_refresh_interval)} minutes.")

    auto_count = None
    if st.session_state.intraday_auto_refresh:
        auto_count = st_autorefresh(interval=int(intraday_refresh_interval) * 60 * 1000, key="intraday_auto_refresh_counter")
        if not is_regular_market_hours():
            st.info("Intraday auto-refresh is enabled and waiting for regular US market hours.")

    auto_due = (
        st.session_state.intraday_auto_refresh
        and bool(settings.polygon_api_key)
        and bool(selected_tickers)
        and is_regular_market_hours()
        and auto_count is not None
        and auto_count != st.session_state.intraday_last_auto_refresh_count
    )

    if run_now or auto_due:
        with st.spinner("Scanning intraday stock snapshots..."):
            results, summary, logs = run_intraday_scan(settings, request)
            storage.save_intraday_scan(results, logs)
        st.session_state.intraday_last_scan_at = datetime.now(EASTERN_TZ).strftime("%Y-%m-%d %H:%M:%S %Z")
        if auto_due:
            st.session_state.intraday_last_auto_refresh_count = auto_count
        st.success(
            f"Intraday scan complete: {summary.accepted} candidates, {summary.watch} watch, "
            f"{summary.avoid} avoid, {summary.errors} errors."
        )

    latest = _format_time_columns(storage.load_intraday_results(), ["as_of"])
    logs = _format_time_columns(storage.load_intraday_logs(), ["created_at"])

    tab_results, tab_logs, tab_watchlist, tab_settings = st.tabs(["Results", "Scan Logs", "Watchlist", "Settings"])
    with tab_results:
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Rows", len(latest))
        col2.metric("Tickers", latest["ticker"].nunique() if not latest.empty else 0)
        col3.metric("Avg Score", f"{latest['total_score'].mean():.1f}" if not latest.empty else "0.0")
        col4.metric("Last Scan", st.session_state.intraday_last_scan_at or "Not run")
        filter_col1, filter_col2, filter_col3, filter_col4 = st.columns([2, 2, 2, 1])
        tickers_filter = filter_col1.multiselect(
            "Filter ticker",
            sorted(latest["ticker"].dropna().unique().tolist()) if not latest.empty else [],
            default=[],
            placeholder="All tickers",
        )
        signals_filter = filter_col2.multiselect(
            "Filter signal",
            sorted(latest["trade_signal"].dropna().unique().tolist()) if not latest.empty else [],
            default=[],
            placeholder="All signals",
        )
        modes_filter = filter_col3.multiselect(
            "Filter mode",
            sorted(latest["signal_mode"].dropna().unique().tolist()) if not latest.empty else [],
            default=[],
            placeholder="All modes",
        )
        min_score_filter = filter_col4.number_input("Min score", min_value=0.0, max_value=100.0, value=0.0, step=5.0)
        filtered = _filter_intraday_results(latest, tickers_filter, signals_filter, modes_filter, float(min_score_filter))
        _render_intraday_table(filtered)
        if not filtered.empty:
            st.download_button("Export Intraday CSV", filtered.to_csv(index=False), "intraday_results.csv", "text/csv")

    with tab_logs:
        st.dataframe(logs, use_container_width=True, hide_index=True)

    with tab_watchlist:
        _render_intraday_watchlist(storage, latest)

    with tab_settings:
        st.json(
            {
                **request.model_dump(),
                "auto_refresh_enabled": st.session_state.intraday_auto_refresh,
                "refresh_interval_minutes": int(intraday_refresh_interval),
            }
        )
        st.caption("Signals are screening labels only. Verify chart, spread, liquidity, and risk before any trade.")


def _render_watchlist(storage: Storage, latest: pd.DataFrame) -> None:
    st.subheader("Watchlist")
    watchlist = storage.load_watchlist()
    if latest.empty:
        st.info("Run a scan first, then add contracts to the watchlist.")
    else:
        formatted = _format_results(latest)
        choices = formatted["contract_ticker"].dropna().tolist()
        with st.form("add_watch_contract"):
            selected_contract = st.selectbox("Contract", choices)
            selected_row = formatted[formatted["contract_ticker"] == selected_contract].iloc[0]
            col1, col2, col3 = st.columns(3)
            entry_price = col1.number_input("Entry price", min_value=0.0, value=float(selected_row.get("mid_price") or 0.0), step=0.01)
            target_price = col2.number_input("Target price", min_value=0.0, value=0.0, step=0.01)
            stop_price = col3.number_input("Stop price", min_value=0.0, value=0.0, step=0.01)
            notes = st.text_area("Notes")
            submitted = st.form_submit_button("Add to Watchlist")
            if submitted:
                storage.add_watch_contract(
                    contract_ticker=selected_contract,
                    underlying=selected_row.get("underlying"),
                    contract_type=selected_row.get("contract_type"),
                    entry_price=float(entry_price) if entry_price else None,
                    target_price=float(target_price) if target_price else None,
                    stop_price=float(stop_price) if stop_price else None,
                    notes=notes,
                )
                st.success("Added to watchlist.")
                st.rerun()

    if watchlist.empty:
        st.dataframe(watchlist, use_container_width=True, hide_index=True)
        return

    selected_underlyings = _render_underlying_filter(watchlist, "watchlist_underlying_filter")
    filtered_watchlist = _filter_by_underlying(watchlist, selected_underlyings)
    st.dataframe(filtered_watchlist, use_container_width=True, hide_index=True)
    open_ids = filtered_watchlist[filtered_watchlist["status"] == "watching"]["id"].tolist()
    if open_ids:
        close_id = st.selectbox("Close watch item", open_ids)
        if st.button("Mark Closed"):
            storage.close_watch_contract(int(close_id))
            st.success("Watch item closed.")
            st.rerun()


def _format_eastern_time(value) -> str:
    if pd.isna(value):
        return value
    timestamp = pd.to_datetime(value, errors="coerce", utc=True)
    if pd.isna(timestamp):
        return value
    return timestamp.tz_convert(EASTERN_TZ).strftime("%Y-%m-%d %H:%M:%S %Z")


def _format_time_columns(df: pd.DataFrame, columns) -> pd.DataFrame:
    if df.empty:
        return df
    formatted = df.copy()
    for column in columns:
        if column in formatted.columns:
            formatted[column] = formatted[column].apply(_format_eastern_time)
    return formatted


def _load_app_preferences() -> dict:
    preferences = dict(DEFAULT_PREFERENCES)
    if not APP_PREFERENCES_PATH.exists():
        return preferences
    try:
        saved = json.loads(APP_PREFERENCES_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return preferences
    if isinstance(saved, dict):
        preferences.update(saved)
    return preferences


def _save_app_preferences(preferences: dict) -> None:
    APP_PREFERENCES_PATH.parent.mkdir(parents=True, exist_ok=True)
    merged = dict(DEFAULT_PREFERENCES)
    if APP_PREFERENCES_PATH.exists():
        try:
            saved = json.loads(APP_PREFERENCES_PATH.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            saved = {}
        if isinstance(saved, dict):
            merged.update(saved)
    merged.update(preferences)
    APP_PREFERENCES_PATH.write_text(json.dumps(merged, indent=2, sort_keys=True), encoding="utf-8")


def _bounded_number(value, minimum, maximum, default):
    try:
        number = type(default)(value)
    except (TypeError, ValueError):
        number = default
    return max(minimum, min(maximum, number))


def _bounded_range(value, minimum, maximum, default):
    if not isinstance(value, (list, tuple)) or len(value) != 2:
        return default
    lower = _bounded_number(value[0], minimum, maximum, default[0])
    upper = _bounded_number(value[1], minimum, maximum, default[1])
    if lower > upper:
        lower, upper = upper, lower
    return lower, upper


def _parse_custom_tickers(value: str) -> list:
    tickers = []
    seen = set()
    for item in (value or "").replace("\n", ",").split(","):
        ticker = item.strip().upper()
        if not ticker or ticker in seen:
            continue
        seen.add(ticker)
        tickers.append(ticker)
    return tickers


def _cleanup_pytest_cache_artifacts(root: Path = None) -> int:
    repo_root = (root or Path.cwd()).resolve()
    removed = 0
    for path in repo_root.glob("pytest-cache-files-*"):
        resolved = path.resolve()
        if not resolved.is_dir():
            continue
        if resolved.parent != repo_root or not resolved.name.startswith("pytest-cache-files-"):
            continue
        shutil.rmtree(resolved, onerror=_make_writable_and_retry)
        removed += 1
    return removed


def _make_writable_and_retry(function, path, _exc_info) -> None:
    os.chmod(path, stat.S_IWRITE)
    function(path)


def main() -> None:
    preferences = _load_app_preferences()
    _init_state(preferences)
    settings = get_settings()
    storage = Storage(settings.db_path)
    storage.initialize()

    with st.sidebar:
        page = st.radio("Page", ["Options Scanner", "Intraday Stocks"], horizontal=True)
    if page == "Intraday Stocks":
        _render_intraday_page(settings, storage, preferences)
        return

    st.title("Local Options Screening Dashboard")
    st.caption("Decision-support screener for conservative swing-trade call and put ideas. No broker execution.")

    with st.sidebar:
        st.header("Settings")
        key_status = "Loaded" if settings.polygon_api_key else "Missing"
        st.metric("Polygon API Key", key_status)
        st.caption("Set POLYGON_API_KEY in .env before scanning live data.")

        fixed_risk = st.number_input("Fixed dollar max risk", min_value=25.0, max_value=10000.0, value=_bounded_number(preferences["fixed_risk"], 25.0, 10000.0, 250.0), step=25.0)
        min_volume = st.number_input("Minimum volume", min_value=0, max_value=100000, value=_bounded_number(preferences["min_volume"], 0, 100000, 50), step=10)
        min_open_interest = st.number_input("Minimum open interest", min_value=0, max_value=100000, value=_bounded_number(preferences["min_open_interest"], 0, 100000, 250), step=25)
        max_spread_pct = st.slider("Maximum bid-ask spread %", 1.0, 50.0, _bounded_number(preferences["max_spread_pct"], 1.0, 50.0, 12.0), 0.5)
        allow_missing_spread = st.checkbox("Allow missing bid-ask spread", value=bool(preferences["allow_missing_spread"]))
        if allow_missing_spread:
            st.warning("Contracts without bid/ask quotes can be ranked, but verify live quotes before trading.")
        ignore_missing_spread_for_signal = st.checkbox(
            "Ignore missing bid-ask for trade signal",
            value=bool(preferences["ignore_missing_spread_for_signal"]),
            disabled=not allow_missing_spread,
        )
        min_dte, max_dte = st.slider("Days to expiration", 1, 180, _bounded_range(preferences["days_to_expiration"], 1, 180, (21, 75)))
        min_delta_abs, max_delta_abs = st.slider("Absolute delta range", 0.05, 0.95, _bounded_range(preferences["absolute_delta_range"], 0.05, 0.95, (0.25, 0.65)), 0.01)
        min_iv, max_iv = st.slider("Implied volatility range", 0.01, 3.0, _bounded_range(preferences["implied_volatility_range"], 0.01, 3.0, (0.05, 1.2)), 0.01)
        max_contracts = st.number_input("Max contracts per ticker", min_value=5, max_value=250, value=_bounded_number(preferences["max_contracts_per_ticker"], 5, 250, 50), step=5)
        st.subheader("Scan Universe")
        ticker_source_options = ["S&P 500", "Custom"]
        ticker_source = st.radio(
            "Ticker source",
            ticker_source_options,
            index=ticker_source_options.index(preferences["ticker_source"]) if preferences["ticker_source"] in ticker_source_options else 0,
            horizontal=True,
        )
        ticker_limit = st.number_input("Ticker scan limit", min_value=1, max_value=503, value=_bounded_number(preferences["ticker_limit"], 1, 503, 50), step=5)
        custom_tickers = st.text_area(
            "Custom tickers",
            value=str(preferences["custom_tickers"]),
            placeholder="AAPL, MSFT, NVDA, SPY, QQQ",
            disabled=ticker_source != "Custom",
            key="options_custom_tickers_input",
        )
        if ticker_source == "Custom":
            parsed_custom_tickers = _parse_custom_tickers(custom_tickers)
            st.caption(f"Custom scan list: {len(parsed_custom_tickers)} ticker(s).")
        st.subheader("Decision Checks")
        use_trend_context = st.checkbox("Add stock trend context", value=bool(preferences["use_trend_context"]))
        require_trend_alignment = st.checkbox(
            "Require trend alignment",
            value=bool(preferences["require_trend_alignment"]),
            disabled=not use_trend_context,
        )
        check_earnings = st.checkbox("Check earnings dates", value=bool(preferences["check_earnings"]))
        avoid_earnings_before_expiration = st.checkbox(
            "Reject earnings before expiration",
            value=bool(preferences["avoid_earnings_before_expiration"]),
            disabled=not check_earnings,
        )
        if check_earnings:
            st.warning("Earnings checks add Polygon API calls and may require Benzinga earnings access.")
        st.subheader("Auto Refresh")
        st.session_state.auto_refresh = st.checkbox("Auto-refresh during market hours", value=st.session_state.auto_refresh)
        refresh_options = ["minutes", "seconds"]
        refresh_unit = st.selectbox(
            "Refresh unit",
            refresh_options,
            index=refresh_options.index(preferences["refresh_unit"]) if preferences["refresh_unit"] in refresh_options else 0,
        )
        default_interval = 15 if refresh_unit == "minutes" else 60
        max_interval = 1440 if refresh_unit == "minutes" else 86400
        refresh_interval = st.number_input(
            "Refresh every",
            min_value=1,
            max_value=max_interval,
            value=_bounded_number(preferences["refresh_interval"], 1, max_interval, default_interval),
            step=1,
            disabled=not st.session_state.auto_refresh,
        )
        refresh_interval_ms = refresh_interval_to_ms(float(refresh_interval), refresh_unit)
        refresh_label = format_refresh_interval(float(refresh_interval), refresh_unit)
        if st.session_state.auto_refresh and refresh_interval_ms < 60 * 1000:
            st.warning("Very short refresh intervals can quickly consume API quota.")
        st.caption("Shorter refreshes rerun the dashboard more often; they do not make delayed market data real-time.")

    try:
        _save_app_preferences(
            {
                "fixed_risk": float(fixed_risk),
                "min_volume": int(min_volume),
                "min_open_interest": int(min_open_interest),
                "max_spread_pct": float(max_spread_pct),
                "allow_missing_spread": bool(allow_missing_spread),
                "days_to_expiration": [int(min_dte), int(max_dte)],
                "absolute_delta_range": [float(min_delta_abs), float(max_delta_abs)],
                "implied_volatility_range": [float(min_iv), float(max_iv)],
                "max_contracts_per_ticker": int(max_contracts),
                "ticker_limit": int(ticker_limit),
                "ticker_source": ticker_source,
                "custom_tickers": custom_tickers,
                "use_trend_context": bool(use_trend_context),
                "require_trend_alignment": bool(require_trend_alignment and use_trend_context),
                "check_earnings": bool(check_earnings),
                "avoid_earnings_before_expiration": bool(avoid_earnings_before_expiration and check_earnings),
                "ignore_missing_spread_for_signal": bool(ignore_missing_spread_for_signal and allow_missing_spread),
                "auto_refresh_enabled": bool(st.session_state.auto_refresh),
                "refresh_unit": refresh_unit,
                "refresh_interval": int(refresh_interval),
            }
        )
    except OSError as exc:
        st.warning(f"Could not save app settings: {exc}")

    tickers, universe_note = load_sp500_tickers()
    if universe_note:
        st.warning(universe_note)

    if ticker_source == "Custom":
        selected_tickers = _parse_custom_tickers(custom_tickers)
        if not selected_tickers:
            st.warning("Add at least one custom ticker to run a custom scan.")
    else:
        selected_tickers = tickers[: int(ticker_limit)]
    if not settings.polygon_api_key:
        st.error("Add POLYGON_API_KEY to .env, then restart Streamlit or rerun the app.")

    tabs = st.tabs(["Ranked Calls", "Ranked Puts", "Ticker Detail", "Rejected", "Scan Logs", "Watchlist", "Settings"])

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
        allow_missing_spread=bool(allow_missing_spread),
        use_trend_context=bool(use_trend_context),
        require_trend_alignment=bool(require_trend_alignment and use_trend_context),
        check_earnings=bool(check_earnings),
        avoid_earnings_before_expiration=bool(avoid_earnings_before_expiration and check_earnings),
        ignore_missing_spread_for_signal=bool(ignore_missing_spread_for_signal and allow_missing_spread),
    )

    run_col, info_col = st.columns([1, 4])
    with run_col:
        run_now = st.button("Run Scan", type="primary", disabled=not bool(settings.polygon_api_key) or not selected_tickers)
    with info_col:
        universe_label = "custom list" if ticker_source == "Custom" else "S&P 500"
        st.write(f"Universe: {universe_label}, {len(selected_tickers)} tickers. Refresh target: {refresh_label} during market hours.")

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
        and bool(selected_tickers)
        and is_regular_market_hours()
        and auto_count is not None
        and auto_count != st.session_state.last_auto_refresh_count
    )

    if run_now or auto_due:
        with st.spinner("Scanning Polygon option chains..."):
            try:
                _cleanup_pytest_cache_artifacts()
            except OSError as exc:
                st.warning(f"Could not remove pytest cache folders: {exc}")
            summary = run_scan(settings, storage, scan_request)
        st.session_state.last_scan_at = datetime.now(EASTERN_TZ).strftime("%Y-%m-%d %H:%M:%S %Z")
        if auto_due:
            st.session_state.last_auto_refresh_count = auto_count
        st.success(f"Scan complete: {summary.accepted} accepted, {summary.rejected} rejected, {summary.errors} errors.")

    latest = storage.load_latest_results()
    rejected = storage.load_latest_rejections()
    logs = storage.load_scan_logs()
    latest = _format_time_columns(latest, ["as_of"])
    rejected = _format_time_columns(rejected, ["as_of"])
    logs = _format_time_columns(logs, ["created_at"])

    calls = latest[latest["contract_type"] == "call"].copy() if not latest.empty else latest
    puts = latest[latest["contract_type"] == "put"].copy() if not latest.empty else latest

    with tabs[0]:
        call_underlyings = _render_underlying_filter(calls, "calls_underlying_filter")
        call_signals = _render_signal_filter(calls, "calls_signal_filter")
        filtered_calls = _filter_by_signal(_filter_by_underlying(calls, call_underlyings), call_signals)
        _render_metric_row(filtered_calls)
        _render_results_table(filtered_calls)
        if not filtered_calls.empty:
            st.download_button("Export Calls CSV", filtered_calls.to_csv(index=False), "ranked_calls.csv", "text/csv")

    with tabs[1]:
        put_underlyings = _render_underlying_filter(puts, "puts_underlying_filter")
        put_signals = _render_signal_filter(puts, "puts_signal_filter")
        filtered_puts = _filter_by_signal(_filter_by_underlying(puts, put_underlyings), put_signals)
        _render_metric_row(filtered_puts)
        _render_results_table(filtered_puts)
        if not filtered_puts.empty:
            st.download_button("Export Puts CSV", filtered_puts.to_csv(index=False), "ranked_puts.csv", "text/csv")

    with tabs[2]:
        detail_tickers = sorted(latest["underlying"].dropna().unique().tolist()) if not latest.empty else selected_tickers
        ticker = st.selectbox("Ticker", detail_tickers)
        detail = latest[latest["underlying"] == ticker].copy() if not latest.empty else latest
        _render_results_table(detail)

    with tabs[3]:
        rejected_underlyings = _render_underlying_filter(rejected, "rejected_underlying_filter")
        st.dataframe(_filter_by_underlying(rejected, rejected_underlyings), use_container_width=True, hide_index=True)

    with tabs[4]:
        st.dataframe(logs, use_container_width=True, hide_index=True)

    with tabs[5]:
        _render_watchlist(storage, latest)

    with tabs[6]:
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
