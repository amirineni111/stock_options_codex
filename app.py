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
from options_screening.market_hours import is_regular_market_hours
from options_screening.refresh import format_refresh_interval, refresh_interval_to_ms
from options_screening.scanner import ScanRequest, run_scan
from options_screening.storage import Storage
from options_screening.universe import load_sp500_tickers


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
    "auto_refresh_enabled": False,
    "refresh_unit": "minutes",
    "refresh_interval": 15,
}


def _init_state(preferences: dict) -> None:
    if "last_scan_at" not in st.session_state:
        st.session_state.last_scan_at = None
    if "auto_refresh" not in st.session_state:
        st.session_state.auto_refresh = bool(preferences["auto_refresh_enabled"])
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
    APP_PREFERENCES_PATH.write_text(json.dumps(preferences, indent=2, sort_keys=True), encoding="utf-8")


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
        min_dte, max_dte = st.slider("Days to expiration", 1, 180, _bounded_range(preferences["days_to_expiration"], 1, 180, (21, 75)))
        min_delta_abs, max_delta_abs = st.slider("Absolute delta range", 0.05, 0.95, _bounded_range(preferences["absolute_delta_range"], 0.05, 0.95, (0.25, 0.65)), 0.01)
        min_iv, max_iv = st.slider("Implied volatility range", 0.01, 3.0, _bounded_range(preferences["implied_volatility_range"], 0.01, 3.0, (0.05, 1.2)), 0.01)
        max_contracts = st.number_input("Max contracts per ticker", min_value=5, max_value=250, value=_bounded_number(preferences["max_contracts_per_ticker"], 5, 250, 50), step=5)
        ticker_limit = st.number_input("Ticker scan limit", min_value=1, max_value=503, value=_bounded_number(preferences["ticker_limit"], 1, 503, 50), step=5)
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
        allow_missing_spread=bool(allow_missing_spread),
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
