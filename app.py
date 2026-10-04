from datetime import datetime
import json
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh

from options_screening.alerts import channel_name, deliver_pending, send_test
from options_screening.config import get_settings
from options_screening.intraday import IntradayScanRequest, run_intraday_scan
from options_screening.market_hours import (
    current_market_phase,
    is_regular_market_hours,
    phase_badge_color,
)
from options_screening.refresh import format_refresh_interval, refresh_interval_to_ms
from options_screening.scanner import ScanRequest, run_scan
from options_screening.storage import Storage
from options_screening.ui_panels import (
    render_alerts_tab,
    render_model_tab,
    render_performance_tab,
)
from options_screening.universe import load_sp100_tickers, load_sp500_tickers


st.set_page_config(page_title="Options Screener", layout="wide")

EASTERN_TZ = ZoneInfo("America/New_York")
# Resolved against this file, not the process working directory: a bare
# "data/app_preferences.json" lands somewhere different depending on where streamlit
# was launched from, so preferences appeared to reset when the app was started from
# another folder.
APP_PREFERENCES_PATH = Path(__file__).resolve().parent / "data" / "app_preferences.json"
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
    "intraday_universe": "S&P 100",
    "intraday_custom_tickers": "",
    "intraday_min_price": 5.0,
    "intraday_max_price": 1000.0,
    # 1.0x means "normal volume for this time of day". The old 0.05 default was
    # compensating for a relative-volume calculation that compared a partial session
    # against a whole one; that maths is fixed, so the threshold means what it says.
    "intraday_min_relative_volume": 1.0,
    "intraday_min_avg_dollar_volume_m": 10.0,
    "intraday_include_shorts": True,
    "intraday_use_higher_timeframes": True,
    "intraday_use_relative_strength": True,
    "intraday_auto_refresh_enabled": False,
    "intraday_refresh_interval": 15,
    "alerts_enabled": True,
    "alert_webhook": "",
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
    ("engine_signal", "Second, independent label: direction from the multi-factor engine run on the underlying's daily bars (momentum, mean reversion, breakout, weekly trend, support/resistance) instead of the moving-average rule. Same contract checks. Both are forward-tested; the Performance tab's 'Direction source' breakdown shows which is right more often.", "ENGINE_BUY_CALL"),
    ("engine_score", "The engine's score for the underlying. 45+ is actionable, 70+ strong.", "58.0"),
    ("engine_reason", "Why the engine label landed where it did.", "engine BUY_CANDIDATE (58pts): Long candidate"),
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
    ("premium_entry", "Entry premium per share: the current mid. One contract costs this times 100.", "4.00 = $400"),
    ("premium_stop", "Premium the position is worth if the underlying reaches its stop, decay included. Floored so a bracket never implies losing the whole premium.", "1.60"),
    ("premium_target", "Premium if the underlying reaches its target, decay included.", "13.42"),
    ("premium_rr", "Reward:risk in PREMIUM terms. Not the underlying's 1.5 - delta, gamma and decay all bend it, and on a low-delta contract they bend it a long way.", "3.93"),
    ("risk_dollars", "Dollars at risk per contract if the stop is reached.", "240.00"),
    ("reward_dollars", "Dollars gained per contract if the target is reached.", "942.20"),
    ("underlying_stop", "Underlying price that triggers the exit. The position is exited on the stock, but the P&L is in premium, so both are shown.", "114.00"),
    ("underlying_target", "Underlying price at the profit target.", "139.00"),
    ("theta_per_premium", "Daily decay as a fraction of the premium paid - the rate that actually kills a long option. 0.0125 means it loses 1.25% of value per day if nothing moves.", "0.0125"),
    ("decay_at_target", "Dollars per contract time decay will consume over the expected hold. Negative, because it is a cost.", "-70.30"),
    ("target_hold_days", "How long the target move plausibly takes, from the underlying's own volatility. Scales with the SQUARE of distance in ATRs, because a random walk covers N x ATR in about N-squared days.", "14.06"),
    ("iv_rank", "Where today's IV sits in this underlying's own recent range, 0 to 1. An absolute IV means nothing alone: 45% is cheap for one name and expensive for another.", "0.58"),
    ("gamma_leverage", "How fast delta accelerates, scaled to compare across names. High is cheap convexity - and fast decay, which is why it sits beside theta_per_premium.", "0.0115"),
    ("underlying_atr14", "14-day ATR of the underlying. The unit the stop distance is quoted in.", "4.00"),
    ("model_prob", "The trained model's P(target before stop), when one is serving. It can only veto, never promote.", "0.47"),
    ("required_prob", "Cost-adjusted breakeven win rate plus a margin. Below this, an otherwise-actionable contract is downgraded.", "0.44"),
]
INTRADAY_COLUMN_GUIDE = [
    ("trade_signal", "The decision. STRONG_BUY / STRONG_SHORT need a 70+ score and higher-timeframe confirmation; BUY_CANDIDATE / SHORT_CANDIDATE need 45+; WATCH_ONLY is a setup a veto downgraded; AVOID is unusable.", "A veto always downgrades rather than hides, so the reason stays visible."),
    ("dominant", "Which direction the weighted components favour: LONG, SHORT, or NEUTRAL.", "A suppressed playbook casts no vote."),
    ("total_score", "Momentum + reversion (regime-weighted) + breakout + confluence + structure + relative strength.", "45 is actionable, 70 is strong. Structure can subtract."),
    ("regime", "Trend strength read from ADX, deciding which playbook is trusted. TREND favours momentum, RANGE favours mean reversion, MIXED blends them.", "ADX 25+ is TREND, 18- is RANGE, between is MIXED."),
    ("suggested_entry", "Last completed bar's close. Signals never use the still-forming candle, so this does not move until the bar closes.", "182.45"),
    ("suggested_stop", "Entry minus the stop distance, which is the widest of 2.5xATR, 0.50% of price, and 8x the round-trip cost.", "178.20"),
    ("suggested_target", "1.5x the stop distance from entry. Reward:risk is fixed, so a wider stop widens the target.", "188.83"),
    ("stop_pct", "Stop distance as a percent of entry.", "2.33"),
    ("rr_ratio", "Reward to risk. Always 1.5 by construction.", "1.5"),
    ("cost_pct", "Estimated round-trip transaction cost as a percent of price. Uses the live quoted spread when available, otherwise a deliberately pessimistic liquidity tier.", "0.08"),
    ("cost_ratio", "Cost as a fraction of the risk taken. This is the drag charged against every trade before any edge is counted.", "0.03"),
    ("model_prob", "The trained model's P(target before stop), when a model is active. It can only veto, never promote.", "0.47"),
    ("required_prob", "Cost-adjusted breakeven win rate plus a margin. Below this an otherwise-actionable signal is downgraded.", "0.44"),
    ("mtf_confluence", "Higher-timeframe agreement. FULL needs both the hourly and daily trend present AND agreeing; a NEUTRAL timeframe is not confirmation.", "FULL +30, PARTIAL +15, UNCONFIRMED/CONFLICT/OPPOSED 0."),
    ("sr_score", "Support/resistance scored relative to the trade direction. A level behind the trade shelters the stop; one ahead blocks the target.", "+25 at structure, -25 when blocked."),
    ("blocked_ahead", "True when a level sits within 1.5xATR of the path to target. Downgrades an otherwise-actionable signal.", "True/False"),
    ("extension_atr", "How many ATRs price sits from EMA20. A STRONG signal beyond 2.0 is downgraded rather than chased.", "1.4"),
    ("rs_vs_spy", "Day change minus SPY's, in percentage points. Folded into the score itself, not added afterwards.", "+1.9 leads the market."),
    ("relative_volume", "This bar's volume against the last 20 bars' average. Bar-for-bar, so it is comparable at any time of day.", "2.0 is twice normal volume."),
    ("avg_dollar_volume", "20-bar average of close x volume. Sets the liquidity gate and the cost tier.", "A million shares of a $3 stock is not a million of a $300 stock."),
    ("rsi14", "Momentum oscillator from 0 to 100. Above 50 supports bullish momentum; below 50 bearish. Extremes favour mean reversion instead.", "Momentum zone 40-65. Oversold below 30, overbought above 70."),
    ("atr14", "Average true range: the unit every stop distance here is quoted in.", "1.85"),
    ("adx14", "Wilder trend strength, roughly 0-100. Drives the regime gate.", "Above 25 trending, below 18 ranging."),
    ("vwap", "Volume-weighted average price for the session so far.", "Longs prefer price above VWAP; shorts below."),
    ("signal_reason", "Plain-English summary of why the row landed where it did, with the numbers in it.", "\"Long candidate (52pts)\" or \"hourly trend is SHORT - countertrend\"."),
    ("risk_notes", "Component detail plus any warnings: liquidity, cost, blocked structure, short-selling risk.", "Reads as sentences, not codes."),
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
    if "alerts_enabled" not in st.session_state:
        st.session_state.alerts_enabled = bool(preferences["alerts_enabled"])
    if "alert_webhook" not in st.session_state:
        st.session_state.alert_webhook = str(preferences["alert_webhook"] or "")


def _alert_url(settings) -> str:
    """This session's push URL: the sidebar override, else OPTIONS_ALERT_WEBHOOK_URL."""
    return (st.session_state.get("alert_webhook") or "").strip() or settings.alert_webhook_url


def _render_alert_settings(settings) -> None:
    with st.expander("Phone alerts", expanded=False):
        st.session_state.alerts_enabled = st.checkbox(
            "Alert on new signals and outcomes",
            value=st.session_state.alerts_enabled,
            help=(
                "When this dashboard's scan arms a signal or resolves one, push it. Off "
                "leaves alerting to the headless runner (start_options_alerts.bat), "
                "which alerts even with no browser open."
            ),
        )
        st.session_state.alert_webhook = st.text_input(
            "Push URL",
            value=st.session_state.alert_webhook,
            placeholder="https://ntfy.sh/your-private-topic",
            help=(
                "An ntfy topic URL (install the ntfy app on your iPhone and subscribe to "
                "the same topic), or a Discord/Slack webhook. Blank uses "
                "OPTIONS_ALERT_WEBHOOK_URL from .env."
            ),
        )
        url = _alert_url(settings)
        st.caption(f"Pushing to {channel_name(url)}." if url else "No push URL - alerts are logged to the Alerts tab only.")
        if st.button("Send test alert", disabled=not url):
            error = send_test(url)
            if error:
                st.error(f"Push failed: {error}")
            else:
                st.success("Sent - check your phone.")
    try:
        _save_app_preferences(
            {
                "alerts_enabled": bool(st.session_state.alerts_enabled),
                "alert_webhook": st.session_state.alert_webhook,
            }
        )
    except OSError as exc:
        st.warning(f"Could not save alert settings: {exc}")


def _deliver_alerts(storage: Storage, settings) -> None:
    """Push whatever the scan just armed or resolved: a toast here, a push if configured."""
    if not st.session_state.get("alerts_enabled", True):
        return  # leave them unclaimed, so the headless runner can push them
    report = deliver_pending(storage, _alert_url(settings), "dashboard")
    for message in report.sent:
        st.toast(message.title, icon="🔔")
    if report.errors:
        st.warning("Alert push failed: " + "; ".join(report.errors))


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
        "engine_signal",
        "engine_score",
        "engine_reason",
        # The premium bracket and the greeks-derived columns, next to the decision
        # they inform rather than at the far right of a 50-column table.
        "premium_entry",
        "premium_stop",
        "premium_target",
        "premium_rr",
        "risk_dollars",
        "reward_dollars",
        "underlying_stop",
        "underlying_target",
        "theta_per_premium",
        "decay_at_target",
        "target_hold_days",
        "iv_rank",
        "gamma_leverage",
        "underlying_atr14",
        "model_prob",
        "required_prob",
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
    # Decision-first ordering: what the signal is, then the bracket it implies, then
    # the score breakdown, then the raw indicators behind it.
    columns = [
        "rank",
        "ticker",
        "trade_signal",
        "dominant",
        "total_score",
        "last_price",
        "day_change_pct",
        "suggested_entry",
        "suggested_stop",
        "suggested_target",
        "stop_pct",
        "target_pct",
        "rr_ratio",
        "cost_pct",
        "cost_ratio",
        "model_prob",
        "required_prob",
        "regime",
        "adx14",
        "momentum_score",
        "reversion_score",
        "breakout_score",
        "mtf_score",
        "mtf_confluence",
        "sr_score",
        "at_key_level",
        "blocked_ahead",
        "nearest_support",
        "nearest_resistance",
        "extension_atr",
        "rs_vs_spy",
        "rs_assessment",
        "relative_volume",
        "avg_dollar_volume",
        "rsi14",
        "ema9",
        "ema20",
        "macd",
        "macd_signal",
        "macd_histogram",
        "atr14",
        "vwap",
        "spread_pct",
        "volume",
        "open",
        "high",
        "low",
        "prev_close",
        "market_phase",
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


def _filter_intraday_results(df: pd.DataFrame, tickers, signals, regimes, min_score: float) -> pd.DataFrame:
    if df.empty:
        return df
    filtered = df.copy()
    if tickers and "ticker" in filtered.columns:
        filtered = filtered[filtered["ticker"].isin(tickers)]
    if signals and "trade_signal" in filtered.columns:
        filtered = filtered[filtered["trade_signal"].isin(signals)]
    if regimes and "regime" in filtered.columns:
        filtered = filtered[filtered["regime"].isin(regimes)]
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
        # No "signal mode" selector any more. Momentum and mean reversion are no
        # longer two scores the user picks between — an ADX regime gate weights them
        # from measured trend strength, so choosing one by hand would be overriding
        # the tape with a guess.
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
        min_relative_volume = st.number_input(
            "Min relative volume",
            min_value=0.0,
            max_value=10.0,
            value=_bounded_number(preferences["intraday_min_relative_volume"], 0.0, 10.0, 1.0),
            step=0.1,
            help=(
                "Bar-for-bar volume against the last 20 bars' average. 1.0 is normal "
                "volume for this time of day, 2.0 is twice normal."
            ),
        )
        min_avg_dollar_volume = st.number_input(
            "Min avg $ volume (millions)",
            min_value=0.0,
            max_value=1000.0,
            value=_bounded_number(preferences["intraday_min_avg_dollar_volume_m"], 0.0, 1000.0, 10.0),
            step=1.0,
            help=(
                "Liquidity gate. Below a tenth of this the name is rejected outright; "
                "below it the score is penalised. Also sets the transaction-cost tier "
                "when no live quote is available."
            ),
        )
        include_shorts = st.checkbox("Include short candidates", value=bool(preferences["intraday_include_shorts"]))
        use_higher_timeframes = st.checkbox(
            "Require higher-timeframe confirmation",
            value=bool(preferences["intraday_use_higher_timeframes"]),
            help=(
                "Score the hourly and daily trend and the support/resistance map. "
                "Full credit needs both timeframes present and agreeing."
            ),
        )
        use_relative_strength = st.checkbox(
            "Score relative strength vs SPY",
            value=bool(preferences["intraday_use_relative_strength"]),
        )
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
                "intraday_universe": intraday_universe,
                "intraday_custom_tickers": intraday_custom_tickers,
                "intraday_min_price": float(min_price),
                "intraday_max_price": float(max_price),
                "intraday_min_relative_volume": float(min_relative_volume),
                "intraday_min_avg_dollar_volume_m": float(min_avg_dollar_volume),
                "intraday_include_shorts": bool(include_shorts),
                "intraday_use_higher_timeframes": bool(use_higher_timeframes),
                "intraday_use_relative_strength": bool(use_relative_strength),
                "intraday_auto_refresh_enabled": bool(st.session_state.intraday_auto_refresh),
                "intraday_refresh_interval": int(intraday_refresh_interval),
            }
        )
    except OSError as exc:
        st.warning(f"Could not save intraday settings: {exc}")

    # Market data for this page comes from Yahoo and needs no key. Polygon is used
    # only to upgrade the estimated transaction cost to a real quoted spread, so its
    # absence degrades the ranking slightly rather than blocking the scan.
    if not settings.polygon_api_key:
        st.info(
            "No POLYGON_API_KEY set. Scanning still works on Yahoo data; transaction "
            "cost falls back to conservative liquidity tiers instead of live spreads."
        )

    request = IntradayScanRequest(
        tickers=selected_tickers,
        min_price=float(min_price),
        max_price=float(max_price),
        min_relative_volume=float(min_relative_volume),
        min_avg_dollar_volume=float(min_avg_dollar_volume) * 1_000_000,
        include_shorts=bool(include_shorts),
        use_higher_timeframes=bool(use_higher_timeframes),
        use_relative_strength=bool(use_relative_strength),
    )

    run_col, info_col = st.columns([1, 4])
    with run_col:
        run_now = st.button("Run Intraday Scan", type="primary", disabled=not selected_tickers)
    with info_col:
        phase = current_market_phase()
        st.write(
            f"{phase_badge_color(phase)} {phase.replace('_', ' ').title()} · "
            f"Universe: {intraday_universe}, {len(selected_tickers)} tickers · "
            f"Refresh every {int(intraday_refresh_interval)} min."
        )

    auto_count = None
    if st.session_state.intraday_auto_refresh:
        auto_count = st_autorefresh(interval=int(intraday_refresh_interval) * 60 * 1000, key="intraday_auto_refresh_counter")
        if not is_regular_market_hours():
            st.info("Intraday auto-refresh is enabled and waiting for regular US market hours.")

    # The tick-counter comparison is the idempotence guard: Streamlit reruns on any
    # widget interaction, so without it every filter click would fire a fresh scan.
    auto_due = (
        st.session_state.intraday_auto_refresh
        and bool(selected_tickers)
        and is_regular_market_hours()
        and auto_count is not None
        and auto_count != st.session_state.intraday_last_auto_refresh_count
    )

    if run_now or auto_due:
        with st.spinner("Scanning intraday stock snapshots..."):
            # Passing storage turns the scan into the full loop: open forward tests
            # are resolved against the fresh bars and new signals are armed.
            results, summary, logs = run_intraday_scan(settings, request, storage=storage)
            storage.save_intraday_scan(results, logs)
        _deliver_alerts(storage, settings)
        st.session_state.intraday_last_scan_at = datetime.now(EASTERN_TZ).strftime("%Y-%m-%d %H:%M:%S %Z")
        if auto_due:
            st.session_state.intraday_last_auto_refresh_count = auto_count
        st.success(
            f"Intraday scan complete: {summary.accepted} candidates, {summary.watch} watch, "
            f"{summary.avoid} avoid, {summary.errors} errors."
        )

    latest = _format_time_columns(storage.load_intraday_results(), ["as_of"])
    logs = _format_time_columns(storage.load_intraday_logs(), ["created_at"])

    (
        tab_results,
        tab_alerts,
        tab_performance,
        tab_model,
        tab_logs,
        tab_watchlist,
        tab_settings,
    ) = st.tabs(["Results", "Alerts", "Performance", "Model", "Scan Logs", "Watchlist", "Settings"])
    with tab_alerts:
        render_alerts_tab(storage, "intraday")
    with tab_performance:
        render_performance_tab(storage, "intraday")
    with tab_model:
        render_model_tab(storage, "intraday")
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
            "Filter regime",
            sorted(latest["regime"].dropna().unique().tolist()) if not latest.empty else [],
            default=[],
            placeholder="All regimes",
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


def _latest_scan_failure(logs: pd.DataFrame):
    """
    Why the tables are empty, when the most recent scan fetched nothing at all.

    Read from the stored logs rather than the scan's return value so it survives the
    reruns auto-refresh triggers; a one-off banner is gone before it is read.
    """
    if logs.empty or "scan_id" not in logs.columns:
        return None
    last_scan = logs[logs["scan_id"] == logs["scan_id"].max()]
    errors = last_scan["error"].dropna()
    errors = errors[errors.astype(str).str.strip() != ""]
    if errors.empty or len(errors) < len(last_scan):
        return None
    sample = str(errors.iloc[0])
    if "Polygon API error 403" in sample or "not entitled" in sample.lower():
        return (
            f"The last scan returned no data: Polygon refused all {len(last_scan)} tickers "
            "with 403 (not entitled). The API key is valid, but its plan does not include "
            "the option chain snapshot endpoint this page depends on. The tables stay empty "
            "until the plan includes options snapshots. The Intraday Stocks page is unaffected."
        )
    return f"The last scan returned no data: all {len(last_scan)} tickers failed. First error: {sample}"


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


def main() -> None:
    preferences = _load_app_preferences()
    _init_state(preferences)
    settings = get_settings()
    storage = Storage(settings.db_path)
    storage.initialize()

    with st.sidebar:
        page = st.radio("Page", ["Options Scanner", "Intraday Stocks"], horizontal=True)
        _render_alert_settings(settings)
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

    # Named rather than indexed: the positional form silently shifted every later tab
    # when two were inserted in the middle.
    (
        tab_calls,
        tab_puts,
        tab_detail,
        tab_alerts,
        tab_performance,
        tab_model,
        tab_rejected,
        tab_logs,
        tab_watchlist,
        tab_settings,
    ) = st.tabs(
        [
            "Ranked Calls",
            "Ranked Puts",
            "Ticker Detail",
            "Alerts",
            "Performance",
            "Model",
            "Rejected",
            "Scan Logs",
            "Watchlist",
            "Settings",
        ]
    )
    with tab_performance:
        render_performance_tab(storage, "options")
    with tab_model:
        render_model_tab(storage, "options")

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
            summary = run_scan(settings, storage, scan_request)
        _deliver_alerts(storage, settings)
        st.session_state.last_scan_at = datetime.now(EASTERN_TZ).strftime("%Y-%m-%d %H:%M:%S %Z")
        if auto_due:
            st.session_state.last_auto_refresh_count = auto_count
        scan_message = (
            f"Scan complete: {summary.accepted} accepted, {summary.rejected} rejected, "
            f"{summary.errors} errors; {summary.armed} new signals tracked, "
            f"{summary.resolved} resolved."
        )
        # A scan where every ticker errored is not a success, and a green banner over
        # empty tables reads as "nothing qualified" rather than "nothing was fetched".
        if summary.errors and not (summary.accepted or summary.rejected):
            st.warning(scan_message)
        else:
            st.success(scan_message)

    latest = storage.load_latest_results()
    rejected = storage.load_latest_rejections()
    logs = storage.load_scan_logs()
    scan_failure = _latest_scan_failure(logs)
    if scan_failure:
        st.error(scan_failure)
    latest = _format_time_columns(latest, ["as_of"])
    rejected = _format_time_columns(rejected, ["as_of"])
    logs = _format_time_columns(logs, ["created_at"])

    calls = latest[latest["contract_type"] == "call"].copy() if not latest.empty else latest
    puts = latest[latest["contract_type"] == "put"].copy() if not latest.empty else latest

    with tab_calls:
        call_underlyings = _render_underlying_filter(calls, "calls_underlying_filter")
        call_signals = _render_signal_filter(calls, "calls_signal_filter")
        filtered_calls = _filter_by_signal(_filter_by_underlying(calls, call_underlyings), call_signals)
        _render_metric_row(filtered_calls)
        _render_results_table(filtered_calls)
        if not filtered_calls.empty:
            st.download_button("Export Calls CSV", filtered_calls.to_csv(index=False), "ranked_calls.csv", "text/csv")

    with tab_puts:
        put_underlyings = _render_underlying_filter(puts, "puts_underlying_filter")
        put_signals = _render_signal_filter(puts, "puts_signal_filter")
        filtered_puts = _filter_by_signal(_filter_by_underlying(puts, put_underlyings), put_signals)
        _render_metric_row(filtered_puts)
        _render_results_table(filtered_puts)
        if not filtered_puts.empty:
            st.download_button("Export Puts CSV", filtered_puts.to_csv(index=False), "ranked_puts.csv", "text/csv")

    with tab_detail:
        detail_tickers = sorted(latest["underlying"].dropna().unique().tolist()) if not latest.empty else selected_tickers
        ticker = st.selectbox("Ticker", detail_tickers)
        detail = latest[latest["underlying"] == ticker].copy() if not latest.empty else latest
        _render_results_table(detail)

    with tab_alerts:
        render_alerts_tab(storage, "options")

    with tab_rejected:
        rejected_underlyings = _render_underlying_filter(rejected, "rejected_underlying_filter")
        st.dataframe(_filter_by_underlying(rejected, rejected_underlyings), use_container_width=True, hide_index=True)

    with tab_logs:
        st.dataframe(logs, use_container_width=True, hide_index=True)

    with tab_watchlist:
        _render_watchlist(storage, latest)

    with tab_settings:
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
