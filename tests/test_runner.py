"""The headless runner's schedule and its reading of the dashboard's saved settings."""
from datetime import datetime, timezone

from options_screening.config import AppSettings
from options_screening.runner import (
    alert_url, in_scan_window, intraday_request, next_wake, options_request, parse_tickers,
)


def test_next_wake_lands_just_after_the_next_quarter_hour():
    now = datetime(2026, 10, 5, 14, 37, 0, tzinfo=timezone.utc)
    assert next_wake(now, 60) == datetime(2026, 10, 5, 14, 46, 0, tzinfo=timezone.utc)
    # Inside the lag of the boundary that just passed: wake for that one.
    early = datetime(2026, 10, 5, 14, 45, 30, tzinfo=timezone.utc)
    assert next_wake(early, 60) == datetime(2026, 10, 5, 14, 46, 0, tzinfo=timezone.utc)


def test_the_options_window_runs_fifteen_minutes_late_for_delayed_data():
    # 2026-10-05 is a Monday; 13:35 UTC is 09:35 ET.
    assert not in_scan_window(datetime(2026, 10, 5, 13, 35, tzinfo=timezone.utc), "options")
    assert in_scan_window(datetime(2026, 10, 5, 13, 35, tzinfo=timezone.utc), "intraday")
    # 16:10 ET: the options lane is still catching up on the last quarter-hour.
    assert in_scan_window(datetime(2026, 10, 5, 20, 10, tzinfo=timezone.utc), "options")
    assert not in_scan_window(datetime(2026, 10, 5, 20, 10, tzinfo=timezone.utc), "intraday")
    # Weekends never.
    assert not in_scan_window(datetime(2026, 10, 4, 15, 0, tzinfo=timezone.utc), "options")


def test_the_options_request_mirrors_the_dashboard_settings():
    prefs = {
        "fixed_risk": 1000.0, "min_volume": 0, "min_open_interest": 250,
        "max_spread_pct": 50.0, "days_to_expiration": [17, 77],
        "absolute_delta_range": [0.1, 0.95], "implied_volatility_range": [0.05, 3.0],
        "max_contracts_per_ticker": 25, "allow_missing_spread": True,
        "ignore_missing_spread_for_signal": True, "use_trend_context": True,
        "require_trend_alignment": True, "check_earnings": True,
        "avoid_earnings_before_expiration": False,
    }
    request = options_request(prefs, ["AMD"])
    assert (request.min_days_to_expiration, request.max_days_to_expiration) == (17, 77)
    assert (request.min_abs_delta, request.max_abs_delta) == (0.1, 0.95)
    assert request.fixed_risk == 1000.0 and request.max_contracts_per_ticker == 25
    assert request.allow_missing_spread and request.ignore_missing_spread_for_signal
    assert request.require_trend_alignment


def test_dependent_settings_are_dropped_with_their_parent_like_the_dashboard_does():
    request = options_request(
        {"use_trend_context": False, "require_trend_alignment": True,
         "allow_missing_spread": False, "ignore_missing_spread_for_signal": True},
        ["AMD"],
    )
    assert not request.require_trend_alignment
    assert not request.ignore_missing_spread_for_signal


def test_missing_settings_fall_back_to_the_request_defaults():
    request = options_request({}, ["AMD"])
    assert request.min_days_to_expiration == 21 and request.max_abs_delta == 0.65


def test_the_intraday_request_converts_dollar_volume_from_millions():
    request = intraday_request({"intraday_min_avg_dollar_volume_m": 10.0}, ["AAPL"])
    assert request.min_avg_dollar_volume == 10_000_000.0


def test_tickers_parse_and_dedupe():
    assert parse_tickers("amd, INTC\nAMD,, sofi ") == ["AMD", "INTC", "SOFI"]


def test_env_url_wins_over_the_sidebar_url():
    assert alert_url(AppSettings(alert_webhook_url="https://ntfy.sh/a"), {"alert_webhook": "https://ntfy.sh/b"}) == "https://ntfy.sh/a"
    assert alert_url(AppSettings(), {"alert_webhook": " https://ntfy.sh/b "}) == "https://ntfy.sh/b"
