import pytest

from options_screening.refresh import format_refresh_interval, refresh_interval_to_ms


def test_refresh_interval_to_ms_supports_minutes_and_seconds():
    assert refresh_interval_to_ms(15, "minutes") == 900000
    assert refresh_interval_to_ms(1, "minutes") == 60000
    assert refresh_interval_to_ms(30, "seconds") == 30000


def test_refresh_interval_to_ms_rejects_invalid_values():
    with pytest.raises(ValueError):
        refresh_interval_to_ms(0, "minutes")
    with pytest.raises(ValueError):
        refresh_interval_to_ms(1, "hours")


def test_format_refresh_interval():
    assert format_refresh_interval(15, "minutes") == "15 minutes"
    assert format_refresh_interval(1, "minutes") == "1 minute"
    assert format_refresh_interval(30, "seconds") == "30 seconds"


def test_sleep_interval_wakes_just_after_reopening_and_stays_within_browser_limits():
    from datetime import datetime, timedelta, timezone

    from options_screening.refresh import MAX_TIMER_MS, WAKE_LAG_SECONDS, sleep_interval_ms

    now = datetime(2026, 10, 7, 22, 0, tzinfo=timezone.utc)
    assert sleep_interval_ms(now, now + timedelta(hours=1)) == (3600 + WAKE_LAG_SECONDS) * 1000
    # A long holiday weekend fits; anything longer is capped rather than overflowing
    # into an immediate, looping timer.
    assert sleep_interval_ms(now, now + timedelta(days=4)) < MAX_TIMER_MS
    assert sleep_interval_ms(now, now + timedelta(days=40)) == MAX_TIMER_MS


def test_a_recent_scan_from_anywhere_suppresses_the_next_auto_scan():
    from datetime import datetime, timedelta, timezone

    from options_screening.refresh import scan_is_stale

    now = datetime(2026, 10, 7, 15, 0, tzinfo=timezone.utc)
    fifteen_minutes = 15 * 60 * 1000
    assert scan_is_stale(None, now, fifteen_minutes)
    assert not scan_is_stale(now - timedelta(minutes=3), now, fifteen_minutes)
    # A tick that lands a few seconds short of the full interval still scans.
    assert scan_is_stale(now - timedelta(minutes=14, seconds=50), now, fifteen_minutes)
