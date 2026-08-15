"""
The shared timestamp parser and session clock.

Every source in this repo writes into the same SQLite columns, so one parser has to
accept all of their formats. A silent ``None`` here is expensive: in the forex sibling
it is what made trade durations unmeasurable for months.
"""
from datetime import date, datetime, timezone

import pytest

from options_screening import timeutil as tu


class TestParseTs:
    def test_accepts_iso_with_a_zulu_suffix(self):
        assert tu.parse_ts("2026-08-14T13:45:02Z") == datetime(
            2026, 8, 14, 13, 45, 2, tzinfo=timezone.utc
        )

    def test_accepts_a_bare_sqlite_current_timestamp_as_utc(self):
        """SQLite writes "2026-08-14 13:45:02" with no zone; it is UTC here."""
        assert tu.parse_ts("2026-08-14 13:45:02") == datetime(
            2026, 8, 14, 13, 45, 2, tzinfo=timezone.utc
        )

    def test_accepts_epoch_seconds_and_milliseconds_as_the_same_instant(self):
        """Yahoo returns seconds, Polygon returns milliseconds, into one column."""
        assert tu.parse_ts(1786901102) == tu.parse_ts(1786901102000)

    def test_truncates_over_long_fractional_seconds_rather_than_failing(self):
        """OANDA-style 9-digit nanoseconds; fromisoformat rejects more than 6 on 3.9."""
        parsed = tu.parse_ts("2026-08-14T13:45:02.123456789Z")
        assert parsed is not None and parsed.microsecond == 123456

    def test_unparseable_input_is_none_not_an_exception(self):
        assert tu.parse_ts("not-a-time") is None
        assert tu.parse_ts("") is None
        assert tu.parse_ts(None) is None

    def test_every_result_is_timezone_aware(self):
        for value in ("2026-08-14 13:45:02", 1786901102, datetime(2026, 1, 1)):
            assert tu.parse_ts(value).tzinfo is not None

    def test_utc_now_is_aware(self):
        assert tu.utc_now().tzinfo is not None


class TestExchangeDate:
    def test_an_evening_scan_still_reports_the_current_trading_day(self):
        """
        The regression: DTE was counted from ``date.today()``. At 21:30 ET the UTC
        date has already rolled over, so every evening scan shifted the whole expiry
        window by a day relative to the DTE the scorer computed.
        """
        assert tu.exchange_date("2026-08-15T01:30:00Z") == date(2026, 8, 14)

    def test_midday_is_unambiguous(self):
        assert tu.exchange_date("2026-08-14T17:00:00Z") == date(2026, 8, 14)


class TestSessionClock:
    def test_progress_is_zero_at_the_open_and_one_at_the_close(self):
        assert tu.session_progress("2026-08-14T13:30:00Z") == 0.0
        assert tu.session_progress("2026-08-14T20:00:00Z") == 1.0

    def test_progress_is_clipped_outside_the_session_not_extrapolated(self):
        """
        Clipping keeps pre- and after-hours bars at the endpoints instead of handing
        the model large out-of-range values it has almost no examples of.
        """
        assert tu.session_progress("2026-08-14T09:00:00Z") == 0.0
        assert tu.session_progress("2026-08-14T23:00:00Z") == 1.0

    def test_minutes_since_open_is_negative_before_the_open(self):
        assert tu.minutes_since_open_at("2026-08-14T13:00:00Z") == -30.0

    def test_minutes_since_open_is_keyed_to_the_timestamps_own_date(self):
        """
        A feature must describe the bar it was built from, or every backfill and
        retrain silently shifts. The same UTC wall time is a different point in the
        session in January (EST) and July (EDT), and the offset must follow the
        timestamp's own date rather than today's.
        """
        winter = tu.minutes_since_open_at("2020-01-02T15:00:00Z")  # 10:00 EST
        summer = tu.minutes_since_open_at("2020-07-02T15:00:00Z")  # 11:00 EDT
        assert winter == 30.0
        assert summer == 90.0

    def test_is_regular_at_rejects_the_weekend(self):
        assert tu.is_regular_at("2026-08-15T17:00:00Z") is False  # Saturday
        assert tu.is_regular_at("2026-08-14T17:00:00Z") is True

    def test_minutes_between_measures_the_gap(self):
        assert tu.minutes_between("2026-08-14T13:00:00Z", "2026-08-14T14:30:00Z") == 90.0

    def test_minutes_between_is_none_when_either_end_is_unparseable(self):
        assert tu.minutes_between("junk", "2026-08-14T14:30:00Z") is None
        assert tu.minutes_between("2026-08-14T13:00:00Z", None) is None


class TestToIso:
    def test_round_trips_every_input_format_to_one_canonical_string(self):
        """
        One uniform format across every writer is what makes lexicographic string
        ordering equal chronological ordering, which the forward-test resolver relies
        on when selecting "bars after entry".
        """
        expected = tu.to_iso("2026-08-14T13:45:02Z")
        assert tu.to_iso("2026-08-14 13:45:02") == expected
        assert tu.to_iso(datetime(2026, 8, 14, 13, 45, 2, tzinfo=timezone.utc)) == expected

    def test_string_order_matches_time_order(self):
        earlier = tu.to_iso("2026-08-14T13:45:02Z")
        later = tu.to_iso("2026-08-14T13:50:02Z")
        assert earlier < later
