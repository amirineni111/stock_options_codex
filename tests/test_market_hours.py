from datetime import date, datetime
from zoneinfo import ZoneInfo

from options_screening.market_hours import (
    current_market_phase,
    in_scan_window,
    is_early_close,
    is_regular_market_hours,
    next_scan_window_start,
    nyse_holidays,
)


def test_market_hours_true_during_regular_session():
    now = datetime(2026, 4, 24, 10, 0, tzinfo=ZoneInfo("America/New_York"))
    assert is_regular_market_hours(now)


def test_market_hours_false_on_weekend():
    now = datetime(2026, 4, 25, 10, 0, tzinfo=ZoneInfo("America/New_York"))
    assert not is_regular_market_hours(now)


ET = ZoneInfo("America/New_York")


def test_nyse_holidays_match_the_published_2026_and_2027_calendars():


    assert sorted(nyse_holidays(2026)) == [
        date(2026, 1, 1), date(2026, 1, 19), date(2026, 2, 16), date(2026, 4, 3),
        date(2026, 5, 25), date(2026, 6, 19), date(2026, 7, 3), date(2026, 9, 7),
        date(2026, 11, 26), date(2026, 12, 25),
    ]
    # 2027: Juneteenth and Christmas fall on Saturdays, July 4th on a Sunday.
    assert sorted(nyse_holidays(2027)) == [
        date(2027, 1, 1), date(2027, 1, 18), date(2027, 2, 15), date(2027, 3, 26),
        date(2027, 5, 31), date(2027, 6, 18), date(2027, 7, 5), date(2027, 9, 6),
        date(2027, 11, 25), date(2027, 12, 24),
    ]
    # New Year's Day 2028 is a Saturday and is not made up on Friday Dec 31.
    assert date(2027, 12, 31) not in nyse_holidays(2027)
    assert not nyse_holidays(2028) & {date(2027, 12, 31), date(2028, 1, 1)}


def test_early_closes_only_on_monday_to_thursday_eves():


    assert is_early_close(date(2026, 11, 27))  # day after Thanksgiving
    assert is_early_close(date(2026, 12, 24))  # Thursday Christmas Eve
    assert not is_early_close(date(2026, 7, 2))  # July 3rd is the observed holiday
    assert is_early_close(date(2029, 7, 3))  # Tuesday
    assert not is_early_close(date(2027, 12, 24))  # a Friday holiday, not a half day


def test_holidays_and_early_closes_read_as_closed():
    thanksgiving = datetime(2026, 11, 26, 11, 0, tzinfo=ET)
    assert not is_regular_market_hours(thanksgiving)
    assert current_market_phase(thanksgiving) == "CLOSED"
    half_day = datetime(2026, 11, 27, 14, 0, tzinfo=ET)
    assert not is_regular_market_hours(half_day)
    assert current_market_phase(half_day) == "AFTER_HOURS"
    assert current_market_phase(datetime(2026, 10, 7, 2, 0, tzinfo=ET)) == "CLOSED"


def test_scan_windows_skip_holidays_and_shorten_on_early_closes():
    assert not in_scan_window(datetime(2026, 11, 26, 11, 0, tzinfo=ET), "options")
    assert in_scan_window(datetime(2026, 11, 27, 13, 15, tzinfo=ET), "options")
    assert not in_scan_window(datetime(2026, 11, 27, 13, 25, tzinfo=ET), "options")
    assert not in_scan_window(datetime(2026, 11, 27, 13, 5, tzinfo=ET), "intraday")


def test_next_scan_window_start_skips_nights_weekends_and_holidays():
    # Wednesday evening -> Thursday morning.
    assert next_scan_window_start(datetime(2026, 10, 7, 18, 0, tzinfo=ET), "options") == datetime(2026, 10, 8, 9, 45, tzinfo=ET)
    # Friday after the close -> Monday.
    assert next_scan_window_start(datetime(2026, 10, 9, 17, 0, tzinfo=ET), "intraday") == datetime(2026, 10, 12, 9, 30, tzinfo=ET)
    # Wednesday before Thanksgiving -> Friday, skipping the holiday.
    assert next_scan_window_start(datetime(2026, 11, 25, 20, 0, tzinfo=ET), "options") == datetime(2026, 11, 27, 9, 45, tzinfo=ET)
    # Inside a window, the next one is tomorrow's.
    assert next_scan_window_start(datetime(2026, 10, 7, 10, 0, tzinfo=ET), "intraday") == datetime(2026, 10, 8, 9, 30, tzinfo=ET)
