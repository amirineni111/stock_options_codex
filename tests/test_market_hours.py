from datetime import datetime
from zoneinfo import ZoneInfo

from options_screening.market_hours import is_regular_market_hours


def test_market_hours_true_during_regular_session():
    now = datetime(2026, 4, 24, 10, 0, tzinfo=ZoneInfo("America/New_York"))
    assert is_regular_market_hours(now)


def test_market_hours_false_on_weekend():
    now = datetime(2026, 4, 25, 10, 0, tzinfo=ZoneInfo("America/New_York"))
    assert not is_regular_market_hours(now)
