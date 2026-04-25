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
