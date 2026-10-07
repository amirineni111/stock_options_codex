from datetime import datetime
from typing import Optional

REFRESH_UNIT_SECONDS = {
    "seconds": 1,
    "minutes": 60,
}

# Browsers run a setInterval delay above 2**31 - 1 ms immediately, which would turn a
# long overnight sleep into a refresh loop.
MAX_TIMER_MS = 2**31 - 1
# Seconds past a scan window's opening to wake, so the first scan is never early.
WAKE_LAG_SECONDS = 30
# A stored scan counts as fresh until this share of the refresh interval has passed.
# The page's timer restarts on every rerun, so a tick lands a little short of a full
# interval after the scan before it; requiring the whole interval would skip every
# other tick.
FRESH_SHARE = 0.8


def refresh_interval_to_ms(value: float, unit: str) -> int:
    if unit not in REFRESH_UNIT_SECONDS:
        raise ValueError(f"Unsupported refresh unit: {unit}")
    if value <= 0:
        raise ValueError("Refresh interval must be positive")
    return int(value * REFRESH_UNIT_SECONDS[unit] * 1000)


def format_refresh_interval(value: float, unit: str) -> str:
    label = unit[:-1] if value == 1 and unit.endswith("s") else unit
    return f"{value:g} {label}"


def sleep_interval_ms(now: datetime, wake_at: datetime) -> int:
    """Timer delay that next fires just after ``wake_at``, within what browsers honour."""
    seconds = (wake_at - now).total_seconds() + WAKE_LAG_SECONDS
    return max(1000, min(int(seconds * 1000), MAX_TIMER_MS))


def scan_is_stale(last_scan_at: Optional[datetime], now: datetime, interval_ms: int) -> bool:
    """
    Whether the latest stored scan is old enough to be worth repeating.

    The stored scan may come from the alert runner, another tab, or this tab before a
    reload or a page switch; any of them makes a fresh scan here a wasted set of calls.
    """
    if last_scan_at is None:
        return True
    return (now - last_scan_at).total_seconds() * 1000 >= interval_ms * FRESH_SHARE
