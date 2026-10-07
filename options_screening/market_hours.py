"""
US equity market phases in America/New_York.

Every function takes an injectable ``now`` so tests never have to reach for the real
clock, and every one returns ``None`` rather than a guess when the answer is
undefined (weekends, before the open).

NYSE full-day holidays and 13:00 early closes are computed from the exchange's
published rules, so a holiday reads CLOSED and nothing scans on it. One-off closures
(a national day of mourning, a weather closure) cannot be derived and are not known.
"""
from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from functools import lru_cache
from typing import FrozenSet, Optional, Tuple
from zoneinfo import ZoneInfo

US_EASTERN = ZoneInfo("America/New_York")

PRE_MARKET_START = time(4, 0)
REGULAR_START = time(9, 30)
REGULAR_END = time(16, 0)
AFTER_HOURS_END = time(20, 0)
EARLY_CLOSE = time(13, 0)

# Each lane's scan window: when it starts, and how many minutes past the close it runs.
# The options lane starts and ends 15 minutes late: the data is 15-minute delayed, so a
# 09:30 scan would price the pre-open and the last quarter-hour is only visible at 16:15.
_SCAN_WINDOWS = {
    "options": (time(9, 45), 20),
    "intraday": (time(9, 30), 0),
}


def _to_eastern(now: Optional[datetime] = None) -> datetime:
    current = now or datetime.now(tz=US_EASTERN)
    if current.tzinfo is None:
        current = current.replace(tzinfo=US_EASTERN)
    return current.astimezone(US_EASTERN)


def _easter(year: int) -> date:
    """Western Easter Sunday (anonymous Gregorian algorithm)."""
    a, b, c = year % 19, year // 100, year % 100
    d, e = divmod(b, 4)
    g = (b - (b + 8) // 25 + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month, day = divmod(h + l - 7 * m + 114, 31)
    return date(year, month, day + 1)


def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    """The ``n``th ``weekday`` (Mon=0) of the month; ``n=-1`` for the last one."""
    if n > 0:
        first = date(year, month, 1)
        return first + timedelta(days=(weekday - first.weekday()) % 7 + 7 * (n - 1))
    last = date(year + month // 12, month % 12 + 1, 1) - timedelta(days=1)
    return last - timedelta(days=(last.weekday() - weekday) % 7)


def _observed(day: date) -> date:
    """Saturday holidays close the Friday before, Sunday ones the Monday after."""
    if day.weekday() == 5:
        return day - timedelta(days=1)
    if day.weekday() == 6:
        return day + timedelta(days=1)
    return day


@lru_cache(maxsize=None)
def nyse_holidays(year: int) -> FrozenSet[date]:
    """Full-day NYSE closures in ``year``."""
    days = {
        _nth_weekday(year, 1, 0, 3),  # Martin Luther King Jr. Day
        _nth_weekday(year, 2, 0, 3),  # Washington's Birthday
        _easter(year) - timedelta(days=2),  # Good Friday
        _nth_weekday(year, 5, 0, -1),  # Memorial Day
        _observed(date(year, 7, 4)),
        _nth_weekday(year, 9, 0, 1),  # Labor Day
        _nth_weekday(year, 11, 3, 4),  # Thanksgiving
        _observed(date(year, 12, 25)),
    }
    # A Saturday New Year's Day is not made up on the Friday: that Friday is the last
    # session of the previous year.
    new_year = date(year, 1, 1)
    if new_year.weekday() != 5:
        days.add(_observed(new_year))
    if year >= 2022:
        days.add(_observed(date(year, 6, 19)))  # Juneteenth
    return frozenset(days)


def is_trading_day(day: date) -> bool:
    return day.weekday() < 5 and day not in nyse_holidays(day.year)


def is_early_close(day: date) -> bool:
    """13:00 ET closes: July 3rd, the day after Thanksgiving, and Christmas Eve."""
    if not is_trading_day(day):
        return False
    thanksgiving = _nth_weekday(day.year, 11, 3, 4)
    # July 3rd and December 24th only close early on Monday-Thursday; on a Friday the
    # holiday itself is observed that day instead.
    return (
        day == thanksgiving + timedelta(days=1)
        or (day in (date(day.year, 7, 3), date(day.year, 12, 24)) and day.weekday() < 4)
    )


def session_close(day: date) -> time:
    return EARLY_CLOSE if is_early_close(day) else REGULAR_END


def is_regular_market_hours(now: Optional[datetime] = None) -> bool:
    local = _to_eastern(now)
    if not is_trading_day(local.date()):
        return False
    return REGULAR_START <= local.time() <= session_close(local.date())


def current_market_phase(now: Optional[datetime] = None) -> str:
    """Returns PRE_MARKET, REGULAR, AFTER_HOURS, or CLOSED."""
    local = _to_eastern(now)
    if not is_trading_day(local.date()):
        return "CLOSED"
    t = local.time()
    if PRE_MARKET_START <= t < REGULAR_START:
        return "PRE_MARKET"
    if REGULAR_START <= t <= session_close(local.date()):
        return "REGULAR"
    if REGULAR_START < t <= AFTER_HOURS_END:
        return "AFTER_HOURS"
    return "CLOSED"


def _scan_window(day: date, lane: str) -> Tuple[datetime, datetime]:
    start, minutes_after_close = _SCAN_WINDOWS[lane]
    end = datetime.combine(day, session_close(day), tzinfo=US_EASTERN)
    return (
        datetime.combine(day, start, tzinfo=US_EASTERN),
        end + timedelta(minutes=minutes_after_close),
    )


def in_scan_window(now: Optional[datetime], lane: str) -> bool:
    """Whether ``lane`` has fresh data to scan at ``now``."""
    local = _to_eastern(now)
    if not is_trading_day(local.date()):
        return False
    start, end = _scan_window(local.date(), lane)
    return start <= local <= end


def next_scan_window_start(now: Optional[datetime], lane: str) -> datetime:
    """When ``lane``'s next scan window opens, strictly after ``now``, in Eastern time."""
    local = _to_eastern(now)
    day = local.date()
    while True:
        if is_trading_day(day):
            start, _ = _scan_window(day, lane)
            if start > local:
                return start
        day += timedelta(days=1)


def market_open_today_utc(now: Optional[datetime] = None) -> Optional[datetime]:
    """Today's 09:30 ET as a UTC datetime, or ``None`` on weekends."""
    local = _to_eastern(now)
    if local.weekday() >= 5:
        return None
    open_local = local.replace(hour=9, minute=30, second=0, microsecond=0)
    return open_local.astimezone(timezone.utc)


def opening_range_end_utc(now: Optional[datetime] = None) -> Optional[datetime]:
    """End of the 09:30-10:00 ET opening range, as UTC."""
    open_utc = market_open_today_utc(now)
    return open_utc + timedelta(minutes=30) if open_utc else None


def minutes_since_open(now: Optional[datetime] = None) -> Optional[float]:
    """Minutes since today's 09:30 ET open; ``None`` on weekends or before the open."""
    open_utc = market_open_today_utc(now)
    if open_utc is None:
        return None
    local = _to_eastern(now)
    delta = (local.astimezone(timezone.utc) - open_utc).total_seconds() / 60
    return delta if delta >= 0 else None


def minutes_to_close(now: Optional[datetime] = None) -> Optional[float]:
    """Minutes until today's close (13:00 ET on early-close days); ``None`` on closed days or after it."""
    local = _to_eastern(now)
    if not is_trading_day(local.date()):
        return None
    close = session_close(local.date())
    close_local = local.replace(hour=close.hour, minute=close.minute, second=0, microsecond=0)
    delta = (close_local - local).total_seconds() / 60
    return delta if delta >= 0 else None


def phase_badge_color(phase: str) -> str:
    return {
        "REGULAR": "🟢",
        "PRE_MARKET": "🟡",
        "AFTER_HOURS": "🟠",
        "CLOSED": "⚫",
    }.get(phase, "⚫")
