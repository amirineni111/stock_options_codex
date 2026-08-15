"""
US equity market phases in America/New_York.

Every function takes an injectable ``now`` so tests never have to reach for the real
clock, and every one returns ``None`` rather than a guess when the answer is
undefined (weekends, before the open).

No holiday calendar in v1. On a market holiday the phase reads REGULAR while no new
bars arrive; the UI surfaces that through the "last bar N min ago" caption rather than
this module pretending to know the NYSE calendar. Early closes (13:00 ET) are
likewise not modelled.
"""
from __future__ import annotations

from datetime import datetime, time, timedelta, timezone
from typing import Optional
from zoneinfo import ZoneInfo

US_EASTERN = ZoneInfo("America/New_York")

PRE_MARKET_START = time(4, 0)
REGULAR_START = time(9, 30)
REGULAR_END = time(16, 0)
AFTER_HOURS_END = time(20, 0)


def _to_eastern(now: Optional[datetime] = None) -> datetime:
    current = now or datetime.now(tz=US_EASTERN)
    if current.tzinfo is None:
        current = current.replace(tzinfo=US_EASTERN)
    return current.astimezone(US_EASTERN)


def is_regular_market_hours(now: Optional[datetime] = None) -> bool:
    local = _to_eastern(now)
    if local.weekday() >= 5:
        return False
    return REGULAR_START <= local.time() <= REGULAR_END


def current_market_phase(now: Optional[datetime] = None) -> str:
    """Returns PRE_MARKET, REGULAR, AFTER_HOURS, or CLOSED."""
    local = _to_eastern(now)
    if local.weekday() >= 5:
        return "CLOSED"
    t = local.time()
    if PRE_MARKET_START <= t < REGULAR_START:
        return "PRE_MARKET"
    if REGULAR_START <= t <= REGULAR_END:
        return "REGULAR"
    if REGULAR_END < t <= AFTER_HOURS_END:
        return "AFTER_HOURS"
    return "CLOSED"


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
    """Minutes until today's 16:00 ET close; ``None`` on weekends or after it."""
    local = _to_eastern(now)
    if local.weekday() >= 5:
        return None
    close_local = local.replace(hour=16, minute=0, second=0, microsecond=0)
    delta = (close_local - local).total_seconds() / 60
    return delta if delta >= 0 else None


def phase_badge_color(phase: str) -> str:
    return {
        "REGULAR": "🟢",
        "PRE_MARKET": "🟡",
        "AFTER_HOURS": "🟠",
        "CLOSED": "⚫",
    }.get(phase, "⚫")
