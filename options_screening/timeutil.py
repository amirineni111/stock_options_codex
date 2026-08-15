"""
One timestamp parser and one intraday session clock, shared by every layer.

Timestamps arrive here in three shapes and all three land in the same SQLite columns:
Polygon's epoch milliseconds, Yahoo's epoch seconds, and SQLite's own bare
``CURRENT_TIMESTAMP`` ("2026-07-28 13:45:02", no zone). Before this module the repo
had a different ad-hoc parse at each call site and used naive ``datetime.utcnow()``
throughout, so tz-aware and tz-naive values sat side by side in one column. In the
forex sibling that exact mix is what made trade durations unmeasurable for months —
the comparison silently produced ``None`` rather than raising.

Everything below returns tz-aware UTC or ``None``. Nothing returns a naive datetime.

Time of day matters more for equities and their options than it does for FX: 09:30 and
16:00 ET are hard boundaries with completely different volatility regimes on either
side, so the model is handed *session progress* rather than a wall-clock hour.
"""
from __future__ import annotations

from datetime import datetime, time, timezone
from typing import Optional, Union
from zoneinfo import ZoneInfo

US_EASTERN = ZoneInfo("America/New_York")

REGULAR_OPEN = time(9, 30)
REGULAR_CLOSE = time(16, 0)
# 09:30 -> 16:00 ET
SESSION_MINUTES = 390.0


def utc_now() -> datetime:
    """
    Current time as a tz-aware UTC datetime.

    Use this in place of ``datetime.utcnow()``, which is deprecated and — worse —
    returns a naive datetime that every downstream comparison then has to guess the
    zone of.
    """
    return datetime.now(tz=timezone.utc)


def parse_ts(value: Union[str, int, float, datetime, None]) -> Optional[datetime]:
    """
    Parse an ISO-8601 string, a SQLite ``CURRENT_TIMESTAMP`` string, an epoch number,
    or a datetime into a tz-aware UTC datetime. Returns ``None`` when unparseable.

    Naive inputs are assumed UTC, which is correct for every source here. Fractional
    seconds longer than 6 digits (which ``fromisoformat`` rejects on 3.9) are
    truncated rather than failing the whole parse.
    """
    if value is None or value == "":
        return None

    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)

    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return _from_epoch(float(value))

    text = str(value).strip()
    if not text:
        return None

    # A bare number arriving as a string is still an epoch.
    if text.lstrip("-").replace(".", "", 1).isdigit():
        try:
            return _from_epoch(float(text))
        except (OverflowError, ValueError, OSError):
            return None

    try:
        if "." in text:
            head, frac = text.split(".", 1)
            # Leading digits are the fraction; whatever follows is the zone suffix.
            i = 0
            while i < len(frac) and frac[i].isdigit():
                i += 1
            digits = frac[:i][:6].ljust(6, "0")
            rest = frac[i:]
            tz = "+00:00" if rest in ("", "Z", "z") else rest
            text = f"{head}.{digits}{tz}"
        elif text.endswith(("Z", "z")):
            text = text[:-1] + "+00:00"
        parsed = datetime.fromisoformat(text)
    except (ValueError, TypeError):
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _from_epoch(value: float) -> Optional[datetime]:
    """
    Epoch seconds or milliseconds to UTC. Polygon returns milliseconds and Yahoo
    returns seconds, so the magnitude decides: anything past ~1e11 is milliseconds
    (1e11 seconds is the year 5138, and 1e11 ms is 1973 — no real quote is either).
    """
    try:
        seconds = value / 1000.0 if abs(value) >= 1e11 else value
        return datetime.fromtimestamp(seconds, tz=timezone.utc)
    except (OverflowError, ValueError, OSError):
        return None


def to_iso(value: Union[str, int, float, datetime, None]) -> Optional[str]:
    """
    Canonical storage form: a UTC ISO-8601 string.

    One uniform format across every writer is what makes lexicographic string
    ordering equal chronological ordering, which the forward-test resolver relies on
    when it selects "bars after entry" without parsing every row.
    """
    parsed = parse_ts(value)
    return parsed.isoformat() if parsed else None


def to_eastern(value: Union[str, int, float, datetime, None]) -> Optional[datetime]:
    """The same instant in America/New_York, for display and session arithmetic."""
    parsed = parse_ts(value)
    return parsed.astimezone(US_EASTERN) if parsed else None


def exchange_date(value: Union[str, int, float, datetime, None] = None):
    """
    The *exchange* calendar date for an instant, defaulting to now.

    Days to expiration must be counted from this, not from ``date.today()``. A scan
    run at 21:00 ET is already the next UTC day, so the local-date version was
    off by one every evening — and it disagreed with the DTE that storage computed
    from a UTC timestamp, so the same contract had two different DTEs in one row.
    """
    eastern = to_eastern(value if value is not None else utc_now())
    return eastern.date() if eastern else None


def minutes_since_open_at(value: Union[str, int, float, datetime, None]) -> Optional[float]:
    """
    Minutes from that day's 09:30 ET open to ``value``. Negative pre-market, above 390
    after the close.

    Keyed to the timestamp's own date, never to "now" — a feature has to describe the
    bar it was built from, or every backfill and retrain silently shifts.
    """
    eastern = to_eastern(value)
    if eastern is None:
        return None
    open_local = eastern.replace(hour=9, minute=30, second=0, microsecond=0)
    return (eastern - open_local).total_seconds() / 60.0


def session_progress(value: Union[str, int, float, datetime, None]) -> Optional[float]:
    """
    Position in the regular session, 0.0 at the open to 1.0 at the close, clipped.

    Clipped rather than extrapolated so pre-market and after-hours bars sit at the
    endpoints instead of handing the model large out-of-range values it has almost no
    examples of.
    """
    minutes = minutes_since_open_at(value)
    if minutes is None:
        return None
    return max(0.0, min(1.0, minutes / SESSION_MINUTES))


def is_regular_at(value: Union[str, int, float, datetime, None]) -> Optional[bool]:
    """True when the timestamp falls inside 09:30-16:00 ET on a weekday."""
    eastern = to_eastern(value)
    if eastern is None:
        return None
    if eastern.weekday() >= 5:
        return False
    return REGULAR_OPEN <= eastern.time() <= REGULAR_CLOSE


def minutes_between(
    start: Union[str, int, float, datetime, None],
    end: Union[str, int, float, datetime, None],
) -> Optional[float]:
    """
    Elapsed minutes between two timestamps, or ``None`` if either is unparseable.

    Used for trade hold duration. Measuring it as ``now - created_at`` instead —
    which is what the forex sibling did — reports the gap between *scans*, not the
    life of the trade, and made wins and losses both average the same ~506 minutes.
    """
    start_dt = parse_ts(start)
    end_dt = parse_ts(end)
    if start_dt is None or end_dt is None:
        return None
    return (end_dt - start_dt).total_seconds() / 60.0
