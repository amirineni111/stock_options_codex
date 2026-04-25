from datetime import datetime, time
from zoneinfo import ZoneInfo


US_EASTERN = ZoneInfo("America/New_York")


def is_regular_market_hours(now: datetime = None) -> bool:
    current = now or datetime.now(tz=US_EASTERN)
    if current.tzinfo is None:
        current = current.replace(tzinfo=US_EASTERN)
    local = current.astimezone(US_EASTERN)
    if local.weekday() >= 5:
        return False
    return time(9, 30) <= local.time() <= time(16, 0)
