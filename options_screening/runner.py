"""
The headless alert runner's logic: when to wake, what to scan, and with which settings.

The dashboard only scans while a browser tab is open and its auto-refresh timer fires,
and that timer is not aligned to anything — a signal can sit unseen for most of an
interval, and nothing resolves overnight unless the tab stays open. The runner
(``scripts/run_alerts.py``) wakes just after every 15-minute boundary, which is the
cadence both data sources move at here: Polygon's Options Starter plan is 15-minute
delayed, and the intraday lane scores on completed 15-minute bars.

It scans with **the dashboard's own saved settings** (``data/app_preferences.json``),
so what the phone is told about is exactly what the dashboard would show. Kept in the
package rather than the script so the schedule and the settings mapping are testable.
"""
from __future__ import annotations

import json
from datetime import datetime, time, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .intraday import IntradayScanRequest
from .market_hours import US_EASTERN
from .scanner import ScanRequest
from .universe import load_sp100_tickers, load_sp500_tickers, normalize_symbol

PREFERENCES_PATH = Path(__file__).resolve().parent.parent / "data" / "app_preferences.json"
STEP_MINUTES = 15

# Scan windows, Eastern time. The options lane starts and ends 15 minutes late: the
# data is 15-minute delayed, so 09:30's scan would price the pre-open and the session's
# last quarter-hour is only visible at 16:15.
_WINDOWS = {
    "options": (time(9, 45), time(16, 20)),
    "intraday": (time(9, 30), time(16, 0)),
}


def next_wake(now: datetime, lag_seconds: float, step_minutes: int = STEP_MINUTES) -> datetime:
    """The next ``step_minutes`` boundary plus ``lag_seconds``, strictly after ``now``."""
    base = now.replace(second=0, microsecond=0)
    base -= timedelta(minutes=base.minute % step_minutes)
    wake = base + timedelta(seconds=lag_seconds)
    while wake <= now:
        wake += timedelta(minutes=step_minutes)
    return wake


def in_scan_window(now: datetime, lane: str) -> bool:
    """Whether ``lane`` has fresh data to scan at ``now``. No holiday calendar."""
    local = now.astimezone(US_EASTERN)
    if local.weekday() >= 5:
        return False
    start, end = _WINDOWS[lane]
    return start <= local.time() <= end


def load_preferences(path: Path = PREFERENCES_PATH) -> Dict[str, Any]:
    try:
        saved = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return saved if isinstance(saved, dict) else {}


def parse_tickers(value: str) -> List[str]:
    """Comma/newline separated symbols, upper-cased, de-duplicated, order kept."""
    tickers: List[str] = []
    seen = set()
    for item in (value or "").replace("\n", ",").split(","):
        ticker = item.strip().upper()
        if ticker and ticker not in seen:
            seen.add(ticker)
            tickers.append(ticker)
    return tickers


def options_tickers(prefs: Dict[str, Any]) -> Tuple[List[str], str]:
    if prefs.get("ticker_source") == "Custom":
        return parse_tickers(prefs.get("custom_tickers", "")), ""
    tickers, note = load_sp500_tickers()
    return tickers[: int(prefs.get("ticker_limit", 50))], note


def intraday_tickers(prefs: Dict[str, Any]) -> Tuple[List[str], str]:
    if prefs.get("intraday_universe") == "Custom":
        return [normalize_symbol(t) for t in parse_tickers(prefs.get("intraday_custom_tickers", ""))], ""
    return load_sp100_tickers()


def options_request(prefs: Dict[str, Any], tickers: List[str]) -> ScanRequest:
    """The ScanRequest the options page would build from the same saved settings."""
    defaults = ScanRequest(tickers=[])
    dte = _pair(prefs.get("days_to_expiration"), (defaults.min_days_to_expiration, defaults.max_days_to_expiration))
    delta = _pair(prefs.get("absolute_delta_range"), (defaults.min_abs_delta, defaults.max_abs_delta))
    iv = _pair(prefs.get("implied_volatility_range"), (defaults.min_iv, defaults.max_iv))
    use_trend = bool(prefs.get("use_trend_context", defaults.use_trend_context))
    check_earnings = bool(prefs.get("check_earnings", defaults.check_earnings))
    allow_missing = bool(prefs.get("allow_missing_spread", defaults.allow_missing_spread))
    return ScanRequest(
        tickers=tickers,
        fixed_risk=float(prefs.get("fixed_risk", defaults.fixed_risk)),
        min_volume=int(prefs.get("min_volume", defaults.min_volume)),
        min_open_interest=int(prefs.get("min_open_interest", defaults.min_open_interest)),
        max_spread_pct=float(prefs.get("max_spread_pct", defaults.max_spread_pct)),
        min_days_to_expiration=int(dte[0]),
        max_days_to_expiration=int(dte[1]),
        min_abs_delta=float(delta[0]),
        max_abs_delta=float(delta[1]),
        min_iv=float(iv[0]),
        max_iv=float(iv[1]),
        max_contracts_per_ticker=int(prefs.get("max_contracts_per_ticker", defaults.max_contracts_per_ticker)),
        allow_missing_spread=allow_missing,
        use_trend_context=use_trend,
        require_trend_alignment=bool(prefs.get("require_trend_alignment", False)) and use_trend,
        check_earnings=check_earnings,
        avoid_earnings_before_expiration=bool(prefs.get("avoid_earnings_before_expiration", False)) and check_earnings,
        ignore_missing_spread_for_signal=bool(prefs.get("ignore_missing_spread_for_signal", False)) and allow_missing,
    )


def intraday_request(prefs: Dict[str, Any], tickers: List[str]) -> IntradayScanRequest:
    """The IntradayScanRequest the intraday page would build from the same settings."""
    defaults = IntradayScanRequest(tickers=[])
    return IntradayScanRequest(
        tickers=tickers,
        min_price=float(prefs.get("intraday_min_price", defaults.min_price)),
        max_price=float(prefs.get("intraday_max_price", defaults.max_price)),
        min_relative_volume=float(prefs.get("intraday_min_relative_volume", defaults.min_relative_volume)),
        min_avg_dollar_volume=float(
            prefs.get("intraday_min_avg_dollar_volume_m", defaults.min_avg_dollar_volume / 1_000_000)
        ) * 1_000_000,
        include_shorts=bool(prefs.get("intraday_include_shorts", defaults.include_shorts)),
        use_higher_timeframes=bool(prefs.get("intraday_use_higher_timeframes", defaults.use_higher_timeframes)),
        use_relative_strength=bool(prefs.get("intraday_use_relative_strength", defaults.use_relative_strength)),
    )


def alert_url(settings, prefs: Dict[str, Any]) -> str:
    """``.env`` wins; otherwise the URL entered in the dashboard's sidebar."""
    return settings.alert_webhook_url or (prefs.get("alert_webhook") or "").strip()


def _pair(value: Any, default: Tuple[float, float]) -> Tuple[float, float]:
    if isinstance(value, (list, tuple)) and len(value) == 2:
        try:
            low, high = float(value[0]), float(value[1])
            return (low, high) if low <= high else (high, low)
        except (TypeError, ValueError):
            pass
    return default


def eastern_stamp(now: Optional[datetime] = None) -> str:
    return (now or datetime.now(US_EASTERN)).astimezone(US_EASTERN).strftime("%H:%M:%S")
