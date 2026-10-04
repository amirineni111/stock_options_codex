"""
The underlying's direction, read by the full multi-factor engine on daily bars.

The options lane originally chose calls or puts from one rule: price above SMA20 above
SMA50. Its contract score measures contract *quality* (liquidity, delta, DTE, IV) and
says nothing about direction, so that one moving-average stack was the entire
directional call. The intraday lane in the same repo already has a far richer engine —
regime-weighted momentum and mean reversion, breakout, higher-timeframe confluence,
support/resistance — and this module runs that same ``score_ticker`` on daily bars,
for a swing horizon that matches a 3-to-11-week option.

It does **not** replace the moving-average rule. Both label every contract, both are
forward-tested, and the scorecard's "Predicted signal" breakdown says which one is
right more often. Swapping one for the other before either has a measured record
would be choosing on opinion.

What changes from the intraday use of the engine, and why:

* **Breakout levels** are the prior 20 sessions' high and low — the daily-chart
  equivalent of the day's range and opening range, which have no daily meaning.
* **Higher timeframes** are the weekly trend (resampled from the same daily bars)
  in the "hourly" slot, whose opposition vetoes a setup, and the SMA20/50 stack in the
  "daily" slot. Both must be present and agree for full confluence, as intraday.
* **Support/resistance** comes from daily pivots, which is the scale a multi-week
  option hold actually has to clear.
* **No session phase**, so the intraday closing-hour bonus never applies.

Completed daily bars only: the read does not repaint during the session.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from . import indicators as ind
from .signals import score_ticker
from .timeutil import parse_ts

# Enough daily bars to seed MACD(26,9), ADX(14) and a 20-session breakout range.
MIN_DAILY_BARS = 40
_BREAKOUT_LOOKBACK = 20

# Engine labels that count as a directional call for options.
_LONG_SIGNALS = frozenset({"STRONG_BUY", "BUY_CANDIDATE"})
_SHORT_SIGNALS = frozenset({"STRONG_SHORT", "SHORT_CANDIDATE"})


def daily_direction_read(
    ticker: str,
    daily_bars: List[Dict[str, Any]],
    sma_trend: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """
    ``{"direction", "signal", "score", "reason"}`` for one underlying, or ``None`` when
    there is not enough history to read it.

    ``direction`` is LONG / SHORT only when the engine's signal is actionable in that
    direction, and NEUTRAL otherwise — a WATCH_ONLY read is not a call.
    """
    bars = [b for b in daily_bars or [] if _complete(b)]
    if len(bars) < MIN_DAILY_BARS:
        return None

    values = ind.compute_all(bars)
    prior = bars[-(_BREAKOUT_LOOKBACK + 1):-1]
    values.update({
        "day_high": max(b["high"] for b in prior),
        "day_low": min(b["low"] for b in prior),
        "or_high": None,
        "or_low": None,
    })

    weekly = ind.compute_trend_direction(weekly_bars(bars))
    sma_direction = {"bullish": "LONG", "bearish": "SHORT"}.get(sma_trend or "")

    scored = score_ticker(
        ticker=ticker,
        last=values.get("close"),
        avg_dollar_volume=values.get("avg_dollar_volume"),
        indicators=values,
        hourly_direction=weekly,
        daily_direction=sma_direction,
        sr_levels=ind.detect_sr_levels(bars),
    )
    signal = scored.get("trade_signal")
    if signal in _LONG_SIGNALS:
        direction = "LONG"
    elif signal in _SHORT_SIGNALS:
        direction = "SHORT"
    else:
        direction = "NEUTRAL"
    return {
        "direction": direction,
        "signal": signal,
        "score": round(float(scored.get("total_score") or 0.0), 1),
        "reason": scored.get("signal_reason") or "",
    }


def weekly_bars(daily_bars: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Daily bars folded into ISO weeks: first open, max high, min low, last close."""
    weeks: Dict[tuple, Dict[str, Any]] = {}
    for bar in daily_bars:
        stamp = parse_ts(bar.get("timestamp"))
        if stamp is None:
            continue
        key = stamp.isocalendar()[:2]
        week = weeks.get(key)
        if week is None:
            weeks[key] = dict(bar)
            continue
        week["high"] = max(week["high"], bar["high"])
        week["low"] = min(week["low"], bar["low"])
        week["close"] = bar["close"]
        week["volume"] = (week.get("volume") or 0) + (bar.get("volume") or 0)
    return [weeks[key] for key in sorted(weeks)]


def _complete(bar: Dict[str, Any]) -> bool:
    return all(bar.get(k) is not None for k in ("open", "high", "low", "close"))

