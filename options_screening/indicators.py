"""
Pure-Python OHLC math shared by the options and intraday paths.

Everything here operates on plain ``List[dict]`` bars — ``{"timestamp", "open",
"high", "low", "close", "volume"}`` with ``timestamp`` a UTC ISO-8601 string — and
returns ``Optional[float]``. Nothing raises on short input; a caller that hands over
nine bars and asks for a 14-period ATR gets ``None`` and is expected to check.

Two reasons it is stdlib-only rather than pandas or a TA package:

1. The scoring path stays trivially testable offline. A test builds twelve dicts and
   asserts a number, with no frame construction and no network.
2. These values end up in a model's feature vector. A hand-written Wilder RSI is
   auditable line by line; a library's is a version-pinned black box that can change
   its smoothing under a minor bump and silently invalidate every trained model.

This module previously lived as four private functions inside ``intraday.py``, where
``scoring.py`` could not reach them and nothing cross-checked them against the
sibling repos' implementations.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from .timeutil import parse_ts


# ── Moving averages ──────────────────────────────────────────────────────────


def _ema(values: List[float], period: int) -> Optional[float]:
    series = _ema_series(values, period)
    return series[-1] if series and series[-1] is not None else None


def _ema_series(values: List[float], period: int) -> List[Optional[float]]:
    """
    EMA at every index, ``None`` until the seed SMA has enough values.

    The seed is a simple average of the first ``period`` values rather than the first
    value alone; seeding on one point makes the early series depend heavily on where
    the window happens to start.
    """
    if period <= 0 or len(values) < period:
        return []
    series: List[Optional[float]] = [None] * len(values)
    ema = sum(values[:period]) / period
    series[period - 1] = ema
    multiplier = 2.0 / (period + 1.0)
    for index in range(period, len(values)):
        ema = (values[index] - ema) * multiplier + ema
        series[index] = ema
    return series


def calculate_ema(closes: List[float], period: int) -> Optional[float]:
    return _ema(closes, period)


# ── Oscillators ──────────────────────────────────────────────────────────────


def calculate_rsi(closes: List[float], period: int = 14) -> Optional[float]:
    """
    Wilder's RSI, rounded to 4dp.

    A perfectly flat series returns 50.0, not 100.0: zero average loss with zero
    average gain is "no information", and reporting it as maximally overbought would
    make every halted or untraded name look like a short setup.
    """
    if period <= 0 or len(closes) <= period:
        return None
    changes = [current - previous for previous, current in zip(closes[:-1], closes[1:])]
    average_gain = sum(max(change, 0.0) for change in changes[:period]) / period
    average_loss = sum(max(-change, 0.0) for change in changes[:period]) / period
    for change in changes[period:]:
        average_gain = ((average_gain * (period - 1)) + max(change, 0.0)) / period
        average_loss = ((average_loss * (period - 1)) + max(-change, 0.0)) / period
    if average_loss == 0:
        return 100.0 if average_gain > 0 else 50.0
    rs = average_gain / average_loss
    return round(100.0 - (100.0 / (1.0 + rs)), 4)


def calculate_macd(
    closes: List[float],
    fast_period: int = 12,
    slow_period: int = 26,
    signal_period: int = 9,
) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """Returns (macd, signal, histogram), all ``None`` if any leg is unavailable."""
    fast = _ema_series(closes, fast_period)
    slow = _ema_series(closes, slow_period)
    if not fast or not slow:
        return None, None, None

    macd_values = [
        fast_value - slow_value
        for fast_value, slow_value in zip(fast, slow)
        if fast_value is not None and slow_value is not None
    ]
    if len(macd_values) < signal_period:
        return None, None, None

    signal = _ema(macd_values, signal_period)
    if signal is None:
        return None, None, None
    macd = macd_values[-1]
    return round(macd, 6), round(signal, 6), round(macd - signal, 6)


def calculate_bollinger_bands(
    closes: List[float],
    period: int = 20,
    std_mult: float = 2.0,
) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    """Returns (upper, middle, lower, width_pct)."""
    if period <= 0 or len(closes) < period:
        return None, None, None, None
    window = closes[-period:]
    middle = sum(window) / period
    variance = sum((value - middle) ** 2 for value in window) / period
    std = variance ** 0.5
    upper = middle + std_mult * std
    lower = middle - std_mult * std
    width_pct = ((upper - lower) / middle * 100) if middle else None
    return (
        round(upper, 6),
        round(middle, 6),
        round(lower, 6),
        round(width_pct, 4) if width_pct is not None else None,
    )


# ── Volatility and trend strength ────────────────────────────────────────────


def calculate_atr(
    highs: List[float],
    lows: List[float],
    closes: List[float],
    period: int = 14,
) -> Optional[float]:
    """Wilder's ATR. This is the unit every stop distance in the repo is quoted in."""
    if period <= 0 or len(closes) < period + 1:
        return None
    if len(highs) != len(closes) or len(lows) != len(closes):
        return None
    true_ranges = [
        max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i] - closes[i - 1]),
        )
        for i in range(1, len(closes))
    ]
    if len(true_ranges) < period:
        return None
    atr = sum(true_ranges[:period]) / period
    for true_range in true_ranges[period:]:
        atr = (atr * (period - 1) + true_range) / period
    return round(atr, 6)


def calculate_adx(
    highs: List[float],
    lows: List[float],
    closes: List[float],
    period: int = 14,
) -> Optional[float]:
    """
    Wilder's ADX (trend strength, roughly 0-100), or ``None`` on insufficient bars.

    Rule of thumb: above 25 is trending, below 18 is ranging. This is the input to the
    regime gate that decides whether momentum or mean reversion gets the weight —
    without it the two playbooks cancel each other out on every mixed tape.
    """
    n = len(closes)
    if period <= 0 or n < period * 2 + 1:
        return None
    if len(highs) != n or len(lows) != n:
        return None

    plus_dm: List[float] = []
    minus_dm: List[float] = []
    true_ranges: List[float] = []
    for i in range(1, n):
        up = highs[i] - highs[i - 1]
        down = lows[i - 1] - lows[i]
        plus_dm.append(up if (up > down and up > 0) else 0.0)
        minus_dm.append(down if (down > up and down > 0) else 0.0)
        true_ranges.append(
            max(
                highs[i] - lows[i],
                abs(highs[i] - closes[i - 1]),
                abs(lows[i] - closes[i - 1]),
            )
        )
    if len(true_ranges) < period:
        return None

    # Wilder-smoothed running sums, seeded with the first `period` values.
    atr = sum(true_ranges[:period])
    pdm = sum(plus_dm[:period])
    mdm = sum(minus_dm[:period])
    dxs: List[float] = []
    for i in range(period, len(true_ranges)):
        atr = atr - atr / period + true_ranges[i]
        pdm = pdm - pdm / period + plus_dm[i]
        mdm = mdm - mdm / period + minus_dm[i]
        if atr == 0:
            continue
        pdi = 100 * pdm / atr
        mdi = 100 * mdm / atr
        denom = pdi + mdi
        dxs.append(100 * abs(pdi - mdi) / denom if denom else 0.0)

    if not dxs:
        return None
    if len(dxs) < period:
        return round(sum(dxs) / len(dxs), 2)
    adx = sum(dxs[:period]) / period
    for dx in dxs[period:]:
        adx = (adx * (period - 1) + dx) / period
    return round(adx, 2)


# ── Volume ───────────────────────────────────────────────────────────────────


def calculate_vwap(bars: List[Dict[str, Any]]) -> Optional[float]:
    """
    Session VWAP over the supplied bars, using each bar's typical price.

    The caller decides the window — pass only today's bars for a session VWAP. Bars
    with no volume are skipped rather than counted at zero weight, so a feed that
    reports ``None`` volume for a halt does not drag the average.
    """
    total_price_volume = 0.0
    total_volume = 0.0
    for bar in bars:
        volume = _first_float(bar.get("volume"))
        if volume is None or volume <= 0:
            continue
        close = _first_float(bar.get("close"))
        if close is None:
            continue
        high = _first_float(bar.get("high"))
        low = _first_float(bar.get("low"))
        typical_price = (high + low + close) / 3.0 if high is not None and low is not None else close
        total_price_volume += typical_price * volume
        total_volume += volume
    if total_volume <= 0:
        return None
    return round(total_price_volume / total_volume, 6)


def calculate_relative_volume(bars: List[Dict[str, Any]], lookback: int = 20) -> Optional[float]:
    """
    Last bar's volume as a multiple of the preceding ``lookback`` bars' average.

    Bar-for-bar, so it is comparable at any point in the session. The day-level RVOL
    this replaced divided a partial day's cumulative volume by a *whole* prior
    session's, which made the ratio structurally small all morning and forced the
    default threshold down to 0.05 to compensate.
    """
    if len(bars) < 2:
        return None
    prior = bars[-(lookback + 1):-1]
    volumes = [_first_float(bar.get("volume")) or 0.0 for bar in prior]
    if not volumes:
        return None
    average = sum(volumes) / len(volumes)
    if average <= 0:
        return None
    last_volume = _first_float(bars[-1].get("volume")) or 0.0
    return round(last_volume / average, 4)


def average_dollar_volume(bars: List[Dict[str, Any]], lookback: int = 20) -> Optional[float]:
    """
    Mean close x volume over the last ``lookback`` bars — the liquidity measure the
    transaction-cost tiers key off. Dollar volume rather than share volume, because a
    million shares of a $3 stock and of a $300 stock are not the same market.
    """
    window = bars[-lookback:] if len(bars) > lookback else bars
    values = []
    for bar in window:
        close = _first_float(bar.get("close"))
        volume = _first_float(bar.get("volume"))
        if close is None or volume is None:
            continue
        values.append(close * volume)
    if not values:
        return None
    return round(sum(values) / len(values), 2)


# ── Ranges and structure ─────────────────────────────────────────────────────


def range_high_low(
    bars: List[Dict[str, Any]],
    start_dt: Optional[datetime],
) -> Tuple[Optional[float], Optional[float]]:
    """High/low of every bar at or after ``start_dt`` (tz-aware UTC)."""
    if start_dt is None:
        return None, None
    selected = [bar for bar in bars if _bar_ts_at_or_after(bar, start_dt)]
    if not selected:
        return None, None
    return _safe_max(selected, "high"), _safe_min(selected, "low")


def window_high_low(
    bars: List[Dict[str, Any]],
    start_dt: Optional[datetime],
    end_dt: Optional[datetime],
) -> Tuple[Optional[float], Optional[float]]:
    """High/low of bars in ``[start_dt, end_dt)``. Used for the opening range."""
    if start_dt is None or end_dt is None:
        return None, None
    selected = []
    for bar in bars:
        ts = parse_ts(bar.get("timestamp"))
        if ts is not None and start_dt <= ts < end_dt:
            selected.append(bar)
    if not selected:
        return None, None
    return _safe_max(selected, "high"), _safe_min(selected, "low")


# A vote only counts when the quantity behind it clears this fraction of ATR. Both
# siblings vote on the bare sign, which deadlocks the consensus on a quiet tape: a
# MACD histogram of +0.0007 on a $130 stock cast a full LONG vote and cancelled an
# EMA9/EMA20 separation of 3.85 points, so `compute_trend_direction` returned NEUTRAL
# for an unmistakable downtrend. Under the (correct) confluence rule a NEUTRAL higher
# timeframe is not confirmation, so that one rounding artifact silently cost the setup
# 15-30 points.
_TREND_VOTE_MIN_ATR = 0.05


def compute_trend_direction(bars: List[Dict[str, Any]]) -> str:
    """
    Higher-timeframe direction for 1h/1d bars: 'LONG', 'SHORT' or 'NEUTRAL'.

    Two-vote consensus (EMA9/EMA20 separation, MACD histogram), where each vote must
    clear an ATR-scaled noise floor to be cast — see ``_TREND_VOTE_MIN_ATR``. Scaling
    by ATR rather than by an absolute number keeps the rule identical on a $9 stock
    and a $900 one.

    NEUTRAL is a real answer, not a failure: the confluence scorer requires the higher
    timeframe to be *present and agreeing*, so a NEUTRAL read must never be able to
    masquerade as confirmation.
    """
    if len(bars) < 26:
        return "NEUTRAL"
    closes = [bar["close"] for bar in bars if bar.get("close") is not None]
    if len(closes) < 26:
        return "NEUTRAL"

    highs = [bar["high"] for bar in bars if bar.get("high") is not None]
    lows = [bar["low"] for bar in bars if bar.get("low") is not None]
    atr = None
    if len(highs) == len(closes) and len(lows) == len(closes):
        atr = calculate_atr(highs, lows, closes)
    # With no ATR there is nothing to scale against, so fall back to voting on sign.
    threshold = _TREND_VOTE_MIN_ATR * atr if atr else 0.0

    ema9 = calculate_ema(closes, 9)
    ema20 = calculate_ema(closes, 20)
    _, _, macd_hist = calculate_macd(closes)

    long_votes = 0
    short_votes = 0
    if ema9 is not None and ema20 is not None and abs(ema9 - ema20) > threshold:
        if ema9 > ema20:
            long_votes += 1
        else:
            short_votes += 1
    if macd_hist is not None and abs(macd_hist) > threshold:
        if macd_hist > 0:
            long_votes += 1
        else:
            short_votes += 1

    if long_votes > short_votes:
        return "LONG"
    if short_votes > long_votes:
        return "SHORT"
    return "NEUTRAL"


def detect_sr_levels(
    bars: List[Dict[str, Any]],
    lookback: int = 50,
    n_pivot: int = 3,
) -> List[Dict[str, Any]]:
    """
    Support/resistance by pivot detection, clustered within an ATR-scaled tolerance.

    Returns up to 8 ``{"price", "type", "touches", "strength"}`` dicts sorted by
    strength. The tolerance is ATR-relative rather than a fixed percentage so the same
    code works on a $9 name and a $900 one.
    """
    if len(bars) < n_pivot * 2 + 1:
        return []
    recent = bars[-lookback:] if len(bars) > lookback else bars
    closes = [bar["close"] for bar in recent]
    highs = [bar["high"] for bar in recent]
    lows = [bar["low"] for bar in recent]

    atr = calculate_atr(highs, lows, closes, period=min(14, len(recent) - 1)) or 0.001
    tolerance = atr * 0.5

    pivot_highs: List[float] = []
    pivot_lows: List[float] = []
    n = len(recent)
    for i in range(n_pivot, n - n_pivot):
        left_highs = highs[i - n_pivot:i]
        right_highs = highs[i + 1:i + n_pivot + 1]
        if left_highs and right_highs and highs[i] > max(left_highs) and highs[i] > max(right_highs):
            pivot_highs.append(highs[i])
        left_lows = lows[i - n_pivot:i]
        right_lows = lows[i + 1:i + n_pivot + 1]
        if left_lows and right_lows and lows[i] < min(left_lows) and lows[i] < min(right_lows):
            pivot_lows.append(lows[i])

    def _cluster(prices: List[float], level_type: str) -> List[Dict[str, Any]]:
        if not prices:
            return []
        clusters: List[List[float]] = [[sorted(prices)[0]]]
        for price in sorted(prices)[1:]:
            if price - clusters[-1][-1] <= tolerance:
                clusters[-1].append(price)
            else:
                clusters.append([price])
        result = []
        for cluster in clusters:
            price = sum(cluster) / len(cluster)
            touches = sum(
                1
                for bar in recent
                if (level_type == "R" and abs(bar["high"] - price) <= tolerance * 2)
                or (level_type == "S" and abs(bar["low"] - price) <= tolerance * 2)
            )
            result.append(
                {
                    "price": round(price, 6),
                    "type": level_type,
                    "touches": touches,
                    "strength": float(max(len(cluster), touches)),
                }
            )
        return result

    levels = _cluster(pivot_highs, "R") + _cluster(pivot_lows, "S")
    levels.sort(key=lambda level: level["strength"], reverse=True)
    return levels[:8]


# ── Aggregator ───────────────────────────────────────────────────────────────


def compute_all(bars: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Every indicator from one bar list, ready to merge into a snapshot."""
    if not bars:
        return {}
    closes = [bar["close"] for bar in bars]
    highs = [bar["high"] for bar in bars]
    lows = [bar["low"] for bar in bars]

    macd, macd_signal, macd_histogram = calculate_macd(closes)
    bb_upper, bb_middle, bb_lower, bb_width = calculate_bollinger_bands(closes)

    last = bars[-1]
    return {
        "open": last.get("open"),
        "high": last.get("high"),
        "low": last.get("low"),
        "close": last.get("close"),
        "rsi14": calculate_rsi(closes),
        "ema9": calculate_ema(closes, 9),
        "ema20": calculate_ema(closes, 20),
        "ema50": calculate_ema(closes, 50),
        "macd": macd,
        "macd_signal": macd_signal,
        "macd_histogram": macd_histogram,
        "atr14": calculate_atr(highs, lows, closes),
        "adx14": calculate_adx(highs, lows, closes),
        "bb_upper": bb_upper,
        "bb_middle": bb_middle,
        "bb_lower": bb_lower,
        "bb_width_pct": bb_width,
        "rel_volume": calculate_relative_volume(bars),
        "avg_dollar_volume": average_dollar_volume(bars),
    }


# ── Coercion helpers ─────────────────────────────────────────────────────────
# Defined here rather than duplicated in `polygon.py` and `intraday.py`, where two
# byte-identical copies had drifted apart from each other only by luck.


def _first_float(*values: Any) -> Optional[float]:
    """First value that coerces to a finite float, else ``None``."""
    for value in values:
        if value is None:
            continue
        try:
            result = float(value)
        except (TypeError, ValueError):
            continue
        # NaN and infinities reach here from JSON feeds and pandas alike; they poison
        # every downstream comparison silently, so they are treated as missing.
        if result != result or result in (float("inf"), float("-inf")):
            continue
        return result
    return None


def _first_int(*values: Any) -> Optional[int]:
    """First value that coerces to an int, else ``None``."""
    for value in values:
        if value is None:
            continue
        try:
            return int(value)
        except (TypeError, ValueError):
            continue
    return None


def _bar_ts_at_or_after(bar: Dict[str, Any], start_dt: datetime) -> bool:
    ts = parse_ts(bar.get("timestamp"))
    return ts is not None and ts >= start_dt


def _safe_max(bars: List[Dict[str, Any]], key: str) -> Optional[float]:
    values = [_first_float(bar.get(key)) for bar in bars]
    present = [value for value in values if value is not None]
    return max(present) if present else None


def _safe_min(bars: List[Dict[str, Any]], key: str) -> Optional[float]:
    values = [_first_float(bar.get(key)) for bar in bars]
    present = [value for value in values if value is not None]
    return min(present) if present else None
