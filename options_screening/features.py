"""
The feature contract — the one place serving and training agree on what a setup is.

There is exactly one builder per lane, and both the live scanner and the training
pipeline call it. That is the entire anti-skew mechanism: if serving built its vector
one way and training rebuilt it another, the model would be scored on inputs it never
saw, and nothing would fail loudly enough to notice.

**Editing FEATURE_NAMES means bumping FEATURE_VERSION.** The version travels with
every persisted model and every logged training row, and the scanner refuses to serve
a model whose version does not match — so a changed contract fails closed to
rules-only rather than quietly serving misaligned probabilities.

Features are **direction-relative**: a long and a short with mirror-image setups
produce the same vector, so the model learns "does this pattern work" once instead of
learning long and short as two unrelated regimes on half the data each. ``direction``
is itself a feature, so genuine long/short asymmetry can still be expressed.

Every value is finite. Missing inputs collapse to a neutral default rather than
raising, so a partially populated snapshot still scores.
"""
from __future__ import annotations

import math
from typing import Any, Dict, List, Optional

# ── Intraday equity lane ─────────────────────────────────────────────────────

INTRADAY_FEATURE_NAMES = (
    "direction",
    "rsi_dir",
    "ema_gap_atr_dir",
    "macd_hist_atr_dir",
    "adx14",
    "range_pos_dir",
    "extension_atr_dir",
    "mtf_score",
    "sr_score",
    "rel_volume",
    "log_dollar_volume",
    "cost_ratio",
    "stop_pct",
    "session_progress",
    "total_score",
)
INTRADAY_FEATURE_VERSION = 1

# ── Options lane ─────────────────────────────────────────────────────────────
# The underlying features above are not reused wholesale: the options scan works off
# daily bars and a trend read, not an intraday tape, so it carries what it actually
# measures plus the contract-specific terms that decide whether a correct directional
# call still makes money.

OPTIONS_FEATURE_NAMES = (
    "direction",
    "trend_aligned",
    "abs_delta",
    "dte",
    "iv",
    "iv_rank",
    "spread_pct",
    "log_open_interest",
    "log_volume",
    # The two that decide whether being right on direction is enough.
    "theta_per_premium",
    "gamma_leverage",
    "breakeven_over_expected_move",
    "moneyness",
    "score",
)
OPTIONS_FEATURE_VERSION = 1


def _f(value: Any, default: float = 0.0) -> float:
    """
    Coerce to a finite float, falling back to a neutral default.

    None, NaN and the infinities all reach here from JSON, from pandas, and from
    division in the scorers. Any of them entering a feature vector poisons the whole
    row silently — a NaN propagates through the dot product and the model returns NaN
    for a setup that looked fine.
    """
    if value is None:
        return default
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    if math.isnan(result) or math.isinf(result):
        return default
    return result


def _clip(value: float, low: float, high: float) -> float:
    """
    Bound a feature.

    ATR-normalised ratios blow up when ATR is near zero on an untraded name, and one
    row with a value of 4,000 dominates the standardisation for every other row in
    the training set.
    """
    return max(low, min(value, high))


def _log1p_scaled(value: Any, scale: float = 1.0) -> float:
    """log1p of a non-negative quantity, for the heavily skewed size features."""
    raw = max(0.0, _f(value))
    return round(math.log1p(raw / scale), 6)


def build_intraday_features(result: Any) -> Dict[str, float]:
    """
    The feature vector for one intraday result.

    Takes the finished ``IntradayResult`` rather than the raw indicators, because that
    is the object both the scanner and the stored tracking row have — building from
    anything else reintroduces the possibility of the two disagreeing.
    """
    direction = 1.0 if getattr(result, "dominant", None) == "LONG" else -1.0
    atr = _f(getattr(result, "atr14", None), 0.0)

    # Direction-relative: RSI is mirrored around 50 for a short, so an oversold long
    # and an overbought short present as the same number.
    rsi = _f(getattr(result, "rsi14", None), 50.0)
    rsi_dir = ((rsi - 50.0) / 50.0) * direction

    ema9 = _f(getattr(result, "ema9", None))
    ema20 = _f(getattr(result, "ema20", None))
    ema_gap_atr = ((ema9 - ema20) / atr) if atr > 0 else 0.0

    macd_hist = _f(getattr(result, "macd_histogram", None))
    macd_hist_atr = (macd_hist / atr) if atr > 0 else 0.0

    last = _f(getattr(result, "last_price", None))
    high = _f(getattr(result, "high", None))
    low = _f(getattr(result, "low", None))
    span = high - low
    range_pos = ((last - low) / span) if span > 0 else 0.5
    # Mirrored so "near the favourable end of the day's range" is one number for both
    # directions.
    range_pos_dir = (range_pos - 0.5) * 2.0 * direction

    return {
        "direction": direction,
        "rsi_dir": round(_clip(rsi_dir, -1.0, 1.0), 6),
        "ema_gap_atr_dir": round(_clip(ema_gap_atr * direction, -10.0, 10.0), 6),
        "macd_hist_atr_dir": round(_clip(macd_hist_atr * direction, -10.0, 10.0), 6),
        "adx14": round(_clip(_f(getattr(result, "adx14", None), 20.0), 0.0, 100.0), 6),
        "range_pos_dir": round(_clip(range_pos_dir, -1.0, 1.0), 6),
        "extension_atr_dir": round(
            _clip(_f(getattr(result, "extension_atr", None)) * direction, -10.0, 10.0), 6
        ),
        "mtf_score": round(_clip(_f(getattr(result, "mtf_score", None)), 0.0, 30.0), 6),
        "sr_score": round(_clip(_f(getattr(result, "sr_score", None)), -25.0, 25.0), 6),
        "rel_volume": round(_clip(_f(getattr(result, "relative_volume", None), 1.0), 0.0, 20.0), 6),
        # Dollar volume spans six orders of magnitude across a watchlist; raw values
        # would let one mega-cap dominate the standardisation.
        "log_dollar_volume": _log1p_scaled(getattr(result, "avg_dollar_volume", None), 1e6),
        "cost_ratio": round(_clip(_f(getattr(result, "cost_ratio", None)), 0.0, 1.0), 6),
        # The provisional stop is used when the signal was not actionable, so this is
        # never a zero standing in for "no bracket".
        "stop_pct": round(_clip(_f(getattr(result, "stop_pct", None)), 0.0, 50.0), 6),
        "session_progress": round(_clip(_session_progress(result), 0.0, 1.0), 6),
        "total_score": round(_clip(_f(getattr(result, "total_score", None)), -50.0, 150.0), 6),
    }


def _session_progress(result: Any) -> float:
    from .timeutil import session_progress

    value = session_progress(getattr(result, "bar_timestamp", None))
    return 0.5 if value is None else value


def build_options_features(scored: Any) -> Dict[str, float]:
    """The feature vector for one scored option contract."""
    contract = getattr(scored, "contract", None)
    if contract is None:
        return {name: 0.0 for name in OPTIONS_FEATURE_NAMES}

    direction = 1.0 if contract.contract_type == "call" else -1.0
    underlying = _f(getattr(scored, "underlying_last_price", None)) or _f(
        getattr(contract, "underlying_price", None)
    )
    strike = _f(contract.strike_price)

    # Moneyness, signed so it means the same thing for a call and a put: positive is
    # in the money either way.
    moneyness = ((underlying - strike) / underlying * direction) if underlying > 0 else 0.0

    breakeven_ratio = 0.0
    move = _f(getattr(scored, "expected_move_pct", None))
    distance = _f(getattr(scored, "breakeven_distance_pct", None))
    if move > 0:
        # Above 1.0 the underlying has to beat its own expected move just to reach
        # breakeven — the single most useful "is this priced sanely" number here.
        breakeven_ratio = distance / move

    return {
        "direction": direction,
        "trend_aligned": _f(getattr(scored, "trend_aligned", None), 0.0),
        "abs_delta": round(_clip(abs(_f(contract.delta)), 0.0, 1.0), 6),
        "dte": round(_clip(_f(getattr(scored, "days_to_expiration", None), 30.0), 0.0, 400.0), 6),
        "iv": round(_clip(_f(contract.implied_volatility), 0.0, 5.0), 6),
        # Neutral 0.5 when there is not enough history to rank against.
        "iv_rank": round(_clip(_f(getattr(scored, "iv_rank", None), 0.5), 0.0, 1.0), 6),
        "spread_pct": round(_clip(_f(contract.spread_pct, 25.0), 0.0, 100.0), 6),
        "log_open_interest": _log1p_scaled(contract.open_interest, 100.0),
        "log_volume": _log1p_scaled(contract.volume, 100.0),
        "theta_per_premium": round(
            _clip(_f(getattr(scored, "theta_per_premium", None)), 0.0, 1.0), 6
        ),
        "gamma_leverage": round(_clip(_f(getattr(scored, "gamma_leverage", None)), 0.0, 10.0), 6),
        "breakeven_over_expected_move": round(_clip(breakeven_ratio, 0.0, 10.0), 6),
        "moneyness": round(_clip(moneyness, -1.0, 1.0), 6),
        "score": round(_clip(_f(getattr(scored, "score", None)), 0.0, 100.0), 6),
    }


def to_vector(features: Dict[str, float], names: Any) -> List[float]:
    """
    Ordered vector from a feature dict, missing keys defaulting to neutral.

    Order comes from ``names``, never from dict iteration: a model's coefficients are
    positional, so a reordered vector silently scores every feature against the wrong
    weight.
    """
    return [_f(features.get(name)) for name in names]
