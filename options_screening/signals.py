"""
The scoring engine for the intraday equity lane.

Structure and most constants are ported from the two sibling repos, whose thresholds
were set by forward-testing rather than by assertion; the comments carry that evidence
so a later "simplification" cannot quietly undo it. Where this repo differs from both
siblings, the reason is stated at the constant.

What this replaced: two independent scores (momentum and mean reversion) computed in
full and then combined with ``max()``. That is wrong in a specific way — they are
opposite playbooks, so on a mixed tape the engine would pick whichever happened to
score higher and present it with full confidence, with nothing expressing that the
tape did not support either. The regime gate below weights them by measured trend
strength instead, so a suppressed playbook contributes nothing rather than competing.

The score proposes; a trained model disposes. ``model_prob`` can only ever downgrade
an actionable signal to WATCH_ONLY — it can never promote a setup the rules rejected.
"""
from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Tuple

# ── Regime ───────────────────────────────────────────────────────────────────
# ADX thresholds: above TREND trust momentum, below RANGE trust mean reversion,
# blend linearly between them so the engine does not flip-flop at the boundary.
_ADX_TREND = 25.0
_ADX_RANGE = 18.0

# ── Trade geometry ───────────────────────────────────────────────────────────
# Reward:risk is fixed, so the target is derived from the *final* stop distance —
# widening the stop widens the target and the bracket stays one number.
_STOP_ATR_MULT = 2.5
_RR = 1.5
# Noise floor: never risk less than 0.50% of entry. A stop tighter than ordinary
# intrabar wiggle gets tagged for reasons that have nothing to do with the thesis.
_MIN_STOP_PCT = 0.005
# Cost floor, taken from the forex sibling and absent from the stocks one. The stop
# must be at least this multiple of the round-trip cost, which bounds cost at 1/8 of
# risk before the veto below is even consulted. It matters more here than in either
# sibling because this lane can see a real quoted spread, and equity spreads on the
# thin names a momentum screen surfaces are nothing like an FX major's.
_SPREAD_STOP_MULT = 8.0

# Over-extension: STRONG fires when momentum, breakout and MTF all agree, which is by
# definition late in a move. Beyond this many ATRs from EMA20, wait for a pullback
# instead of buying the local extreme.
_MAX_EXTENSION_ATR = 2.0

# A target below this fraction of entry is inside the noise after costs; such setups
# are shown but not forward-tested, because their outcome measures the spread rather
# than the signal.
MIN_TARGET_PCT = 0.15

# ── Transaction cost ─────────────────────────────────────────────────────────
# Round-trip cost in basis points of price, tiered on 20-day average dollar volume.
# Used only when no real quote is available; when Polygon gives a bid/ask, the
# observed spread wins (see `estimate_cost_pct`).
_COST_TIERS_BPS = (
    (50_000_000.0, 3.0),
    (10_000_000.0, 8.0),
    (2_000_000.0, 20.0),
)
_COST_BPS_THIN = 50.0

# Hard veto on cost as a fraction of risk. This is a backstop for the case where no
# cost floor applies to the stop (no quote, no estimate); once the floor binds it is
# mathematically unreachable — see `_MAX_STOP_ATR_MULT`.
_MAX_COST_RATIO = 0.15

# The veto that actually bites on an expensive instrument.
#
# The cost floor and the cost-ratio veto interact in a way that is easy to miss: once
# the floor binds, stop_pct == cost_pct * _SPREAD_STOP_MULT, so
# cost_ratio == 1 / _SPREAD_STOP_MULT == 0.125 exactly, for *any* spread however
# wide. A 5% spread and a 0.05% spread both report a 12.5% cost ratio and both clear
# the 15% limit. The forex sibling has the same latent hole.
#
# What a huge spread really does is force the stop absurdly wide relative to how far
# the instrument actually moves: at 5% cost the stop lands 40% out and the 1.5R target
# 60% out, which no intraday trade reaches. So the veto is expressed where the harm
# is — a stop more than this many ATRs wide means cost, not volatility, is setting the
# risk, and the trade is uneconomic regardless of the signal.
_MAX_STOP_ATR_MULT = 6.0

# How far above cost-adjusted breakeven a modelled probability must sit. Trading at
# exactly breakeven donates the spread to the market maker and adds variance for
# nothing, so demand a real cushion.
_PROB_MARGIN = 0.04

# ── Score thresholds ─────────────────────────────────────────────────────────
_STRONG_SCORE = 70.0
_ACTIONABLE_SCORE = 45.0
_WATCH_SCORE = 25.0


# ── Components ───────────────────────────────────────────────────────────────


def _macd_magnitude_pts(macd_histogram: float, atr14: Optional[float], macd: Optional[float]) -> float:
    """
    MACD histogram size as 0-15 pts, normalised against ATR.

    Both are in price units so the ratio is unitless and comparable across a $9 name
    and a $900 one; a histogram around 0.3x ATR counts as full strength. Without the
    normalisation this component just ranked expensive stocks highest.
    """
    if atr14 and atr14 > 0:
        return min(15.0, 50.0 * abs(macd_histogram) / atr14)
    return min(15.0, 15 * abs(macd_histogram) / max(abs(macd or 0.0), 0.00001))


def _momentum(
    ema9: Optional[float],
    ema20: Optional[float],
    macd_histogram: Optional[float],
    macd: Optional[float],
    rsi14: Optional[float],
    atr14: Optional[float] = None,
) -> Tuple[float, str, str]:
    """Momentum, max 40 pts. Returns (score, direction_label, reason)."""
    score = 0.0
    reasons: List[str] = []
    direction = "NEUTRAL"

    # EMA alignment (15)
    if ema9 is not None and ema20 is not None:
        if ema9 > ema20:
            score += 15
            direction = "LONG"
            reasons.append("EMA9>EMA20 bullish")
        elif ema9 < ema20:
            score += 15
            direction = "SHORT"
            reasons.append("EMA9<EMA20 bearish")

    # MACD direction and ATR-normalised strength (15)
    if macd_histogram is not None and macd is not None:
        if macd_histogram > 0 and macd > 0:
            pts = _macd_magnitude_pts(macd_histogram, atr14, macd)
            score += pts
            reasons.append(f"MACD bullish (+{pts:.0f}pts)")
        elif macd_histogram < 0 and macd < 0:
            pts = _macd_magnitude_pts(macd_histogram, atr14, macd)
            score += pts
            reasons.append(f"MACD bearish (+{pts:.0f}pts)")
        elif macd_histogram > 0:
            score += 8
            reasons.append("MACD histogram positive crossing")
        elif macd_histogram < 0:
            score += 8
            reasons.append("MACD histogram negative crossing")

    # RSI trending-zone confirmation (10)
    if rsi14 is not None:
        if direction == "LONG" and 40 <= rsi14 <= 65:
            score += 10
            reasons.append(f"RSI {rsi14:.1f} momentum zone")
        elif direction == "SHORT" and 35 <= rsi14 <= 60:
            score += 10
            reasons.append(f"RSI {rsi14:.1f} momentum zone")

    signal = {
        "LONG": "LONG_MOMENTUM",
        "SHORT": "SHORT_MOMENTUM",
    }.get(direction, "NEUTRAL_MOMENTUM")
    return round(score, 1), signal, "; ".join(reasons)


def _mean_reversion(
    rsi14: Optional[float],
    close: Optional[float],
    bb_upper: Optional[float],
    bb_lower: Optional[float],
    bb_middle: Optional[float],
    day_high: Optional[float],
    day_low: Optional[float],
) -> Tuple[float, str, str]:
    """Mean reversion, max 40 pts. Returns (score, direction_label, reason)."""
    score = 0.0
    reasons: List[str] = []
    direction = "NEUTRAL"

    # RSI extreme (20), proportional rather than binary so 29.9 and 15 differ.
    if rsi14 is not None:
        if rsi14 < 30:
            score += min(20.0, 20 * (30 - rsi14) / 30)
            direction = "LONG"
            reasons.append(f"RSI oversold {rsi14:.1f}")
        elif rsi14 > 70:
            score += min(20.0, 20 * (rsi14 - 70) / 30)
            direction = "SHORT"
            reasons.append(f"RSI overbought {rsi14:.1f}")

    # Bollinger proximity (10)
    if close is not None and bb_upper is not None and bb_lower is not None and bb_middle is not None:
        band_width = bb_upper - bb_lower
        if band_width > 0:
            dist_lower = (close - bb_lower) / band_width
            dist_upper = (bb_upper - close) / band_width
            if dist_lower <= 0.15:
                score += 10
                direction = "LONG"
                reasons.append("Price at lower Bollinger Band")
            elif dist_upper <= 0.15:
                score += 10
                direction = "SHORT"
                reasons.append("Price at upper Bollinger Band")
            elif dist_lower <= 0.30:
                score += 5
                direction = direction if direction != "NEUTRAL" else "LONG"
                reasons.append("Price near lower Bollinger Band")
            elif dist_upper <= 0.30:
                score += 5
                direction = direction if direction != "NEUTRAL" else "SHORT"
                reasons.append("Price near upper Bollinger Band")

    # Day-range position (10)
    if close is not None and day_high is not None and day_low is not None and day_high > day_low:
        position = (close - day_low) / (day_high - day_low)
        if position <= 0.30:
            score += 10
            direction = "LONG" if direction == "NEUTRAL" else direction
            reasons.append(f"Bottom 30% of day range ({position * 100:.0f}%)")
        elif position >= 0.70:
            score += 10
            direction = "SHORT" if direction == "NEUTRAL" else direction
            reasons.append(f"Top 30% of day range ({position * 100:.0f}%)")

    signal = {
        "LONG": "LONG_REVERSION",
        "SHORT": "SHORT_REVERSION",
    }.get(direction, "NEUTRAL_REVERSION")
    return round(score, 1), signal, "; ".join(reasons)


def _day_breakout(
    close: Optional[float],
    day_high: Optional[float],
    day_low: Optional[float],
    or_high: Optional[float],
    or_low: Optional[float],
    atr14: Optional[float],
    phase: Optional[str],
    minutes_to_close: Optional[float],
) -> Tuple[float, str, str]:
    """
    Day / opening-range breakout, max 20 pts.

    The liquidity-window bonus is deliberately asymmetric. Forward-testing in the
    stocks sibling found the first hour was the *worst* entry window (27% win rate,
    -0.33R per trade), and a flat near-open bonus was pushing marginal setups over
    the actionable threshold straight into opening chop. Only the closing hour keeps
    the full bonus.
    """
    score = 0.0
    reasons: List[str] = []
    direction = "NEUTRAL"

    if phase == "REGULAR":
        if minutes_to_close is not None and minutes_to_close <= 60:
            score += 10
            reasons.append("Closing high-volume window")
        else:
            score += 5
            reasons.append("Regular session active")

    # Breakout distance in ATR units, strongest of day-extreme or opening range.
    if close is not None and atr14 is not None and atr14 > 0:
        long_pts = short_pts = 0.0
        long_reason = short_reason = ""
        if day_high is not None and close > day_high:
            dist = (close - day_high) / atr14
            long_pts = min(10.0, 10 * dist)
            long_reason = f"Breaking day high ({dist:.1f}xATR)"
        if or_high is not None and close > or_high:
            dist = (close - or_high) / atr14
            pts = min(10.0, 10 * dist)
            if pts > long_pts:
                long_pts, long_reason = pts, f"Breaking opening range high ({dist:.1f}xATR)"
        if day_low is not None and close < day_low:
            dist = (day_low - close) / atr14
            short_pts = min(10.0, 10 * dist)
            short_reason = f"Breaking day low ({dist:.1f}xATR)"
        if or_low is not None and close < or_low:
            dist = (or_low - close) / atr14
            pts = min(10.0, 10 * dist)
            if pts > short_pts:
                short_pts, short_reason = pts, f"Breaking opening range low ({dist:.1f}xATR)"

        if long_pts > short_pts and long_pts > 0:
            score += long_pts
            direction = "LONG"
            reasons.append(long_reason)
        elif short_pts > 0:
            score += short_pts
            direction = "SHORT"
            reasons.append(short_reason)

    signal = {
        "LONG": "LONG_BREAKOUT",
        "SHORT": "SHORT_BREAKOUT",
    }.get(direction, "NEUTRAL_BREAKOUT")
    return round(score, 1), signal, "; ".join(reasons)


def _mtf_confluence(
    dominant: Optional[str],
    hourly_dir: Optional[str],
    daily_dir: Optional[str],
) -> Tuple[float, str]:
    """
    Higher-timeframe confirmation of the intraday read, 0/15/30 pts.

    The intraday direction is the thing *being confirmed* — it is not itself a vote.
    Both siblings shipped a version where it was one of three votes, so a setup whose
    hourly and daily reads were both NEUTRAL scored FULL (+30) on its own say-so. In
    the forex repo that was measured: 103 of 143 FULL rows had a NEUTRAL or absent
    higher timeframe, and 20 had *both* neutral. Those 30 free points are most of what
    let mediocre setups clear the STRONG threshold, which is why the STRONG tier never
    outperformed.

    FULL therefore requires both higher timeframes present *and* agreeing.
    """
    if dominant not in ("LONG", "SHORT"):
        return 0.0, "NONE"

    votes = [d for d in (hourly_dir, daily_dir) if d in ("LONG", "SHORT")]
    if not votes:
        return 0.0, "UNCONFIRMED"

    agree = sum(1 for v in votes if v == dominant)
    if agree < len(votes):
        return 0.0, "OPPOSED" if agree == 0 else "CONFLICT"
    return (30.0, "FULL") if len(votes) == 2 else (15.0, "PARTIAL")


def _sr_proximity(
    close: Optional[float],
    atr14: Optional[float],
    sr_levels: List[Dict[str, Any]],
    dominant_direction: str,
) -> Tuple[float, str, bool, bool, Optional[float], Optional[float]]:
    """
    Structure scored *relative to the trade direction*, -25 to +25.

    A level only helps when it sits **behind** the trade — support beneath a long,
    resistance above a short — because that is where the stop shelters. A level
    directly **ahead** is a wall that caps the move before the target is reached.

    A direction-blind version of this rewarded both cases identically, so a long
    pinned under resistance scored the same +25 as a long bouncing off support.
    """
    if not sr_levels or not close or not atr14 or atr14 <= 0:
        return 0.0, "", False, False, None, None

    supports = [lv["price"] for lv in sr_levels if lv["type"] == "S" and lv["price"] <= close]
    resistances = [lv["price"] for lv in sr_levels if lv["type"] == "R" and lv["price"] >= close]
    nearest_support = max(supports) if supports else None
    nearest_resistance = min(resistances) if resistances else None

    if dominant_direction not in ("LONG", "SHORT"):
        return 0.0, "", False, False, nearest_support, nearest_resistance

    if dominant_direction == "LONG":
        behind, ahead = nearest_support, nearest_resistance
        behind_label, ahead_label = "support", "resistance"
    else:
        behind, ahead = nearest_resistance, nearest_support
        behind_label, ahead_label = "resistance", "support"

    score = 0.0
    reasons: List[str] = []
    at_key_level = False
    blocked_ahead = False

    if behind is not None:
        dist = abs(close - behind) / atr14
        if dist <= 0.3:
            score += 25
            at_key_level = True
            reasons.append(f"AT {behind_label} {behind:.2f} (entry at structure)")
        elif dist <= 1.0:
            score += 15
            reasons.append(f"Near {behind_label} {behind:.2f}")

    if ahead is not None:
        dist = abs(ahead - close) / atr14
        # The target sits ~3.75x ATR out (1.5 RR on a 2.5x ATR stop), so a level
        # inside 1.5x ATR means the trade is very unlikely to reach it unimpeded.
        if dist <= 1.5:
            score -= 25
            blocked_ahead = True
            reasons.append(f"BLOCKED by {ahead_label} {ahead:.2f} ({dist:.1f}xATR ahead)")
        elif dist <= 2.5:
            score -= 10
            reasons.append(f"{ahead_label.capitalize()} {ahead:.2f} close ahead ({dist:.1f}xATR)")

    return (
        round(max(-25.0, min(score, 25.0)), 1),
        "; ".join(reasons),
        at_key_level,
        blocked_ahead,
        nearest_support,
        nearest_resistance,
    )


# ── Cost and geometry ────────────────────────────────────────────────────────


def estimate_cost_pct(
    avg_dollar_volume: Optional[float],
    observed_spread_pct: Optional[float] = None,
) -> float:
    """
    Estimated round-trip transaction cost as a percentage of price.

    An observed bid/ask spread wins when one is available: crossing once on entry and
    once on exit costs approximately the full spread, and a measured number always
    beats a tier. Polygon supplies this on the snapshot; Yahoo does not, which is why
    the fallback exists at all.

    With no quote and no liquidity data this returns the *most pessimistic* tier. An
    unmeasured cost is not a zero cost, and treating it as one is how a screener talks
    itself into names nobody is quoting.
    """
    if observed_spread_pct is not None and observed_spread_pct >= 0:
        return float(observed_spread_pct)
    if avg_dollar_volume is None or avg_dollar_volume <= 0:
        return _COST_BPS_THIN / 100.0
    for floor, bps in _COST_TIERS_BPS:
        if avg_dollar_volume >= floor:
            return bps / 100.0
    return _COST_BPS_THIN / 100.0


def breakeven_win_rate(rr: float = _RR, cost_ratio: float = 0.0) -> float:
    """
    The win rate at which this bracket exactly breaks even, including cost.

    expectancy = p*(rr - c) - (1 - p)*(1 + c) = 0  =>  p = (1 + c) / (1 + rr)

    with everything in units of the stop distance. At rr=1.5 and zero cost this is
    exactly 0.400, which is why an edgeless system lands near 40% — and why a measured
    40% win rate is indistinguishable from entering at random.
    """
    return (1.0 + cost_ratio) / (1.0 + rr)


def _regime_weights(adx14: Optional[float]) -> Tuple[float, float, str]:
    """
    How much to trust momentum vs mean reversion, given trend strength.

    Returns (momentum_weight, reversion_weight, label). Blended linearly between the
    two thresholds rather than switched, so a tape hovering at ADX 21 does not flip
    playbook between consecutive scans.

    With no ADX at all both weights are 1.0: that is the old summing behaviour, and it
    is the honest answer when trend strength is unknown rather than a guess in either
    direction.
    """
    if adx14 is None:
        return 1.0, 1.0, "UNKNOWN"
    if adx14 >= _ADX_TREND:
        return 1.0, 0.0, "TREND"
    if adx14 <= _ADX_RANGE:
        return 0.0, 1.0, "RANGE"
    t = (adx14 - _ADX_RANGE) / (_ADX_TREND - _ADX_RANGE)
    return round(t, 3), round(1 - t, 3), "MIXED"


def trade_levels(
    direction: str,
    entry: Optional[float],
    atr14: Optional[float],
    cost_pct: Optional[float] = None,
) -> Dict[str, Any]:
    """
    The entry/stop/target bracket, or ``{}`` when it cannot be computed.

    The stop is the **widest of three floors** — volatility, noise, and cost:

        stop_dist = max(2.5 x ATR,  0.50% of entry,  8 x round-trip cost)

    The third term is the forex sibling's and is the one the stocks sibling lacks.
    Without it a wide-spread name gets a stop derived purely from ATR, the cost ratio
    silently exceeds the veto threshold, and the setup is thrown away — when the
    correct response is a proportionally wider stop and a proportionally wider target.
    """
    if direction not in ("LONG", "SHORT") or not entry or not atr14 or atr14 <= 0:
        return {}

    volatility_floor = _STOP_ATR_MULT * atr14
    noise_floor = _MIN_STOP_PCT * entry
    cost_floor = (cost_pct / 100.0) * entry * _SPREAD_STOP_MULT if cost_pct else 0.0
    stop_dist = max(volatility_floor, noise_floor, cost_floor)

    binding = "volatility"
    if stop_dist == cost_floor and cost_floor > volatility_floor:
        binding = "cost"
    elif stop_dist == noise_floor and noise_floor > volatility_floor:
        binding = "noise"

    target_dist = _RR * stop_dist
    if direction == "LONG":
        stop, target = entry - stop_dist, entry + target_dist
    else:
        stop, target = entry + stop_dist, entry - target_dist

    return {
        "suggested_entry": round(entry, 4),
        "suggested_stop": round(stop, 4),
        "suggested_target": round(target, 4),
        "stop_dollars": round(stop_dist, 4),
        "target_dollars": round(target_dist, 4),
        "stop_pct": round(stop_dist / entry * 100, 4),
        "target_pct": round(target_dist / entry * 100, 4),
        "rr_ratio": round(_RR, 2),
        "stop_binding": binding,
        # How far the final stop sits from what volatility alone would ask for. The
        # uneconomic-instrument veto reads this.
        "stop_atr_mult": round(stop_dist / atr14, 3),
    }


# ── The scorer ───────────────────────────────────────────────────────────────


def score_ticker(
    ticker: str,
    last: Optional[float],
    avg_dollar_volume: Optional[float],
    indicators: Dict[str, Any],
    phase: Optional[str] = None,
    minutes_to_close: Optional[float] = None,
    min_avg_dollar_volume: float = 0.0,
    hourly_direction: Optional[str] = None,
    daily_direction: Optional[str] = None,
    sr_levels: Optional[List[Dict[str, Any]]] = None,
    observed_spread_pct: Optional[float] = None,
    model_prob: Optional[float] = None,
    strength_bonus: float = 0.0,
) -> Dict[str, Any]:
    """
    Every score component plus the final ``trade_signal``, as one flat dict.

    ``strength_bonus`` (relative strength vs SPY) is passed *in* rather than added to
    the result afterwards. That is deliberate: it means the number driving the
    decision, the number shown on the dashboard, and the number the model trains on
    are the same number. Bolting it on after the fact is how those three quietly
    diverge.

    ``model_prob`` is the trained model's P(target before stop). It is a veto only.
    """
    close = indicators.get("close")
    rsi14 = indicators.get("rsi14")
    ema9 = indicators.get("ema9")
    ema20 = indicators.get("ema20")
    macd = indicators.get("macd")
    macd_hist = indicators.get("macd_histogram")
    atr14 = indicators.get("atr14")
    adx14 = indicators.get("adx14")
    bb_upper = indicators.get("bb_upper")
    bb_lower = indicators.get("bb_lower")
    bb_middle = indicators.get("bb_middle")
    day_high = indicators.get("day_high")
    day_low = indicators.get("day_low")
    or_high = indicators.get("or_high")
    or_low = indicators.get("or_low")

    risk_notes: List[str] = []
    thin_liquidity = illiquid = False
    if min_avg_dollar_volume > 0 and avg_dollar_volume is not None:
        if avg_dollar_volume < min_avg_dollar_volume * 0.1:
            illiquid = True
            risk_notes.append(
                f"Illiquid: avg ${avg_dollar_volume:,.0f}/day (min ${min_avg_dollar_volume:,.0f})"
            )
        elif avg_dollar_volume < min_avg_dollar_volume:
            thin_liquidity = True
            risk_notes.append(
                f"Thin liquidity: avg ${avg_dollar_volume:,.0f}/day (min ${min_avg_dollar_volume:,.0f})"
            )

    mom_raw, mom_signal, mom_reason = _momentum(ema9, ema20, macd_hist, macd, rsi14, atr14)
    rev_raw, rev_signal, rev_reason = _mean_reversion(
        rsi14, close, bb_upper, bb_lower, bb_middle, day_high, day_low
    )
    brk_score, brk_signal, brk_reason = _day_breakout(
        close, day_high, day_low, or_high, or_low, atr14, phase, minutes_to_close
    )

    w_mom, w_rev, regime = _regime_weights(adx14)
    mom_score = round(mom_raw * w_mom, 1)
    rev_score = round(rev_raw * w_rev, 1)

    # Weighted dominant direction — a suppressed playbook gets no vote.
    scored = ((mom_signal, mom_score), (rev_signal, rev_score), (brk_signal, brk_score))
    long_w = sum(sc for sig, sc in scored if "LONG" in sig)
    short_w = sum(sc for sig, sc in scored if "SHORT" in sig)
    dominant = "LONG" if long_w > short_w else ("SHORT" if short_w > long_w else "NEUTRAL")

    mtf_bonus, mtf_confluence = _mtf_confluence(dominant, hourly_direction, daily_direction)
    (
        sr_bonus,
        sr_reason,
        at_key_level,
        blocked_ahead,
        nearest_support,
        nearest_resistance,
    ) = _sr_proximity(close, atr14, sr_levels or [], dominant)

    total = mom_score + rev_score + brk_score + mtf_bonus + sr_bonus + strength_bonus
    if thin_liquidity:
        total = max(0.0, total - 20)

    # Cost ratio against the stop actually used, so it reflects real drag on
    # expectancy rather than an abstract bps number.
    entry_px = last or close
    cost_pct = estimate_cost_pct(avg_dollar_volume, observed_spread_pct)
    provisional = trade_levels(dominant, entry_px, atr14, cost_pct)
    prov_stop_pct = provisional.get("stop_pct") or 0.0
    cost_ratio = round(cost_pct / prov_stop_pct, 4) if prov_stop_pct > 0 else None
    stop_atr_mult = provisional.get("stop_atr_mult")

    ratio_veto = cost_ratio is not None and cost_ratio > _MAX_COST_RATIO
    # Cost has pushed the stop so far past what the instrument actually moves that
    # the target is unreachable. This is the branch that fires on a wide spread; the
    # ratio veto above cannot, once the cost floor binds.
    width_veto = stop_atr_mult is not None and stop_atr_mult > _MAX_STOP_ATR_MULT
    cost_veto = ratio_veto or width_veto

    if width_veto:
        risk_notes.append(
            f"Cost forces a {stop_atr_mult:.1f}xATR stop ({prov_stop_pct:.2f}% of price) "
            f"- beyond {_MAX_STOP_ATR_MULT:.0f}xATR the target is out of reach"
        )
    elif ratio_veto:
        risk_notes.append(
            f"Cost {cost_ratio:.0%} of risk (est. {cost_pct:.2f}% round trip vs "
            f"{prov_stop_pct:.2f}% stop) - above {_MAX_COST_RATIO:.0%} limit"
        )
    elif provisional.get("stop_binding") == "cost":
        risk_notes.append("Stop widened to clear the round-trip cost")

    ahead_label = "resistance" if dominant == "LONG" else "support"
    if blocked_ahead:
        risk_notes.append(f"Nearby {ahead_label} blocks the path to target")

    hourly_opposes = (
        dominant in ("LONG", "SHORT")
        and hourly_direction in ("LONG", "SHORT")
        and hourly_direction != dominant
    )

    be_p = breakeven_win_rate(_RR, cost_ratio or 0.0)
    required_p = round(be_p + _PROB_MARGIN, 4)
    prob_veto = model_prob is not None and model_prob < required_p

    # Vetoes are evaluated before promotions, in this order, and each one downgrades
    # rather than deletes so the pattern stays visible in the dashboard.
    if illiquid:
        trade_signal = "AVOID"
        reason = f"Too illiquid (avg ${(avg_dollar_volume or 0):,.0f}/day)"
    elif width_veto:
        trade_signal = "AVOID"
        reason = (
            f"Est. cost {cost_pct:.2f}% forces a {stop_atr_mult:.1f}xATR stop "
            f"- uneconomic to trade"
        )
    elif ratio_veto:
        trade_signal = "AVOID"
        reason = f"Est. cost {cost_ratio:.0%} of risk exceeds {_MAX_COST_RATIO:.0%} limit"
    elif hourly_opposes and total >= _ACTIONABLE_SCORE:
        trade_signal = "WATCH_ONLY"
        reason = f"{dominant} setup ({total:.0f}pts) but hourly trend is {hourly_direction} - countertrend"
    elif blocked_ahead and total >= _ACTIONABLE_SCORE:
        trade_signal = "WATCH_ONLY"
        reason = f"{dominant} setup ({total:.0f}pts) but {ahead_label} blocks the target"
    elif prob_veto and total >= _ACTIONABLE_SCORE:
        trade_signal = "WATCH_ONLY"
        reason = (
            f"{dominant} setup ({total:.0f}pts) but model P(win)={model_prob:.0%} "
            f"< {required_p:.0%} required"
        )
    elif total >= _STRONG_SCORE and dominant == "LONG" and mtf_bonus >= 15:
        trade_signal = "STRONG_BUY"
        reason = f"Strong long setup ({total:.0f}pts, MTF:{mtf_confluence})"
    elif total >= _STRONG_SCORE and dominant == "SHORT" and mtf_bonus >= 15:
        trade_signal = "STRONG_SHORT"
        reason = f"Strong short setup ({total:.0f}pts, MTF:{mtf_confluence})"
    elif total >= _ACTIONABLE_SCORE and dominant == "LONG":
        trade_signal = "BUY_CANDIDATE"
        reason = f"Long candidate ({total:.0f}pts)"
    elif total >= _ACTIONABLE_SCORE and dominant == "SHORT":
        trade_signal = "SHORT_CANDIDATE"
        reason = f"Short candidate ({total:.0f}pts)"
    elif total >= _WATCH_SCORE:
        trade_signal = "WATCH_ONLY"
        reason = f"Mixed signals ({total:.0f}pts)"
    else:
        trade_signal = "AVOID"
        reason = f"No clear setup ({total:.0f}pts)"

    extension_atr: Optional[float] = None
    if close is not None and ema20 is not None and atr14:
        extension_atr = round((close - ema20) / atr14, 2)
    if trade_signal in ("STRONG_BUY", "STRONG_SHORT") and extension_atr is not None:
        if trade_signal == "STRONG_BUY" and extension_atr > _MAX_EXTENSION_ATR:
            trade_signal = "WATCH_ONLY"
            reason = f"Extended {extension_atr:.1f}xATR above EMA20 - wait for pullback"
        elif trade_signal == "STRONG_SHORT" and extension_atr < -_MAX_EXTENSION_ATR:
            trade_signal = "WATCH_ONLY"
            reason = f"Extended {abs(extension_atr):.1f}xATR below EMA20 - wait for pullback"

    if model_prob is not None and trade_signal not in ("AVOID", "WATCH_ONLY"):
        reason += f" - P(win) {model_prob:.0%} vs {required_p:.0%} needed"

    if trade_signal == "SHORT_CANDIDATE" or trade_signal == "STRONG_SHORT":
        risk_notes.append("Short selling can create large losses; use only if approved and risk-controlled.")

    levels = provisional if trade_signal not in ("AVOID", "WATCH_ONLY") else {}
    if levels and levels.get("target_pct", 100.0) < MIN_TARGET_PCT:
        risk_notes.append(f"Target {levels['target_pct']:.2f}% of price - thin edge")

    signal_parts = []
    if mom_reason:
        signal_parts.append(f"Momentum: {mom_reason}")
    if rev_reason:
        signal_parts.append(f"Reversion: {rev_reason}")
    if brk_reason:
        signal_parts.append(f"Breakout: {brk_reason}")
    if mtf_confluence != "NONE":
        signal_parts.append(
            f"MTF: {mtf_confluence} ({hourly_direction or '?'}/{daily_direction or '?'})"
        )
    if sr_reason:
        signal_parts.append(f"S/R: {sr_reason}")

    return {
        "momentum_score": mom_score,
        "reversion_score": rev_score,
        "breakout_score": brk_score,
        "adx14": adx14,
        "regime": regime,
        "suggested_entry": levels.get("suggested_entry"),
        "suggested_stop": levels.get("suggested_stop"),
        "suggested_target": levels.get("suggested_target"),
        "stop_dollars": levels.get("stop_dollars"),
        "target_dollars": levels.get("target_dollars"),
        "stop_pct": levels.get("stop_pct"),
        "target_pct": levels.get("target_pct"),
        "rr_ratio": levels.get("rr_ratio"),
        "mtf_score": mtf_bonus,
        "mtf_confluence": mtf_confluence,
        "sr_score": sr_bonus,
        "at_key_level": at_key_level,
        "blocked_ahead": blocked_ahead,
        "nearest_support": nearest_support,
        "nearest_resistance": nearest_resistance,
        "sr_levels_json": json.dumps(sr_levels[:5]) if sr_levels else None,
        "extension_atr": extension_atr,
        "cost_pct": round(cost_pct, 4),
        "cost_ratio": cost_ratio,
        "model_prob": model_prob,
        "required_prob": required_p,
        "dominant": dominant,
        # The levels the trade *would* use, exposed even when the signal is not
        # actionable, so feature extraction always sees a real stop size rather than
        # a zero standing in for "no bracket".
        "prov_stop_pct": provisional.get("stop_pct"),
        "prov_target_pct": provisional.get("target_pct"),
        "total_score": round(total, 1),
        "trade_signal": trade_signal,
        "signal_reason": reason,
        "risk_notes": "; ".join(risk_notes + signal_parts),
        "market_phase": phase,
    }
