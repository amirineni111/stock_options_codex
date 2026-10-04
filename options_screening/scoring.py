"""
Contract filtering, scoring, and the decision context shown next to each row.

The score here answers "is this contract well formed?" — liquid, tight, in the delta
and DTE bands you asked for. It deliberately does **not** answer "will this trade make
money"; the directional read comes from `signals.py` and the measured odds come from
the model. Keeping the two separate is what lets the model veto without being able to
promote something the filters already rejected.
"""
from __future__ import annotations

from datetime import date
from typing import List, Optional, Tuple

from .greeks import (
    expected_move_pct,
    gamma_leverage,
    iv_rank,
    premium_trade_levels,
    project_premium,
    theta_per_premium,
)
from .indicators import calculate_atr
from .models import MarketContext, OptionContract, RejectedContract, ScoredContract
from .signals import _RR, _STOP_ATR_MULT
from .timeutil import exchange_date

# ── Score component weights ──────────────────────────────────────────────────
# Five components summing to 100. These are contract-quality weights, not edge
# weights: liquidity and spread dominate because they are the two things that
# reliably cost money on entry regardless of whether the directional call is right.
_W_LIQUIDITY = 25.0
_W_SPREAD = 25.0
_W_DELTA = 20.0
_W_EXPIRATION = 15.0
_W_IV = 15.0

# Volume and open interest stop earning score at 2x the configured minimum. Past
# that, more open interest does not make a fill meaningfully easier, and letting it
# keep scoring would rank mega-cap weeklies above everything else on size alone.
_LIQUIDITY_SATURATION = 2.0
_LIQUIDITY_VOLUME_SHARE = 10.0
_LIQUIDITY_OI_SHARE = 15.0

# ── Signal-stage floors ──────────────────────────────────────────────────────
# The filter stage honours the user's thresholds exactly. The *signal* stage applies
# these floors on top, so that loosening a filter to see more rows cannot silently
# promote an unfillable contract to a candidate. They are floors, not overrides: a
# stricter user setting always wins.
_SIGNAL_MAX_SPREAD_PCT = 20.0
_SIGNAL_MIN_VOLUME = 10
_SIGNAL_MIN_OPEN_INTEREST = 100

# On a plan with no bid/ask the entry is the last trade, so a contract that has not
# traded for an hour is priced at a number that may no longer exist. Its outcome would
# measure the stale print, not the call.
_MAX_PRICE_AGE_MINUTES = 60.0

# The second label's names, kept apart from the moving-average rule's so the scorecard
# can grade the two directional reads against each other.
ENGINE_CALL = "ENGINE_BUY_CALL"
ENGINE_PUT = "ENGINE_BUY_PUT"

# The +/- move used for the scenario P&L columns.
_SCENARIO_MOVE_PCT = 0.02

# Downgrade when decay eats more than this share of the premium before the target can
# plausibly be reached. At a third, the underlying has to cover the thesis *and* a
# third of the premium just to break even — which is a volatility bet wearing a
# directional costume.
_MAX_DECAY_SHARE = 0.33

# IV shift assumed in the adverse scenario, in vol points. A move against a long
# option is very often accompanied by IV *rising* for puts and falling for calls, but
# modelling that asymmetry needs a skew surface this repo does not have. Zero is the
# honest placeholder; the vega column exposes the sensitivity so the reader can apply
# their own view.
_SCENARIO_IV_CHANGE = 0.0


def days_to_expiration(contract: OptionContract, today: Optional[date] = None) -> int:
    """
    Calendar days to expiry, counted from the *exchange* date.

    ``date.today()`` is the local date, which after 20:00 ET is already tomorrow in
    UTC and disagreed with the DTE storage computed from a UTC timestamp — the same
    contract carried two different DTEs in one row. Both now come from here.
    """
    base = today or exchange_date()
    return (contract.expiration_date - base).days


def score_contract(
    contract: OptionContract,
    settings,
    market_context: Optional[MarketContext] = None,
    today: Optional[date] = None,
    iv_history: Optional[List[float]] = None,
) -> Tuple[Optional[ScoredContract], Optional[RejectedContract]]:
    """
    Returns exactly one of (scored, None) or (None, rejection).

    ``iv_history`` is this underlying's recent implied-volatility observations, used
    for the IV rank. It is passed in rather than fetched so this stays a pure function
    of its inputs.
    """
    today = today or exchange_date()
    rejection = _validate_contract(contract, settings, market_context, today=today)
    if rejection:
        return None, rejection

    spread_pct = contract.spread_pct or 0.0
    abs_delta = abs(contract.delta or 0.0)
    dte = days_to_expiration(contract, today)
    iv = contract.implied_volatility or 0.0
    volume = contract.volume or 0
    oi = contract.open_interest or 0
    mid = contract.mid_price or 0.0

    volume_ratio = min(volume / max(settings.min_volume, 1), _LIQUIDITY_SATURATION) / _LIQUIDITY_SATURATION
    oi_ratio = min(oi / max(settings.min_open_interest, 1), _LIQUIDITY_SATURATION) / _LIQUIDITY_SATURATION
    liquidity_score = min(
        _W_LIQUIDITY,
        volume_ratio * _LIQUIDITY_VOLUME_SHARE + oi_ratio * _LIQUIDITY_OI_SHARE,
    )
    # An unavailable spread scores zero rather than being treated as tight. An
    # unmeasured cost is not a zero cost, and scoring it as one is how a screener
    # talks itself into contracts nobody is quoting.
    if contract.spread_pct is None:
        spread_score = 0.0
    else:
        spread_score = max(0.0, _W_SPREAD * (1.0 - spread_pct / settings.max_spread_pct))
    # Delta and DTE score as triangles peaking at the middle of the requested band:
    # the edges of a band are where you asked to stop looking, not where you wanted
    # to be.
    delta_midpoint = (settings.min_abs_delta + settings.max_abs_delta) / 2.0
    delta_width = max((settings.max_abs_delta - settings.min_abs_delta) / 2.0, 0.01)
    delta_score = max(0.0, _W_DELTA * (1.0 - abs(abs_delta - delta_midpoint) / delta_width))
    dte_midpoint = (settings.min_days_to_expiration + settings.max_days_to_expiration) / 2.0
    dte_width = max((settings.max_days_to_expiration - settings.min_days_to_expiration) / 2.0, 1.0)
    expiration_score = max(0.0, _W_EXPIRATION * (1.0 - abs(dte - dte_midpoint) / dte_width))
    # Monotonically prefers cheaper IV within the band: for a long option, IV is the
    # price paid, and the band's upper edge is the most expensive thing you allowed.
    iv_score = max(0.0, _W_IV * (1.0 - (iv - settings.min_iv) / max(settings.max_iv - settings.min_iv, 0.01)))

    score = round(liquidity_score + spread_score + delta_score + expiration_score + iv_score, 2)
    max_contracts = int(settings.fixed_risk // (mid * 100)) if mid > 0 else 0
    premium_at_risk = round(max_contracts * mid * 100, 2)
    if contract.contract_type == "call":
        breakeven = contract.strike_price + mid
    else:
        breakeven = contract.strike_price - mid

    decision_context = _decision_context(
        contract,
        settings,
        market_context,
        breakeven,
        max_contracts,
        premium_at_risk,
        today=today,
        iv_history=iv_history,
    )

    result = ScoredContract(
        contract=contract,
        score=score,
        score_components={
            "liquidity": round(liquidity_score, 2),
            "spread": round(spread_score, 2),
            "delta": round(delta_score, 2),
            "expiration": round(expiration_score, 2),
            "iv": round(iv_score, 2),
        },
        max_contracts_by_risk=max_contracts,
        premium_at_risk=premium_at_risk,
        breakeven=round(breakeven, 2),
        reason=_accepted_reason(contract),
        **decision_context,
    )
    return result, None


def score_contracts(
    contracts: List[OptionContract],
    settings,
    market_context: Optional[MarketContext] = None,
    today: Optional[date] = None,
    iv_history: Optional[List[float]] = None,
) -> Tuple[List[ScoredContract], List[RejectedContract]]:
    # Resolved once for the whole batch, so a scan that straddles midnight ET cannot
    # score its first ticker against a different DTE than its last.
    today = today or exchange_date()
    accepted: List[ScoredContract] = []
    rejected: List[RejectedContract] = []
    for contract in contracts:
        scored, rejection = score_contract(
            contract, settings, market_context, today=today, iv_history=iv_history
        )
        if scored:
            accepted.append(scored)
        elif rejection:
            rejected.append(rejection)
    accepted.sort(key=lambda item: item.score, reverse=True)
    return accepted, rejected


def _validate_contract(
    contract: OptionContract,
    settings,
    market_context: Optional[MarketContext] = None,
    today: Optional[date] = None,
) -> Optional[RejectedContract]:
    today = today or exchange_date()
    reasons = []
    if contract.contract_type not in {"call", "put"}:
        reasons.append("unsupported contract type")
    dte = days_to_expiration(contract, today)
    if dte < settings.min_days_to_expiration or dte > settings.max_days_to_expiration:
        reasons.append("outside DTE range")
    if contract.mid_price is None or contract.mid_price <= 0:
        reasons.append("missing positive price")
    if contract.spread_pct is None and not settings.allow_missing_spread:
        reasons.append("spread too wide or unavailable")
    elif contract.spread_pct is not None and contract.spread_pct > settings.max_spread_pct:
        reasons.append("spread too wide or unavailable")
    if (contract.volume or 0) < settings.min_volume:
        reasons.append("volume below minimum")
    if (contract.open_interest or 0) < settings.min_open_interest:
        reasons.append("open interest below minimum")
    if contract.delta is None or abs(contract.delta) < settings.min_abs_delta or abs(contract.delta) > settings.max_abs_delta:
        reasons.append("delta outside range")
    if contract.implied_volatility is None or contract.implied_volatility < settings.min_iv or contract.implied_volatility > settings.max_iv:
        reasons.append("IV outside range")
    if contract.mid_price and contract.mid_price * 100 > settings.fixed_risk:
        reasons.append("one contract premium exceeds fixed risk")
    # Either directional read can satisfy the filter. If only the moving-average stack
    # could, every contract the engine calls would be rejected before it was labelled,
    # and the comparison between the two reads would only ever see their overlap.
    if (
        getattr(settings, "require_trend_alignment", False)
        and not _trend_aligned(contract, market_context)
        and not _engine_aligned(contract, market_context)
    ):
        reasons.append("trend not aligned")
    if getattr(settings, "avoid_earnings_before_expiration", False) and market_context and market_context.earnings_date:
        if today <= market_context.earnings_date <= contract.expiration_date:
            reasons.append("earnings before expiration")

    if not reasons:
        return None
    return RejectedContract(
        underlying=contract.underlying,
        contract_ticker=contract.contract_ticker,
        contract_type=contract.contract_type,
        reason=", ".join(reasons),
        as_of=contract.as_of,
    )


def _accepted_reason(contract: OptionContract) -> str:
    if contract.spread_pct is None:
        return "Accepted: matched filters, but bid-ask spread is unavailable; verify quote before trading."
    return "Accepted: liquid, defined-risk premium, and within conservative swing filters."


def _trend_aligned(contract: OptionContract, market_context: Optional[MarketContext]) -> bool:
    if not market_context:
        return False
    if contract.contract_type == "call":
        return market_context.trend_signal == "bullish"
    if contract.contract_type == "put":
        return market_context.trend_signal == "bearish"
    return False


def _engine_aligned(contract: OptionContract, market_context: Optional[MarketContext]) -> bool:
    if not market_context or not market_context.engine_direction:
        return False
    wanted = "LONG" if contract.contract_type == "call" else "SHORT"
    return market_context.engine_direction == wanted


def price_age_minutes(contract: OptionContract) -> Optional[float]:
    """Minutes since an *unquoted* contract last traded; None when quoted or unknown."""
    if contract.bid is not None and contract.ask is not None:
        return None
    if contract.last_trade_at is None:
        return None
    return max(0.0, (contract.as_of - contract.last_trade_at).total_seconds() / 60.0)


def _decision_context(
    contract: OptionContract,
    settings,
    market_context: Optional[MarketContext],
    breakeven: float,
    max_contracts: int,
    premium_at_risk: float,
    today: Optional[date] = None,
    iv_history: Optional[List[float]] = None,
) -> dict:
    underlying_price = _underlying_price(contract, market_context)
    dte = days_to_expiration(contract, today)
    iv = contract.implied_volatility or 0.0
    move_pct = expected_move_pct(iv, dte)
    breakeven_distance_pct = _breakeven_distance_pct(contract, breakeven, underlying_price)
    expected_move_ok = None
    if move_pct is not None and breakeven_distance_pct is not None:
        expected_move_ok = breakeven_distance_pct <= move_pct

    # The underlying's own volatility, used both to size the bracket and to convert a
    # price distance into a plausible holding period for the decay charge.
    daily_atr = _daily_atr(market_context)
    levels = _premium_bracket(contract, underlying_price, daily_atr, dte)

    # Scenarios are charged decay for the time the move is assumed to take, rather
    # than being quoted as instantaneous.
    hold_days = levels.get("target_hold_days") or 0.0
    favorable_value, favorable_pnl = _scenario_value(
        contract, underlying_price, max_contracts, premium_at_risk, _SCENARIO_MOVE_PCT, hold_days
    )
    adverse_value, adverse_pnl = _scenario_value(
        contract, underlying_price, max_contracts, premium_at_risk, -_SCENARIO_MOVE_PCT, hold_days
    )
    trend_aligned = _trend_aligned(contract, market_context) if market_context else None

    return {
        "underlying_last_price": _round_optional(underlying_price),
        "sma20": _round_optional(market_context.sma20 if market_context else None),
        "sma50": _round_optional(market_context.sma50 if market_context else None),
        "trend_signal": market_context.trend_signal if market_context else "unknown",
        "trend_aligned": trend_aligned,
        "earnings_date": market_context.earnings_date if market_context else None,
        "earnings_warning": market_context.earnings_warning if market_context else "not checked",
        "breakeven_distance_pct": breakeven_distance_pct,
        "expected_move_pct": move_pct,
        "expected_move_to_breakeven_ok": expected_move_ok,
        "favorable_2pct_value": favorable_value,
        "favorable_2pct_pnl": favorable_pnl,
        "adverse_2pct_value": adverse_value,
        "adverse_2pct_pnl": adverse_pnl,
        "days_to_expiration": dte,
        "underlying_atr14": _round_optional(daily_atr),
        # Greeks that were fetched and stored but never read by anything until now.
        "theta_per_premium": theta_per_premium(contract),
        "gamma_leverage": gamma_leverage(contract, underlying_price),
        "iv_rank": iv_rank(contract.implied_volatility, iv_history or []),
        "premium_entry": levels.get("premium_entry"),
        "premium_stop": levels.get("premium_stop"),
        "premium_target": levels.get("premium_target"),
        "underlying_stop": levels.get("underlying_stop"),
        "underlying_target": levels.get("underlying_target"),
        "risk_dollars": levels.get("risk_dollars"),
        "reward_dollars": levels.get("reward_dollars"),
        "premium_rr": levels.get("premium_rr"),
        "target_hold_days": levels.get("target_hold_days"),
        "decay_at_target": levels.get("decay_at_target"),
        "decision_checklist": _decision_checklist(contract, trend_aligned, expected_move_ok, market_context),
        **_trade_signal(contract, settings, market_context, trend_aligned, expected_move_ok, levels),
    }


def _daily_atr(market_context: Optional[MarketContext]) -> Optional[float]:
    """ATR(14) from the daily bars carried on the market context, if they are there."""
    if not market_context or not market_context.daily_bars:
        return None
    bars = market_context.daily_bars
    highs = [b.get("high") for b in bars]
    lows = [b.get("low") for b in bars]
    closes = [b.get("close") for b in bars]
    if any(v is None for v in highs + lows + closes):
        return None
    return calculate_atr(highs, lows, closes)


def _premium_bracket(
    contract: OptionContract,
    underlying_price: Optional[float],
    daily_atr: Optional[float],
    dte: int,
) -> dict:
    """
    The underlying's ATR bracket expressed in premium terms.

    Uses the same geometry as the equity lane — a 2.5x ATR stop and a 1.5R target — so
    a stop means the same thing on both pages. What differs is that the *premium*
    reward:risk is not 1.5: delta, gamma and decay all bend it, and on a low-delta
    contract they bend it a long way. That number is surfaced as ``premium_rr``
    rather than assumed.
    """
    if not daily_atr or not underlying_price:
        return {}
    stop_distance = _STOP_ATR_MULT * daily_atr
    return premium_trade_levels(
        contract,
        underlying_price=underlying_price,
        underlying_stop_distance=stop_distance,
        underlying_target_distance=_RR * stop_distance,
        daily_atr=daily_atr,
        dte=dte,
    )


def _underlying_price(contract: OptionContract, market_context: Optional[MarketContext]) -> Optional[float]:
    if market_context and market_context.last_price is not None:
        return market_context.last_price
    return contract.underlying_price


def _breakeven_distance_pct(
    contract: OptionContract,
    breakeven: float,
    underlying_price: Optional[float],
) -> Optional[float]:
    if underlying_price is None or underlying_price <= 0:
        return None
    if contract.contract_type == "call":
        distance = breakeven - underlying_price
    else:
        distance = underlying_price - breakeven
    return round(max(distance, 0.0) / underlying_price * 100, 2)


def _scenario_value(
    contract: OptionContract,
    underlying_price: Optional[float],
    max_contracts: int,
    premium_at_risk: float,
    move_pct: float,
    holding_days: float = 0.0,
) -> Tuple[Optional[float], Optional[float]]:
    """
    Position value after a ``move_pct`` move in the position's favour (or against it,
    for a negative value), ``holding_days`` later.

    ``move_pct`` is expressed in the *option's* favour, so the sign is flipped for a
    put before being handed to the underlying-frame projection.

    This used to be delta + gamma only, with no time term at all — which for a
    21-75 DTE hold omits what is usually the largest component of the P&L and made
    every favourable scenario read better than it is.
    """
    if underlying_price is None or underlying_price <= 0 or max_contracts <= 0 or contract.mid_price is None:
        return None, None
    signed_move_pct = -move_pct if contract.contract_type == "put" else move_pct
    projected = project_premium(
        contract,
        underlying_move=underlying_price * signed_move_pct,
        holding_days=holding_days,
        iv_change_points=_SCENARIO_IV_CHANGE,
    )
    if projected is None:
        return None, None
    estimated_value = round(projected * 100 * max_contracts, 2)
    return estimated_value, round(estimated_value - premium_at_risk, 2)


def _decision_checklist(
    contract: OptionContract,
    trend_aligned: Optional[bool],
    expected_move_ok: Optional[bool],
    market_context: Optional[MarketContext],
) -> str:
    items = []
    items.append("trend ok" if trend_aligned else "trend check needed")
    items.append("spread ok" if contract.spread_pct is not None else "verify bid/ask")
    if expected_move_ok is None:
        items.append("expected move unknown")
    else:
        items.append("breakeven within expected move" if expected_move_ok else "breakeven beyond expected move")
    if market_context and market_context.earnings_date:
        items.append("earnings before expiration")
    elif market_context and market_context.earnings_warning == "none found before expiration":
        items.append("no earnings found before expiration")
    else:
        items.append("earnings not checked")
    return "; ".join(items)


def _trade_signal(
    contract: OptionContract,
    settings,
    market_context: Optional[MarketContext],
    trend_aligned: Optional[bool],
    expected_move_ok: Optional[bool],
    levels: Optional[dict] = None,
) -> dict:
    levels = levels or {}
    avoid_reasons = []
    watch_reasons = []
    # Objections that are about direction rather than about the contract. The second
    # label ignores these: it has its own directional read.
    trend_reasons = []
    # Tracked separately from the reason strings so the income-structure suggestion
    # can tell "the only problem is direction" from "nobody is quoting this".
    liquidity_ok = True

    ignore_missing_spread = getattr(settings, "ignore_missing_spread_for_signal", False)
    if contract.spread_pct is None:
        liquidity_ok = False
        if not ignore_missing_spread:
            watch_reasons.append("bid/ask spread unavailable")
    elif contract.spread_pct > min(settings.max_spread_pct, _SIGNAL_MAX_SPREAD_PCT):
        liquidity_ok = False
        watch_reasons.append("bid/ask spread is wide")

    age = price_age_minutes(contract)
    if age is not None and age > _MAX_PRICE_AGE_MINUTES:
        watch_reasons.append(f"last trade {_age_text(age)} ago - price may be stale")

    if trend_aligned is False:
        trend_reasons.append("trend is not aligned")
    elif trend_aligned is None:
        trend_reasons.append("trend is unknown")
    watch_reasons.extend(trend_reasons)

    if expected_move_ok is False:
        watch_reasons.append("breakeven is beyond rough expected move")
    elif expected_move_ok is None:
        watch_reasons.append("expected move is unknown")

    if market_context and market_context.earnings_date:
        watch_reasons.append("earnings before expiration")

    if contract.volume is None or contract.open_interest is None:
        liquidity_ok = False
        watch_reasons.append("liquidity data incomplete")
    elif (
        contract.volume < max(settings.min_volume, _SIGNAL_MIN_VOLUME)
        or contract.open_interest < max(settings.min_open_interest, _SIGNAL_MIN_OPEN_INTEREST)
    ):
        liquidity_ok = False
        watch_reasons.append("liquidity is thin")

    # Decay gate. Theta is per day and the bracket knows how long the move is assumed
    # to take, so this is answerable rather than a rule of thumb: if the premium
    # decays by more than this fraction before the target can plausibly be reached,
    # the position is fighting the clock harder than it is expressing a view.
    decay_share = _decay_share_of_premium(contract, levels)
    if decay_share is not None and decay_share > _MAX_DECAY_SHARE:
        watch_reasons.append(
            f"time decay costs {decay_share:.0%} of premium over the expected hold"
        )

    if contract.mid_price is None or contract.mid_price <= 0:
        avoid_reasons.append("option price unavailable")
    if contract.mid_price and contract.mid_price * 100 > settings.fixed_risk:
        avoid_reasons.append("one contract exceeds fixed risk")

    quality_reasons = [r for r in watch_reasons if r not in trend_reasons]
    engine = _engine_signal(contract, market_context, avoid_reasons, quality_reasons)

    if avoid_reasons:
        return {"trade_signal": "AVOID", "signal_reason": "; ".join(avoid_reasons), **engine}

    if watch_reasons:
        signal = "WATCH_ONLY"
        if _income_structure_applies(trend_aligned, liquidity_ok, watch_reasons):
            signal = _income_signal(contract)
        return {"trade_signal": signal, "signal_reason": "; ".join(watch_reasons), **engine}

    if contract.contract_type == "call":
        return {"trade_signal": "BUY_CALL_CANDIDATE", "signal_reason": "trend, liquidity, spread, and expected move checks passed", **engine}
    if contract.contract_type == "put":
        return {"trade_signal": "BUY_PUT_CANDIDATE", "signal_reason": "trend, liquidity, spread, and expected move checks passed", **engine}
    return {"trade_signal": "AVOID", "signal_reason": "unsupported contract type", **engine}


def _engine_signal(
    contract: OptionContract,
    market_context: Optional[MarketContext],
    avoid_reasons: List[str],
    quality_reasons: List[str],
) -> dict:
    """
    The second label: the engine's daily read decides direction, and the contract
    must pass every contract-quality check the first label applies. Only the
    direction source differs, so a difference in outcome is about direction.
    """
    if not market_context or market_context.engine_direction is None:
        return {"engine_signal": None, "engine_reason": "no daily engine read", "engine_score": None}
    score = market_context.engine_score
    read = f"engine {market_context.engine_signal} ({score:.0f}pts)" if score is not None else "engine"
    wanted = "LONG" if contract.contract_type == "call" else "SHORT"
    if avoid_reasons:
        signal, reason = "AVOID", "; ".join(avoid_reasons)
    elif market_context.engine_direction != wanted:
        signal, reason = "WATCH_ONLY", f"{read} does not call {wanted}"
    elif quality_reasons:
        signal, reason = "WATCH_ONLY", "; ".join(quality_reasons)
    else:
        signal = ENGINE_CALL if wanted == "LONG" else ENGINE_PUT
        reason = f"{read}: {market_context.engine_reason}"
    return {"engine_signal": signal, "engine_reason": reason, "engine_score": score}


def _age_text(minutes: float) -> str:
    if minutes >= 1440:
        return f"{minutes / 1440:.1f} days"
    if minutes >= 90:
        return f"{minutes / 60:.1f} h"
    return f"{minutes:.0f} min"


def _decay_share_of_premium(contract: OptionContract, levels: dict) -> Optional[float]:
    """Fraction of the premium theta consumes over the assumed hold to target."""
    per_day = theta_per_premium(contract)
    hold_days = levels.get("target_hold_days")
    if per_day is None or not hold_days:
        return None
    return round(per_day * hold_days, 4)


def _income_structure_applies(
    trend_aligned: Optional[bool],
    liquidity_ok: bool,
    watch_reasons: List[str],
) -> bool:
    """
    Whether suggesting the *sold* structure instead is actually sensible.

    Only when the sole objection is that the trend points the other way — selling
    premium into a trend that is against the long side is a real alternative. It is
    not an alternative when the contract is illiquid or unquoted: the old rule fired
    on ``trend_aligned is False and spread_pct is not None``, so a thinly traded
    contract that merely happened to have a quote was labelled COVERED_CALL_ONLY on
    the strength of a liquidity warning, recommending a sale of something nobody is
    trading.
    """
    if trend_aligned is not False or not liquidity_ok:
        return False
    return all("trend" in reason for reason in watch_reasons)


def _income_signal(contract: OptionContract) -> str:
    if contract.contract_type == "call":
        return "COVERED_CALL_ONLY"
    if contract.contract_type == "put":
        return "CASH_SECURED_PUT_ONLY"
    return "WATCH_ONLY"


def _round_optional(value: Optional[float]) -> Optional[float]:
    return round(value, 2) if value is not None else None
