"""
Option premium maths: decay-aware scenarios, IV rank, and the premium bracket.

Theta and vega were fetched from Polygon, stored in SQLite, and then never read by
anything. For a screener whose whole output is long single-leg calls and puts that is
the wrong omission to have: over a 21-75 day hold, decay is usually the largest term
in the P&L, and the scenario columns that ignored it were quietly optimistic on every
row.

Everything here is a first/second-order Taylor expansion around the current quote:

    dP  ~  delta*dS  +  0.5*gamma*dS^2  +  theta*dt  +  vega*dIV

Greeks are taken from Polygon rather than solved locally. That is a deliberate
constraint, not laziness: a local Black-Scholes solve needs a dividend and rate
assumption per name, and getting those subtly wrong produces greeks that look
plausible and are consistently biased. Polygon's are at least the same numbers the
rest of the market is quoting from.

**Sign and unit conventions**, which are the easiest thing to get wrong here:

* ``theta`` is per calendar day and already negative for a long option.
* ``vega`` is per one *percentage point* of implied volatility (1.0 = 100 vol points),
  so an IV move from 0.30 to 0.32 is ``dIV = 2.0`` vega units.
* ``implied_volatility`` is a decimal (0.30 = 30%).
"""
from __future__ import annotations

import math
from typing import Any, Dict, List, Optional, Sequence

from .models import OptionContract

# A long option cannot be worth less than nothing, and in practice a position is
# abandoned well before zero. The premium stop is floored here so a bracket never
# implies losing more than this fraction of the premium paid.
_MAX_PREMIUM_LOSS_FRACTION = 0.60

# When the underlying's daily range is unknown there is no principled way to convert a
# price distance into a holding period, so fall back to this. Chosen to sit inside the
# 21-75 DTE window the screener targets without assuming a near-instant move.
_FALLBACK_HOLD_DAYS = 5.0

# A trade is not held to expiry; cap the assumed hold so the decay charge stays
# realistic even when the target is far away in ATR terms. Under diffusion scaling the
# standard 3.75x-ATR target already works out near 14 days, so this leaves room for a
# wider bracket without letting a far target imply a multi-month hold.
_MAX_HOLD_DAYS = 30.0


def project_premium(
    contract: OptionContract,
    underlying_move: float,
    holding_days: float = 0.0,
    iv_change_points: float = 0.0,
) -> Optional[float]:
    """
    Projected option premium after an underlying move, time passing, and an IV shift.

    ``underlying_move`` is in dollars and signed in the *underlying's* direction (a
    negative number is a fall), not in the option's favour — the delta sign handles
    that, so a put with delta -0.4 gains on a negative move without the caller having
    to flip anything.

    Returns ``None`` when there is no usable quote. Floored at zero: the expansion is
    local and will happily go negative on a large adverse move, which an option
    cannot.
    """
    mid = contract.mid_price
    if mid is None or mid <= 0:
        return None

    delta = contract.delta or 0.0
    gamma = contract.gamma or 0.0
    theta = contract.theta or 0.0
    vega = contract.vega or 0.0

    move = _clamp_to_delta_validity(underlying_move, delta, gamma)
    directional = delta * move + 0.5 * gamma * move ** 2
    decay = theta * max(holding_days, 0.0)
    vol = vega * iv_change_points
    return max(0.0, mid + directional + decay + vol)


def _clamp_to_delta_validity(move: float, delta: float, gamma: float) -> float:
    """
    Limit the move to the range over which a delta+gamma expansion still means
    something.

    The quadratic term is *always positive* and grows without bound, so a large
    adverse move eventually projects a **higher** premium than the entry — on a $4
    call with delta 0.45 and gamma 0.03, a $500 drop in the underlying came out at
    $3,529. That is not a small inaccuracy at the tail; it inverts the sign of the
    answer, and it silently produced brackets where the "stop" sat above the entry
    and the computed risk was negative.

    Real delta saturates: it cannot leave [0, 1] for a call or [-1, 0] for a put. The
    expansion stays sensible exactly while ``delta + gamma * move`` is inside that
    band, so the move is clamped to that window. Beyond it the projection flattens
    rather than reversing, which is the correct qualitative behaviour — a deep
    out-of-the-money option stops responding to further moves.

    Every distance this repo actually projects (a few percent, or 2.5-3.75x ATR) sits
    well inside the window; this is a guard on the tail, not a change to normal
    output.
    """
    if gamma <= 0:
        return move
    if delta >= 0:  # call-like
        lower, upper = -delta / gamma, (1.0 - delta) / gamma
    else:  # put-like
        lower, upper = (-1.0 - delta) / gamma, -delta / gamma
    return max(lower, min(move, upper))


def estimate_holding_days(
    price_distance: Optional[float],
    daily_atr: Optional[float],
    dte: Optional[int] = None,
) -> float:
    """
    How long the underlying plausibly takes to travel ``price_distance``.

    **Diffusion scaling, not linear.** ATR is roughly a one-day standard deviation and
    volatility grows with the square root of time, so covering N times ATR takes on
    the order of N-squared days, not N::

        sigma_t = sigma_1 * sqrt(t)   =>   t = (distance / atr)^2

    Dividing distance by ATR instead — the obvious thing, and what this did first — is
    wrong twice over. It understates the hold badly (a 3.75x-ATR target reads as under
    four days when the diffusive estimate is about fourteen), and understating the hold
    understates decay, which is precisely the bias the whole decay model exists to
    remove. It is also degenerate for the standard bracket: target distance is always
    ``_RR * _STOP_ATR_MULT * atr``, so ATR cancels and every contract reports the same
    number however volatile its underlying.

    This is the conservative end of the range: a genuine trend arrives sooner than
    diffusion predicts. Erring toward a longer hold errs toward charging more decay,
    which is the right way to be wrong when the product is long premium.

    Capped at ``_MAX_HOLD_DAYS`` and never beyond the contract's own remaining life.
    """
    if price_distance is None or daily_atr is None or daily_atr <= 0:
        days = _FALLBACK_HOLD_DAYS
    else:
        days = (abs(price_distance) / daily_atr) ** 2
    days = max(0.5, min(days, _MAX_HOLD_DAYS))
    if dte is not None and dte > 0:
        days = min(days, float(dte))
    return round(days, 2)


def premium_trade_levels(
    contract: OptionContract,
    underlying_price: Optional[float],
    underlying_stop_distance: Optional[float],
    underlying_target_distance: Optional[float],
    daily_atr: Optional[float],
    dte: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Translate the underlying's ATR bracket into premium terms.

    An option position is exited on the *underlying* reaching a level, but the P&L is
    in premium, so both need to be stated. The stop is charged decay for the time the
    adverse move takes; the target is charged decay for the time the favourable move
    takes. Those are different holding periods and using one number for both would
    flatter whichever side moves faster.

    Returns ``{}`` when the bracket cannot be computed. All ``*_dollars`` values are
    per contract, i.e. already multiplied by 100.
    """
    mid = contract.mid_price
    if (
        mid is None
        or mid <= 0
        or underlying_price is None
        or not underlying_stop_distance
        or not underlying_target_distance
    ):
        return {}

    # A call profits when the underlying rises, a put when it falls.
    direction = 1.0 if contract.contract_type == "call" else -1.0

    stop_days = estimate_holding_days(underlying_stop_distance, daily_atr, dte)
    target_days = estimate_holding_days(underlying_target_distance, daily_atr, dte)

    stop_premium = project_premium(
        contract, underlying_move=-direction * underlying_stop_distance, holding_days=stop_days
    )
    target_premium = project_premium(
        contract, underlying_move=direction * underlying_target_distance, holding_days=target_days
    )
    if stop_premium is None or target_premium is None:
        return {}

    # Never imply a loss larger than this fraction of the premium: the expansion is
    # unreliable that far out, and a real position is abandoned long before zero.
    floor = mid * (1.0 - _MAX_PREMIUM_LOSS_FRACTION)
    stop_premium = max(stop_premium, floor)

    risk_per_contract = (mid - stop_premium) * 100
    reward_per_contract = (target_premium - mid) * 100
    if risk_per_contract <= 0:
        return {}

    return {
        "premium_entry": round(mid, 4),
        "premium_stop": round(stop_premium, 4),
        "premium_target": round(target_premium, 4),
        "underlying_stop": round(underlying_price - direction * underlying_stop_distance, 4),
        "underlying_target": round(underlying_price + direction * underlying_target_distance, 4),
        "risk_dollars": round(risk_per_contract, 2),
        "reward_dollars": round(reward_per_contract, 2),
        # Reward:risk in *premium* terms, which is what the position actually earns.
        # It is not the underlying's 1.5 — decay and the delta/gamma curve both bend
        # it, and on a low-delta contract they bend it a long way.
        "premium_rr": round(reward_per_contract / risk_per_contract, 3),
        "stop_hold_days": stop_days,
        "target_hold_days": target_days,
        "decay_at_target": round((contract.theta or 0.0) * target_days * 100, 2),
    }


def theta_per_premium(contract: OptionContract) -> Optional[float]:
    """
    Daily decay as a fraction of the premium paid — the rate that actually kills a
    long option.

    Returned positive for a decaying long (theta is negative), so larger is worse. A
    value of 0.02 means the position loses 2% of its value per calendar day if nothing
    moves, which over a two-week hold is a 25% headwind before the thesis is even
    tested.
    """
    mid = contract.mid_price
    if mid is None or mid <= 0 or contract.theta is None:
        return None
    return round(-contract.theta / mid, 6)


def gamma_leverage(contract: OptionContract, underlying_price: Optional[float]) -> Optional[float]:
    """
    How fast delta accelerates, scaled to be comparable across names and strikes.

    ``gamma * S^2 / premium`` is unitless: it says how much the delta exposure grows
    for a 1% underlying move, relative to what was paid. High values are cheap
    convexity; they also mean the position decays fast, which is why it travels
    alongside ``theta_per_premium`` rather than instead of it.
    """
    mid = contract.mid_price
    if mid is None or mid <= 0 or not underlying_price or contract.gamma is None:
        return None
    return round(contract.gamma * (underlying_price ** 2) / (mid * 10000), 6)


def iv_rank(current_iv: Optional[float], history: Sequence[float]) -> Optional[float]:
    """
    Where today's implied volatility sits in its own recent range, 0.0 to 1.0.

    The options analogue of relative strength: an absolute IV of 45% means nothing on
    its own, because it is cheap for one name and historically expensive for another.
    What matters for a *long* option is whether you are paying more than usual for
    this specific underlying.

    Uses the percentile of observations at or below the current value rather than the
    min/max range, so one spike in the history does not compress every later reading
    into the bottom of the scale. Returns ``None`` below a usable sample.
    """
    if current_iv is None:
        return None
    values = [v for v in history if v is not None and v > 0]
    if len(values) < 10:
        return None
    at_or_below = sum(1 for v in values if v <= current_iv)
    return round(at_or_below / len(values), 4)


def expected_move_pct(iv: Optional[float], dte: Optional[int]) -> Optional[float]:
    """
    One standard deviation of underlying movement over the contract's remaining life,
    as a percentage: ``IV * sqrt(dte / 365)``.

    Calendar days rather than trading days, matching how IV is quoted.
    """
    if not iv or not dte or dte <= 0:
        return None
    return round(iv * math.sqrt(dte / 365.0) * 100, 4)
