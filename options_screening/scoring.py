from datetime import date
from typing import List, Tuple

from .models import OptionContract, RejectedContract, ScoredContract


def days_to_expiration(contract: OptionContract, today: date = None) -> int:
    base = today or date.today()
    return (contract.expiration_date - base).days


def score_contract(contract: OptionContract, settings) -> Tuple[ScoredContract, RejectedContract]:
    rejection = _validate_contract(contract, settings)
    if rejection:
        return None, rejection

    spread_pct = contract.spread_pct or 0.0
    abs_delta = abs(contract.delta or 0.0)
    dte = days_to_expiration(contract)
    iv = contract.implied_volatility or 0.0
    volume = contract.volume or 0
    oi = contract.open_interest or 0
    mid = contract.mid_price or 0.0

    liquidity_score = min(25.0, (min(volume / max(settings.min_volume, 1), 2.0) / 2.0) * 10.0 + (min(oi / max(settings.min_open_interest, 1), 2.0) / 2.0) * 15.0)
    spread_score = max(0.0, 25.0 * (1.0 - spread_pct / settings.max_spread_pct))
    delta_midpoint = (settings.min_abs_delta + settings.max_abs_delta) / 2.0
    delta_width = max((settings.max_abs_delta - settings.min_abs_delta) / 2.0, 0.01)
    delta_score = max(0.0, 20.0 * (1.0 - abs(abs_delta - delta_midpoint) / delta_width))
    dte_midpoint = (settings.min_days_to_expiration + settings.max_days_to_expiration) / 2.0
    dte_width = max((settings.max_days_to_expiration - settings.min_days_to_expiration) / 2.0, 1.0)
    expiration_score = max(0.0, 15.0 * (1.0 - abs(dte - dte_midpoint) / dte_width))
    iv_score = max(0.0, 15.0 * (1.0 - (iv - settings.min_iv) / max(settings.max_iv - settings.min_iv, 0.01)))

    score = round(liquidity_score + spread_score + delta_score + expiration_score + iv_score, 2)
    max_contracts = int(settings.fixed_risk // (mid * 100)) if mid > 0 else 0
    premium_at_risk = round(max_contracts * mid * 100, 2)
    if contract.contract_type == "call":
        breakeven = contract.strike_price + mid
    else:
        breakeven = contract.strike_price - mid

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
        reason="Accepted: liquid, defined-risk premium, and within conservative swing filters.",
    )
    return result, None


def score_contracts(contracts: List[OptionContract], settings) -> Tuple[List[ScoredContract], List[RejectedContract]]:
    accepted: List[ScoredContract] = []
    rejected: List[RejectedContract] = []
    for contract in contracts:
        scored, rejection = score_contract(contract, settings)
        if scored:
            accepted.append(scored)
        elif rejection:
            rejected.append(rejection)
    accepted.sort(key=lambda item: item.score, reverse=True)
    return accepted, rejected


def _validate_contract(contract: OptionContract, settings) -> RejectedContract:
    reasons = []
    if contract.contract_type not in {"call", "put"}:
        reasons.append("unsupported contract type")
    dte = days_to_expiration(contract)
    if dte < settings.min_days_to_expiration or dte > settings.max_days_to_expiration:
        reasons.append("outside DTE range")
    if contract.mid_price is None or contract.mid_price <= 0:
        reasons.append("missing positive price")
    if contract.spread_pct is None or contract.spread_pct > settings.max_spread_pct:
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

    if not reasons:
        return None
    return RejectedContract(
        underlying=contract.underlying,
        contract_ticker=contract.contract_ticker,
        contract_type=contract.contract_type,
        reason=", ".join(reasons),
        as_of=contract.as_of,
    )
