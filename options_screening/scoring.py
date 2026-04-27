from datetime import date
import math
from typing import List, Optional, Tuple

from .models import MarketContext, OptionContract, RejectedContract, ScoredContract


def days_to_expiration(contract: OptionContract, today: date = None) -> int:
    base = today or date.today()
    return (contract.expiration_date - base).days


def score_contract(
    contract: OptionContract,
    settings,
    market_context: Optional[MarketContext] = None,
) -> Tuple[ScoredContract, RejectedContract]:
    rejection = _validate_contract(contract, settings, market_context)
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
    if contract.spread_pct is None:
        spread_score = 0.0
    else:
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

    decision_context = _decision_context(contract, settings, market_context, breakeven, max_contracts, premium_at_risk)

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
) -> Tuple[List[ScoredContract], List[RejectedContract]]:
    accepted: List[ScoredContract] = []
    rejected: List[RejectedContract] = []
    for contract in contracts:
        scored, rejection = score_contract(contract, settings, market_context)
        if scored:
            accepted.append(scored)
        elif rejection:
            rejected.append(rejection)
    accepted.sort(key=lambda item: item.score, reverse=True)
    return accepted, rejected


def _validate_contract(contract: OptionContract, settings, market_context: Optional[MarketContext] = None) -> RejectedContract:
    reasons = []
    if contract.contract_type not in {"call", "put"}:
        reasons.append("unsupported contract type")
    dte = days_to_expiration(contract)
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
    if getattr(settings, "require_trend_alignment", False) and not _trend_aligned(contract, market_context):
        reasons.append("trend not aligned")
    if getattr(settings, "avoid_earnings_before_expiration", False) and market_context and market_context.earnings_date:
        if date.today() <= market_context.earnings_date <= contract.expiration_date:
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


def _decision_context(
    contract: OptionContract,
    settings,
    market_context: Optional[MarketContext],
    breakeven: float,
    max_contracts: int,
    premium_at_risk: float,
) -> dict:
    underlying_price = _underlying_price(contract, market_context)
    dte = days_to_expiration(contract)
    iv = contract.implied_volatility or 0.0
    expected_move_pct = round(iv * math.sqrt(max(dte, 0) / 365.0) * 100, 2) if iv and dte > 0 else None
    breakeven_distance_pct = _breakeven_distance_pct(contract, breakeven, underlying_price)
    expected_move_ok = None
    if expected_move_pct is not None and breakeven_distance_pct is not None:
        expected_move_ok = breakeven_distance_pct <= expected_move_pct

    favorable_value, favorable_pnl = _scenario_value(contract, underlying_price, max_contracts, premium_at_risk, 0.02)
    adverse_value, adverse_pnl = _scenario_value(contract, underlying_price, max_contracts, premium_at_risk, -0.02)
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
        "expected_move_pct": expected_move_pct,
        "expected_move_to_breakeven_ok": expected_move_ok,
        "favorable_2pct_value": favorable_value,
        "favorable_2pct_pnl": favorable_pnl,
        "adverse_2pct_value": adverse_value,
        "adverse_2pct_pnl": adverse_pnl,
        "decision_checklist": _decision_checklist(contract, trend_aligned, expected_move_ok, market_context),
        **_trade_signal(contract, settings, market_context, trend_aligned, expected_move_ok),
    }


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
) -> Tuple[Optional[float], Optional[float]]:
    if underlying_price is None or underlying_price <= 0 or max_contracts <= 0 or contract.mid_price is None:
        return None, None
    signed_move_pct = move_pct
    if contract.contract_type == "put":
        signed_move_pct = -move_pct
    underlying_move = underlying_price * signed_move_pct
    delta = contract.delta or 0.0
    gamma = contract.gamma or 0.0
    estimated_option_price = max(0.0, contract.mid_price + delta * underlying_move + 0.5 * gamma * underlying_move * underlying_move)
    estimated_value = round(estimated_option_price * 100 * max_contracts, 2)
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
) -> dict:
    avoid_reasons = []
    watch_reasons = []

    ignore_missing_spread = getattr(settings, "ignore_missing_spread_for_signal", False)
    if contract.spread_pct is None and not ignore_missing_spread:
        watch_reasons.append("bid/ask spread unavailable")
    elif contract.spread_pct is None and ignore_missing_spread:
        pass
    elif contract.spread_pct > min(settings.max_spread_pct, 20.0):
        watch_reasons.append("bid/ask spread is wide")

    if trend_aligned is False:
        watch_reasons.append("trend is not aligned")
    elif trend_aligned is None:
        watch_reasons.append("trend is unknown")

    if expected_move_ok is False:
        watch_reasons.append("breakeven is beyond rough expected move")
    elif expected_move_ok is None:
        watch_reasons.append("expected move is unknown")

    if market_context and market_context.earnings_date:
        watch_reasons.append("earnings before expiration")

    if contract.volume is None or contract.open_interest is None:
        watch_reasons.append("liquidity data incomplete")
    elif contract.volume < max(settings.min_volume, 10) or contract.open_interest < max(settings.min_open_interest, 100):
        watch_reasons.append("liquidity is thin")

    if contract.mid_price is None or contract.mid_price <= 0:
        avoid_reasons.append("option price unavailable")
    if contract.mid_price and contract.mid_price * 100 > settings.fixed_risk:
        avoid_reasons.append("one contract exceeds fixed risk")

    if avoid_reasons:
        return {"trade_signal": "AVOID", "signal_reason": "; ".join(avoid_reasons)}

    if watch_reasons:
        signal = "WATCH_ONLY"
        if _covered_or_cash_secured_candidate(contract, trend_aligned):
            signal = _income_signal(contract)
        return {"trade_signal": signal, "signal_reason": "; ".join(watch_reasons)}

    if contract.contract_type == "call":
        return {"trade_signal": "BUY_CALL_CANDIDATE", "signal_reason": "trend, liquidity, spread, and expected move checks passed"}
    if contract.contract_type == "put":
        return {"trade_signal": "BUY_PUT_CANDIDATE", "signal_reason": "trend, liquidity, spread, and expected move checks passed"}
    return {"trade_signal": "AVOID", "signal_reason": "unsupported contract type"}


def _covered_or_cash_secured_candidate(contract: OptionContract, trend_aligned: Optional[bool]) -> bool:
    return trend_aligned is False and contract.spread_pct is not None


def _income_signal(contract: OptionContract) -> str:
    if contract.contract_type == "call":
        return "COVERED_CALL_ONLY"
    if contract.contract_type == "put":
        return "CASH_SECURED_PUT_ONLY"
    return "WATCH_ONLY"


def _round_optional(value: Optional[float]) -> Optional[float]:
    return round(value, 2) if value is not None else None
