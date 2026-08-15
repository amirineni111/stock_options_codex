"""
Pydantic domain models.

``as_of`` defaults come from ``timeutil.utc_now`` rather than ``datetime.utcnow``:
the latter is deprecated and returns a *naive* datetime, which then sat in the same
SQLite columns as the tz-aware values written elsewhere. Mixing the two is what makes
a later duration calculation silently return nothing instead of raising.
"""
from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from .timeutil import utc_now


class OptionContract(BaseModel):
    underlying: str
    contract_ticker: str
    contract_type: str
    expiration_date: date
    strike_price: float
    bid: Optional[float] = None
    ask: Optional[float] = None
    last_price: Optional[float] = None
    open_interest: Optional[int] = None
    volume: Optional[int] = None
    implied_volatility: Optional[float] = None
    delta: Optional[float] = None
    gamma: Optional[float] = None
    theta: Optional[float] = None
    vega: Optional[float] = None
    underlying_price: Optional[float] = None
    as_of: datetime = Field(default_factory=utc_now)

    @property
    def mid_price(self) -> Optional[float]:
        if self.bid is not None and self.ask is not None and self.ask > 0:
            return round((self.bid + self.ask) / 2, 4)
        return self.last_price

    @property
    def spread_pct(self) -> Optional[float]:
        mid = self.mid_price
        if mid is None or mid <= 0 or self.bid is None or self.ask is None:
            return None
        return round(((self.ask - self.bid) / mid) * 100, 4)


class TradeLevels(BaseModel):
    """
    An entry/stop/target bracket in premium terms, per contract.

    Options are quoted per share but traded in hundreds, so ``*_dollars`` fields are
    already multiplied by 100 — the number a user compares against their fixed risk.
    """

    entry: Optional[float] = None
    stop: Optional[float] = None
    target: Optional[float] = None
    stop_dollars: Optional[float] = None
    target_dollars: Optional[float] = None
    stop_pct: Optional[float] = None
    target_pct: Optional[float] = None
    rr_ratio: Optional[float] = None
    cost_ratio: Optional[float] = None


class ScoredContract(BaseModel):
    contract: OptionContract
    score: float
    score_components: Dict[str, float]
    max_contracts_by_risk: int
    premium_at_risk: float
    breakeven: float
    reason: str
    underlying_last_price: Optional[float] = None
    sma20: Optional[float] = None
    sma50: Optional[float] = None
    trend_signal: Optional[str] = None
    trend_aligned: Optional[bool] = None
    earnings_date: Optional[date] = None
    earnings_warning: Optional[str] = None
    breakeven_distance_pct: Optional[float] = None
    expected_move_pct: Optional[float] = None
    expected_move_to_breakeven_ok: Optional[bool] = None
    favorable_2pct_value: Optional[float] = None
    favorable_2pct_pnl: Optional[float] = None
    adverse_2pct_value: Optional[float] = None
    adverse_2pct_pnl: Optional[float] = None
    decision_checklist: Optional[str] = None
    trade_signal: Optional[str] = None
    signal_reason: Optional[str] = None


class MarketContext(BaseModel):
    underlying: str
    last_price: Optional[float] = None
    sma20: Optional[float] = None
    sma50: Optional[float] = None
    trend_signal: str = "unknown"
    earnings_date: Optional[date] = None
    earnings_warning: Optional[str] = None
    # Full daily OHLCV bars, carried alongside the summary statistics so the signal
    # layer can compute ATR/ADX/structure without a second fetch. Not persisted.
    daily_bars: List[Dict[str, Any]] = Field(default_factory=list)


class RejectedContract(BaseModel):
    underlying: str
    contract_ticker: str
    contract_type: str
    reason: str
    as_of: datetime = Field(default_factory=utc_now)
