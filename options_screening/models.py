from datetime import date, datetime
from typing import Dict, Optional

from pydantic import BaseModel, Field


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
    as_of: datetime = Field(default_factory=datetime.utcnow)

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


class MarketContext(BaseModel):
    underlying: str
    last_price: Optional[float] = None
    sma20: Optional[float] = None
    sma50: Optional[float] = None
    trend_signal: str = "unknown"
    earnings_date: Optional[date] = None
    earnings_warning: Optional[str] = None


class RejectedContract(BaseModel):
    underlying: str
    contract_ticker: str
    contract_type: str
    reason: str
    as_of: datetime = Field(default_factory=datetime.utcnow)
