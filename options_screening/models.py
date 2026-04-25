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


class RejectedContract(BaseModel):
    underlying: str
    contract_ticker: str
    contract_type: str
    reason: str
    as_of: datetime = Field(default_factory=datetime.utcnow)
