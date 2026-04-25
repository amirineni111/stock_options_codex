from datetime import date, timedelta

from options_screening.models import OptionContract
from options_screening.scanner import ScanRequest
from options_screening.scoring import score_contract


def _contract(**overrides):
    data = {
        "underlying": "AAPL",
        "contract_ticker": "O:AAPL260619C00200000",
        "contract_type": "call",
        "expiration_date": date.today() + timedelta(days=45),
        "strike_price": 200.0,
        "bid": 2.4,
        "ask": 2.6,
        "last_price": 2.5,
        "open_interest": 1000,
        "volume": 200,
        "implied_volatility": 0.45,
        "delta": 0.42,
        "underlying_price": 195.0,
    }
    data.update(overrides)
    return OptionContract(**data)


def test_scores_valid_contract():
    request = ScanRequest(tickers=["AAPL"], fixed_risk=500)
    scored, rejected = score_contract(_contract(), request)

    assert rejected is None
    assert scored.score > 0
    assert scored.max_contracts_by_risk == 2
    assert scored.premium_at_risk == 500
    assert scored.breakeven == 202.5


def test_rejects_wide_spread():
    request = ScanRequest(tickers=["AAPL"], max_spread_pct=5)
    scored, rejected = score_contract(_contract(bid=1.0, ask=2.0), request)

    assert scored is None
    assert "spread" in rejected.reason


def test_rejects_contract_above_fixed_risk():
    request = ScanRequest(tickers=["AAPL"], fixed_risk=100)
    scored, rejected = score_contract(_contract(bid=2.4, ask=2.6), request)

    assert scored is None
    assert "fixed risk" in rejected.reason
