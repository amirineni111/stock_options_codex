from options_screening.polygon import PolygonClient


def test_parse_polygon_chain_snapshot():
    client = PolygonClient("test")
    item = {
        "details": {
            "ticker": "O:AAPL260619C00200000",
            "contract_type": "call",
            "expiration_date": "2026-06-19",
            "strike_price": 200,
        },
        "last_quote": {"bid": 2.4, "ask": 2.6},
        "day": {"close": 2.5, "volume": 125},
        "open_interest": 1200,
        "implied_volatility": 0.43,
        "greeks": {"delta": 0.41, "gamma": 0.02, "theta": -0.05, "vega": 0.14},
        "underlying_asset": {"price": 195.1},
    }

    contract = client._parse_chain_snapshot("AAPL", item)

    assert contract.contract_ticker == "O:AAPL260619C00200000"
    assert contract.contract_type == "call"
    assert contract.mid_price == 2.5
    assert contract.spread_pct == 8.0
    assert contract.delta == 0.41
