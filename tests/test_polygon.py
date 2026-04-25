import httpx
import pytest

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


def test_polygon_http_errors_redact_api_key(monkeypatch):
    api_key = "secret-polygon-key"

    class FakeResponse:
        status_code = 403

        def raise_for_status(self):
            request = httpx.Request(
                "GET",
                f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers/AAPL?apiKey={api_key}",
            )
            response = httpx.Response(403, request=request)
            raise httpx.HTTPStatusError("forbidden", request=request, response=response)

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def get(self, *args, **kwargs):
            return FakeResponse()

    monkeypatch.setattr(httpx, "Client", FakeClient)

    client = PolygonClient(api_key)
    with pytest.raises(RuntimeError) as exc_info:
        client.get_stock_price("AAPL")

    message = str(exc_info.value)
    assert api_key not in message
    assert "apiKey=REDACTED" in message
