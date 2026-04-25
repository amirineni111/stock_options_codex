from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Optional

import httpx

from .models import OptionContract


class PolygonClient:
    base_url = "https://api.polygon.io"

    def __init__(self, api_key: str, timeout_seconds: float = 20.0) -> None:
        if not api_key:
            raise ValueError("POLYGON_API_KEY is required")
        self.api_key = api_key
        self.timeout_seconds = timeout_seconds

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        query = dict(params or {})
        query["apiKey"] = self.api_key
        url = f"{self.base_url}{path}"
        safe_query = dict(query)
        safe_query["apiKey"] = "REDACTED"
        safe_url = str(httpx.URL(url, params=safe_query))
        with httpx.Client(timeout=self.timeout_seconds) as client:
            try:
                response = client.get(url, params=query)
            except httpx.RequestError as exc:
                raise RuntimeError(f"Polygon API request failed for {safe_url}: {exc.__class__.__name__}") from None
            try:
                response.raise_for_status()
            except httpx.HTTPStatusError as exc:
                raise RuntimeError(f"Polygon API error {exc.response.status_code} for {safe_url}") from None
            return response.json()

    def get_option_chain_snapshots(
        self,
        underlying: str,
        expiration_gte: Optional[date] = None,
        expiration_lte: Optional[date] = None,
        limit: int = 250,
    ) -> List[OptionContract]:
        params: Dict[str, Any] = {"limit": limit}
        if expiration_gte:
            params["expiration_date.gte"] = expiration_gte.isoformat()
        if expiration_lte:
            params["expiration_date.lte"] = expiration_lte.isoformat()

        items: List[Dict[str, Any]] = []
        path = f"/v3/snapshot/options/{underlying.upper()}"
        while path:
            payload = self._get(path, params)
            items.extend(payload.get("results") or [])
            next_url = payload.get("next_url")
            if not next_url:
                break
            path = next_url.replace(self.base_url, "")
            params = {}
            if "apiKey=" in path:
                path = path.split("apiKey=")[0].rstrip("?&")

        return [self._parse_chain_snapshot(underlying, item) for item in items]

    def get_stock_price(self, ticker: str) -> Optional[float]:
        payload = self._get(f"/v2/snapshot/locale/us/markets/stocks/tickers/{ticker.upper()}")
        ticker_data = payload.get("ticker") or {}
        for section, key in [("day", "c"), ("prevDay", "c"), ("lastTrade", "p")]:
            value = (ticker_data.get(section) or {}).get(key)
            if value:
                return float(value)
        return None

    def _parse_chain_snapshot(self, underlying: str, item: Dict[str, Any]) -> OptionContract:
        details = item.get("details") or {}
        greeks = item.get("greeks") or {}
        day = item.get("day") or {}
        last_trade = item.get("last_trade") or {}
        last_quote = item.get("last_quote") or {}
        underlying_asset = item.get("underlying_asset") or {}

        last_price = _first_float(day.get("close"), day.get("last_price"), last_trade.get("price"))
        bid = _first_float(last_quote.get("bid"), item.get("bid"))
        ask = _first_float(last_quote.get("ask"), item.get("ask"))
        volume = _first_int(day.get("volume"), item.get("volume"))
        open_interest = _first_int(item.get("open_interest"), details.get("open_interest"))
        underlying_price = _first_float(underlying_asset.get("price"), item.get("underlying_price"))

        return OptionContract(
            underlying=underlying.upper(),
            contract_ticker=details.get("ticker") or item.get("ticker") or "",
            contract_type=(details.get("contract_type") or details.get("type") or "").lower(),
            expiration_date=date.fromisoformat(details["expiration_date"]),
            strike_price=float(details["strike_price"]),
            bid=bid,
            ask=ask,
            last_price=last_price,
            open_interest=open_interest,
            volume=volume,
            implied_volatility=_first_float(item.get("implied_volatility")),
            delta=_first_float(greeks.get("delta")),
            gamma=_first_float(greeks.get("gamma")),
            theta=_first_float(greeks.get("theta")),
            vega=_first_float(greeks.get("vega")),
            underlying_price=underlying_price,
            as_of=datetime.utcnow(),
        )


def _first_float(*values: Any) -> Optional[float]:
    for value in values:
        if value is None:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def _first_int(*values: Any) -> Optional[int]:
    for value in values:
        if value is None:
            continue
        try:
            return int(value)
        except (TypeError, ValueError):
            continue
    return None
