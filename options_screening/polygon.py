"""
The Polygon.io REST client — the only place this repo talks to Polygon.

Two things here that the sibling repos do not have, because their providers do not
need them:

* **Retry with jittered backoff on 429/5xx.** yfinance and OANDA tolerate a bare
  ``raise_for_status``; Polygon has hard per-minute quotas that a 500-ticker scan
  walks straight into, and losing a whole scan to one 429 is not acceptable. The
  sibling rule still holds above this layer — a failure that survives the retries
  degrades to a log row, it does not abort the scan.
* **Cursor-preserving pagination.** See ``get_option_chain_snapshots``.

The API key never appears in an exception message or a log. ``_get`` builds a
separate ``safe_url`` with the key redacted and raises with that; ``tests/test_polygon.py``
pins the behaviour.
"""
from __future__ import annotations

import random
import time as _time
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional

import httpx

from .indicators import _first_float, _first_int
from .models import MarketContext, OptionContract
from .timeutil import exchange_date, utc_now

# Retry policy. Three attempts is enough to ride out a quota boundary (Polygon's
# window is a minute, and the caller is polling on a multi-second cadence anyway)
# without turning a genuine outage into a 30-second UI freeze.
MAX_ATTEMPTS = 3
BACKOFF_BASE_SECONDS = 0.75
BACKOFF_MAX_SECONDS = 8.0
RETRY_STATUS_CODES = frozenset({408, 429, 500, 502, 503, 504})

# Polygon caps page size at 250. Pagination is additionally bounded by a page count
# so a malformed `next_url` can never spin forever.
MAX_PAGE_SIZE = 250
MAX_PAGES = 40


class PolygonClient:
    base_url = "https://api.polygon.io"

    def __init__(self, api_key: str, timeout_seconds: float = 20.0) -> None:
        if not api_key:
            raise ValueError("POLYGON_API_KEY is required")
        self.api_key = api_key
        self.timeout_seconds = timeout_seconds

    # ── Transport ────────────────────────────────────────────────────────────

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        query = dict(params or {})
        query["apiKey"] = self.api_key
        url = f"{self.base_url}{path}"

        safe_query = dict(query)
        safe_query["apiKey"] = "REDACTED"
        safe_url = str(httpx.URL(url, params=safe_query))

        last_error: Optional[str] = None
        for attempt in range(1, MAX_ATTEMPTS + 1):
            with httpx.Client(timeout=self.timeout_seconds) as client:
                try:
                    response = client.get(url, params=query)
                except httpx.RequestError as exc:
                    last_error = (
                        f"Polygon API request failed for {safe_url}: {exc.__class__.__name__}"
                    )
                    if attempt < MAX_ATTEMPTS:
                        _sleep_before_retry(attempt, None)
                        continue
                    raise RuntimeError(last_error) from None

                status = response.status_code
                if status in RETRY_STATUS_CODES and attempt < MAX_ATTEMPTS:
                    _sleep_before_retry(attempt, response.headers.get("Retry-After"))
                    continue

                try:
                    response.raise_for_status()
                except httpx.HTTPStatusError as exc:
                    # Polygon's own message is the difference between "the key is
                    # wrong" and "the plan does not include this endpoint", and a bare
                    # status code cannot tell the two apart.
                    detail = _error_detail(exc.response, self.api_key)
                    raise RuntimeError(
                        f"Polygon API error {exc.response.status_code} for {safe_url}"
                        + (f": {detail}" if detail else "")
                    ) from None
                return response.json()

        # Only reachable if every attempt hit a retryable status.
        raise RuntimeError(last_error or f"Polygon API gave up after {MAX_ATTEMPTS} attempts for {safe_url}")

    # ── Options ──────────────────────────────────────────────────────────────

    def get_option_chain_snapshots(
        self,
        underlying: str,
        expiration_gte: Optional[date] = None,
        expiration_lte: Optional[date] = None,
        page_size: int = MAX_PAGE_SIZE,
        max_contracts: Optional[int] = None,
        strike_gte: Optional[float] = None,
        strike_lte: Optional[float] = None,
    ) -> List[OptionContract]:
        """
        Every contract in the expiry window, paginated.

        ``page_size`` is Polygon's per-request ``limit``; ``max_contracts`` is the real
        cap on how many contracts come back. They used to be the same argument, which
        meant the UI's "Max contracts per ticker" set the page size and then the loop
        paginated to exhaustion anyway — the label was wrong and the API cost was
        unbounded.

        Pagination follows ``next_url`` by parsing it rather than string-slicing it.
        The previous ``path.split("apiKey=")[0]`` discarded everything after the key,
        so whenever Polygon ordered the params ``?apiKey=...&cursor=...`` the cursor
        was thrown away and page 2 re-fetched page 1 — duplicate contracts, and a loop
        that only terminated because the response kept claiming there was a next page.
        """
        params: Dict[str, Any] = {"limit": max(1, min(page_size, MAX_PAGE_SIZE))}
        if expiration_gte:
            params["expiration_date.gte"] = expiration_gte.isoformat()
        if expiration_lte:
            params["expiration_date.lte"] = expiration_lte.isoformat()
        if strike_gte is not None:
            params["strike_price.gte"] = strike_gte
        if strike_lte is not None:
            params["strike_price.lte"] = strike_lte

        items: List[Dict[str, Any]] = []
        path = f"/v3/snapshot/options/{underlying.upper()}"
        seen_cursors = set()

        for _ in range(MAX_PAGES):
            payload = self._get(path, params)
            items.extend(payload.get("results") or [])

            if max_contracts is not None and len(items) >= max_contracts:
                items = items[:max_contracts]
                break

            next_url = payload.get("next_url")
            if not next_url:
                break

            parsed = httpx.URL(next_url)
            cursor = parsed.params.get("cursor")
            # A repeated cursor means the server is handing back the same page; stop
            # rather than trusting `next_url` to eventually be absent.
            if cursor:
                if cursor in seen_cursors:
                    break
                seen_cursors.add(cursor)

            path = parsed.path
            params = {key: value for key, value in parsed.params.items() if key != "apiKey"}

        return [self._parse_chain_snapshot(underlying, item) for item in items]

    def get_option_contract_snapshot(
        self,
        underlying: str,
        contract_ticker: str,
    ) -> Optional[OptionContract]:
        """
        One contract's current snapshot, for resolving an open forward-tested trade.

        Resolution reads the contract's own mid rather than inferring a premium from
        the underlying, so theta and IV changes are in the recorded outcome instead of
        being modelled away. The call count is bounded by the number of open tracked
        positions, not by the universe size.
        """
        payload = self._get(
            f"/v3/snapshot/options/{underlying.upper()}/{contract_ticker.upper()}"
        )
        item = payload.get("results")
        if not item:
            return None
        try:
            return self._parse_chain_snapshot(underlying, item)
        except (KeyError, TypeError, ValueError):
            return None

    def get_option_bars(
        self,
        contract_ticker: str,
        start: date,
        end: date,
        minutes: int = 15,
    ) -> List[Dict[str, Any]]:
        """
        Intraday OHLCV bars for one contract, oldest first, as canonical bar dicts.

        This is what lets an option forward-test be resolved against the contract's
        own *path* rather than whatever mid a scan happened to observe: a target or
        stop touched and retraced between two scans is visible in a bar's high/low and
        invisible in a snapshot. Available on the Options Starter plan, which has no
        quotes or trades.
        """
        payload = self._get(
            f"/v2/aggs/ticker/{contract_ticker.upper()}/range/{int(minutes)}/minute/"
            f"{start.isoformat()}/{end.isoformat()}",
            {"adjusted": "true", "sort": "asc", "limit": 50000},
        )
        return _aggs_to_bars(payload)

    # ── Equities ─────────────────────────────────────────────────────────────

    def get_stock_price(self, ticker: str) -> Optional[float]:
        payload = self._get(f"/v2/snapshot/locale/us/markets/stocks/tickers/{ticker.upper()}")
        ticker_data = payload.get("ticker") or {}
        for section, key in [("day", "c"), ("prevDay", "c"), ("lastTrade", "p")]:
            value = (ticker_data.get(section) or {}).get(key)
            if value:
                return float(value)
        return None

    def get_stock_snapshots(self, tickers: List[str]) -> List[Dict[str, Any]]:
        if not tickers:
            return []
        try:
            payload = self._get(
                "/v2/snapshot/locale/us/markets/stocks/tickers",
                {"tickers": ",".join(t.upper() for t in tickers), "include_otc": "false"},
            )
            return payload.get("tickers") or []
        except RuntimeError as exc:
            # A 403 here means the plan does not include the bulk snapshot endpoint,
            # which is a permissions fact rather than a transient failure — fall back
            # to per-ticker. Anything else is a real error and must surface.
            if "Polygon API error 403" not in str(exc):
                raise

        snapshots = []
        for ticker in tickers:
            payload = self._get(f"/v2/snapshot/locale/us/markets/stocks/tickers/{ticker.upper()}")
            ticker_data = payload.get("ticker")
            if ticker_data:
                snapshots.append(ticker_data)
        return snapshots

    def get_market_context(
        self,
        ticker: str,
        start: date,
        end: date,
        earnings_end: Optional[date] = None,
        check_earnings: bool = False,
        today: Optional[date] = None,
    ) -> MarketContext:
        bars = self.get_daily_bars(ticker, start, end)
        closes = [bar["close"] for bar in bars]
        last_price = closes[-1] if closes else self.get_stock_price(ticker)
        sma20 = _simple_average(closes[-20:]) if len(closes) >= 20 else None
        sma50 = _simple_average(closes[-50:]) if len(closes) >= 50 else None
        trend_signal = _trend_signal(last_price, sma20, sma50)

        earnings_date = None
        earnings_warning = "not checked"
        if check_earnings:
            try:
                earnings_date = self.get_next_earnings_date(
                    ticker, today or exchange_date(), earnings_end or end
                )
                earnings_warning = (
                    "before expiration" if earnings_date else "none found before expiration"
                )
            except RuntimeError as exc:
                earnings_warning = str(exc)

        return MarketContext(
            underlying=ticker.upper(),
            last_price=last_price,
            sma20=sma20,
            sma50=sma50,
            trend_signal=trend_signal,
            earnings_date=earnings_date,
            earnings_warning=earnings_warning,
            daily_bars=bars,
        )

    def get_daily_bars(self, ticker: str, start: date, end: date) -> List[Dict[str, Any]]:
        """
        Daily OHLCV bars as the canonical bar dicts the indicator layer expects.

        Full bars rather than closes alone: ATR and ADX need highs and lows, and the
        stop distance the whole risk model is quoted in is an ATR multiple.
        """
        payload = self._get(
            f"/v2/aggs/ticker/{ticker.upper()}/range/1/day/{start.isoformat()}/{end.isoformat()}",
            {"adjusted": "true", "sort": "asc", "limit": 5000},
        )
        return _aggs_to_bars(payload)

    def get_daily_closes(self, ticker: str, start: date, end: date) -> List[float]:
        return [bar["close"] for bar in self.get_daily_bars(ticker, start, end)]

    def get_next_earnings_date(self, ticker: str, start: date, end: date) -> Optional[date]:
        payload = self._get(
            "/benzinga/v1/earnings",
            {
                "ticker": ticker.upper(),
                "date.gte": start.isoformat(),
                "date.lte": end.isoformat(),
                "sort": "date.asc",
                "limit": 1,
            },
        )
        results = payload.get("results") or []
        if not results:
            return None
        raw_date = results[0].get("date")
        return date.fromisoformat(raw_date) if raw_date else None

    # ── Parsing ──────────────────────────────────────────────────────────────

    def _parse_chain_snapshot(self, underlying: str, item: Dict[str, Any]) -> OptionContract:
        details = item.get("details") or {}
        greeks = item.get("greeks") or {}
        day = item.get("day") or {}
        last_trade = item.get("last_trade") or {}
        last_quote = item.get("last_quote") or {}
        underlying_asset = item.get("underlying_asset") or {}
        # Nanoseconds since the epoch: the time of the day bar's last update, which on
        # a plan without quotes is the time of the price being used.
        last_updated = _first_float(day.get("last_updated"), last_trade.get("sip_timestamp"))

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
            last_trade_at=_epoch_ns_to_datetime(last_updated),
            as_of=utc_now(),
        )


# ── Module helpers ───────────────────────────────────────────────────────────


def _sleep_before_retry(attempt: int, retry_after: Optional[str]) -> None:
    """
    Exponential backoff with full jitter, honouring ``Retry-After`` when the server
    sends one. Jitter matters because a scan fans out across tickers: without it every
    retry lands in the same instant and re-triggers the same quota.
    """
    if retry_after:
        try:
            _time.sleep(min(float(retry_after), BACKOFF_MAX_SECONDS))
            return
        except (TypeError, ValueError):
            pass
    ceiling = min(BACKOFF_BASE_SECONDS * (2 ** (attempt - 1)), BACKOFF_MAX_SECONDS)
    _time.sleep(random.uniform(0.0, ceiling))


def _error_detail(response: Any, api_key: str) -> str:
    """Polygon's explanation from an error body, key redacted. Empty if there is none."""
    try:
        payload = response.json()
    except Exception:
        return ""
    if not isinstance(payload, dict):
        return ""
    detail = str(payload.get("message") or payload.get("error") or "")
    return detail.replace(api_key, "REDACTED")[:300]


def _aggs_to_bars(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """An aggregates payload as the canonical bar dicts the indicator layer expects."""
    bars: List[Dict[str, Any]] = []
    for item in payload.get("results") or []:
        close = _first_float(item.get("c"))
        if close is None:
            continue
        bars.append(
            {
                "timestamp": _epoch_ms_to_iso(item.get("t")),
                "open": _first_float(item.get("o")),
                "high": _first_float(item.get("h")),
                "low": _first_float(item.get("l")),
                "close": close,
                "volume": _first_float(item.get("v")) or 0.0,
            }
        )
    return bars


def _epoch_ns_to_datetime(value: Optional[float]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromtimestamp(value / 1e9, tz=timezone.utc)
    except (OverflowError, ValueError, OSError):
        return None


def _epoch_ms_to_iso(value: Any) -> Optional[str]:
    ms = _first_float(value)
    if ms is None:
        return None
    try:
        return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).isoformat()
    except (OverflowError, ValueError, OSError):
        return None


def _simple_average(values: List[float]) -> Optional[float]:
    if not values:
        return None
    return round(sum(values) / len(values), 4)


def _trend_signal(last_price: Optional[float], sma20: Optional[float], sma50: Optional[float]) -> str:
    if last_price is None or sma20 is None or sma50 is None:
        return "unknown"
    if last_price > sma20 > sma50:
        return "bullish"
    if last_price < sma20 < sma50:
        return "bearish"
    return "mixed"
