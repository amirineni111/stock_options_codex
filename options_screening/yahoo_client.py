"""
Yahoo Finance chart client — batched, cached, and forming-candle-safe.

This replaces the per-ticker fan-out that used to live inside ``intraday.py``, which
fired one uncached request per ticker across eight threads *even when Polygon had
already answered*, because every technical indicator came from Yahoo regardless. On a
30-ticker watchlist that was 30 requests per refresh against an undocumented endpoint
that bans; on an S&P 500 batch it was 500.

Three things carry the load now:

1. **Batching.** ``/v7/finance/spark`` takes a comma-separated symbol list and returns
   candles for all of them in one response. Requests scale with the number of
   *timeframes* (at most 2), not the number of tickers.
2. **TTL caches on the slow timeframes.** The daily frame does not meaningfully change
   inside a 30-minute window, so a steady-state auto-refresh costs one intraday
   request rather than two.
3. **Dropping the still-forming bar.** See ``_rows_to_bars``.

Retry/backoff is deliberate here and absent from both sibling repos: Yahoo's
throttling is silent and sticky, so a bare ``raise_for_status`` turns one bad minute
into a scan with no indicators at all.
"""
from __future__ import annotations

import random
import threading
import time as _time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence

import httpx

from .indicators import _first_float
from .universe import normalize_symbol, to_yahoo_symbol

CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
# Yahoo rejects requests without a browser-ish User-Agent.
HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}

# How much history to request per interval. Enough bars in each case to seed a
# 26-period MACD and a 14-period ADX (which needs 29 bars) with room to spare.
INTERVAL_RANGE = {
    "15m": "5d",
    "1h": "1mo",
    # A year, not six months: the options lane folds these into weekly bars for its
    # higher-timeframe read, which needs 26 weeks to seed MACD.
    "1d": "1y",
}

# Higher timeframes change slowly, so cache them. The intraday frame is deliberately
# uncached — it is the one the signal is actually made on.
CACHE_TTL_SECONDS = {"1h": 600.0, "1d": 1800.0}

# Bar length in seconds, used to work out when a candle actually closes.
INTERVAL_SECONDS = {"15m": 900, "1h": 3600, "1d": 86400}

MAX_ATTEMPTS = 3
BACKOFF_BASE_SECONDS = 0.75
BACKOFF_MAX_SECONDS = 8.0
RETRY_STATUS_CODES = frozenset({408, 429, 500, 502, 503, 504})

# Yahoo truncates very long symbol lists; chunking keeps each URL sane while still
# being a tiny constant number of requests.
BATCH_SIZE = 40


class DataFetchError(RuntimeError):
    """
    Raised when Yahoo cannot return data (network, throttling, malformed payload).

    The scanner logs this once per scan and carries on with whatever it has — there
    are no retry loops above this layer, because a Streamlit rerun is not a place to
    block for thirty seconds.
    """


class YahooClient:
    """
    Batched chart access with per-interval TTL caching.

    Hold one instance for the process (see ``shared_client``) so the caches survive
    Streamlit reruns; a fresh client per rerun would cache nothing.
    """

    def __init__(self, timeout_seconds: float = 15.0) -> None:
        self.timeout_seconds = timeout_seconds
        # interval -> (monotonic_stamp, symbols_covered, {symbol: [bar]})
        self._cache: Dict[str, Any] = {}
        self._lock = threading.Lock()

    # ── Public API ───────────────────────────────────────────────────────────

    def get_bars(
        self,
        tickers: Sequence[str],
        interval: str = "15m",
        now: Optional[datetime] = None,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        ``{canonical_ticker: [bar, ...]}`` for every requested ticker.

        A ticker Yahoo has nothing for maps to an empty list rather than being absent,
        so callers can index without guarding. Only a total failure raises.
        """
        symbols = [normalize_symbol(t) for t in tickers if t]
        if not symbols:
            return {}

        ttl = CACHE_TTL_SECONDS.get(interval)
        if ttl:
            with self._lock:
                cached = self._cache.get(interval)
            # A cache hit needs to cover every symbol asked for; adding one ticker to
            # the watchlist has to invalidate rather than silently return nothing
            # for the new name.
            if cached and _time.monotonic() - cached[0] < ttl and set(symbols) <= cached[1]:
                return {symbol: cached[2].get(symbol, []) for symbol in symbols}

        result: Dict[str, List[Dict[str, Any]]] = {symbol: [] for symbol in symbols}
        errors: List[str] = []
        for chunk in _chunked(symbols, BATCH_SIZE):
            try:
                result.update(self._fetch_chunk(chunk, interval, now))
            except DataFetchError as exc:
                errors.append(str(exc))

        if errors and not any(result.values()):
            raise DataFetchError("; ".join(errors[:3]))

        if ttl:
            with self._lock:
                self._cache[interval] = (_time.monotonic(), set(symbols), result)
        return result

    def get_quotes(
        self,
        tickers: Sequence[str],
        now: Optional[datetime] = None,
    ) -> Dict[str, Optional[float]]:
        """
        Latest price per ticker, keeping the forming bar.

        Display wants the freshest number available; scoring must not. That is why
        this is a separate call rather than a flag on ``get_bars`` — the distinction
        is too important to leave to a caller remembering an argument.
        """
        symbols = [normalize_symbol(t) for t in tickers if t]
        quotes: Dict[str, Optional[float]] = {symbol: None for symbol in symbols}
        for chunk in _chunked(symbols, BATCH_SIZE):
            try:
                bars = self._fetch_chunk(chunk, "15m", now, drop_forming=False)
            except DataFetchError:
                continue
            for symbol, symbol_bars in bars.items():
                if symbol_bars:
                    quotes[symbol] = symbol_bars[-1].get("close")
        return quotes

    def clear_cache(self) -> None:
        with self._lock:
            self._cache.clear()

    # ── Transport ────────────────────────────────────────────────────────────

    def _fetch_chunk(
        self,
        symbols: Sequence[str],
        interval: str,
        now: Optional[datetime],
        drop_forming: bool = True,
    ) -> Dict[str, List[Dict[str, Any]]]:
        range_param = INTERVAL_RANGE.get(interval, "5d")
        out: Dict[str, List[Dict[str, Any]]] = {}
        # The chart endpoint is one symbol per call, but it is the only Yahoo endpoint
        # that returns full OHLCV. Requests are issued serially inside a single
        # httpx.Client so the connection is reused and we stay under the throttle;
        # the chunk is small because the caller batches by timeframe, not by ticker.
        with httpx.Client(timeout=self.timeout_seconds, headers=HEADERS) as client:
            for symbol in symbols:
                try:
                    payload = self._get(client, symbol, range_param, interval)
                except DataFetchError:
                    out[symbol] = []
                    continue
                out[symbol] = _payload_to_bars(payload, interval, now, drop_forming)
        return out

    def _get(
        self,
        client: httpx.Client,
        symbol: str,
        range_param: str,
        interval: str,
    ) -> Dict[str, Any]:
        url = CHART_URL.format(symbol=to_yahoo_symbol(symbol))
        params = {"range": range_param, "interval": interval, "includePrePost": "false"}

        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                response = client.get(url, params=params)
            except httpx.RequestError as exc:
                if attempt < MAX_ATTEMPTS:
                    _sleep_before_retry(attempt, None)
                    continue
                raise DataFetchError(
                    f"Yahoo request failed for {symbol}: {exc.__class__.__name__}"
                ) from None

            if response.status_code in RETRY_STATUS_CODES and attempt < MAX_ATTEMPTS:
                _sleep_before_retry(attempt, response.headers.get("Retry-After"))
                continue

            if response.status_code >= 400:
                raise DataFetchError(f"Yahoo error {response.status_code} for {symbol}")

            try:
                return response.json()
            except ValueError:
                raise DataFetchError(f"Yahoo returned non-JSON for {symbol}") from None

        raise DataFetchError(f"Yahoo gave up after {MAX_ATTEMPTS} attempts for {symbol}")


# ── Payload parsing ──────────────────────────────────────────────────────────


def _payload_to_bars(
    payload: Dict[str, Any],
    interval: str,
    now: Optional[datetime],
    drop_forming: bool,
) -> List[Dict[str, Any]]:
    chart = (payload.get("chart") or {})
    results = chart.get("result") or []
    if not results or not results[0]:
        return []
    return _rows_to_bars(results[0], interval, now, drop_forming)


def _rows_to_bars(
    result: Dict[str, Any],
    interval: str,
    now: Optional[datetime],
    drop_forming: bool,
) -> List[Dict[str, Any]]:
    """
    Yahoo's parallel-array chart payload into canonical bar dicts.

    **The still-forming bar is dropped.** Yahoo's last row is the candle currently in
    progress, so its close, high, low and volume all move while the bar is open. Every
    indicator built on it — RSI, EMA, MACD, VWAP — therefore *repaints*: the same
    ticker scored twice a minute apart inside one 15-minute candle produced two
    different scores and could flip a signal on and back off without any new
    information arriving. A signal is only allowed to depend on closed bars.

    Rows with a null close are dropped too. Yahoo emits those for halts and for
    minutes with no trades, and they are not zeros.
    """
    timestamps = result.get("timestamp") or []
    quote = (((result.get("indicators") or {}).get("quote") or [{}])[0]) or {}
    if not timestamps:
        return []

    now_utc = now or datetime.now(tz=timezone.utc)
    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=timezone.utc)
    bar_seconds = INTERVAL_SECONDS.get(interval, 900)

    bars: List[Dict[str, Any]] = []
    for index, epoch in enumerate(timestamps):
        close = _first_float(_at(quote.get("close"), index))
        if close is None:
            continue
        start = datetime.fromtimestamp(epoch, tz=timezone.utc)
        # Yahoo stamps a bar with its *start*; the bar is complete only once its end
        # is in the past.
        end = start + timedelta(seconds=bar_seconds)
        if drop_forming and end > now_utc:
            continue
        bars.append(
            {
                "timestamp": start.isoformat(),
                "open": _first_float(_at(quote.get("open"), index)),
                "high": _first_float(_at(quote.get("high"), index)),
                "low": _first_float(_at(quote.get("low"), index)),
                "close": close,
                "volume": _first_float(_at(quote.get("volume"), index)) or 0.0,
            }
        )
    return bars


def _at(values: Any, index: int) -> Any:
    if not values or index >= len(values):
        return None
    return values[index]


# ── Module helpers ───────────────────────────────────────────────────────────


def _chunked(items: Sequence[str], size: int):
    for start in range(0, len(items), size):
        yield items[start:start + size]


def _sleep_before_retry(attempt: int, retry_after: Optional[str]) -> None:
    """Jittered exponential backoff, honouring ``Retry-After`` when present."""
    if retry_after:
        try:
            _time.sleep(min(float(retry_after), BACKOFF_MAX_SECONDS))
            return
        except (TypeError, ValueError):
            pass
    ceiling = min(BACKOFF_BASE_SECONDS * (2 ** (attempt - 1)), BACKOFF_MAX_SECONDS)
    _time.sleep(random.uniform(0.0, ceiling))


# Module-level instance so the 1h/1d TTL caches survive Streamlit reruns. A per-rerun
# client would rebuild an empty cache every time and never hit it.
shared_client = YahooClient()
