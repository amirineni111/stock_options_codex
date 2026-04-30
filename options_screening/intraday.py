from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from pydantic import BaseModel
import requests

from .config import AppSettings
from .polygon import PolygonClient

YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
YAHOO_HEADERS = {"User-Agent": "Mozilla/5.0"}


class IntradayScanRequest(BaseModel):
    tickers: List[str]
    mode: str = "Both"
    min_price: float = 5.0
    max_price: float = 1000.0
    min_relative_volume: float = 0.05
    min_day_change_pct: float = 0.5
    max_spread_pct: float = 1.0
    include_shorts: bool = True


class IntradayResult(BaseModel):
    rank: int = 0
    ticker: str
    last_price: Optional[float] = None
    day_change_pct: Optional[float] = None
    volume: Optional[int] = None
    relative_volume: Optional[float] = None
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    prev_close: Optional[float] = None
    minute_price: Optional[float] = None
    spread_pct: Optional[float] = None
    signal_mode: str = "None"
    momentum_score: float = 0.0
    mean_reversion_score: float = 0.0
    total_score: float = 0.0
    trade_signal: str = "WATCH_ONLY"
    signal_reason: str = ""
    risk_notes: str = ""
    as_of: datetime


class IntradayScanSummary(BaseModel):
    scanned: int = 0
    accepted: int = 0
    watch: int = 0
    avoid: int = 0
    errors: int = 0


def run_intraday_scan(settings: AppSettings, request: IntradayScanRequest) -> Tuple[List[IntradayResult], IntradayScanSummary, List[Dict]]:
    client = PolygonClient(settings.polygon_api_key, settings.request_timeout_seconds)
    summary = IntradayScanSummary(scanned=len(request.tickers))
    logs: List[Dict] = []
    results: List[IntradayResult] = []

    provider = "polygon"
    try:
        snapshots = client.get_stock_snapshots(request.tickers)
    except Exception as exc:
        if "Polygon API error 403" not in str(exc):
            summary.errors = len(request.tickers)
            return [], summary, [{"ticker": "ALL", "error": str(exc), "created_at": datetime.utcnow().isoformat()}]
        provider = "yahoo"
        snapshots, fallback_logs = _fetch_yahoo_intraday_snapshots(request.tickers)
        logs.extend(fallback_logs)

    seen = set()
    for snapshot in snapshots:
        ticker = (snapshot.get("ticker") or "").upper()
        if ticker:
            seen.add(ticker)
        try:
            result = score_intraday_snapshot(snapshot, request)
            results.append(result)
            if result.trade_signal == "AVOID":
                summary.avoid += 1
            elif result.trade_signal == "WATCH_ONLY":
                summary.watch += 1
            else:
                summary.accepted += 1
            logs.append({"ticker": ticker, "signal": result.trade_signal, "error": None, "created_at": result.as_of.isoformat(), "provider": provider})
        except Exception as exc:
            summary.errors += 1
            logs.append({"ticker": ticker or "UNKNOWN", "signal": None, "error": str(exc), "created_at": datetime.utcnow().isoformat(), "provider": provider})

    missing = [ticker for ticker in request.tickers if ticker.upper() not in seen]
    for ticker in missing:
        summary.errors += 1
        logs.append({"ticker": ticker.upper(), "signal": None, "error": "No snapshot returned", "created_at": datetime.utcnow().isoformat(), "provider": provider})

    results.sort(key=lambda item: item.total_score, reverse=True)
    for index, result in enumerate(results, start=1):
        result.rank = index
    return results, summary, logs


def _fetch_yahoo_intraday_snapshots(tickers: List[str]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    snapshots = []
    logs = [{"ticker": "ALL", "signal": None, "error": "Polygon stock snapshots denied; using Yahoo Finance delayed chart fallback", "created_at": datetime.utcnow().isoformat(), "provider": "yahoo"}]
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(_fetch_yahoo_snapshot, ticker): ticker for ticker in tickers}
        for future in as_completed(futures):
            ticker = futures[future]
            try:
                snapshots.append(future.result())
            except Exception as exc:
                logs.append({"ticker": ticker.upper(), "signal": None, "error": str(exc), "created_at": datetime.utcnow().isoformat(), "provider": "yahoo"})
    return snapshots, logs


def _fetch_yahoo_snapshot(ticker: str) -> Dict[str, Any]:
    response = requests.get(
        YAHOO_CHART_URL.format(ticker=ticker.upper()),
        params={"range": "5d", "interval": "15m", "includePrePost": "false"},
        headers=YAHOO_HEADERS,
        timeout=15,
    )
    response.raise_for_status()
    payload = response.json()
    result = ((payload.get("chart") or {}).get("result") or [None])[0]
    if not result:
        raise RuntimeError("Yahoo Finance returned no chart result")
    return _yahoo_chart_to_snapshot(ticker, result)


def _yahoo_chart_to_snapshot(ticker: str, result: Dict[str, Any]) -> Dict[str, Any]:
    meta = result.get("meta") or {}
    timestamps = result.get("timestamp") or []
    quote = (((result.get("indicators") or {}).get("quote") or [{}])[0]) or {}
    timezone = ZoneInfo(meta.get("exchangeTimezoneName") or "America/New_York")
    rows = []
    for index, timestamp in enumerate(timestamps):
        close = _list_value(quote.get("close"), index)
        if close is None:
            continue
        rows.append(
            {
                "time": datetime.fromtimestamp(timestamp, tz=timezone),
                "open": _list_value(quote.get("open"), index),
                "high": _list_value(quote.get("high"), index),
                "low": _list_value(quote.get("low"), index),
                "close": close,
                "volume": _list_value(quote.get("volume"), index) or 0,
            }
        )
    if not rows:
        raise RuntimeError("Yahoo Finance returned no usable 15-minute bars")

    latest_day = rows[-1]["time"].date()
    today_rows = [row for row in rows if row["time"].date() == latest_day]
    previous_rows = [row for row in rows if row["time"].date() < latest_day]
    if not today_rows:
        today_rows = rows[-1:]
    prev_close = _first_float(meta.get("previousClose"))
    if previous_rows:
        prev_close = previous_rows[-1]["close"]
    prev_volume = sum(int(row["volume"] or 0) for row in previous_rows if row["time"].date() == (previous_rows[-1]["time"].date() if previous_rows else latest_day))

    last = today_rows[-1]
    day_volume = sum(int(row["volume"] or 0) for row in today_rows)
    return {
        "ticker": ticker.upper(),
        "todaysChangePerc": ((last["close"] - prev_close) / prev_close * 100.0) if prev_close else None,
        "day": {
            "o": today_rows[0]["open"],
            "h": max(row["high"] for row in today_rows if row["high"] is not None),
            "l": min(row["low"] for row in today_rows if row["low"] is not None),
            "c": last["close"],
            "v": day_volume,
        },
        "prevDay": {"c": prev_close, "v": prev_volume},
        "min": {"c": last["close"]},
        "lastTrade": {"p": last["close"]},
        "lastQuote": {},
    }


def score_intraday_snapshot(snapshot: Dict[str, Any], request: IntradayScanRequest) -> IntradayResult:
    ticker = (snapshot.get("ticker") or "").upper()
    day = snapshot.get("day") or {}
    prev_day = snapshot.get("prevDay") or {}
    minute = snapshot.get("min") or {}
    last_trade = snapshot.get("lastTrade") or {}
    last_quote = snapshot.get("lastQuote") or {}

    last_price = _first_float(last_trade.get("p"), minute.get("c"), day.get("c"), prev_day.get("c"))
    open_price = _first_float(day.get("o"))
    high = _first_float(day.get("h"))
    low = _first_float(day.get("l"))
    prev_close = _first_float(prev_day.get("c"))
    minute_price = _first_float(minute.get("c"))
    volume = _first_int(day.get("v"))
    prev_volume = _first_int(prev_day.get("v"))
    day_change_pct = _first_float(snapshot.get("todaysChangePerc"))
    if day_change_pct is None and last_price is not None and prev_close:
        day_change_pct = ((last_price - prev_close) / prev_close) * 100.0
    relative_volume = (volume / prev_volume) if volume is not None and prev_volume else None
    spread_pct = _spread_pct(last_quote, last_price)

    momentum_score, momentum_side, momentum_reason = _momentum(snapshot, request, last_price, open_price, day_change_pct, relative_volume, minute_price)
    reversion_score, reversion_side, reversion_reason = _mean_reversion(request, last_price, high, low, day_change_pct, relative_volume)

    mode = request.mode
    candidates = []
    if mode in {"Momentum", "Both"}:
        candidates.append(("Momentum", momentum_score, momentum_side, momentum_reason))
    if mode in {"Mean Reversion", "Both"}:
        candidates.append(("Mean Reversion", reversion_score, reversion_side, reversion_reason))
    signal_mode, total_score, side, signal_reason = max(candidates, key=lambda item: item[1]) if candidates else ("None", 0.0, "watch", "No mode selected")

    trade_signal, final_reason, risk_notes = _classify_signal(
        request=request,
        side=side,
        signal_reason=signal_reason,
        last_price=last_price,
        volume=volume,
        relative_volume=relative_volume,
        day_change_pct=day_change_pct,
        spread_pct=spread_pct,
    )
    if trade_signal in {"WATCH_ONLY", "AVOID"}:
        total_score = min(total_score, 49.0 if trade_signal == "WATCH_ONLY" else 0.0)

    return IntradayResult(
        ticker=ticker,
        last_price=_round(last_price),
        day_change_pct=_round(day_change_pct),
        volume=volume,
        relative_volume=_round(relative_volume),
        open=_round(open_price),
        high=_round(high),
        low=_round(low),
        prev_close=_round(prev_close),
        minute_price=_round(minute_price),
        spread_pct=_round(spread_pct),
        signal_mode=signal_mode,
        momentum_score=round(momentum_score, 2),
        mean_reversion_score=round(reversion_score, 2),
        total_score=round(total_score, 2),
        trade_signal=trade_signal,
        signal_reason=final_reason,
        risk_notes=risk_notes,
        as_of=datetime.utcnow(),
    )


def _momentum(
    snapshot: Dict[str, Any],
    request: IntradayScanRequest,
    last_price: Optional[float],
    open_price: Optional[float],
    day_change_pct: Optional[float],
    relative_volume: Optional[float],
    minute_price: Optional[float],
) -> Tuple[float, str, str]:
    if last_price is None or open_price is None or day_change_pct is None:
        return 0.0, "watch", "Missing price or day-change data"
    bullish = day_change_pct >= request.min_day_change_pct and last_price >= open_price
    bearish = request.include_shorts and day_change_pct <= -request.min_day_change_pct and last_price <= open_price
    if not bullish and not bearish:
        return 0.0, "watch", "Momentum conditions not met"

    direction = 1 if bullish else -1
    change_score = min(abs(day_change_pct) / max(request.min_day_change_pct, 0.01), 3.0) / 3.0 * 35.0
    relvol_score = min((relative_volume or 0.0) / max(request.min_relative_volume, 0.01), 3.0) / 3.0 * 30.0
    open_score = min(abs(last_price - open_price) / max(open_price, 0.01) * 100.0, 2.0) / 2.0 * 20.0
    minute_score = 15.0 if minute_price is not None and (last_price - minute_price) * direction >= 0 else 7.5
    side = "long" if bullish else "short"
    return change_score + relvol_score + open_score + minute_score, side, f"{side} momentum: day change and price-vs-open aligned"


def _mean_reversion(
    request: IntradayScanRequest,
    last_price: Optional[float],
    high: Optional[float],
    low: Optional[float],
    day_change_pct: Optional[float],
    relative_volume: Optional[float],
) -> Tuple[float, str, str]:
    if last_price is None or high is None or low is None or high <= low or day_change_pct is None:
        return 0.0, "watch", "Missing range or day-change data"
    range_position = (last_price - low) / (high - low)
    oversold = day_change_pct <= -request.min_day_change_pct and range_position <= 0.30
    overbought = request.include_shorts and day_change_pct >= request.min_day_change_pct and range_position >= 0.70
    if not oversold and not overbought:
        return 0.0, "watch", "Mean reversion conditions not met"

    change_score = min(abs(day_change_pct) / max(request.min_day_change_pct, 0.01), 3.0) / 3.0 * 35.0
    relvol_score = min((relative_volume or 0.0) / max(request.min_relative_volume, 0.01), 3.0) / 3.0 * 25.0
    extension_score = (1.0 - range_position) * 30.0 if oversold else range_position * 30.0
    side = "long" if oversold else "short"
    return change_score + relvol_score + extension_score + 10.0, side, f"{side} mean reversion: price extended near day {'low' if oversold else 'high'}"


def _classify_signal(
    request: IntradayScanRequest,
    side: str,
    signal_reason: str,
    last_price: Optional[float],
    volume: Optional[int],
    relative_volume: Optional[float],
    day_change_pct: Optional[float],
    spread_pct: Optional[float],
) -> Tuple[str, str, str]:
    avoid = []
    watch = []
    risk = ["Decision-support only; no broker execution."]

    if last_price is None:
        avoid.append("last price unavailable")
    elif last_price < request.min_price or last_price > request.max_price:
        avoid.append("outside price range")
    if volume is None:
        watch.append("volume unavailable")
    if relative_volume is None:
        watch.append("relative volume unavailable")
    elif relative_volume < request.min_relative_volume:
        watch.append("relative volume below threshold")
    if day_change_pct is None:
        watch.append("day change unavailable")
    elif abs(day_change_pct) < request.min_day_change_pct:
        watch.append("day change below threshold")
    if spread_pct is not None and spread_pct > request.max_spread_pct:
        avoid.append("spread above threshold")
    elif spread_pct is None:
        risk.append("Bid/ask unavailable; verify execution quality.")
    if side == "short" and not request.include_shorts:
        avoid.append("short candidates disabled")

    if avoid:
        return "AVOID", "; ".join(avoid), " ".join(risk)
    if watch or side == "watch":
        return "WATCH_ONLY", "; ".join(watch or [signal_reason]), " ".join(risk)
    if side == "long" and "mean reversion" in signal_reason:
        return "MEAN_REVERSION_LONG", signal_reason, " ".join(risk)
    if side == "short" and "mean reversion" in signal_reason:
        risk.append("Short selling can create large losses; use only if approved and risk-controlled.")
        return "MEAN_REVERSION_SHORT", signal_reason, " ".join(risk)
    if side == "short":
        risk.append("Short selling can create large losses; use only if approved and risk-controlled.")
        return "SHORT_CANDIDATE", signal_reason, " ".join(risk)
    return "BUY_CANDIDATE", signal_reason, " ".join(risk)


def _spread_pct(last_quote: Dict[str, Any], last_price: Optional[float]) -> Optional[float]:
    bid = _first_float(last_quote.get("p"), last_quote.get("bid"), last_quote.get("bid_price"))
    ask = _first_float(last_quote.get("P"), last_quote.get("ask"), last_quote.get("ask_price"))
    if bid is None or ask is None or ask <= 0:
        return None
    mid = (bid + ask) / 2.0
    if mid <= 0:
        return None
    return ((ask - bid) / mid) * 100.0


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


def _list_value(values, index: int):
    if not values or index >= len(values):
        return None
    return values[index]


def _round(value: Optional[float]) -> Optional[float]:
    return round(value, 4) if value is not None else None
