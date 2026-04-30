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
    use_rsi_confirmation: bool = True
    use_trend_confirmation: bool = True
    rsi_period: int = 14


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
    rsi14: Optional[float] = None
    ema9: Optional[float] = None
    ema20: Optional[float] = None
    macd: Optional[float] = None
    macd_signal: Optional[float] = None
    macd_histogram: Optional[float] = None
    vwap: Optional[float] = None
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
        snapshots, fallback_logs = _fetch_yahoo_intraday_snapshots(request.tickers, request.rsi_period)
        logs.extend(fallback_logs)
    else:
        if request.use_rsi_confirmation or request.use_trend_confirmation:
            indicator_logs = _enrich_snapshots_with_yahoo_indicators(snapshots, request.rsi_period)
            logs.extend(indicator_logs)

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


def _fetch_yahoo_intraday_snapshots(tickers: List[str], rsi_period: int = 14) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    snapshots = []
    logs = [{"ticker": "ALL", "signal": None, "error": "Polygon stock snapshots denied; using Yahoo Finance delayed chart fallback", "created_at": datetime.utcnow().isoformat(), "provider": "yahoo"}]
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(_fetch_yahoo_snapshot, ticker, rsi_period): ticker for ticker in tickers}
        for future in as_completed(futures):
            ticker = futures[future]
            try:
                snapshots.append(future.result())
            except Exception as exc:
                logs.append({"ticker": ticker.upper(), "signal": None, "error": str(exc), "created_at": datetime.utcnow().isoformat(), "provider": "yahoo"})
    return snapshots, logs


def _enrich_snapshots_with_yahoo_indicators(snapshots: List[Dict[str, Any]], period: int) -> List[Dict[str, Any]]:
    logs = []
    by_ticker = {(snapshot.get("ticker") or "").upper(): snapshot for snapshot in snapshots if snapshot.get("ticker")}
    indicator_keys = ("rsi14", "ema9", "ema20", "macd", "macd_signal", "macd_histogram", "vwap")
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(_fetch_yahoo_snapshot, ticker, period): ticker for ticker in by_ticker}
        for future in as_completed(futures):
            ticker = futures[future]
            try:
                yahoo_snapshot = future.result()
                for key in indicator_keys:
                    value = _first_float(yahoo_snapshot.get(key))
                    if value is not None:
                        by_ticker[ticker][key] = value
            except Exception as exc:
                logs.append(
                    {
                        "ticker": ticker,
                        "signal": None,
                        "error": f"technical indicators unavailable from Yahoo: {exc}",
                        "created_at": datetime.utcnow().isoformat(),
                        "provider": "yahoo",
                    }
                )
    return logs


def _fetch_yahoo_snapshot(ticker: str, rsi_period: int = 14) -> Dict[str, Any]:
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
    return _yahoo_chart_to_snapshot(ticker, result, rsi_period)


def _yahoo_chart_to_snapshot(ticker: str, result: Dict[str, Any], rsi_period: int = 14) -> Dict[str, Any]:
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
    closes = [row["close"] for row in rows if row["close"] is not None]
    macd, macd_signal, macd_histogram = _calculate_macd(closes)
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
        "rsi14": _calculate_rsi(closes, rsi_period),
        "ema9": _calculate_ema(closes, 9),
        "ema20": _calculate_ema(closes, 20),
        "macd": macd,
        "macd_signal": macd_signal,
        "macd_histogram": macd_histogram,
        "vwap": _calculate_vwap(today_rows),
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
    rsi14 = _first_float(snapshot.get("rsi14"), snapshot.get("rsi"))
    ema9 = _first_float(snapshot.get("ema9"))
    ema20 = _first_float(snapshot.get("ema20"))
    macd = _first_float(snapshot.get("macd"))
    macd_signal = _first_float(snapshot.get("macd_signal"))
    macd_histogram = _first_float(snapshot.get("macd_histogram"), snapshot.get("macd_hist"))
    vwap = _first_float(snapshot.get("vwap"))
    volume = _first_int(day.get("v"))
    prev_volume = _first_int(prev_day.get("v"))
    day_change_pct = _first_float(snapshot.get("todaysChangePerc"))
    if day_change_pct is None and last_price is not None and prev_close:
        day_change_pct = ((last_price - prev_close) / prev_close) * 100.0
    relative_volume = (volume / prev_volume) if volume is not None and prev_volume else None
    spread_pct = _spread_pct(last_quote, last_price)

    momentum_score, momentum_side, momentum_reason = _momentum(
        request,
        last_price,
        open_price,
        day_change_pct,
        relative_volume,
        minute_price,
        rsi14,
        ema9,
        ema20,
        macd_histogram,
        vwap,
    )
    reversion_score, reversion_side, reversion_reason = _mean_reversion(
        request,
        last_price,
        high,
        low,
        day_change_pct,
        relative_volume,
        rsi14,
    )

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
        rsi14=_round(rsi14),
        ema9=_round(ema9),
        ema20=_round(ema20),
        macd=_round(macd),
        macd_signal=_round(macd_signal),
        macd_histogram=_round(macd_histogram),
        vwap=_round(vwap),
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
    request: IntradayScanRequest,
    last_price: Optional[float],
    open_price: Optional[float],
    day_change_pct: Optional[float],
    relative_volume: Optional[float],
    minute_price: Optional[float],
    rsi14: Optional[float],
    ema9: Optional[float],
    ema20: Optional[float],
    macd_histogram: Optional[float],
    vwap: Optional[float],
) -> Tuple[float, str, str]:
    if last_price is None or open_price is None or day_change_pct is None:
        return 0.0, "watch", "Missing price or day-change data"
    bullish = day_change_pct >= request.min_day_change_pct and last_price >= open_price
    bearish = request.include_shorts and day_change_pct <= -request.min_day_change_pct and last_price <= open_price
    if not bullish and not bearish:
        return 0.0, "watch", "Momentum conditions not met"

    direction = 1 if bullish else -1
    side = "long" if bullish else "short"
    rsi_score, rsi_reason, rsi_blocks = _momentum_rsi_score(side, rsi14, request.use_rsi_confirmation)
    if rsi_blocks:
        return 0.0, "watch", rsi_reason
    technical_score, technical_reason, technical_blocks = _momentum_technical_score(
        side,
        last_price,
        ema9,
        ema20,
        macd_histogram,
        vwap,
        request.use_trend_confirmation,
    )
    if technical_blocks:
        return 0.0, "watch", technical_reason

    change_score = min(abs(day_change_pct) / max(request.min_day_change_pct, 0.01), 3.0) / 3.0 * 25.0
    relvol_score = min((relative_volume or 0.0) / max(request.min_relative_volume, 0.01), 3.0) / 3.0 * 20.0
    open_score = min(abs(last_price - open_price) / max(open_price, 0.01) * 100.0, 2.0) / 2.0 * 15.0
    minute_score = 10.0 if minute_price is not None and (last_price - minute_price) * direction >= 0 else 5.0
    total = change_score + relvol_score + open_score + minute_score + rsi_score + technical_score
    return total, side, f"{side} momentum: day change, price-vs-open, {rsi_reason}, and {technical_reason}"


def _mean_reversion(
    request: IntradayScanRequest,
    last_price: Optional[float],
    high: Optional[float],
    low: Optional[float],
    day_change_pct: Optional[float],
    relative_volume: Optional[float],
    rsi14: Optional[float],
) -> Tuple[float, str, str]:
    if last_price is None or high is None or low is None or high <= low or day_change_pct is None:
        return 0.0, "watch", "Missing range or day-change data"
    range_position = (last_price - low) / (high - low)
    oversold = day_change_pct <= -request.min_day_change_pct and range_position <= 0.30
    overbought = request.include_shorts and day_change_pct >= request.min_day_change_pct and range_position >= 0.70
    if not oversold and not overbought:
        return 0.0, "watch", "Mean reversion conditions not met"

    side = "long" if oversold else "short"
    rsi_score, rsi_reason, rsi_blocks = _reversion_rsi_score(side, rsi14, request.use_rsi_confirmation)
    if rsi_blocks:
        return 0.0, "watch", rsi_reason

    change_score = min(abs(day_change_pct) / max(request.min_day_change_pct, 0.01), 3.0) / 3.0 * 30.0
    relvol_score = min((relative_volume or 0.0) / max(request.min_relative_volume, 0.01), 3.0) / 3.0 * 20.0
    extension_score = (1.0 - range_position) * 30.0 if oversold else range_position * 30.0
    total = change_score + relvol_score + extension_score + rsi_score
    return total, side, f"{side} mean reversion: price extended near day {'low' if oversold else 'high'} and {rsi_reason}"


def _momentum_rsi_score(side: str, rsi14: Optional[float], use_confirmation: bool) -> Tuple[float, str, bool]:
    if not use_confirmation:
        return 15.0, "RSI confirmation disabled", False
    if rsi14 is None:
        return 7.5, "RSI unavailable", False
    if side == "long":
        if 50.0 <= rsi14 <= 70.0:
            return 15.0, f"RSI14 {rsi14:.1f} confirms bullish momentum", False
        if 45.0 <= rsi14 < 50.0 or 70.0 < rsi14 <= 80.0:
            return 7.5, f"RSI14 {rsi14:.1f} is a soft bullish confirmation", False
        return 0.0, f"RSI14 {rsi14:.1f} does not confirm bullish momentum", True
    if 30.0 <= rsi14 <= 50.0:
        return 15.0, f"RSI14 {rsi14:.1f} confirms bearish momentum", False
    if 20.0 <= rsi14 < 30.0 or 50.0 < rsi14 <= 55.0:
        return 7.5, f"RSI14 {rsi14:.1f} is a soft bearish confirmation", False
    return 0.0, f"RSI14 {rsi14:.1f} does not confirm bearish momentum", True


def _reversion_rsi_score(side: str, rsi14: Optional[float], use_confirmation: bool) -> Tuple[float, str, bool]:
    if not use_confirmation:
        return 20.0, "RSI confirmation disabled", False
    if rsi14 is None:
        return 10.0, "RSI unavailable", False
    if side == "long":
        if rsi14 <= 30.0:
            return 20.0, f"RSI14 {rsi14:.1f} confirms oversold conditions", False
        if rsi14 <= 45.0:
            return max(0.0, (45.0 - rsi14) / 15.0 * 20.0), f"RSI14 {rsi14:.1f} is mildly oversold", False
        return 0.0, f"RSI14 {rsi14:.1f} is not oversold enough for long mean reversion", True
    if rsi14 >= 70.0:
        return 20.0, f"RSI14 {rsi14:.1f} confirms overbought conditions", False
    if rsi14 >= 55.0:
        return max(0.0, (rsi14 - 55.0) / 15.0 * 20.0), f"RSI14 {rsi14:.1f} is mildly overbought", False
    return 0.0, f"RSI14 {rsi14:.1f} is not overbought enough for short mean reversion", True


def _momentum_technical_score(
    side: str,
    last_price: Optional[float],
    ema9: Optional[float],
    ema20: Optional[float],
    macd_histogram: Optional[float],
    vwap: Optional[float],
    use_confirmation: bool,
) -> Tuple[float, str, bool]:
    if not use_confirmation:
        return 15.0, "EMA/MACD/VWAP confirmation disabled", False
    checks = []
    missing = []

    if last_price is not None and ema9 is not None and ema20 is not None:
        ema_aligned = last_price >= ema9 >= ema20 if side == "long" else last_price <= ema9 <= ema20
        checks.append(("EMA9/EMA20", ema_aligned))
    else:
        missing.append("EMA")

    if macd_histogram is not None:
        macd_aligned = macd_histogram >= 0.0 if side == "long" else macd_histogram <= 0.0
        checks.append(("MACD histogram", macd_aligned))
    else:
        missing.append("MACD")

    if last_price is not None and vwap is not None:
        vwap_aligned = last_price >= vwap if side == "long" else last_price <= vwap
        checks.append(("VWAP", vwap_aligned))
    else:
        missing.append("VWAP")

    if not checks:
        return 7.5, "EMA/MACD/VWAP unavailable", False

    aligned_count = sum(1 for _, aligned in checks if aligned)
    if aligned_count == len(checks):
        reason = "EMA/MACD/VWAP confirm momentum"
        if missing:
            reason = f"{reason}; missing {', '.join(missing)}"
        return 15.0, reason, False
    if aligned_count == 0 and len(checks) >= 2:
        failed = ", ".join(name for name, _ in checks)
        return 0.0, f"{failed} do not confirm {side} momentum", True

    score = aligned_count / len(checks) * 15.0
    aligned = ", ".join(name for name, is_aligned in checks if is_aligned)
    failed = ", ".join(name for name, is_aligned in checks if not is_aligned)
    reason = f"mixed EMA/MACD/VWAP confirmation"
    if aligned:
        reason = f"{reason}; aligned: {aligned}"
    if failed:
        reason = f"{reason}; weak: {failed}"
    if missing:
        reason = f"{reason}; missing: {', '.join(missing)}"
    return score, reason, False


def _calculate_rsi(closes: List[float], period: int = 14) -> Optional[float]:
    if period <= 0 or len(closes) <= period:
        return None
    changes = []
    for previous, current in zip(closes[:-1], closes[1:]):
        change = current - previous
        changes.append(change)
    gains = [max(change, 0.0) for change in changes[:period]]
    losses = [max(-change, 0.0) for change in changes[:period]]
    average_gain = sum(gains) / period
    average_loss = sum(losses) / period
    for change in changes[period:]:
        average_gain = ((average_gain * (period - 1)) + max(change, 0.0)) / period
        average_loss = ((average_loss * (period - 1)) + max(-change, 0.0)) / period
    if average_loss == 0:
        return 100.0 if average_gain > 0 else 50.0
    rs = average_gain / average_loss
    return round(100.0 - (100.0 / (1.0 + rs)), 4)


def _calculate_ema(closes: List[float], period: int) -> Optional[float]:
    series = _calculate_ema_series(closes, period)
    return series[-1] if series else None


def _calculate_ema_series(values: List[float], period: int) -> List[Optional[float]]:
    if period <= 0 or len(values) < period:
        return []
    series: List[Optional[float]] = [None] * len(values)
    ema = sum(values[:period]) / period
    series[period - 1] = ema
    multiplier = 2.0 / (period + 1.0)
    for index in range(period, len(values)):
        ema = (values[index] - ema) * multiplier + ema
        series[index] = ema
    return series


def _calculate_macd(closes: List[float]) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    fast = _calculate_ema_series(closes, 12)
    slow = _calculate_ema_series(closes, 26)
    if not fast or not slow:
        return None, None, None

    macd_values = []
    for fast_value, slow_value in zip(fast, slow):
        if fast_value is not None and slow_value is not None:
            macd_values.append(fast_value - slow_value)
    if len(macd_values) < 9:
        return None, None, None

    signal = _calculate_ema(macd_values, 9)
    macd = macd_values[-1]
    if signal is None:
        return None, None, None
    return round(macd, 4), round(signal, 4), round(macd - signal, 4)


def _calculate_vwap(rows: List[Dict[str, Any]]) -> Optional[float]:
    total_price_volume = 0.0
    total_volume = 0.0
    for row in rows:
        volume = _first_float(row.get("volume"))
        if volume is None or volume <= 0:
            continue
        high = _first_float(row.get("high"))
        low = _first_float(row.get("low"))
        close = _first_float(row.get("close"))
        if close is None:
            continue
        typical_price = (high + low + close) / 3.0 if high is not None and low is not None else close
        total_price_volume += typical_price * volume
        total_volume += volume
    if total_volume <= 0:
        return None
    return round(total_price_volume / total_volume, 4)


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
