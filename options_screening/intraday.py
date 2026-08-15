"""
The intraday equity lane: fetch -> indicators -> score -> rank.

This is orchestration only. Indicator maths lives in ``indicators.py`` and the scoring
engine in ``signals.py``, both of which the options lane also uses; the data client is
``yahoo_client.py``. Previously all three were inlined here, which is why the option
scorer could not reach the indicator functions and why nothing cross-checked them
against the sibling repos.

The scan runs in two phases, following the forex sibling:

* **Phase 1 is parallel and pure** — fetch, compute indicators, produce a provisional
  score. No shared state, no writes.
* **Phase 2 is sequential** — anything needing cross-ticker state (relative strength
  needs SPY, which needs every ticker's day change) and then a **full rescore** through
  the same scorer. Rescoring rather than patching the result is what keeps the
  displayed score, the decided score, and the trained score one number.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Tuple

from pydantic import BaseModel

from . import indicators as ind
from .config import AppSettings
from .features import (
    INTRADAY_FEATURE_NAMES,
    INTRADAY_FEATURE_VERSION,
    build_intraday_features,
    to_vector,
)
from .market_hours import (
    current_market_phase,
    market_open_today_utc,
    minutes_since_open,
    minutes_to_close,
    opening_range_end_utc,
)
from .polygon import PolygonClient
from .relative_strength import calculate_rs, rs_assessment, rs_bonus
from .signals import MIN_TARGET_PCT, score_ticker
from .timeutil import parse_ts, utc_now
from .training import load_serving_model
from .universe import normalize_symbol
from .yahoo_client import DataFetchError, shared_client

# The benchmark relative strength is measured against. Fetched with the watchlist in
# the same batched request, so it costs nothing extra.
BENCHMARK = "SPY"

# The timeframe signals are made on, and the two higher timeframes that must confirm.
SIGNAL_INTERVAL = "15m"
HOURLY_INTERVAL = "1h"
DAILY_INTERVAL = "1d"

# Parallel width for the pure compute phase. Deliberately modest: the point of
# batching the fetches was to stop hammering Yahoo, and a wide pool here would only
# help if fetching were still per-ticker, which it is not.
MAX_WORKERS = 4

# Minimum closed bars before a ticker is scoreable at all — enough to seed a
# 26-period MACD and leave the ADX something to smooth.
MIN_BARS = 30

# A 15-minute regular session is 26 bars (09:30-16:00).
SESSION_BARS = 26

# Forward-testing in the stocks sibling found entries armed in the opening hour were
# the biggest loss bucket (27% win rate, -0.33R per trade; removing them flipped the
# whole system positive). Signals in this window still display — they are simply not
# armed as trades.
OPEN_CHOP_MINUTES = 60.0


class IntradayScanRequest(BaseModel):
    tickers: List[str]
    min_price: float = 5.0
    max_price: float = 1000.0
    # 1.0 is "normal volume for this time of day". The old default was 0.05, which was
    # not a threshold at all — it was compensation for a relative-volume calculation
    # that divided a partial session's volume by a whole prior session's. That maths is
    # fixed (`indicators.calculate_relative_volume` is bar-for-bar now), so the
    # threshold can mean what it says.
    min_relative_volume: float = 1.0
    min_avg_dollar_volume: float = 10_000_000.0
    include_shorts: bool = True
    use_higher_timeframes: bool = True
    use_relative_strength: bool = True


class IntradayResult(BaseModel):
    # `model_prob` / `model_mode` collide with pydantic's reserved `model_` prefix.
    # The names are the ones the sibling repos and the DB columns use, so the guard is
    # relaxed rather than the fields renamed.
    model_config = {"protected_namespaces": ()}

    rank: int = 0
    ticker: str
    last_price: Optional[float] = None
    day_change_pct: Optional[float] = None
    volume: Optional[int] = None
    relative_volume: Optional[float] = None
    avg_dollar_volume: Optional[float] = None
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    prev_close: Optional[float] = None
    rsi14: Optional[float] = None
    ema9: Optional[float] = None
    ema20: Optional[float] = None
    macd: Optional[float] = None
    macd_signal: Optional[float] = None
    macd_histogram: Optional[float] = None
    atr14: Optional[float] = None
    adx14: Optional[float] = None
    vwap: Optional[float] = None
    spread_pct: Optional[float] = None

    # Scoring
    regime: str = "UNKNOWN"
    dominant: str = "NEUTRAL"
    momentum_score: float = 0.0
    reversion_score: float = 0.0
    breakout_score: float = 0.0
    mtf_score: float = 0.0
    mtf_confluence: str = "NONE"
    sr_score: float = 0.0
    total_score: float = 0.0

    # Structure
    at_key_level: bool = False
    blocked_ahead: bool = False
    nearest_support: Optional[float] = None
    nearest_resistance: Optional[float] = None
    extension_atr: Optional[float] = None

    # Relative strength
    rs_vs_spy: Optional[float] = None
    rs_assessment: Optional[str] = None

    # Trade levels and cost
    suggested_entry: Optional[float] = None
    suggested_stop: Optional[float] = None
    suggested_target: Optional[float] = None
    stop_dollars: Optional[float] = None
    target_dollars: Optional[float] = None
    stop_pct: Optional[float] = None
    target_pct: Optional[float] = None
    rr_ratio: Optional[float] = None
    cost_pct: Optional[float] = None
    cost_ratio: Optional[float] = None
    model_prob: Optional[float] = None
    required_prob: Optional[float] = None

    trade_signal: str = "WATCH_ONLY"
    signal_reason: str = ""
    risk_notes: str = ""
    market_phase: Optional[str] = None
    bar_timestamp: Optional[str] = None
    as_of: datetime


class IntradayScanSummary(BaseModel):
    scanned: int = 0
    accepted: int = 0
    watch: int = 0
    avoid: int = 0
    errors: int = 0


def run_intraday_scan(
    settings: AppSettings,
    request: IntradayScanRequest,
    now: Optional[datetime] = None,
    client=None,
    storage=None,
) -> Tuple[List[IntradayResult], IntradayScanSummary, List[Dict[str, Any]]]:
    """
    Run one scan. When ``storage`` is supplied the scan also closes the loop: open
    forward-tests are resolved against the fresh bars *before* new signals are armed,
    so a setup cannot be re-armed in the same pass that stops it out.
    """
    now = now or utc_now()
    client = client or shared_client
    summary = IntradayScanSummary(scanned=len(request.tickers))
    logs: List[Dict[str, Any]] = []

    tickers = [normalize_symbol(t) for t in request.tickers if t]
    fetch_list = sorted(set(tickers) | {BENCHMARK})

    # ── Phase 1: parallel, pure, no shared state ─────────────────────────────
    frames = _fetch_frames(client, fetch_list, request, now, logs)
    if not frames.get(SIGNAL_INTERVAL):
        summary.errors = len(tickers)
        return [], summary, logs

    phase = current_market_phase(now)
    open_utc = market_open_today_utc(now)
    or_end_utc = opening_range_end_utc(now)
    mins_to_close = minutes_to_close(now)
    spreads = _observed_spreads(settings, tickers, logs, now)

    model = model_mode = None
    if storage is not None:
        try:
            model, model_mode = load_serving_model(storage, "intraday")
        except Exception as exc:
            logs.append(_log("ALL", None, f"model load failed, serving rules only: {exc}", now))

    contexts: Dict[str, Dict[str, Any]] = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(_build_context, ticker, frames, request, open_utc, or_end_utc): ticker
            for ticker in fetch_list
        }
        for future in as_completed(futures):
            ticker = futures[future]
            try:
                context = future.result()
                if context:
                    contexts[ticker] = context
            except Exception as exc:  # one bad ticker must never abort a scan
                logs.append(_log(ticker, None, f"context build failed: {exc}", now))

    # ── Phase 2: sequential — cross-ticker state, then a full rescore ────────
    spy_change = (contexts.get(BENCHMARK) or {}).get("day_change_pct")

    results: List[IntradayResult] = []
    for ticker in tickers:
        context = contexts.get(ticker)
        if context is None:
            summary.errors += 1
            logs.append(_log(ticker, None, "no usable bars returned", now))
            continue
        try:
            result = _score_context(
                ticker=ticker,
                context=context,
                request=request,
                spy_change=spy_change,
                phase=phase,
                mins_to_close=mins_to_close,
                observed_spread_pct=spreads.get(ticker),
                now=now,
                model=model,
                model_mode=model_mode,
            )
        except Exception as exc:
            summary.errors += 1
            logs.append(_log(ticker, None, str(exc), now))
            continue

        results.append(result)
        if result.trade_signal == "AVOID":
            summary.avoid += 1
        elif result.trade_signal == "WATCH_ONLY":
            summary.watch += 1
        else:
            summary.accepted += 1
        logs.append(_log(ticker, result.trade_signal, None, now))

    # ── Phase 3: close the loop ─────────────────────────────────────────────
    # Resolve first, then arm. The other order would let a setup that just stopped
    # out be re-armed in the same pass, before the cooldown has anything to act on.
    if storage is not None:
        signal_bars = frames.get(SIGNAL_INTERVAL) or {}
        for ticker in tickers:
            try:
                storage.resolve_intraday_signals(ticker, signal_bars.get(ticker) or [], now=now)
            except Exception as exc:
                logs.append(_log(ticker, None, f"tracking eval failed: {exc}", now))
        for result in results:
            try:
                _arm_signal(storage, result, now, model_mode)
            except Exception as exc:
                logs.append(_log(result.ticker, None, f"tracking record failed: {exc}", now))

    results.sort(key=lambda item: item.total_score, reverse=True)
    for index, result in enumerate(results, start=1):
        result.rank = index
    return results, summary, logs


def _arm_signal(
    storage,
    result: IntradayResult,
    now: datetime,
    model_mode: Optional[str] = None,
) -> None:
    """Arm one result for forward testing, if it clears the gates."""
    armable, _ = is_armable(result, now)
    if not armable:
        return
    storage.record_tracked_signal(
        lane="intraday",
        ticker=result.ticker,
        signal=result.trade_signal,
        direction=1 if result.dominant == "LONG" else -1,
        entry=result.suggested_entry,
        stop=result.suggested_stop,
        target=result.suggested_target,
        entry_ts=result.bar_timestamp or now.isoformat(),
        stop_dollars=result.stop_dollars,
        target_dollars=result.target_dollars,
        atr14=result.atr14,
        # The feature vector as it was at arm time — see storage.record_tracked_signal.
        features=build_intraday_features(result),
        feature_version=INTRADAY_FEATURE_VERSION,
        model_prob=result.model_prob,
        required_prob=result.required_prob,
        # Rows scored by a *gating* model are a censored sample — only the trades it
        # allowed have outcomes — so they can never be pooled with shadow rows when
        # judging that model. Recording which mode produced the probability is the
        # only thing that keeps the two separable later.
        model_mode=model_mode,
        cost_pct=result.cost_pct,
        cost_ratio=result.cost_ratio,
        total_score=result.total_score,
    )


# ── Phase 1 helpers ──────────────────────────────────────────────────────────


def _fetch_frames(
    client,
    tickers: List[str],
    request: IntradayScanRequest,
    now: datetime,
    logs: List[Dict[str, Any]],
) -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
    """
    At most three batched requests per scan, regardless of watchlist size.

    The higher timeframes are skipped entirely when confirmation is off, and they are
    TTL-cached inside the client, so a steady-state auto-refresh usually costs exactly
    one intraday fetch. The version this replaced issued one request *per ticker* —
    and did so even when Polygon had already answered.
    """
    wanted = [SIGNAL_INTERVAL]
    if request.use_higher_timeframes:
        wanted += [HOURLY_INTERVAL, DAILY_INTERVAL]

    frames: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
    for interval in wanted:
        try:
            frames[interval] = client.get_bars(tickers, interval, now=now)
        except DataFetchError as exc:
            frames[interval] = {}
            logs.append(_log("ALL", None, f"{interval} fetch failed: {exc}", now))
    return frames


def _build_context(
    ticker: str,
    frames: Dict[str, Dict[str, List[Dict[str, Any]]]],
    request: IntradayScanRequest,
    open_utc: Optional[datetime],
    or_end_utc: Optional[datetime],
) -> Optional[Dict[str, Any]]:
    """
    Everything the scorer needs for one ticker, from already-fetched bars.

    Returned as a plain dict — the ``ctx`` pattern — so phase 2 can re-run the scorer
    against identical inputs rather than trying to patch a finished result.
    """
    bars = (frames.get(SIGNAL_INTERVAL) or {}).get(ticker) or []
    if len(bars) < MIN_BARS:
        return None

    values = ind.compute_all(bars)
    last_bar = bars[-1]
    close = last_bar.get("close")

    day_bars = _todays_bars(bars, open_utc)
    # Day extremes exclude the bar being scored, so the extreme can actually be broken
    # by it. Including it makes every close a new day high by definition and the
    # breakout component never fires.
    prior_day_bars = day_bars[:-1] if len(day_bars) > 1 else []
    day_high, day_low = ind.range_high_low(prior_day_bars, open_utc)
    or_high, or_low = ind.window_high_low(bars, open_utc, or_end_utc)

    prev_close = _prev_session_close(bars, open_utc)
    day_change_pct = (
        round((close - prev_close) / prev_close * 100, 4)
        if close is not None and prev_close
        else None
    )

    hourly_direction = daily_direction = None
    sr_levels: List[Dict[str, Any]] = []
    if request.use_higher_timeframes:
        hourly_bars = (frames.get(HOURLY_INTERVAL) or {}).get(ticker) or []
        daily_bars = (frames.get(DAILY_INTERVAL) or {}).get(ticker) or []
        hourly_direction = ind.compute_trend_direction(hourly_bars) if hourly_bars else None
        daily_direction = ind.compute_trend_direction(daily_bars) if daily_bars else None
        # Structure comes off the hourly frame: 15m pivots are noise, and daily pivots
        # are too far away to constrain a trade inside one session.
        sr_levels = ind.detect_sr_levels(hourly_bars) if hourly_bars else []

    values.update(
        {"day_high": day_high, "day_low": day_low, "or_high": or_high, "or_low": or_low}
    )

    return {
        "indicators": values,
        "last": close,
        "day_change_pct": day_change_pct,
        "prev_close": prev_close,
        "volume": int(last_bar.get("volume") or 0),
        "vwap": ind.calculate_vwap(day_bars),
        "avg_dollar_volume": values.get("avg_dollar_volume"),
        "hourly_direction": hourly_direction,
        "daily_direction": daily_direction,
        "sr_levels": sr_levels,
        "bar_timestamp": last_bar.get("timestamp"),
        "session_open": day_bars[0].get("open") if day_bars else None,
        "session_high": max((b["high"] for b in day_bars if b.get("high") is not None), default=None),
        "session_low": min((b["low"] for b in day_bars if b.get("low") is not None), default=None),
    }


def _todays_bars(
    bars: List[Dict[str, Any]],
    open_utc: Optional[datetime],
) -> List[Dict[str, Any]]:
    """
    Bars from today's open onward. Falls back to the last session's worth when the
    market is closed, so a weekend scan still works against Friday's tape.
    """
    if open_utc is None:
        return bars[-SESSION_BARS:]
    selected = [b for b in bars if (parse_ts(b.get("timestamp")) or open_utc) >= open_utc]
    return selected or bars[-SESSION_BARS:]


def _prev_session_close(
    bars: List[Dict[str, Any]],
    open_utc: Optional[datetime],
) -> Optional[float]:
    """Last close strictly before today's open — the correct base for day change."""
    if open_utc is None:
        return bars[-2]["close"] if len(bars) >= 2 else None
    prior = [b for b in bars if (parse_ts(b.get("timestamp")) or open_utc) < open_utc]
    return prior[-1]["close"] if prior else None


def _observed_spreads(
    settings: AppSettings,
    tickers: List[str],
    logs: List[Dict[str, Any]],
    now: datetime,
) -> Dict[str, Optional[float]]:
    """
    Real bid/ask spread percentages from Polygon, when the plan allows it.

    A measured spread always beats a liquidity-tier estimate, and it is one batched
    call. When it is unavailable the scorer falls back to the tiers, which are
    deliberately pessimistic — so losing this degrades the ranking, never the safety.
    """
    if not settings.polygon_api_key or not tickers:
        return {}
    try:
        client = PolygonClient(settings.polygon_api_key, settings.request_timeout_seconds)
        snapshots = client.get_stock_snapshots(tickers)
    except Exception as exc:
        logs.append(_log("ALL", None, f"quote spreads unavailable, using cost tiers: {exc}", now))
        return {}

    spreads: Dict[str, Optional[float]] = {}
    for snapshot in snapshots:
        ticker = normalize_symbol(snapshot.get("ticker") or "")
        quote = snapshot.get("lastQuote") or {}
        bid = ind._first_float(quote.get("p"), quote.get("bid"))
        ask = ind._first_float(quote.get("P"), quote.get("ask"))
        if bid is None or ask is None or ask <= 0:
            continue
        mid = (bid + ask) / 2.0
        if mid > 0:
            spreads[ticker] = round((ask - bid) / mid * 100, 4)
    return spreads


# ── Phase 2 helper ───────────────────────────────────────────────────────────


def _score_context(
    ticker: str,
    context: Dict[str, Any],
    request: IntradayScanRequest,
    spy_change: Optional[float],
    phase: Optional[str],
    mins_to_close: Optional[float],
    observed_spread_pct: Optional[float],
    now: datetime,
    model=None,
    model_mode: Optional[str] = None,
) -> IntradayResult:
    """
    Score once to learn the direction, then rescore with relative strength folded in.

    Two passes rather than one because the bonus depends on the direction and the
    direction depends on the score. Adding the bonus to the finished total instead
    would leave the displayed and the decided score disagreeing by up to 10 points —
    more than the gap between WATCH_ONLY and an actionable candidate.
    """
    values = context["indicators"]

    def _score(strength_bonus: float, model_prob: Optional[float] = None) -> Dict[str, Any]:
        return score_ticker(
            ticker=ticker,
            last=context["last"],
            avg_dollar_volume=context["avg_dollar_volume"],
            indicators=values,
            phase=phase,
            minutes_to_close=mins_to_close,
            min_avg_dollar_volume=request.min_avg_dollar_volume,
            hourly_direction=context["hourly_direction"],
            daily_direction=context["daily_direction"],
            sr_levels=context["sr_levels"],
            observed_spread_pct=observed_spread_pct,
            model_prob=model_prob,
            strength_bonus=strength_bonus,
        )

    base = _score(0.0)

    rs = assessment = None
    bonus = 0.0
    if request.use_relative_strength:
        rs = calculate_rs(context["day_change_pct"], spy_change)
        assessment = rs_assessment(rs)
        bonus = rs_bonus(assessment, base["dominant"])

    scored = _score(bonus) if bonus else base

    # Model pass. A shadow model scores and logs but changes nothing: it is the only
    # way to learn what it would do to the trades it wants to block, because once it
    # is gating those trades stop happening and stop being measurable.
    if model is not None and scored["dominant"] in ("LONG", "SHORT"):
        try:
            features = build_intraday_features(_feature_source(context, scored))
            probability = model.predict_one(to_vector(features, INTRADAY_FEATURE_NAMES))
        except Exception:
            probability = None  # a bad model must not take the scan down
        if probability is not None:
            if model_mode == "active":
                # Rescore so the veto flows through the same path every other gate
                # uses, rather than being patched onto a finished result.
                scored = _score(bonus, model_prob=probability)
            else:
                scored = {**scored, "model_prob": probability}

    scored = _apply_instrument_filters(scored, context, request)

    return IntradayResult(
        ticker=ticker,
        last_price=_round(context["last"]),
        day_change_pct=_round(context["day_change_pct"]),
        volume=context["volume"],
        relative_volume=_round(values.get("rel_volume")),
        avg_dollar_volume=_round(context["avg_dollar_volume"]),
        open=_round(context["session_open"]),
        high=_round(context["session_high"]),
        low=_round(context["session_low"]),
        prev_close=_round(context["prev_close"]),
        rsi14=_round(values.get("rsi14")),
        ema9=_round(values.get("ema9")),
        ema20=_round(values.get("ema20")),
        macd=_round(values.get("macd")),
        macd_signal=_round(values.get("macd_signal")),
        macd_histogram=_round(values.get("macd_histogram")),
        atr14=_round(values.get("atr14")),
        adx14=_round(values.get("adx14")),
        vwap=_round(context["vwap"]),
        spread_pct=_round(observed_spread_pct),
        regime=scored["regime"],
        dominant=scored["dominant"],
        momentum_score=scored["momentum_score"],
        reversion_score=scored["reversion_score"],
        breakout_score=scored["breakout_score"],
        mtf_score=scored["mtf_score"],
        mtf_confluence=scored["mtf_confluence"],
        sr_score=scored["sr_score"],
        total_score=scored["total_score"],
        at_key_level=scored["at_key_level"],
        blocked_ahead=scored["blocked_ahead"],
        nearest_support=_round(scored["nearest_support"]),
        nearest_resistance=_round(scored["nearest_resistance"]),
        extension_atr=scored["extension_atr"],
        rs_vs_spy=rs,
        rs_assessment=assessment,
        suggested_entry=scored["suggested_entry"],
        suggested_stop=scored["suggested_stop"],
        suggested_target=scored["suggested_target"],
        stop_dollars=scored["stop_dollars"],
        target_dollars=scored["target_dollars"],
        stop_pct=scored["stop_pct"],
        target_pct=scored["target_pct"],
        rr_ratio=scored["rr_ratio"],
        cost_pct=scored["cost_pct"],
        cost_ratio=scored["cost_ratio"],
        model_prob=scored["model_prob"],
        required_prob=scored["required_prob"],
        trade_signal=scored["trade_signal"],
        signal_reason=scored["signal_reason"],
        risk_notes=scored["risk_notes"],
        market_phase=scored["market_phase"],
        bar_timestamp=context["bar_timestamp"],
        as_of=now,
    )


def _feature_source(context: Dict[str, Any], scored: Dict[str, Any]) -> SimpleNamespace:
    """
    The fields ``build_intraday_features`` reads, as one attribute bag.

    Serving builds features from this rather than from a finished ``IntradayResult``
    so the model can be consulted *before* the result is constructed — which is what
    lets an active model's veto flow through a genuine rescore instead of being
    patched onto the output. Field names match the result's exactly, so the training
    path (which does read the stored result) sees the identical contract.
    """
    values = context["indicators"]
    return SimpleNamespace(
        dominant=scored["dominant"],
        atr14=values.get("atr14"),
        adx14=values.get("adx14"),
        rsi14=values.get("rsi14"),
        ema9=values.get("ema9"),
        ema20=values.get("ema20"),
        macd_histogram=values.get("macd_histogram"),
        relative_volume=values.get("rel_volume"),
        avg_dollar_volume=context["avg_dollar_volume"],
        last_price=context["last"],
        high=context["session_high"],
        low=context["session_low"],
        extension_atr=scored["extension_atr"],
        mtf_score=scored["mtf_score"],
        sr_score=scored["sr_score"],
        cost_ratio=scored["cost_ratio"],
        # The provisional stop, so a non-actionable setup still reports a real bracket
        # size rather than a zero standing in for "no bracket".
        stop_pct=scored.get("stop_pct") or scored.get("prov_stop_pct"),
        total_score=scored["total_score"],
        bar_timestamp=context["bar_timestamp"],
    )


def _apply_instrument_filters(
    scored: Dict[str, Any],
    context: Dict[str, Any],
    request: IntradayScanRequest,
) -> Dict[str, Any]:
    """
    Price band, shorts toggle, and relative volume.

    These describe the *instrument* rather than the setup, so they are applied after
    scoring: the score stays a property of the tape, and these downgrade rather than
    delete so a filtered name still appears with a reason instead of vanishing.
    """
    last = context["last"]
    rel_volume = context["indicators"].get("rel_volume")

    if last is None:
        return {**scored, "trade_signal": "AVOID", "signal_reason": "last price unavailable"}
    if last < request.min_price or last > request.max_price:
        return {
            **scored,
            "trade_signal": "AVOID",
            "signal_reason": (
                f"price ${last:,.2f} outside "
                f"${request.min_price:,.0f}-${request.max_price:,.0f}"
            ),
        }

    if scored["trade_signal"] in ("AVOID", "WATCH_ONLY"):
        return scored

    if scored["dominant"] == "SHORT" and not request.include_shorts:
        return {
            **scored,
            "trade_signal": "WATCH_ONLY",
            "signal_reason": "Short candidates are disabled in this scan",
        }

    if rel_volume is not None and rel_volume < request.min_relative_volume:
        return {
            **scored,
            "trade_signal": "WATCH_ONLY",
            "signal_reason": (
                f"{scored['signal_reason']} - relative volume {rel_volume:.2f}x "
                f"below {request.min_relative_volume:.2f}x"
            ),
        }

    return scored


def is_armable(result: IntradayResult, now: Optional[datetime] = None) -> Tuple[bool, str]:
    """
    Whether a signal should be forward-tested — separate from whether it displays.

    Two gates, both ported with their evidence:

    * **Opening chop.** Entries armed in the first hour were the biggest loss bucket in
      the stocks sibling's forward test. They still display; they are not measured.
    * **Thin edge.** A target under ``MIN_TARGET_PCT`` of entry measures the spread
      rather than the signal, so recording its outcome would poison the training set
      with noise labelled as skill.
    """
    if result.trade_signal in ("AVOID", "WATCH_ONLY"):
        return False, "not actionable"
    if result.suggested_stop is None or result.suggested_target is None:
        return False, "no bracket"
    since_open = minutes_since_open(now)
    if since_open is not None and since_open < OPEN_CHOP_MINUTES:
        return False, f"opening hour ({since_open:.0f} min since open)"
    if (result.target_pct or 0.0) < MIN_TARGET_PCT:
        return False, f"thin edge (target {result.target_pct:.2f}%)"
    return True, ""


# ── Small helpers ────────────────────────────────────────────────────────────


def _log(
    ticker: str,
    signal: Optional[str],
    error: Optional[str],
    now: datetime,
) -> Dict[str, Any]:
    return {
        "ticker": ticker,
        "signal": signal,
        "error": error,
        "created_at": now.isoformat(),
        "provider": "yahoo",
    }


def _round(value: Optional[float]) -> Optional[float]:
    return round(value, 4) if value is not None else None
