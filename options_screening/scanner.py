from __future__ import annotations

from datetime import date, timedelta
from statistics import median
from typing import Any, Dict, List, Optional, Tuple

from pydantic import BaseModel

from .config import AppSettings
from .direction import daily_direction_read
from .features import (
    OPTIONS_FEATURE_NAMES,
    OPTIONS_FEATURE_VERSION,
    build_options_features,
    to_vector,
)
from .models import MarketContext, OptionContract, RejectedContract, ScoredContract
from .polygon import PolygonClient, _simple_average, _trend_signal
from .scoring import ENGINE_CALL, ENGINE_PUT, score_contracts
from .signals import _PROB_MARGIN, breakeven_win_rate
from .storage import Storage
from .timeutil import exchange_date, parse_ts
from .training import load_serving_model
from .universe import normalize_symbol
from .yahoo_client import DataFetchError, shared_client as yahoo_client

# Daily-bar lookback for the trend context. 110 calendar days is ~75 trading days:
# enough to seed a 50-period SMA and a 14-period ADX with room for holidays.
_CONTEXT_LOOKBACK_DAYS = 110

# Signals worth forward-testing. WATCH_ONLY and AVOID are not armed, and neither are
# the income-structure labels: this repo screens long premium, so measuring a
# suggestion to *sell* against a long bracket would record a meaningless number.
ACTIONABLE_OPTION_SIGNALS = frozenset({"BUY_CALL_CANDIDATE", "BUY_PUT_CANDIDATE"})
# The second label's actionable values. Armed and graded alongside the first so the
# scorecard can say which directional read is right more often.
ACTIONABLE_ENGINE_SIGNALS = frozenset({ENGINE_CALL, ENGINE_PUT})

# Round-trip cost charged to a forward test when the plan returns no bid/ask (the
# Options Starter plan has none). Open-interest tiers, floored at a one-tick ($0.05)
# spread on the premium: a $0.40 contract cannot be quoted tighter than 12.5% however
# liquid it is. Deliberately on the wide side, on the same principle as the equity
# lane's liquidity tiers — an unmeasured cost is not a zero cost, and charging zero
# would make every unquoted outcome read better than it was.
_UNQUOTED_SPREAD_TIERS = ((5000, 3.0), (1000, 5.0), (250, 8.0), (0, 12.0))
_MIN_TICK = 0.05
_MAX_ESTIMATED_SPREAD_PCT = 50.0

# Strikes fetched either side of the underlying's price. Wide enough to hold a 0.10
# delta on a two-month, 60%-IV name; the per-ticker cap then keeps the nearest.
_STRIKE_BAND = 0.25


class ScanRequest(BaseModel):
    tickers: List[str]
    fixed_risk: float = 250.0
    min_volume: int = 50
    min_open_interest: int = 250
    max_spread_pct: float = 12.0
    min_days_to_expiration: int = 21
    max_days_to_expiration: int = 75
    min_abs_delta: float = 0.25
    max_abs_delta: float = 0.65
    min_iv: float = 0.05
    max_iv: float = 1.2
    max_contracts_per_ticker: int = 50
    allow_missing_spread: bool = False
    use_trend_context: bool = True
    require_trend_alignment: bool = False
    check_earnings: bool = False
    avoid_earnings_before_expiration: bool = False
    ignore_missing_spread_for_signal: bool = True


class ScanSummary(BaseModel):
    accepted: int = 0
    rejected: int = 0
    errors: int = 0
    armed: int = 0
    resolved: int = 0


def run_scan(
    settings: AppSettings,
    storage: Storage,
    request: ScanRequest,
    today: Optional[date] = None,
) -> ScanSummary:
    client = PolygonClient(settings.polygon_api_key, settings.request_timeout_seconds)
    storage.start_scan(request.model_dump())

    summary = ScanSummary()
    # Exchange date, not local date: an evening scan would otherwise shift the whole
    # expiry window by a day relative to the DTE the scorer computes.
    today = today or exchange_date()
    expiration_gte = today + timedelta(days=request.min_days_to_expiration)
    expiration_lte = today + timedelta(days=request.max_days_to_expiration)
    all_accepted: List[ScoredContract] = []
    all_rejected: List[RejectedContract] = []

    # Every quote seen this scan, keyed by contract. Used to resolve open
    # forward-tests without a second round of API calls.
    observed_mids: Dict[str, float] = {}

    # The underlying's daily bars and price come from Yahoo, in one batched pass for
    # the whole universe. An options-only Polygon plan does not include the stock
    # snapshot at all, and its free stock tier is 5 calls a minute — a 50-ticker
    # scan would spend ten minutes in retry backoff.
    daily_bars, live_prices, underlying_error = _load_underlyings(request.tickers, request)

    for ticker in request.tickers:
        try:
            market_context = _load_market_context(
                client, ticker, today, expiration_lte, request,
                daily_bars=daily_bars.get(normalize_symbol(ticker)),
                live_price=live_prices.get(normalize_symbol(ticker)),
                data_error=underlying_error,
            )
            contracts = _fetch_chain(
                client, ticker, expiration_gte, expiration_lte,
                request.max_contracts_per_ticker, market_context.last_price,
            )
            for contract in contracts:
                if contract.mid_price:
                    observed_mids[contract.contract_ticker] = contract.mid_price

            # IV rank needs this underlying's own history, not the universe's: 45% is
            # cheap for one name and historically expensive for another. Recorded
            # before it is read, so today counts once it has a reading.
            storage.record_iv(ticker, _chain_session(contracts, today), _chain_iv(contracts))
            iv_history = storage.load_iv_history(ticker)
            accepted, rejected = score_contracts(
                contracts, request, market_context, today=today, iv_history=iv_history
            )
            all_accepted.extend(accepted)
            all_rejected.extend(rejected)
            summary.accepted += len(accepted)
            summary.rejected += len(rejected)
            storage.log_ticker(ticker, len(accepted), len(rejected), None)
        except Exception as exc:  # Dashboard should keep scanning other symbols.
            summary.errors += 1
            storage.log_ticker(ticker, 0, 0, _sanitize_error(str(exc), settings.polygon_api_key))

    # Model pass. The rules have already proposed; the model can only downgrade.
    model_mode = _apply_model(storage, all_accepted)

    all_accepted.sort(key=lambda item: item.score, reverse=True)
    storage.save_results(all_accepted)
    storage.save_rejections(all_rejected)

    # Close the loop: resolve first, then arm, so a contract that just hit its stop
    # cannot be re-armed in the same pass.
    summary.resolved, summary.armed = _resolve_and_arm(
        storage, all_accepted, observed_mids, today, settings, model_mode
    )

    storage.finish_scan(summary.model_dump())
    return summary


def _apply_model(storage: Storage, accepted: List[ScoredContract]) -> Optional[str]:
    """
    Score each accepted contract and let an *active* model veto. Returns the mode.

    The veto is the same shape as the equity lane's: below the cost-adjusted breakeven
    plus a margin, an otherwise-actionable contract is downgraded to WATCH_ONLY with
    the numbers in the reason. It can never promote — a contract the filters rejected
    never reaches this function at all.

    A shadow model records its probability and changes nothing, which is the only way
    to learn what it would have done to the trades it wants to block: once it is
    gating, those trades stop happening and stop being measurable.
    """
    try:
        model, mode = load_serving_model(storage, "options")
    except Exception:
        return None
    if model is None:
        return None

    for scored in accepted:
        try:
            features = build_options_features(scored)
            probability = model.predict_one(to_vector(features, OPTIONS_FEATURE_NAMES))
        except Exception:
            continue  # a bad model must not take the scan down

        scored.model_prob = round(probability, 4)
        # The contract's own spread is the round-trip cost, in percent of premium.
        cost_ratio = _option_cost_ratio(scored)
        required = round(breakeven_win_rate(cost_ratio=cost_ratio) + _PROB_MARGIN, 4)
        scored.required_prob = required

        if mode == "active" and probability < required and scored.trade_signal in ACTIONABLE_OPTION_SIGNALS:
            scored.trade_signal = "WATCH_ONLY"
            scored.signal_reason = (
                f"model P(win)={probability:.0%} < {required:.0%} required "
                f"at {cost_ratio:.0%} cost of risk"
            )
    return mode


def _option_cost_ratio(scored: ScoredContract) -> float:
    """
    Round-trip cost as a fraction of the risk taken on this contract.

    Option spreads are percentage points of premium, not basis points of price, so
    this is materially larger than the equity lane's and moves the breakeven bar a
    long way. Falls back to a pessimistic value when the spread is unquoted, on the
    same principle as the equity cost tiers: an unmeasured cost is not a zero cost.
    """
    spread_pct = scored.contract.spread_pct
    entry = scored.premium_entry
    stop = scored.premium_stop
    if spread_pct is None:
        return 0.25
    if not entry or stop is None or entry <= stop:
        return min(1.0, spread_pct / 100.0)
    risk_fraction = (entry - stop) / entry
    return min(1.0, (spread_pct / 100.0) / risk_fraction) if risk_fraction > 0 else 1.0


def _resolve_and_arm(
    storage: Storage,
    accepted: List[ScoredContract],
    observed_mids: Dict[str, float],
    today: date,
    settings: AppSettings,
    model_mode: Optional[str] = None,
) -> Tuple[int, int]:
    """
    Resolve open option forward-tests, then arm the newly actionable ones. Returns
    ``(resolved, armed)``.

    Each open contract's own 15-minute bars since entry are fetched, so a stop or
    target touched *between* scans is still seen in a bar's low or high. Contracts the
    bars cannot be fetched for fall back to the mid this scan observed. Contracts that
    are still open but no longer appear in the chain are snapshotted individually for
    that mid. Both call counts are bounded by the number of open positions, not by the
    universe size.
    """
    resolved = armed = 0
    try:
        open_rows = storage.load_open_tracked("options")
        paths: Dict[str, List[Dict[str, Any]]] = {}
        if open_rows and settings.polygon_api_key:
            client = PolygonClient(settings.polygon_api_key, settings.request_timeout_seconds)
            paths = _option_paths(client, open_rows, today)
            for row in open_rows:
                contract_ticker = row.get("contract_ticker")
                if not contract_ticker or contract_ticker in observed_mids:
                    continue
                try:
                    snapshot = client.get_option_contract_snapshot(row["ticker"], contract_ticker)
                except Exception:
                    continue  # unresolvable this pass; it stays open
                if snapshot and snapshot.mid_price:
                    observed_mids[contract_ticker] = snapshot.mid_price
        resolved = storage.resolve_options_signals(observed_mids, today=today, bars=paths)
    except Exception as exc:
        storage.log_ticker("ALL", 0, 0, f"Tracking eval failed: {_sanitize_error(str(exc), settings.polygon_api_key)}")

    # Best score first, so the one contract per underlying and label that the dedupe
    # lets through is the best-formed one.
    for scored in sorted(accepted, key=lambda item: item.score, reverse=True):
        try:
            if _arm_contract(storage, scored, today, model_mode):
                armed += 1
            if _arm_contract(storage, scored, today, model_mode, engine=True):
                armed += 1
        except Exception as exc:
            storage.log_ticker(
                scored.contract.underlying, 0, 0,
                f"Tracking record failed: {_sanitize_error(str(exc), settings.polygon_api_key)}",
            )
    return resolved, armed


def _option_paths(
    client: PolygonClient,
    open_rows: List[Dict[str, Any]],
    today: date,
) -> Dict[str, List[Dict[str, Any]]]:
    """
    15-minute bars from each open contract's entry date to today.

    A contract is only present in the result when its bars were actually fetched —
    an empty list means "fetched, nothing traded", which is different from "could
    not fetch", and the resolver treats the two differently.
    """
    paths: Dict[str, List[Dict[str, Any]]] = {}
    for row in open_rows:
        contract_ticker = row.get("contract_ticker")
        if not contract_ticker or contract_ticker in paths:
            continue
        entry = parse_ts(row.get("entry_ts") or row.get("created_at"))
        start = exchange_date(entry) if entry else today - timedelta(days=45)
        try:
            paths[contract_ticker] = client.get_option_bars(contract_ticker, start, today)
        except Exception:
            continue
    return paths


def _arm_contract(
    storage: Storage,
    scored: ScoredContract,
    today: date,
    model_mode: Optional[str] = None,
    engine: bool = False,
) -> Optional[int]:
    """
    Arm one scored contract for forward testing under one of its two labels, if that
    label is actionable. Returns the tracking id, or None.

    Engine-labelled rows are recorded for the scorecard only: no feature vector (so
    they never enter model training until that read has earned it, and a contract
    both labels picked is not a duplicate training row) and no model fields (the
    model never gated them, so pooling them with gated rows would corrupt the model
    report).
    """
    signal = scored.engine_signal if engine else scored.trade_signal
    if signal not in (ACTIONABLE_ENGINE_SIGNALS if engine else ACTIONABLE_OPTION_SIGNALS):
        return None
    if scored.premium_stop is None or scored.premium_target is None:
        return None

    contract = scored.contract
    return storage.record_tracked_signal(
        lane="options",
        ticker=contract.underlying,
        contract_ticker=contract.contract_ticker,
        signal=signal,
        # Always +1: the position is long premium either way, and the call/put
        # distinction is already carried by the bracket and by `direction` in the
        # feature vector. Encoding it here too would double-count it.
        direction=1,
        entry=scored.premium_entry,
        stop=scored.premium_stop,
        target=scored.premium_target,
        entry_ts=contract.as_of.isoformat(),
        stop_dollars=(scored.premium_entry or 0.0) - (scored.premium_stop or 0.0),
        target_dollars=(scored.premium_target or 0.0) - (scored.premium_entry or 0.0),
        atr14=scored.underlying_atr14,
        expiration_date=contract.expiration_date.isoformat(),
        features=None if engine else build_options_features(scored),
        feature_version=None if engine else OPTIONS_FEATURE_VERSION,
        model_prob=None if engine else scored.model_prob,
        required_prob=None if engine else scored.required_prob,
        # Gated rows are a censored sample and must never be pooled with shadow rows
        # when judging the model that produced them.
        model_mode=None if engine else model_mode,
        # The contract's own quoted spread is the round-trip cost, and on options it
        # is percentage points rather than basis points. Estimated when unquoted.
        cost_pct=tracked_cost_pct(contract),
        # Where to act on the underlying, for the alert: the position is managed on
        # the stock even though its P&L is in premium.
        underlying_price=scored.underlying_last_price,
        underlying_stop=scored.underlying_stop,
        underlying_target=scored.underlying_target,
        total_score=scored.engine_score if engine else scored.score,
        one_per_underlying=True,
    )


def _chain_session(contracts: List[OptionContract], today: date) -> date:
    """
    The trading session the chain's prices come from: the latest last-trade date.

    Not the scan date. A weekend or holiday scan sees Friday's prices, and filing them
    under Sunday would count one session twice in the IV rank.
    """
    sessions = [exchange_date(c.last_trade_at) for c in contracts if c.last_trade_at]
    return max(sessions) if sessions else today


def _chain_iv(contracts: List[OptionContract]) -> Optional[float]:
    """
    One IV reading for the underlying: the median across the near-the-money contracts
    fetched. The median, so one stale deep strike cannot move the day's reading.
    """
    ivs = [c.implied_volatility for c in contracts if c.implied_volatility and c.implied_volatility > 0]
    return round(median(ivs), 6) if ivs else None


def tracked_cost_pct(contract: OptionContract) -> Optional[float]:
    """The quoted spread when there is one, else a conservative estimate of it."""
    if contract.spread_pct is not None:
        return contract.spread_pct
    mid = contract.mid_price
    if not mid or mid <= 0:
        return None
    open_interest = contract.open_interest or 0
    tier = next(pct for floor, pct in _UNQUOTED_SPREAD_TIERS if open_interest >= floor)
    tick_floor = _MIN_TICK / mid * 100.0
    return round(min(_MAX_ESTIMATED_SPREAD_PCT, max(tier, tick_floor)), 4)


def _sanitize_error(message: str, api_key: Optional[str] = None) -> str:
    if not message:
        return message
    safe = message
    if api_key:
        safe = safe.replace(api_key, "REDACTED")
    return safe


def _fetch_chain(
    client: PolygonClient,
    ticker: str,
    expiration_gte: date,
    expiration_lte: date,
    max_contracts: int,
    spot: Optional[float],
) -> List[OptionContract]:
    """
    The ``max_contracts`` contracts nearest the money, calls and puts alike.

    Polygon returns a chain sorted by symbol — nearest expiry, lowest strike, calls
    before puts — so capping the raw response spent the whole budget on deep
    in-the-money calls of one expiry (delta 0.99, no open interest) and never reached
    a put. With the underlying's price known, only strikes within ``_STRIKE_BAND`` are
    fetched, and the cap keeps the ones nearest the money across every expiry. Without
    a price, the old capped fetch is the fallback.
    """
    if not spot:
        return client.get_option_chain_snapshots(
            ticker, expiration_gte=expiration_gte, expiration_lte=expiration_lte,
            max_contracts=max_contracts,
        )
    chain = client.get_option_chain_snapshots(
        ticker, expiration_gte=expiration_gte, expiration_lte=expiration_lte,
        strike_gte=round(spot * (1 - _STRIKE_BAND), 2),
        strike_lte=round(spot * (1 + _STRIKE_BAND), 2),
    )
    chain.sort(key=lambda c: (abs(c.strike_price - spot), c.expiration_date, c.contract_type))
    return chain[:max_contracts]


def _load_underlyings(
    tickers: List[str],
    request: ScanRequest,
) -> Tuple[Dict[str, List[Dict[str, Any]]], Dict[str, Optional[float]], Optional[str]]:
    """
    ``(daily_bars, live_prices, error)`` for every underlying, from Yahoo.

    Daily bars are completed sessions only (the yahoo client drops the forming
    candle), so SMA and ATR do not repaint intraday. The live price is the latest
    15-minute close *including* the forming bar — the freshest number there is, and
    the one the bracket is centred on.
    """
    if not request.use_trend_context and not request.check_earnings:
        return {}, {}, None
    try:
        bars = yahoo_client.get_bars(tickers, "1d")
    except DataFetchError as exc:
        return {}, {}, f"underlying data unavailable from Yahoo: {exc}"
    try:
        prices = yahoo_client.get_quotes(tickers)
    except Exception:
        prices = {}  # the last daily close is a usable, if staler, fallback
    return bars, prices, None


def _load_market_context(
    client: PolygonClient,
    ticker: str,
    today: date,
    expiration_lte: date,
    request: ScanRequest,
    daily_bars: Optional[List[Dict[str, Any]]] = None,
    live_price: Optional[float] = None,
    data_error: Optional[str] = None,
) -> MarketContext:
    if not request.use_trend_context and not request.check_earnings:
        return MarketContext(underlying=ticker.upper())

    bars = daily_bars or []
    closes = [bar["close"] for bar in bars if bar.get("close") is not None]
    last_price = live_price or (closes[-1] if closes else None)
    sma20 = _simple_average(closes[-20:]) if len(closes) >= 20 else None
    sma50 = _simple_average(closes[-50:]) if len(closes) >= 50 else None

    earnings_date = None
    earnings_warning = data_error or "not checked"
    if request.check_earnings:
        try:
            earnings_date = client.get_next_earnings_date(ticker, today, expiration_lte)
            earnings_warning = "before expiration" if earnings_date else "none found before expiration"
        except Exception as exc:
            message = _sanitize_error(str(exc), client.api_key)
            # The Benzinga feed is a separate entitlement; say so in words rather than
            # printing a 403 URL into every row.
            earnings_warning = (
                "earnings check unavailable on this Polygon plan"
                if "403" in message else message
            )

    trend_signal = _trend_signal(last_price, sma20, sma50)
    try:
        engine = daily_direction_read(ticker, bars, trend_signal) if request.use_trend_context else None
    except Exception:
        engine = None  # a bad read must cost the second label, not the scan
    engine = engine or {}

    return MarketContext(
        underlying=ticker.upper(),
        last_price=last_price,
        sma20=sma20,
        sma50=sma50,
        trend_signal=trend_signal,
        earnings_date=earnings_date,
        earnings_warning=earnings_warning,
        engine_direction=engine.get("direction"),
        engine_signal=engine.get("signal"),
        engine_score=engine.get("score"),
        engine_reason=engine.get("reason"),
        daily_bars=bars,
    )
