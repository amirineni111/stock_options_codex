from __future__ import annotations

from datetime import date, timedelta
from typing import Dict, List, Optional

from pydantic import BaseModel

from .config import AppSettings
from .features import (
    OPTIONS_FEATURE_NAMES,
    OPTIONS_FEATURE_VERSION,
    build_options_features,
    to_vector,
)
from .models import MarketContext, RejectedContract, ScoredContract
from .polygon import PolygonClient
from .scoring import score_contracts
from .signals import _PROB_MARGIN, breakeven_win_rate
from .storage import Storage
from .timeutil import exchange_date
from .training import load_serving_model

# Daily-bar lookback for the trend context. 110 calendar days is ~75 trading days:
# enough to seed a 50-period SMA and a 14-period ADX with room for holidays.
_CONTEXT_LOOKBACK_DAYS = 110

# Signals worth forward-testing. WATCH_ONLY and AVOID are not armed, and neither are
# the income-structure labels: this repo screens long premium, so measuring a
# suggestion to *sell* against a long bracket would record a meaningless number.
ACTIONABLE_OPTION_SIGNALS = frozenset({"BUY_CALL_CANDIDATE", "BUY_PUT_CANDIDATE"})


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

    for ticker in request.tickers:
        try:
            market_context = _load_market_context(client, ticker, today, expiration_lte, request)
            contracts = client.get_option_chain_snapshots(
                ticker,
                expiration_gte=expiration_gte,
                expiration_lte=expiration_lte,
                max_contracts=request.max_contracts_per_ticker,
            )
            for contract in contracts:
                if contract.mid_price:
                    observed_mids[contract.contract_ticker] = contract.mid_price

            # IV rank needs this underlying's own history, not the universe's: 45% is
            # cheap for one name and historically expensive for another.
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
    _resolve_and_arm(storage, all_accepted, observed_mids, today, settings, model_mode)

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
) -> None:
    """
    Resolve open option forward-tests against the mids this scan already saw, then arm
    the newly actionable ones.

    Contracts that are still open but no longer appear in the chain (they fell outside
    the DTE window, or the scan covered a different ticker set) are fetched
    individually — that call count is bounded by the number of open positions, not by
    the universe size.
    """
    try:
        open_rows = storage.load_open_tracked("options")
        missing = [
            row["contract_ticker"]
            for row in open_rows
            if row.get("contract_ticker") and row["contract_ticker"] not in observed_mids
        ]
        if missing and settings.polygon_api_key:
            client = PolygonClient(settings.polygon_api_key, settings.request_timeout_seconds)
            for row in open_rows:
                contract_ticker = row.get("contract_ticker")
                if contract_ticker not in missing:
                    continue
                try:
                    snapshot = client.get_option_contract_snapshot(row["ticker"], contract_ticker)
                except Exception:
                    continue  # unresolvable this pass; it stays open
                if snapshot and snapshot.mid_price:
                    observed_mids[contract_ticker] = snapshot.mid_price
        storage.resolve_options_signals(observed_mids, today=today)
    except Exception as exc:
        storage.log_ticker("ALL", 0, 0, f"Tracking eval failed: {_sanitize_error(str(exc), settings.polygon_api_key)}")

    for scored in accepted:
        try:
            _arm_contract(storage, scored, today, model_mode)
        except Exception as exc:
            storage.log_ticker(
                scored.contract.underlying, 0, 0,
                f"Tracking record failed: {_sanitize_error(str(exc), settings.polygon_api_key)}",
            )


def _arm_contract(
    storage: Storage,
    scored: ScoredContract,
    today: date,
    model_mode: Optional[str] = None,
) -> None:
    """Arm one scored contract for forward testing, if it clears the gates."""
    if scored.trade_signal not in ACTIONABLE_OPTION_SIGNALS:
        return
    if scored.premium_stop is None or scored.premium_target is None:
        return

    contract = scored.contract
    storage.record_tracked_signal(
        lane="options",
        ticker=contract.underlying,
        contract_ticker=contract.contract_ticker,
        signal=scored.trade_signal,
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
        features=build_options_features(scored),
        feature_version=OPTIONS_FEATURE_VERSION,
        model_prob=scored.model_prob,
        required_prob=scored.required_prob,
        # Gated rows are a censored sample and must never be pooled with shadow rows
        # when judging the model that produced them.
        model_mode=model_mode,
        # The contract's own quoted spread is the round-trip cost, and on options it
        # is percentage points rather than basis points.
        cost_pct=contract.spread_pct,
        total_score=scored.score,
    )


def _sanitize_error(message: str, api_key: Optional[str] = None) -> str:
    if not message:
        return message
    safe = message
    if api_key:
        safe = safe.replace(api_key, "REDACTED")
    return safe


def _load_market_context(
    client: PolygonClient,
    ticker: str,
    today: date,
    expiration_lte: date,
    request: ScanRequest,
) -> MarketContext:
    if not request.use_trend_context and not request.check_earnings:
        return MarketContext(underlying=ticker.upper())
    try:
        return client.get_market_context(
            ticker,
            start=today - timedelta(days=_CONTEXT_LOOKBACK_DAYS),
            end=today,
            earnings_end=expiration_lte,
            check_earnings=request.check_earnings,
            today=today,
        )
    except Exception as exc:
        warning = _sanitize_error(str(exc), client.api_key)
        return MarketContext(underlying=ticker.upper(), earnings_warning=warning)
