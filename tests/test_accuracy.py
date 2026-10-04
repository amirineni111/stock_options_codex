"""
The four accuracy fixes: a real IV history, one tracked bet per underlying, no
trading on stale prints, and the daily engine as a second, graded directional read.
"""
import math
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from options_screening import scorecard
from options_screening.direction import daily_direction_read, weekly_bars
from options_screening.features import OPTIONS_FEATURE_VERSION
from options_screening.greeks import iv_rank
from options_screening.models import MarketContext, OptionContract
from options_screening.polygon import PolygonClient
from options_screening.scanner import ScanRequest, _chain_iv, _chain_session
from options_screening.scoring import ENGINE_CALL, score_contract
from options_screening.storage import Storage

AS_OF = datetime(2026, 10, 5, 15, 0, tzinfo=timezone.utc)


@pytest.fixture()
def storage(tmp_path) -> Storage:
    store = Storage(Path(tmp_path) / "test.sqlite3")
    store.initialize()
    return store


def _contract(**overrides):
    data = dict(
        underlying="AAPL", contract_ticker="O:AAPL261120C00200000", contract_type="call",
        expiration_date=date.today() + timedelta(days=45), strike_price=200.0,
        bid=None, ask=None, last_price=2.5, open_interest=1000, volume=200,
        implied_volatility=0.45, delta=0.42, as_of=AS_OF,
        last_trade_at=AS_OF - timedelta(minutes=20),
    )
    data.update(overrides)
    return OptionContract(**data)


REQUEST = ScanRequest(tickers=["AAPL"], fixed_risk=500, allow_missing_spread=True,
                      ignore_missing_spread_for_signal=True)
BULLISH = dict(underlying="AAPL", last_price=199.0, sma20=195.0, sma50=190.0, trend_signal="bullish")


# ── 1. IV history is daily, and survives scan-result pruning ─────────────────


def test_a_days_scans_fold_into_one_iv_reading(storage):
    for iv in (0.40, 0.50, 0.60):
        storage.record_iv("aapl", date(2026, 10, 5), iv)
    storage.record_iv("AAPL", date(2026, 10, 6), 0.30)
    assert storage.load_iv_history("AAPL") == [pytest.approx(0.30), pytest.approx(0.50)]


def test_iv_history_outlives_the_ten_scan_results_window(storage):
    for day in range(12):
        storage.record_iv("AAPL", date(2026, 9, 1) + timedelta(days=day), 0.3 + day / 100)
    for _ in range(11):
        storage.start_scan({})   # prunes scan_results to the last 10 scans
    history = storage.load_iv_history("AAPL")
    assert len(history) == 12
    assert iv_rank(0.36, history) == pytest.approx(7 / 12, abs=1e-4)


def test_chain_iv_is_the_median_and_ignores_missing_values():
    contracts = [_contract(implied_volatility=v) for v in (0.30, 0.40, 2.50, None, 0.0)]
    assert _chain_iv(contracts) == pytest.approx(0.40)
    assert _chain_iv([]) is None


def test_a_weekend_scan_files_its_iv_under_the_session_it_priced():
    friday_close = datetime(2026, 10, 2, 19, 59, tzinfo=timezone.utc)
    contracts = [_contract(last_trade_at=friday_close), _contract(last_trade_at=friday_close - timedelta(days=1))]
    assert _chain_session(contracts, today=date(2026, 10, 4)) == date(2026, 10, 2)
    assert _chain_session([_contract(last_trade_at=None)], today=date(2026, 10, 5)) == date(2026, 10, 5)


# ── 2. One tracked bet per underlying and label ──────────────────────────────


def _arm(storage, contract_ticker, signal="BUY_CALL_CANDIDATE", ticker="AMD"):
    return storage.record_tracked_signal(
        lane="options", ticker=ticker, contract_ticker=contract_ticker, signal=signal,
        direction=1, entry=4.0, stop=2.4, target=7.0, entry_ts=AS_OF.isoformat(),
        stop_dollars=1.6, one_per_underlying=True,
    )


def test_five_calls_on_one_stock_are_tracked_as_one_bet(storage):
    armed = [_arm(storage, f"O:AMD261120C00{180 + i}000") for i in range(5)]
    assert sum(1 for tracking_id in armed if tracking_id) == 1


def test_each_label_and_side_holds_its_own_bet(storage):
    assert _arm(storage, "O:AMD261120C00180000")
    # The engine may pick the very same contract: a separate prediction to grade.
    assert _arm(storage, "O:AMD261120C00180000", signal=ENGINE_CALL)
    assert _arm(storage, "O:AMD261120P00180000", signal="BUY_PUT_CANDIDATE")
    assert _arm(storage, "O:INTC261120C00030000", ticker="INTC")


def test_the_intraday_lane_keeps_its_per_instrument_dedupe(storage):
    kwargs = dict(lane="intraday", ticker="NVDA", direction=1, entry=100.0, stop=97.5,
                  target=103.75, entry_ts=AS_OF.isoformat())
    assert storage.record_tracked_signal(signal="BUY_CANDIDATE", **kwargs)
    assert storage.record_tracked_signal(signal="STRONG_BUY", **kwargs) is None


# ── 3. Stale prices ──────────────────────────────────────────────────────────


def test_an_unquoted_contract_that_has_not_traded_for_hours_is_not_a_candidate():
    scored, _ = score_contract(
        _contract(last_trade_at=AS_OF - timedelta(hours=26)), REQUEST, MarketContext(**BULLISH)
    )
    assert scored.trade_signal == "WATCH_ONLY"
    assert "last trade 1.1 days ago - price may be stale" in scored.signal_reason


def test_a_recent_print_is_fine():
    scored, _ = score_contract(_contract(), REQUEST, MarketContext(**BULLISH))
    assert scored.trade_signal == "BUY_CALL_CANDIDATE"


def test_a_quoted_contract_is_never_stale_on_its_last_trade():
    scored, _ = score_contract(
        _contract(bid=2.4, ask=2.6, last_trade_at=AS_OF - timedelta(days=3)),
        REQUEST, MarketContext(**BULLISH),
    )
    assert scored.trade_signal == "BUY_CALL_CANDIDATE"


def test_the_snapshot_last_update_time_is_parsed():
    item = {
        "details": {"ticker": "O:AAPL261120C00200000", "contract_type": "call",
                    "expiration_date": "2026-11-20", "strike_price": 200},
        "day": {"close": 2.5, "volume": 20, "last_updated": 1790875442769000000},
        "greeks": {}, "underlying_asset": {"ticker": "AAPL"},
    }
    contract = PolygonClient("test")._parse_chain_snapshot("AAPL", item)
    assert contract.last_trade_at == datetime.fromtimestamp(1790875442.769, tz=timezone.utc)


# ── 4. The daily engine as a second label ────────────────────────────────────


def _daily(n, drift=0.6, price=100.0, start=date(2025, 9, 29)):  # a Monday
    bars, day = [], start
    while len(bars) < n:
        if day.weekday() < 5:
            wiggle = 1.5 * math.sin(len(bars) / 3.0)
            close = price + wiggle
            bars.append({
                "timestamp": datetime(day.year, day.month, day.day, 20, tzinfo=timezone.utc).isoformat(),
                "open": close - 0.3, "high": close + 1.0, "low": close - 1.0, "close": close,
                "volume": 2_000_000,
            })
            price += drift
        day += timedelta(days=1)
    return bars


def test_weekly_bars_fold_a_week_into_one_candle():
    week = weekly_bars(_daily(10))
    assert len(week) == 2
    assert week[0]["high"] == max(b["high"] for b in _daily(10)[:5])


def test_the_engine_needs_enough_history_to_read():
    assert daily_direction_read("AAPL", _daily(20)) is None


def test_the_engine_reads_a_steady_uptrend_as_long_and_a_downtrend_as_short():
    up = daily_direction_read("AAPL", _daily(250), "bullish")
    down = daily_direction_read("AAPL", _daily(250, drift=-0.6, price=300.0), "bearish")
    # Too little drift for the noise is no call at all, not a weak one.
    flat = daily_direction_read("AAPL", _daily(250, drift=-0.3, price=300.0), "bearish")
    assert up["direction"] == "LONG", up
    assert down["direction"] == "SHORT", down
    assert flat["direction"] == "NEUTRAL", flat


def _engine_context(direction, **kw):
    return MarketContext(**{**BULLISH, **kw}, engine_direction=direction,
                         engine_signal="BUY_CANDIDATE", engine_score=58.0, engine_reason="Long candidate")


def test_the_engine_label_follows_its_own_direction_read():
    scored, _ = score_contract(_contract(), REQUEST, _engine_context("LONG"))
    assert scored.engine_signal == ENGINE_CALL
    assert scored.engine_score == 58.0

    put, _ = score_contract(_contract(contract_type="put", delta=-0.42), REQUEST, _engine_context("LONG"))
    assert put.engine_signal == "WATCH_ONLY" and "does not call SHORT" in put.engine_reason


def test_the_engine_label_still_needs_a_well_formed_contract():
    scored, _ = score_contract(
        _contract(last_trade_at=AS_OF - timedelta(hours=5)), REQUEST, _engine_context("LONG")
    )
    assert scored.engine_signal == "WATCH_ONLY" and "stale" in scored.engine_reason


def test_the_engine_label_ignores_the_moving_average_objection():
    """A bearish SMA stack blocks the rule's call; the engine has its own read."""
    bearish = dict(last_price=185.0, sma20=190.0, sma50=195.0, trend_signal="bearish")
    scored, _ = score_contract(_contract(), REQUEST, _engine_context("LONG", **bearish))
    assert scored.trade_signal == "WATCH_ONLY"
    assert scored.engine_signal == ENGINE_CALL


def test_trend_alignment_filter_accepts_either_directional_read():
    request = REQUEST.model_copy(update={"require_trend_alignment": True})
    bearish = dict(last_price=185.0, sma20=190.0, sma50=195.0, trend_signal="bearish")
    _, rejected = score_contract(_contract(), request, _engine_context("LONG", **bearish))
    assert rejected is None
    _, rejected = score_contract(_contract(), request, _engine_context("SHORT", **bearish))
    assert "trend not aligned" in rejected.reason


def test_engine_rows_are_graded_but_never_trained_on(storage):
    storage.record_tracked_signal(
        lane="options", ticker="AMD", contract_ticker="O:AMD261120C00180000", signal=ENGINE_CALL,
        direction=1, entry=4.0, stop=2.4, target=7.0, entry_ts=AS_OF.isoformat(),
        stop_dollars=1.6, one_per_underlying=True,
    )
    storage.resolve_options_signals({"O:AMD261120C00180000": 7.5})
    assert storage.load_training_rows("options", OPTIONS_FEATURE_VERSION) == []
    graded = scorecard.annotate(storage.load_predictions("options"))
    assert list(graded["source"]) == ["daily engine"]
    assert list(graded["verdict"]) == ["SUCCESS"]


def test_scorecard_sources_name_each_directional_read():
    assert scorecard.source("BUY_CALL_CANDIDATE", "O:X") == "trend rule"
    assert scorecard.source(ENGINE_CALL, "O:X") == "daily engine"
    assert scorecard.source("STRONG_BUY", None) == "intraday engine"
