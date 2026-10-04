"""
Option forward-tests resolved against the contract's own 15-minute bars.

The snapshot-only resolver could only see the mid a scan happened to observe, so a
target touched and retraced between scans was invisible. These pin the path-based
replacement, its conservative fills, and the fallback when bars cannot be fetched.
"""
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from options_screening.models import MarketContext, OptionContract
from options_screening.polygon import PolygonClient
from options_screening.scanner import ScanRequest, _load_market_context, tracked_cost_pct
from options_screening.storage import Storage

ENTRY_TS = "2026-08-14T14:00:00+00:00"
NOW = datetime(2026, 8, 14, 18, 0, tzinfo=timezone.utc)
TODAY = date(2026, 8, 14)
CONTRACT = "O:AAPL260918C00125000"


@pytest.fixture()
def storage(tmp_path) -> Storage:
    store = Storage(Path(tmp_path) / "test.sqlite3")
    store.initialize()
    return store


def _arm(storage, **kw):
    fields = dict(
        lane="options", ticker="AAPL", contract_ticker=CONTRACT,
        signal="BUY_CALL_CANDIDATE", direction=1, entry=4.00, stop=2.40, target=7.00,
        entry_ts=ENTRY_TS, stop_dollars=1.60, cost_pct=5.0, expiration_date="2026-09-18",
        underlying_price=120.0, underlying_stop=115.0, underlying_target=127.5,
    )
    fields.update(kw)
    return storage.record_tracked_signal(**fields)


def _bars(rows, start="2026-08-14T14:15:00+00:00"):
    """Bars from (open, low, high, close) tuples, 15 minutes apart."""
    base = datetime.fromisoformat(start)
    return [
        {"timestamp": (base + timedelta(minutes=15 * i)).isoformat(),
         "open": o, "low": lo, "high": hi, "close": c, "volume": 10}
        for i, (o, lo, hi, c) in enumerate(rows)
    ]


def _outcome(storage):
    return storage.load_outcomes("options").iloc[0]


def test_a_target_touched_between_scans_is_still_a_success(storage):
    """The mid this scan sees is back between the levels; the bar high is not."""
    _arm(storage)
    bars = {CONTRACT: _bars([(4.0, 3.9, 4.4, 4.2), (4.2, 4.1, 7.2, 4.6)])}
    resolved = storage.resolve_options_signals({CONTRACT: 4.60}, today=TODAY, now=NOW, bars=bars)
    assert resolved == 1
    outcome = _outcome(storage)
    assert (outcome["outcome"], outcome["exit_reason"]) == ("WIN", "TARGET")
    assert outcome["exit_price"] == pytest.approx(7.00)
    # Held from entry to the bar that hit it, not until the scan happened to look.
    assert outcome["hold_minutes"] == 30


def test_a_bar_spanning_both_levels_reads_as_the_stop(storage):
    _arm(storage)
    bars = {CONTRACT: _bars([(4.0, 2.2, 7.5, 4.0)])}
    storage.resolve_options_signals({}, today=TODAY, now=NOW, bars=bars)
    assert _outcome(storage)["exit_reason"] == "STOP"


def test_a_gap_through_the_stop_fills_at_the_open_not_the_stop(storage):
    """Crediting the stop price on an overnight gap would understate the loss."""
    _arm(storage)
    bars = {CONTRACT: _bars([(1.80, 1.60, 2.00, 1.90)])}
    storage.resolve_options_signals({}, today=TODAY, now=NOW, bars=bars)
    assert _outcome(storage)["exit_price"] == pytest.approx(1.80)


def test_a_gap_through_the_target_is_credited_only_at_the_target(storage):
    _arm(storage)
    bars = {CONTRACT: _bars([(8.00, 7.80, 8.50, 8.20)])}
    storage.resolve_options_signals({}, today=TODAY, now=NOW, bars=bars)
    assert _outcome(storage)["exit_price"] == pytest.approx(7.00)


def test_bars_before_entry_cannot_resolve_a_trade(storage):
    _arm(storage)
    bars = {CONTRACT: _bars([(4.0, 1.0, 9.0, 4.0)], start="2026-08-14T13:30:00+00:00")}
    assert storage.resolve_options_signals({}, today=TODAY, now=NOW, bars=bars) == 0


def test_fetched_bars_override_a_stale_snapshot_mid(storage):
    """
    An illiquid contract's snapshot price can be a trade from before entry. When its
    bars were fetched and show nothing after entry, the snapshot is not evidence.
    """
    _arm(storage)
    assert storage.resolve_options_signals({CONTRACT: 2.00}, today=TODAY, now=NOW, bars={CONTRACT: []}) == 0


def test_without_bars_the_snapshot_mid_still_resolves(storage):
    _arm(storage)
    assert storage.resolve_options_signals({CONTRACT: 7.50}, today=TODAY, now=NOW, bars={}) == 1


def test_expiry_falls_back_to_the_last_bar_close_when_unquoted(storage):
    _arm(storage)
    bars = {CONTRACT: _bars([(4.0, 3.9, 4.4, 4.30)])}
    storage.resolve_options_signals({}, today=date(2026, 9, 17), now=NOW, bars=bars)
    outcome = _outcome(storage)
    assert outcome["exit_reason"] == "EXPIRY"
    assert outcome["exit_price"] == pytest.approx(4.30)


def test_the_underlying_levels_are_stored_for_the_alert(storage):
    _arm(storage)
    row = storage.load_open_tracked("options")[0]
    assert (row["underlying_stop"], row["underlying_target"]) == (115.0, 127.5)


# ── Cost when the plan has no quotes ─────────────────────────────────────────


def _contract(**kw):
    fields = dict(
        underlying="AAPL", contract_ticker=CONTRACT, contract_type="call",
        expiration_date=date(2026, 9, 18), strike_price=125.0, last_price=4.00,
        open_interest=1500,
    )
    fields.update(kw)
    return OptionContract(**fields)


def test_a_quoted_spread_is_used_as_is():
    assert tracked_cost_pct(_contract(bid=3.9, ask=4.1)) == pytest.approx(5.0)


def test_an_unquoted_contract_is_charged_an_estimated_spread_not_zero():
    assert tracked_cost_pct(_contract()) == 5.0
    assert tracked_cost_pct(_contract(open_interest=10)) == 12.0


def test_a_cheap_contract_cannot_be_estimated_tighter_than_one_tick():
    # $0.05 on a $0.40 premium is 12.5%, whatever the open interest.
    assert tracked_cost_pct(_contract(last_price=0.40, open_interest=50000)) == pytest.approx(12.5)


# ── Underlying context from Yahoo bars ───────────────────────────────────────


class _NoEarnings:
    api_key = "k"

    def get_next_earnings_date(self, *args):
        raise RuntimeError("Polygon API error 403 for https://api.polygon.io/benzinga?apiKey=REDACTED")


def _daily(closes):
    return [{"timestamp": str(i), "open": c, "high": c + 1, "low": c - 1, "close": c} for i, c in enumerate(closes)]


def test_trend_comes_from_the_daily_bars_and_the_live_price():
    request = ScanRequest(tickers=["AAPL"])
    bars = _daily([100 + i for i in range(60)])
    context = _load_market_context(_NoEarnings(), "AAPL", TODAY, TODAY, request, daily_bars=bars, live_price=161.0)
    assert context.last_price == 161.0
    assert context.trend_signal == "bullish"
    assert len(context.daily_bars) == 60


def test_an_unentitled_earnings_check_says_so_in_words():
    request = ScanRequest(tickers=["AAPL"], check_earnings=True)
    context = _load_market_context(_NoEarnings(), "AAPL", TODAY, TODAY, request, daily_bars=_daily([100] * 60))
    assert context.earnings_warning == "earnings check unavailable on this Polygon plan"


def test_trend_context_off_skips_the_underlying_entirely():
    request = ScanRequest(tickers=["AAPL"], use_trend_context=False)
    assert _load_market_context(_NoEarnings(), "AAPL", TODAY, TODAY, request) == MarketContext(underlying="AAPL")


# ── Polygon option bars ──────────────────────────────────────────────────────


def test_option_bars_parse_into_canonical_bars(monkeypatch):
    calls = []

    def fake_get(self, path, params=None):
        calls.append(path)
        return {"results": [{"t": 1786716900000, "o": 4.0, "h": 4.4, "l": 3.9, "c": 4.2, "v": 12}]}

    monkeypatch.setattr(PolygonClient, "_get", fake_get)
    bars = PolygonClient("test").get_option_bars(CONTRACT, date(2026, 8, 14), date(2026, 8, 15))
    assert calls == [f"/v2/aggs/ticker/{CONTRACT}/range/15/minute/2026-08-14/2026-08-15"]
    assert bars[0]["high"] == 4.4 and bars[0]["timestamp"].endswith("+00:00")


# ── Which contracts a capped scan spends its budget on ───────────────────────


class _FakeChainClient:
    def __init__(self, contracts):
        self.contracts = contracts
        self.calls = []

    def get_option_chain_snapshots(self, ticker, **kwargs):
        self.calls.append(kwargs)
        return list(self.contracts)


def test_the_capped_chain_keeps_the_contracts_nearest_the_money():
    from options_screening.scanner import _fetch_chain

    chain = [
        _contract(contract_ticker=f"{kind}{strike}", contract_type=kind, strike_price=strike)
        for strike in (80, 90, 98, 100, 103, 115) for kind in ("call", "put")
    ]
    client = _FakeChainClient(chain)
    picked = _fetch_chain(client, "AAPL", TODAY, TODAY, 4, spot=100.0)
    assert sorted((c.strike_price, c.contract_type) for c in picked) == [
        (98, "call"), (98, "put"), (100, "call"), (100, "put"),
    ]
    assert client.calls[0]["strike_gte"] == 75.0 and client.calls[0]["strike_lte"] == 125.0


def test_without_a_price_the_chain_falls_back_to_the_capped_fetch():
    from options_screening.scanner import _fetch_chain

    client = _FakeChainClient([])
    _fetch_chain(client, "AAPL", TODAY, TODAY, 25, spot=None)
    assert client.calls[0]["max_contracts"] == 25 and "strike_gte" not in client.calls[0]
