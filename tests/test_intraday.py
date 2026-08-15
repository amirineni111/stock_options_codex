"""
The intraday scan pipeline, end to end against a fake client.

Worth an integration test rather than trusting the unit tests of each half: the scan
is where batching, the forming-candle rule, the two-phase rescore and the arming gates
all have to hold *together*, and each of them fails silently — the dashboard keeps
rendering and the numbers merely become wrong.
"""
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

import app
from options_screening.config import AppSettings
from options_screening.intraday import (
    OPEN_CHOP_MINUTES,
    IntradayScanRequest,
    is_armable,
    run_intraday_scan,
)
from options_screening.storage import Storage
from options_screening.universe import (
    load_sp100_tickers,
    normalize_symbol,
    to_yahoo_symbol,
)

# 2026-08-14 is a Friday. 09:30 ET = 13:30 UTC; 14:00 ET = 18:00 UTC.
OPEN_UTC = datetime(2026, 8, 14, 13, 30, tzinfo=timezone.utc)
MIDDAY = datetime(2026, 8, 14, 18, 0, tzinfo=timezone.utc)
JUST_AFTER_OPEN = datetime(2026, 8, 14, 13, 50, tzinfo=timezone.utc)

SLOPES = {"AAPL": 0.35, "NVDA": -0.30}


def _series(n, start_price, per_bar, interval_min, first, wobble=0.15):
    """Deterministic trending bars with enough wiggle for ATR/ADX to be non-zero."""
    out = []
    price = start_price
    for i in range(n):
        ts = first + timedelta(minutes=interval_min * i)
        w = wobble * (1 if i % 3 else -1)
        out.append(
            {
                "timestamp": ts.isoformat(),
                "open": round(price, 4),
                "high": round(price + abs(per_bar) + wobble, 4),
                "low": round(price - abs(per_bar) - wobble, 4),
                "close": round(price + per_bar + w * 0.1, 4),
                "volume": 200000 + (i % 5) * 10000,
            }
        )
        price += per_bar
    return out


class FakeClient:
    """Stands in for YahooClient — the scanner only ever calls get_bars."""

    def __init__(self):
        self.calls = []

    def get_bars(self, tickers, interval, now=None):
        self.calls.append((interval, tuple(sorted(tickers))))
        out = {}
        for ticker in tickers:
            slope = SLOPES.get(ticker, 0.04)
            if interval == "15m":
                out[ticker] = _series(60, 180.0, slope, 15, OPEN_UTC - timedelta(minutes=15 * 48))
            elif interval == "1h":
                out[ticker] = _series(60, 170.0, slope * 2.3, 60, OPEN_UTC - timedelta(hours=60))
            else:
                out[ticker] = _series(60, 120.0, slope * 5.7, 1440, OPEN_UTC - timedelta(days=60))
        return out


def _armable_result(**overrides):
    """A minimal actionable result, for testing the arming gates in isolation."""
    from options_screening.intraday import IntradayResult

    fields = {
        "ticker": "AAPL",
        "trade_signal": "BUY_CANDIDATE",
        "suggested_entry": 100.0,
        "suggested_stop": 97.5,
        "suggested_target": 103.75,
        "target_pct": 3.75,
        "as_of": MIDDAY,
    }
    fields.update(overrides)
    return IntradayResult(**fields)


@pytest.fixture()
def env():
    settings = AppSettings(polygon_api_key=None)
    request = IntradayScanRequest(tickers=["AAPL", "NVDA"], min_avg_dollar_volume=0.0)
    return settings, request


class TestFetchDiscipline:
    def test_requests_scale_with_timeframes_not_with_watchlist_size(self, env):
        """
        The regression: the scanner issued one uncached Yahoo request *per ticker*
        across eight threads, and did so even when Polygon had already answered,
        because every indicator came from Yahoo regardless. A 30-name watchlist meant
        30 requests per refresh against an endpoint that bans.
        """
        settings, request = env
        client = FakeClient()
        run_intraday_scan(settings, request, now=MIDDAY, client=client)
        assert len(client.calls) == 3
        assert [interval for interval, _ in client.calls] == ["15m", "1h", "1d"]

    def test_a_bigger_watchlist_does_not_cost_more_requests(self, env):
        settings, _ = env
        big = IntradayScanRequest(
            tickers=["AAPL", "NVDA", "MSFT", "AMD", "TSLA", "META"],
            min_avg_dollar_volume=0.0,
        )
        client = FakeClient()
        run_intraday_scan(settings, big, now=MIDDAY, client=client)
        assert len(client.calls) == 3

    def test_the_benchmark_rides_along_but_is_not_a_result(self, env):
        settings, request = env
        client = FakeClient()
        results, _, _ = run_intraday_scan(settings, request, now=MIDDAY, client=client)
        assert all("SPY" in tickers for _, tickers in client.calls)
        assert {r.ticker for r in results} == {"AAPL", "NVDA"}

    def test_higher_timeframes_are_skipped_when_confirmation_is_off(self, env):
        settings, _ = env
        request = IntradayScanRequest(
            tickers=["AAPL"], min_avg_dollar_volume=0.0, use_higher_timeframes=False
        )
        client = FakeClient()
        run_intraday_scan(settings, request, now=MIDDAY, client=client)
        assert len(client.calls) == 1


class TestDeterminism:
    def test_the_same_bars_and_clock_produce_the_same_scores(self, env):
        """
        If this fails, something in the path is reading the wall clock or depending on
        an open candle — which is exactly the repaint failure, and it is invisible in
        stored data because the bar has closed by the time you look.
        """
        settings, request = env
        first, _, _ = run_intraday_scan(settings, request, now=MIDDAY, client=FakeClient())
        again, _, _ = run_intraday_scan(settings, request, now=MIDDAY, client=FakeClient())
        assert [(r.ticker, r.total_score, r.trade_signal) for r in first] == [
            (r.ticker, r.total_score, r.trade_signal) for r in again
        ]

    def test_results_are_ranked_by_score(self, env):
        settings, request = env
        results, _, _ = run_intraday_scan(settings, request, now=MIDDAY, client=FakeClient())
        assert [r.rank for r in results] == list(range(1, len(results) + 1))
        scores = [r.total_score for r in results]
        assert scores == sorted(scores, reverse=True)

    def test_the_summary_accounts_for_every_ticker(self, env):
        settings, request = env
        results, summary, _ = run_intraday_scan(
            settings, request, now=MIDDAY, client=FakeClient()
        )
        assert summary.scanned == 2
        assert summary.accepted + summary.watch + summary.avoid + summary.errors == 2


class TestDegradation:
    def test_a_ticker_with_no_bars_is_an_error_row_not_a_dead_scan(self, env):
        settings, request = env

        class PartialClient(FakeClient):
            def get_bars(self, tickers, interval, now=None):
                bars = super().get_bars(tickers, interval, now)
                bars["NVDA"] = []
                return bars

        results, summary, logs = run_intraday_scan(
            settings, request, now=MIDDAY, client=PartialClient()
        )
        assert {r.ticker for r in results} == {"AAPL"}
        assert summary.errors == 1
        assert any(log["ticker"] == "NVDA" and log["error"] for log in logs)

    def test_a_total_fetch_failure_returns_empty_rather_than_raising(self, env):
        settings, request = env

        class DeadClient:
            def get_bars(self, tickers, interval, now=None):
                return {}

        results, summary, _ = run_intraday_scan(
            settings, request, now=MIDDAY, client=DeadClient()
        )
        assert results == []
        assert summary.errors == 2

    def test_every_scanned_ticker_produces_a_log_row(self, env):
        settings, request = env
        _, _, logs = run_intraday_scan(settings, request, now=MIDDAY, client=FakeClient())
        logged = {log["ticker"] for log in logs}
        assert {"AAPL", "NVDA"} <= logged


class TestArmingGates:
    def test_nothing_is_armed_during_the_opening_hour(self, env):
        """
        Forward-testing in the stocks sibling found entries armed in the opening hour
        were the biggest loss bucket (27% win rate, -0.33R per trade). They still
        display — they are simply not measured as trades.
        """
        settings, request = env
        results, _, _ = run_intraday_scan(
            settings, request, now=JUST_AFTER_OPEN, client=FakeClient()
        )
        assert results, "precondition: the scan produced rows"
        assert all(not is_armable(r, JUST_AFTER_OPEN)[0] for r in results)

    def test_the_gate_blocks_a_setup_that_would_otherwise_be_armed(self):
        """
        Isolated from the scan so the reason is unambiguous: an identical actionable
        result is armable at midday and refused inside the opening hour.
        """
        actionable = _armable_result()
        assert is_armable(actionable, MIDDAY)[0]

        armed, why = is_armable(actionable, JUST_AFTER_OPEN)
        assert not armed
        assert "opening hour" in why

    def test_a_thin_target_is_not_forward_tested(self):
        """
        A target inside the noise measures the spread rather than the signal, so
        recording its outcome would poison the training set with noise labelled as
        skill.
        """
        thin = _armable_result(target_pct=0.01)
        armed, why = is_armable(thin, MIDDAY)
        assert not armed
        assert "thin edge" in why

    def test_a_non_actionable_signal_is_never_armed(self, env):
        settings, request = env
        results, _, _ = run_intraday_scan(settings, request, now=MIDDAY, client=FakeClient())
        for result in results:
            if result.trade_signal in ("AVOID", "WATCH_ONLY"):
                armable, why = is_armable(result, MIDDAY)
                assert not armable and why == "not actionable"

    def test_an_armable_signal_carries_a_complete_bracket(self, env):
        settings, request = env
        results, _, _ = run_intraday_scan(settings, request, now=MIDDAY, client=FakeClient())
        for result in results:
            if is_armable(result, MIDDAY)[0]:
                assert result.suggested_entry is not None
                assert result.suggested_stop is not None
                assert result.suggested_target is not None
                assert result.rr_ratio == pytest.approx(1.5)


class TestInstrumentFilters:
    def test_a_price_outside_the_band_is_avoided_with_a_reason(self, env):
        settings, _ = env
        request = IntradayScanRequest(
            tickers=["AAPL"], min_avg_dollar_volume=0.0, max_price=1.0
        )
        results, _, _ = run_intraday_scan(settings, request, now=MIDDAY, client=FakeClient())
        assert results[0].trade_signal == "AVOID"
        assert "outside" in results[0].signal_reason

    def test_disabling_shorts_downgrades_rather_than_hides_them(self, env):
        """Filtered names keep appearing with a reason, so the pattern stays visible."""
        settings, _ = env
        request = IntradayScanRequest(
            tickers=["NVDA"], min_avg_dollar_volume=0.0, include_shorts=False
        )
        results, _, _ = run_intraday_scan(settings, request, now=MIDDAY, client=FakeClient())
        assert results[0].ticker == "NVDA"
        assert results[0].trade_signal not in ("BUY_CANDIDATE", "SHORT_CANDIDATE", "STRONG_SHORT")


class TestSymbolFormats:
    def test_share_classes_are_normalised_to_the_polygon_form(self):
        """
        The regression: the S&P 500 scraper rewrote the dot to a *slash* (``BRK/B``),
        which no provider here accepts — so those symbols silently errored out of
        every scan while the hard-coded lists in the same file used ``BRK.B``.
        """
        for variant in ("BRK.B", "BRK-B", "BRK/B", "brk.b"):
            assert normalize_symbol(variant) == "BRK.B"

    def test_yahoo_gets_the_dash_form_it_expects(self):
        assert to_yahoo_symbol("BRK.B") == "BRK-B"
        assert to_yahoo_symbol("AAPL") == "AAPL"

    def test_sp100_universe_is_available_offline(self):
        tickers, note = load_sp100_tickers()
        assert "AAPL" in tickers and "MSFT" in tickers
        assert note == ""


class TestStorageRoundTrip:
    def test_the_latest_scan_replaces_the_previous_one(self, env, tmp_path):
        settings, request = env
        storage = Storage(Path(tmp_path) / "screen.sqlite3")
        storage.initialize()

        results, _, logs = run_intraday_scan(
            settings, request, now=MIDDAY, client=FakeClient()
        )
        storage.save_intraday_scan(results, logs)
        storage.save_intraday_scan(results[:1], logs[:1])

        frame = storage.load_intraday_results()
        assert len(frame) == 1
        for column in ("ema9", "macd_histogram", "vwap", "atr14", "suggested_stop"):
            assert column in frame.columns


class TestAppHelpers:
    def test_custom_ticker_parsing_dedupes_and_uppercases(self):
        assert app._parse_custom_tickers("aapl, msft\nspy, AAPL") == ["AAPL", "MSFT", "SPY"]

    def test_preferences_keep_the_two_ticker_lists_separate(self, tmp_path, monkeypatch):
        path = Path(tmp_path) / "app_preferences.json"
        path.write_text(
            json.dumps(
                {
                    "ticker_source": "Custom",
                    "custom_tickers": "AAPL, MSFT",
                    "intraday_universe": "Custom",
                    "intraday_custom_tickers": "",
                }
            ),
            encoding="utf-8",
        )
        monkeypatch.setattr(app, "APP_PREFERENCES_PATH", path)

        preferences = app._load_app_preferences()
        assert preferences["custom_tickers"] == "AAPL, MSFT"
        assert preferences["intraday_custom_tickers"] == ""
