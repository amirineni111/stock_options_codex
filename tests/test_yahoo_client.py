"""
The Yahoo data client.

The property worth protecting here is that **a signal never depends on an open
candle**. Everything else in the repo can be re-derived; a repainting signal cannot
be detected after the fact, because by the time you look at the stored row the bar has
closed and the number looks stable.
"""
from datetime import datetime, timedelta, timezone

import pytest

from options_screening.yahoo_client import YahooClient, _rows_to_bars

# Three 15m bars starting 17:30, 17:45, 18:00 UTC.
STARTS = [1786901400, 1786902300, 1786903200]
PAYLOAD = {
    "timestamp": STARTS,
    "indicators": {
        "quote": [
            {
                "open": [101.0, 101.5, 102.0],
                "high": [102.0, 102.5, 103.0],
                "low": [100.5, 101.0, 101.5],
                "close": [101.5, 102.0, 102.5],
                "volume": [1000, 2000, 3000],
            }
        ]
    },
}


def _at(offset_minutes: float) -> datetime:
    """A clock offset from the start of the last bar."""
    return datetime.fromtimestamp(STARTS[-1] + offset_minutes * 60, tz=timezone.utc)


class TestFormingCandle:
    def test_the_open_candle_is_dropped_from_scoring_bars(self):
        bars = _rows_to_bars(PAYLOAD, "15m", _at(7), drop_forming=True)
        assert [b["close"] for b in bars] == [101.5, 102.0]

    def test_the_open_candle_is_kept_for_quotes(self):
        """Display wants the freshest price; scoring must not have it."""
        bars = _rows_to_bars(PAYLOAD, "15m", _at(7), drop_forming=False)
        assert [b["close"] for b in bars] == [101.5, 102.0, 102.5]

    def test_scoring_twice_inside_one_candle_sees_identical_bars(self):
        """
        The regression: the client returned Yahoo's last row, which is the candle
        currently in progress. Its close, high, low and volume all move while the bar
        is open, so RSI/EMA/MACD/VWAP built on it repainted — the same ticker scored
        two minutes apart produced two different scores, and a signal could switch on
        and back off with no new information having arrived.
        """
        first = _rows_to_bars(PAYLOAD, "15m", _at(2), drop_forming=True)
        later = _rows_to_bars(PAYLOAD, "15m", _at(12), drop_forming=True)
        assert [b["close"] for b in first] == [b["close"] for b in later]

    def test_the_candle_becomes_available_once_it_actually_closes(self):
        after = _rows_to_bars(PAYLOAD, "15m", _at(16), drop_forming=True)
        assert [b["close"] for b in after] == [101.5, 102.0, 102.5]

    def test_a_daily_bar_stays_open_for_a_whole_day(self):
        """
        Completeness is per interval, not a fixed lookback: today's daily bar is still
        forming four hours into the session, while yesterday's is closed.
        """
        day = 86400
        payload = {
            "timestamp": [STARTS[0] - 2 * day, STARTS[0] - day, STARTS[0]],
            "indicators": {
                "quote": [
                    {
                        "open": [99.0, 100.0, 101.0],
                        "high": [100.0, 101.0, 102.0],
                        "low": [98.0, 99.0, 100.0],
                        "close": [99.5, 100.5, 101.5],
                        "volume": [10, 20, 30],
                    }
                ]
            },
        }
        bars = _rows_to_bars(payload, "1d", _at(240), drop_forming=True)
        assert [b["close"] for b in bars] == [99.5, 100.5]


class TestMalformedPayloads:
    def test_null_closes_are_dropped_not_read_as_zero(self):
        """Yahoo emits these for halts and untraded minutes. They are not zeros."""
        payload = {
            "timestamp": STARTS[:2],
            "indicators": {
                "quote": [
                    {
                        "open": [1.0, 2.0],
                        "high": [1.0, 2.0],
                        "low": [1.0, 2.0],
                        "close": [None, 2.0],
                        "volume": [10, 20],
                    }
                ]
            },
        }
        bars = _rows_to_bars(payload, "15m", _at(60), drop_forming=True)
        assert [b["close"] for b in bars] == [2.0]

    def test_an_empty_payload_gives_an_empty_list(self):
        assert _rows_to_bars({}, "15m", _at(60), True) == []
        assert _rows_to_bars({"timestamp": []}, "15m", _at(60), True) == []

    def test_bars_carry_a_uniform_utc_iso_timestamp(self):
        bars = _rows_to_bars(PAYLOAD, "15m", _at(60), drop_forming=True)
        assert all(b["timestamp"].endswith("+00:00") for b in bars)
        timestamps = [b["timestamp"] for b in bars]
        assert timestamps == sorted(timestamps), "string order must equal time order"


class TestCaching:
    @pytest.fixture()
    def client(self, monkeypatch):
        c = YahooClient()
        calls = []

        def fake_chunk(symbols, interval, now, drop_forming=True):
            calls.append((tuple(symbols), interval))
            return {s: [{"close": 1.0}] for s in symbols}

        monkeypatch.setattr(c, "_fetch_chunk", fake_chunk)
        return c, calls

    def test_slow_timeframes_are_served_from_cache(self, client):
        c, calls = client
        c.get_bars(["AAPL", "MSFT"], "1d")
        c.get_bars(["AAPL", "MSFT"], "1d")
        assert len(calls) == 1

    def test_adding_a_ticker_invalidates_the_cache(self, client):
        """
        A hit requires the cached set to *cover* the request. Without the subset
        check the new ticker would silently come back with no bars at all.
        """
        c, calls = client
        c.get_bars(["AAPL", "MSFT"], "1d")
        result = c.get_bars(["AAPL", "MSFT", "NVDA"], "1d")
        assert len(calls) == 2
        assert result["NVDA"] == [{"close": 1.0}]

    def test_the_signal_timeframe_is_never_cached(self, client):
        """It is the one the decision is made on; a stale one is a wrong one."""
        c, calls = client
        c.get_bars(["AAPL"], "15m")
        c.get_bars(["AAPL"], "15m")
        assert len(calls) == 2

    def test_requesting_no_tickers_makes_no_requests(self, client):
        c, calls = client
        assert c.get_bars([], "15m") == {}
        assert calls == []

    def test_every_requested_ticker_is_present_in_the_result(self, client):
        """Callers index the result directly, so a missing ticker must be [] not absent."""
        c, _ = client
        result = c.get_bars(["AAPL", "MSFT"], "1d")
        assert set(result) == {"AAPL", "MSFT"}
