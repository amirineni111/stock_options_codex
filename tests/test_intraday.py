from pathlib import Path

import app

from options_screening.intraday import (
    IntradayScanRequest,
    _calculate_ema,
    _calculate_macd,
    _calculate_rsi,
    _calculate_vwap,
    _yahoo_chart_to_snapshot,
    score_intraday_snapshot,
)
from options_screening.storage import Storage
from options_screening.universe import load_sp100_tickers


def _snapshot(**overrides):
    data = {
        "ticker": "AAPL",
        "todaysChangePerc": 1.6,
        "day": {"o": 100.0, "h": 103.0, "l": 99.5, "c": 102.0, "v": 1_000_000},
        "prevDay": {"c": 100.4, "v": 5_000_000},
        "min": {"c": 101.8},
        "lastTrade": {"p": 102.0},
        "lastQuote": {"p": 101.95, "P": 102.05},
    }
    data.update(overrides)
    return data


def test_parse_custom_tickers_dedupes_and_uppercases():
    assert app._parse_custom_tickers("aapl, msft\nspy, AAPL") == ["AAPL", "MSFT", "SPY"]


def test_sp100_universe_available():
    tickers, note = load_sp100_tickers()

    assert "AAPL" in tickers
    assert "MSFT" in tickers
    assert note == ""


def test_momentum_buy_candidate():
    request = IntradayScanRequest(tickers=["AAPL"], mode="Momentum", min_relative_volume=0.05)

    result = score_intraday_snapshot(_snapshot(), request)

    assert result.trade_signal == "BUY_CANDIDATE"
    assert result.signal_mode == "Momentum"
    assert result.total_score > 0


def test_mean_reversion_short_candidate():
    request = IntradayScanRequest(tickers=["AAPL"], mode="Mean Reversion", min_relative_volume=0.05)
    snapshot = _snapshot(
        todaysChangePerc=2.5,
        day={"o": 100.0, "h": 103.0, "l": 98.0, "c": 102.9, "v": 1_000_000},
        min={"c": 102.9},
        lastTrade={"p": 102.9},
    )

    result = score_intraday_snapshot(snapshot, request)

    assert result.trade_signal == "MEAN_REVERSION_SHORT"
    assert result.signal_mode == "Mean Reversion"
    assert "Short selling" in result.risk_notes


def test_rsi_can_downgrade_momentum_candidate():
    request = IntradayScanRequest(tickers=["AAPL"], mode="Momentum", min_relative_volume=0.05)

    result = score_intraday_snapshot(_snapshot(rsi14=84.0), request)

    assert result.trade_signal == "WATCH_ONLY"
    assert result.total_score == 0.0
    assert "RSI14" in result.signal_reason


def test_trend_indicators_can_downgrade_momentum_candidate():
    request = IntradayScanRequest(tickers=["AAPL"], mode="Momentum", min_relative_volume=0.05)

    result = score_intraday_snapshot(
        _snapshot(rsi14=60.0, ema9=103.0, ema20=104.0, macd_histogram=-0.2, vwap=103.0),
        request,
    )

    assert result.trade_signal == "WATCH_ONLY"
    assert result.total_score == 0.0
    assert "do not confirm long momentum" in result.signal_reason


def test_rsi_can_confirm_mean_reversion_candidate():
    request = IntradayScanRequest(tickers=["AAPL"], mode="Mean Reversion", min_relative_volume=0.05)
    snapshot = _snapshot(
        todaysChangePerc=-2.5,
        day={"o": 100.0, "h": 103.0, "l": 98.0, "c": 98.2, "v": 1_000_000},
        min={"c": 98.2},
        lastTrade={"p": 98.2},
        rsi14=28.0,
    )

    result = score_intraday_snapshot(snapshot, request)

    assert result.trade_signal == "MEAN_REVERSION_LONG"
    assert result.rsi14 == 28.0
    assert "oversold" in result.signal_reason


def test_watch_and_avoid_downgrades():
    request = IntradayScanRequest(tickers=["AAPL"], mode="Both", min_relative_volume=1.0, max_spread_pct=0.5)

    watch = score_intraday_snapshot(_snapshot(lastQuote={}), request)
    avoid = score_intraday_snapshot(_snapshot(lastQuote={"p": 100.0, "P": 103.0}), request)

    assert watch.trade_signal == "WATCH_ONLY"
    assert "relative volume" in watch.signal_reason
    assert avoid.trade_signal == "AVOID"
    assert "spread" in avoid.signal_reason


def test_intraday_storage_replaces_latest_results(tmp_path):
    storage = Storage(Path(tmp_path) / "screen.sqlite3")
    storage.initialize()
    request = IntradayScanRequest(tickers=["AAPL"], mode="Momentum", min_relative_volume=0.05)
    first = score_intraday_snapshot(_snapshot(ticker="AAPL"), request)
    second = score_intraday_snapshot(_snapshot(ticker="MSFT"), request)

    storage.save_intraday_scan([first], [{"ticker": "AAPL", "signal": first.trade_signal, "error": None, "created_at": first.as_of.isoformat()}])
    storage.save_intraday_scan([second], [{"ticker": "MSFT", "signal": second.trade_signal, "error": None, "created_at": second.as_of.isoformat()}])
    frame = storage.load_intraday_results()

    assert len(frame) == 1
    assert frame.iloc[0]["ticker"] == "MSFT"
    assert "ema9" in frame.columns
    assert "macd_histogram" in frame.columns
    assert "vwap" in frame.columns


def test_yahoo_chart_to_snapshot_shape():
    result = {
        "meta": {"exchangeTimezoneName": "America/New_York", "previousClose": 100.0},
        "timestamp": [1777559400, 1777560300, 1777561200],
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

    snapshot = _yahoo_chart_to_snapshot("AAPL", result)

    assert snapshot["ticker"] == "AAPL"
    assert snapshot["lastTrade"]["p"] == 102.5
    assert snapshot["day"]["v"] == 6000
    assert snapshot["prevDay"]["c"] == 100.0


def test_calculate_rsi_from_recent_closes():
    closes = [100, 101, 102, 101, 103, 104, 103, 105, 106, 107, 106, 108, 109, 110, 111]

    assert _calculate_rsi(closes, 14) == 82.3529


def test_calculate_momentum_indicators():
    closes = [float(value) for value in range(100, 140)]
    rows = [
        {"high": close + 0.5, "low": close - 0.5, "close": close, "volume": 1000}
        for close in closes[-8:]
    ]
    macd, signal, histogram = _calculate_macd(closes)

    assert _calculate_ema(closes, 9) is not None
    assert _calculate_ema(closes, 20) is not None
    assert macd is not None
    assert signal is not None
    assert histogram is not None
    assert _calculate_vwap(rows) == 135.5
