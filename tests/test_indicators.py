"""
Indicator maths.

These are pure functions over bar dicts, so every test here is offline and exact.
Several pin values that were previously computed inside ``intraday.py`` — keeping the
numbers identical is how we know extracting the module did not change any behaviour.
"""
from datetime import datetime, timedelta, timezone

import pytest

from options_screening import indicators as ind


def _bars(closes, high_pad=0.5, low_pad=0.5, volume=1000, start=None, minutes=15):
    start = start or datetime(2026, 8, 14, 13, 30, tzinfo=timezone.utc)
    return [
        {
            "timestamp": (start + timedelta(minutes=minutes * i)).isoformat(),
            "open": c,
            "high": c + high_pad,
            "low": c - low_pad,
            "close": c,
            "volume": volume,
        }
        for i, c in enumerate(closes)
    ]


class TestOscillators:
    def test_rsi_matches_the_value_the_old_inline_implementation_produced(self):
        """Pinned so the extraction out of intraday.py is provably behaviour-neutral."""
        closes = [100, 101, 102, 101, 103, 104, 103, 105, 106, 107, 106, 108, 109, 110, 111]
        assert ind.calculate_rsi(closes, 14) == 82.3529

    def test_flat_series_is_fifty_not_a_hundred(self):
        """
        Zero average loss with zero average gain is "no information". Reporting it as
        100 would make every halted or untraded name look maximally overbought, and
        hand the mean-reversion scorer a full-strength short on a stock that has not
        traded.
        """
        assert ind.calculate_rsi([5.0] * 20, 14) == 50.0

    def test_monotonic_rise_pins_rsi_at_a_hundred(self):
        assert ind.calculate_rsi([float(i) for i in range(30)], 14) == 100.0

    def test_rsi_needs_more_bars_than_its_period(self):
        assert ind.calculate_rsi([1.0, 2.0, 3.0], 14) is None

    def test_macd_of_a_straight_line_has_a_flat_histogram(self):
        macd, signal, hist = ind.calculate_macd([float(v) for v in range(100, 140)])
        assert macd is not None and signal is not None
        assert hist == pytest.approx(0.0, abs=1e-3)

    def test_macd_returns_all_none_when_too_short(self):
        assert ind.calculate_macd([1.0, 2.0, 3.0]) == (None, None, None)


class TestVolume:
    def test_vwap_matches_the_value_the_old_inline_implementation_produced(self):
        rows = [
            {"high": c + 0.5, "low": c - 0.5, "close": c, "volume": 1000}
            for c in [float(v) for v in range(132, 140)]
        ]
        assert ind.calculate_vwap(rows) == 135.5

    def test_vwap_skips_zero_volume_bars_rather_than_weighting_them(self):
        rows = [
            {"high": 10.0, "low": 10.0, "close": 10.0, "volume": 0},
            {"high": 20.0, "low": 20.0, "close": 20.0, "volume": 100},
        ]
        assert ind.calculate_vwap(rows) == 20.0

    def test_relative_volume_is_bar_for_bar_not_day_over_day(self):
        """
        The regression: RVOL used to be today's *cumulative* volume over the prior
        session's *whole* volume, so the ratio was structurally small all morning and
        the default threshold had to be dropped to 0.05 to compensate. Bar-for-bar,
        a bar at 3x the recent average reads as 3.0 at any time of day.
        """
        bars = _bars([10.0] * 21, volume=1000)
        bars[-1]["volume"] = 3000
        assert ind.calculate_relative_volume(bars, lookback=20) == 3.0

    def test_relative_volume_is_none_when_there_is_no_volume_history(self):
        bars = _bars([10.0] * 5, volume=0)
        assert ind.calculate_relative_volume(bars) is None

    def test_average_dollar_volume_prices_the_shares(self):
        """A million shares of a $3 stock and of a $300 stock are not one market."""
        cheap = _bars([3.0] * 20, volume=1_000_000)
        dear = _bars([300.0] * 20, volume=1_000_000)
        assert ind.average_dollar_volume(dear) == 100 * ind.average_dollar_volume(cheap)


class TestTrendDirection:
    def test_a_negligible_macd_histogram_cannot_cancel_a_decisive_ema_separation(self):
        """
        The regression, inherited from both sibling repos: votes were cast on the bare
        sign of each quantity. On a clean downtrend the MACD histogram lands at
        +0.0007 through pure floating-point residue and cast a full LONG vote, which
        tied the 1-1 vote against an EMA9/EMA20 gap of 3.85 points and returned
        NEUTRAL. Under the confluence rule a NEUTRAL higher timeframe is not
        confirmation, so that artifact silently cost the setup 15-30 points.
        """
        closes = [170.0 - 0.7 * i for i in range(60)]
        bars = _bars(closes, high_pad=0.85, low_pad=0.85, minutes=60)
        _, _, hist = ind.calculate_macd(closes)

        assert abs(hist) < 0.01, "precondition: histogram is numerically negligible"
        assert ind.compute_trend_direction(bars) == "SHORT"

    def test_uptrend_reads_long(self):
        bars = _bars([100.0 + 0.8 * i for i in range(60)], high_pad=0.9, low_pad=0.9, minutes=60)
        assert ind.compute_trend_direction(bars) == "LONG"

    def test_flat_tape_reads_neutral(self):
        bars = _bars([100.0] * 60, minutes=60)
        assert ind.compute_trend_direction(bars) == "NEUTRAL"

    def test_too_few_bars_reads_neutral_rather_than_guessing(self):
        assert ind.compute_trend_direction(_bars([1.0] * 10)) == "NEUTRAL"


class TestVolatility:
    def test_atr_is_none_below_its_period(self):
        assert ind.calculate_atr([1.0] * 5, [0.5] * 5, [0.8] * 5, period=14) is None

    def test_adx_is_none_below_twice_its_period(self):
        closes = [float(i) for i in range(20)]
        assert ind.calculate_adx(closes, closes, closes, period=14) is None

    def test_adx_reports_maximum_on_a_one_directional_tape(self):
        closes = [100.0 + i for i in range(60)]
        highs = [c + 0.5 for c in closes]
        lows = [c - 0.5 for c in closes]
        assert ind.calculate_adx(highs, lows, closes) == pytest.approx(100.0, abs=1.0)

    def test_mismatched_series_lengths_return_none_rather_than_indexing_off_the_end(self):
        assert ind.calculate_atr([1.0] * 20, [1.0] * 5, [1.0] * 20) is None
        assert ind.calculate_adx([1.0] * 40, [1.0] * 40, [1.0] * 5) is None


class TestRangesAndStructure:
    def test_window_high_low_is_half_open_so_the_opening_range_excludes_its_end(self):
        start = datetime(2026, 8, 14, 13, 30, tzinfo=timezone.utc)
        bars = _bars([10.0, 20.0, 30.0], start=start, minutes=15)
        high, low = ind.window_high_low(bars, start, start + timedelta(minutes=30))
        assert (high, low) == (20.5, 9.5)

    def test_range_high_low_returns_none_without_a_start(self):
        assert ind.range_high_low(_bars([1.0, 2.0]), None) == (None, None)

    def test_sr_levels_are_sorted_by_strength(self):
        closes = [100 + (5 if i % 10 == 0 else 0) for i in range(60)]
        levels = ind.detect_sr_levels(_bars([float(c) for c in closes]))
        strengths = [lv["strength"] for lv in levels]
        assert strengths == sorted(strengths, reverse=True)


class TestCoercion:
    def test_nan_and_infinity_are_treated_as_missing(self):
        """
        Both reach here from JSON feeds and from pandas, and both poison every
        downstream comparison silently rather than raising.
        """
        assert ind._first_float(float("nan")) is None
        assert ind._first_float(float("inf")) is None
        assert ind._first_float(float("-inf")) is None

    def test_first_usable_value_wins(self):
        assert ind._first_float(None, "not-a-number", 3) == 3.0
        assert ind._first_int(None, "x", "7") == 7

    def test_all_unusable_returns_none(self):
        assert ind._first_float(None, "x") is None
        assert ind._first_int(None, "x") is None


class TestComputeAll:
    def test_empty_bars_give_an_empty_dict_rather_than_raising(self):
        assert ind.compute_all([]) == {}

    def test_short_history_yields_none_indicators_but_still_returns_the_bar(self):
        result = ind.compute_all(_bars([10.0, 11.0, 12.0]))
        assert result["close"] == 12.0
        assert result["rsi14"] is None
        assert result["adx14"] is None
