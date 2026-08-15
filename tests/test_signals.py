"""
The scoring engine.

Several of these encode behaviour that was measured in the sibling repos' production
data rather than hypothesised, so the docstrings carry the evidence that motivated the
rule. A "simplification" that removes one of these reintroduces a known loss.
"""
import pytest

from options_screening.signals import (
    _ACTIONABLE_SCORE,
    _MAX_COST_RATIO,
    _mtf_confluence,
    _regime_weights,
    _sr_proximity,
    breakeven_win_rate,
    estimate_cost_pct,
    score_ticker,
    trade_levels,
)


def _indicators(**overrides):
    """A clean bullish-momentum setup in a trending regime."""
    base = {
        "close": 100.0,
        "rsi14": 55.0,
        "ema9": 100.5,
        "ema20": 99.0,
        "macd": 0.5,
        "macd_histogram": 0.3,
        "atr14": 1.0,
        "adx14": 30.0,
        "bb_upper": 104.0,
        "bb_middle": 100.0,
        "bb_lower": 96.0,
        "day_high": 101.0,
        "day_low": 97.0,
        "or_high": 100.8,
        "or_low": 98.0,
    }
    base.update(overrides)
    return base


class TestRegimeGate:
    def test_a_strong_trend_suppresses_the_reversion_playbook(self):
        assert _regime_weights(30.0) == (1.0, 0.0, "TREND")

    def test_a_quiet_range_suppresses_the_momentum_playbook(self):
        assert _regime_weights(15.0) == (0.0, 1.0, "RANGE")

    def test_the_blend_is_continuous_between_the_thresholds(self):
        """
        Blended rather than switched, so a tape hovering at ADX 21 does not flip
        playbook between consecutive scans and change its mind about the direction.
        """
        w_mom, w_rev, label = _regime_weights(21.5)
        assert label == "MIXED"
        assert w_mom == pytest.approx(0.5, abs=0.01)
        assert w_mom + w_rev == pytest.approx(1.0)

    def test_unknown_trend_strength_keeps_both_playbooks(self):
        """The honest answer when ADX is unavailable, rather than a guess either way."""
        assert _regime_weights(None) == (1.0, 1.0, "UNKNOWN")

    def test_opposite_playbooks_cannot_both_contribute_in_a_clear_trend(self):
        result = score_ticker("X", 100.0, 1e9, _indicators(adx14=40.0, rsi14=25.0))
        assert result["regime"] == "TREND"
        assert result["reversion_score"] == 0.0


class TestMTFConfluence:
    def test_two_neutral_higher_timeframes_are_not_confirmation(self):
        """
        The regression, measured in the forex sibling: the intraday direction used to
        be one of three votes, so a setup whose hourly and daily reads were both
        NEUTRAL scored FULL (+30) on its own say-so. 103 of 143 historical FULL rows
        had a NEUTRAL or absent higher timeframe and 20 had *both* neutral. Those free
        points are most of what let mediocre setups clear the STRONG threshold, which
        is why that tier never outperformed.
        """
        assert _mtf_confluence("LONG", "NEUTRAL", "NEUTRAL") == (0.0, "UNCONFIRMED")
        assert _mtf_confluence("LONG", None, None) == (0.0, "UNCONFIRMED")

    def test_full_requires_both_timeframes_present_and_agreeing(self):
        assert _mtf_confluence("LONG", "LONG", "LONG") == (30.0, "FULL")

    def test_one_agreeing_timeframe_is_partial_not_full(self):
        assert _mtf_confluence("LONG", "LONG", "NEUTRAL") == (15.0, "PARTIAL")

    def test_any_opposition_forfeits_the_whole_bonus(self):
        assert _mtf_confluence("LONG", "LONG", "SHORT") == (0.0, "CONFLICT")
        assert _mtf_confluence("LONG", "SHORT", "SHORT") == (0.0, "OPPOSED")

    def test_a_directionless_setup_scores_nothing(self):
        assert _mtf_confluence("NEUTRAL", "LONG", "LONG") == (0.0, "NONE")


class TestStructureIsDirectionAware:
    def test_a_long_sheltered_by_support_is_rewarded(self):
        levels = [{"price": 99.9, "type": "S", "touches": 3, "strength": 3.0}]
        score, _, at_key, blocked, _, _ = _sr_proximity(100.0, 1.0, levels, "LONG")
        assert score == 25.0 and at_key and not blocked

    def test_a_long_pinned_under_resistance_is_penalised(self):
        """
        The regression: a direction-blind version awarded +25 for proximity to *any*
        level, so a long trapped beneath resistance scored the same as a long bouncing
        off support. Combined with the MTF bug this is what manufactured STRONG rows.
        """
        levels = [{"price": 100.5, "type": "R", "touches": 3, "strength": 3.0}]
        score, _, _, blocked, _, _ = _sr_proximity(100.0, 1.0, levels, "LONG")
        assert score == -25.0 and blocked

    def test_the_same_level_flips_sign_when_the_trade_flips(self):
        levels = [{"price": 100.5, "type": "R", "touches": 3, "strength": 3.0}]
        long_score, _, _, _, _, _ = _sr_proximity(100.0, 1.0, levels, "LONG")
        short_score, _, _, _, _, _ = _sr_proximity(100.0, 1.0, levels, "SHORT")
        assert long_score < 0 < short_score

    def test_distant_structure_is_ignored(self):
        levels = [{"price": 140.0, "type": "R", "touches": 3, "strength": 3.0}]
        score, _, _, blocked, _, _ = _sr_proximity(100.0, 1.0, levels, "LONG")
        assert score == 0.0 and not blocked


class TestCostMath:
    def test_unknown_liquidity_gets_the_most_pessimistic_tier(self):
        """
        An unmeasured cost is not a zero cost. Treating it as one is how a screener
        talks itself into names nobody is quoting.
        """
        assert estimate_cost_pct(None) == estimate_cost_pct(0.0)
        assert estimate_cost_pct(None) > estimate_cost_pct(1e9)

    def test_an_observed_spread_beats_the_liquidity_tier(self):
        """A measured number always beats an estimate keyed off a proxy."""
        assert estimate_cost_pct(1e9, observed_spread_pct=0.42) == 0.42

    def test_cost_falls_as_liquidity_rises(self):
        tiers = [estimate_cost_pct(v) for v in (1e9, 2e7, 5e6, 1e5)]
        assert tiers == sorted(tiers)

    def test_breakeven_is_forty_percent_at_rr_one_point_five_with_no_cost(self):
        """
        p = (1 + c) / (1 + rr). This is why an edgeless system lands near 40%, and why
        a measured 40% win rate is indistinguishable from entering at random.
        """
        assert breakeven_win_rate(1.5, 0.0) == pytest.approx(0.4)

    def test_cost_raises_the_bar(self):
        assert breakeven_win_rate(1.5, 0.15) > breakeven_win_rate(1.5, 0.0)

    def test_an_expensive_instrument_is_vetoed_on_stop_width_not_cost_ratio(self):
        """
        The subtle one. Once the cost floor binds,
        ``stop_pct == cost_pct * _SPREAD_STOP_MULT``, so ``cost_ratio`` is pinned at
        exactly 1/8 = 12.5% for *any* spread however wide — a 5% spread and a 0.05%
        spread both report 12.5% and both clear the 15% limit. The ratio veto is
        therefore unreachable whenever the floor is active (the forex sibling has the
        same latent hole).

        What a huge spread actually does is force the stop far past what the
        instrument moves, putting the target out of reach. That is what is vetoed.
        """
        result = score_ticker("X", 100.0, 1e9, _indicators(), observed_spread_pct=5.0)
        assert result["cost_ratio"] == pytest.approx(0.125), "precondition: ratio is pinned"
        assert result["trade_signal"] == "AVOID"
        assert "uneconomic" in result["signal_reason"]

    def test_a_normal_spread_widens_the_stop_without_vetoing(self):
        result = score_ticker("X", 100.0, 1e9, _indicators(),
                              hourly_direction="LONG", daily_direction="LONG",
                              observed_spread_pct=0.4)
        assert result["trade_signal"] != "AVOID"
        assert result["stop_pct"] == pytest.approx(3.2)


class TestStopFloors:
    def test_the_stop_is_the_widest_of_volatility_noise_and_cost(self):
        volatility = trade_levels("LONG", 100.0, 1.0, cost_pct=0.01)
        assert volatility["stop_dollars"] == pytest.approx(2.5)
        assert volatility["stop_binding"] == "volatility"

    def test_a_tiny_atr_is_floored_by_noise(self):
        """A stop inside ordinary intrabar wiggle gets tagged for reasons unrelated
        to the thesis."""
        levels = trade_levels("LONG", 100.0, 0.01, cost_pct=0.0)
        assert levels["stop_dollars"] == pytest.approx(0.5)
        assert levels["stop_binding"] == "noise"

    def test_a_wide_spread_widens_the_stop_rather_than_killing_the_setup(self):
        """
        The forex sibling's third floor, which the stocks sibling lacks. Without it a
        wide-spread name gets an ATR-only stop, the cost ratio silently blows through
        the veto, and the setup is discarded — when the correct response is a
        proportionally wider stop and a proportionally wider target.
        """
        levels = trade_levels("LONG", 100.0, 1.0, cost_pct=0.5)
        assert levels["stop_binding"] == "cost"
        assert levels["stop_dollars"] == pytest.approx(4.0)

    def test_reward_to_risk_is_fixed_so_the_target_tracks_the_stop(self):
        for cost in (0.0, 0.5):
            levels = trade_levels("LONG", 100.0, 1.0, cost_pct=cost)
            ratio = levels["target_dollars"] / levels["stop_dollars"]
            assert ratio == pytest.approx(1.5)

    def test_the_cost_floor_keeps_the_cost_ratio_inside_the_veto(self):
        levels = trade_levels("LONG", 100.0, 1.0, cost_pct=0.5)
        assert 0.5 / levels["stop_pct"] <= _MAX_COST_RATIO

    def test_a_short_brackets_the_other_way(self):
        levels = trade_levels("SHORT", 100.0, 1.0, cost_pct=0.0)
        assert levels["suggested_stop"] > 100.0 > levels["suggested_target"]

    def test_no_direction_means_no_bracket(self):
        assert trade_levels("NEUTRAL", 100.0, 1.0) == {}
        assert trade_levels("LONG", None, 1.0) == {}
        assert trade_levels("LONG", 100.0, None) == {}


class TestGating:
    def test_the_model_can_only_veto_never_promote(self):
        """
        The rules propose and the model disposes; the two never swap roles. A model
        certain of a setup the rules scored below the actionable threshold must not be
        able to manufacture a trade.
        """
        weak = _indicators(ema9=99.0, ema20=99.1, macd=0.0, macd_histogram=0.0,
                           rsi14=50.0, adx14=20.0, day_high=120.0, day_low=80.0,
                           or_high=120.0, or_low=80.0)
        optimistic = score_ticker("X", 100.0, 1e9, weak, model_prob=0.99)
        assert optimistic["total_score"] < _ACTIONABLE_SCORE
        assert optimistic["trade_signal"] not in ("STRONG_BUY", "BUY_CANDIDATE")

    def test_a_pessimistic_model_downgrades_an_otherwise_actionable_setup(self):
        strong = _indicators()
        without = score_ticker("X", 100.0, 1e9, strong,
                               hourly_direction="LONG", daily_direction="LONG")
        with_veto = score_ticker("X", 100.0, 1e9, strong,
                                 hourly_direction="LONG", daily_direction="LONG",
                                 model_prob=0.05)
        assert without["trade_signal"] not in ("AVOID", "WATCH_ONLY")
        assert with_veto["trade_signal"] == "WATCH_ONLY"
        assert "P(win)" in with_veto["signal_reason"]

    def test_a_countertrend_setup_is_downgraded(self):
        """
        The gate only applies to setups that would otherwise be actionable, so the
        base score has to clear the threshold on its own — here via structure, since
        an opposing hourly forfeits the whole confluence bonus.
        """
        support = [{"price": 99.9, "type": "S", "touches": 4, "strength": 4.0}]
        aligned = score_ticker("X", 100.0, 1e9, _indicators(), sr_levels=support,
                               hourly_direction="LONG")
        against = score_ticker("X", 100.0, 1e9, _indicators(), sr_levels=support,
                               hourly_direction="SHORT")

        assert aligned["total_score"] >= _ACTIONABLE_SCORE
        assert aligned["trade_signal"] in ("BUY_CANDIDATE", "STRONG_BUY")
        assert against["trade_signal"] == "WATCH_ONLY"
        assert "countertrend" in against["signal_reason"]

    def test_illiquid_names_are_avoided_outright(self):
        result = score_ticker("X", 100.0, 100.0, _indicators(),
                              min_avg_dollar_volume=10_000_000.0)
        assert result["trade_signal"] == "AVOID"
        assert "illiquid" in result["signal_reason"].lower()

    def test_every_outcome_carries_a_human_readable_reason(self):
        for kwargs in (
            {},
            {"model_prob": 0.05},
            {"hourly_direction": "SHORT"},
            {"min_avg_dollar_volume": 1e12},
        ):
            result = score_ticker("X", 100.0, 1e9, _indicators(), **kwargs)
            assert result["signal_reason"], kwargs

    def test_relative_strength_is_inside_the_score_not_added_afterwards(self):
        """
        Feeding the bonus through the scorer is what keeps the displayed score, the
        decided score and the trained score one number.
        """
        plain = score_ticker("X", 100.0, 1e9, _indicators())
        boosted = score_ticker("X", 100.0, 1e9, _indicators(), strength_bonus=10.0)
        assert boosted["total_score"] == pytest.approx(plain["total_score"] + 10.0)


class TestProvisionalLevels:
    def test_levels_are_exposed_even_when_the_signal_is_not_actionable(self):
        """
        Feature extraction must always see a real stop size; a zero standing in for
        "no bracket" would be learned as a genuinely tiny stop.
        """
        result = score_ticker("X", 100.0, 1e9, _indicators(),
                              hourly_direction="SHORT")
        assert result["trade_signal"] == "WATCH_ONLY"
        assert result["suggested_stop"] is None
        assert result["prov_stop_pct"] is not None and result["prov_stop_pct"] > 0
