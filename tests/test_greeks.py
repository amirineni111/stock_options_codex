"""
Option premium maths.

Theta and vega were fetched from Polygon and stored in SQLite from the first commit,
and read by nothing. For a screener that only ever proposes long single-leg calls and
puts, decay over a 21-75 day hold is usually the largest term in the P&L, so every
scenario column was quietly optimistic until these landed.
"""
from datetime import date, timedelta

import pytest

from options_screening.greeks import (
    _MAX_HOLD_DAYS,
    estimate_holding_days,
    expected_move_pct,
    gamma_leverage,
    iv_rank,
    premium_trade_levels,
    project_premium,
    theta_per_premium,
)
from options_screening.models import OptionContract

TODAY = date(2026, 8, 14)


def _contract(**overrides):
    fields = dict(
        underlying="AAPL",
        contract_ticker="O:AAPL260918C00125000",
        contract_type="call",
        expiration_date=TODAY + timedelta(days=45),
        strike_price=125.0,
        bid=3.90,
        ask=4.10,
        implied_volatility=0.30,
        delta=0.45,
        gamma=0.03,
        theta=-0.05,
        vega=0.12,
        underlying_price=124.0,
        volume=400,
        open_interest=2500,
    )
    fields.update(overrides)
    return OptionContract(**fields)


class TestProjection:
    def test_a_favourable_move_raises_the_premium(self):
        projected = project_premium(_contract(), underlying_move=5.0)
        assert projected > 4.0

    def test_a_put_gains_on_a_falling_underlying(self):
        """
        ``underlying_move`` is signed in the underlying's frame, not the position's,
        so the delta sign does the work and no caller has to remember to flip it.
        """
        put = _contract(contract_type="put", delta=-0.45)
        assert project_premium(put, underlying_move=-5.0) > 4.0
        assert project_premium(put, underlying_move=+5.0) < 4.0

    def test_time_passing_costs_the_position_money(self):
        flat_now = project_premium(_contract(), underlying_move=0.0, holding_days=0.0)
        flat_later = project_premium(_contract(), underlying_move=0.0, holding_days=10.0)
        assert flat_later == pytest.approx(flat_now - 0.5)

    def test_decay_reduces_an_otherwise_identical_favourable_move(self):
        """The specific optimism the old delta+gamma-only scenario carried."""
        instant = project_premium(_contract(), underlying_move=5.0, holding_days=0.0)
        realistic = project_premium(_contract(), underlying_move=5.0, holding_days=14.0)
        assert realistic < instant

    def test_vega_moves_the_premium_with_implied_volatility(self):
        richer = project_premium(_contract(), underlying_move=0.0, iv_change_points=5.0)
        assert richer == pytest.approx(4.0 + 0.6)

    def test_a_catastrophic_move_does_not_project_a_higher_premium(self):
        """
        The regression: the gamma term is always positive and unbounded, so a large
        adverse move projected a *higher* premium than entry — a $500 drop on a $4
        call came out at $3,529. It inverted the sign of the answer and produced
        brackets whose stop sat above the entry and whose risk was negative.

        Delta saturates in reality, so the move is clamped to the window where the
        expansion still means something.
        """
        entry = _contract().mid_price
        crashed = project_premium(_contract(), underlying_move=-500.0)
        assert 0.0 <= crashed < entry

    def test_the_projection_flattens_rather_than_reversing_past_the_valid_range(self):
        """Deep out of the money, further moves stop mattering — they must not help."""
        far = project_premium(_contract(), underlying_move=-100.0)
        further = project_premium(_contract(), underlying_move=-1000.0)
        assert far == pytest.approx(further)

    def test_normal_sized_moves_are_untouched_by_the_clamp(self):
        """The guard is on the tail; it must not alter the distances actually used."""
        contract = _contract()
        manual = contract.mid_price + 0.45 * 5.0 + 0.5 * 0.03 * 25.0
        assert project_premium(contract, underlying_move=5.0) == pytest.approx(manual)

    def test_no_quote_means_no_projection(self):
        assert project_premium(_contract(bid=None, ask=None, last_price=None), 5.0) is None


class TestHoldingPeriod:
    def test_distance_scales_with_the_square_of_time_not_linearly(self):
        """
        ATR is about a one-day sigma and volatility grows with sqrt(t), so covering
        N x ATR takes on the order of N^2 days. Dividing distance by ATR — the obvious
        thing — understates the hold badly, and understating the hold understates
        decay, which is the exact bias the decay model exists to remove.
        """
        assert estimate_holding_days(price_distance=15.0, daily_atr=4.0) == pytest.approx(14.06)

    def test_a_farther_target_takes_disproportionately_longer(self):
        near = estimate_holding_days(2.0, 1.0)
        far = estimate_holding_days(4.0, 1.0)
        assert far == pytest.approx(4 * near)

    def test_the_estimate_is_not_degenerate_across_volatilities(self):
        """
        Under linear scaling the standard bracket cancels ATR entirely — every
        contract reports the same hold however volatile its underlying. It must not.
        """
        calm = estimate_holding_days(price_distance=3.75 * 1.0, daily_atr=1.0)
        wild = estimate_holding_days(price_distance=8.0, daily_atr=4.0)
        assert calm != wild

    def test_the_hold_never_exceeds_the_contracts_life(self):
        assert estimate_holding_days(1000.0, 1.0, dte=3) == 3.0

    def test_the_hold_is_capped(self):
        assert estimate_holding_days(1000.0, 1.0) == _MAX_HOLD_DAYS

    def test_missing_volatility_falls_back_rather_than_dividing_by_zero(self):
        assert estimate_holding_days(10.0, 0.0) > 0
        assert estimate_holding_days(None, None) > 0


class TestPremiumBracket:
    def test_the_bracket_is_stated_in_both_frames(self):
        """
        The position is exited on the *underlying* reaching a level, but the P&L is in
        premium, so both have to be reported.
        """
        levels = premium_trade_levels(
            _contract(), underlying_price=124.0,
            underlying_stop_distance=10.0, underlying_target_distance=15.0,
            daily_atr=4.0, dte=45,
        )
        assert levels["underlying_stop"] == 114.0
        assert levels["underlying_target"] == 139.0
        assert levels["premium_stop"] < levels["premium_entry"] < levels["premium_target"]

    def test_premium_reward_to_risk_is_not_the_underlyings(self):
        """
        Delta, gamma and decay all bend it, and on a low-delta contract they bend it a
        long way. Surfacing the underlying's 1.5 as if it were the position's would be
        the single most misleading number on the page.
        """
        levels = premium_trade_levels(
            _contract(), 124.0, 10.0, 15.0, daily_atr=4.0, dte=45
        )
        assert levels["premium_rr"] != pytest.approx(1.5, abs=0.2)

    def test_the_stop_never_implies_losing_the_whole_premium(self):
        levels = premium_trade_levels(
            _contract(), 124.0, 100.0, 150.0, daily_atr=4.0, dte=45
        )
        assert levels["premium_stop"] > 0
        assert levels["risk_dollars"] < levels["premium_entry"] * 100

    def test_a_put_brackets_the_other_way(self):
        put = _contract(contract_type="put", delta=-0.45)
        levels = premium_trade_levels(put, 124.0, 10.0, 15.0, daily_atr=4.0, dte=45)
        assert levels["underlying_stop"] == 134.0
        assert levels["underlying_target"] == 109.0

    def test_decay_is_reported_alongside_the_target(self):
        levels = premium_trade_levels(_contract(), 124.0, 10.0, 15.0, daily_atr=4.0, dte=45)
        assert levels["decay_at_target"] < 0
        assert levels["target_hold_days"] > levels["stop_hold_days"]

    def test_no_bracket_without_the_inputs(self):
        assert premium_trade_levels(_contract(), None, 10.0, 15.0, 4.0) == {}
        assert premium_trade_levels(_contract(), 124.0, None, 15.0, 4.0) == {}


class TestDerivedGreeks:
    def test_theta_per_premium_is_positive_for_a_decaying_long(self):
        """Larger is worse, so it reads the same way as every other cost here."""
        assert theta_per_premium(_contract()) == pytest.approx(0.0125)

    def test_a_cheaper_contract_decays_faster_in_relative_terms(self):
        cheap = theta_per_premium(_contract(bid=0.45, ask=0.55))
        dear = theta_per_premium(_contract(bid=9.90, ask=10.10))
        assert cheap > dear

    def test_gamma_leverage_is_unitless_and_comparable(self):
        assert gamma_leverage(_contract(), 124.0) > 0
        assert gamma_leverage(_contract(gamma=None), 124.0) is None

    def test_expected_move_grows_with_the_square_root_of_time(self):
        assert expected_move_pct(0.30, 365) == pytest.approx(30.0)
        assert expected_move_pct(0.30, 91) == pytest.approx(15.0, abs=0.1)

    def test_expected_move_needs_both_inputs(self):
        assert expected_move_pct(None, 45) is None
        assert expected_move_pct(0.3, 0) is None


class TestIVRank:
    HISTORY = [0.20, 0.22, 0.24, 0.25, 0.27, 0.28, 0.30, 0.32, 0.35, 0.40, 0.45, 0.50]

    def test_rank_places_todays_iv_in_its_own_recent_range(self):
        """
        An absolute IV of 45% means nothing alone — it is cheap for one name and
        historically expensive for another. What matters when buying premium is
        whether you are paying more than usual for *this* underlying.
        """
        assert iv_rank(0.20, self.HISTORY) == pytest.approx(1 / 12, abs=0.01)
        assert iv_rank(0.50, self.HISTORY) == 1.0

    def test_rank_uses_a_percentile_so_one_spike_does_not_flatten_the_scale(self):
        """A min/max range would compress every later reading into the bottom."""
        with_spike = self.HISTORY + [5.0]
        assert iv_rank(0.30, with_spike) > 0.5

    def test_too_little_history_returns_none_rather_than_a_confident_guess(self):
        assert iv_rank(0.30, [0.2, 0.3, 0.4]) is None
        assert iv_rank(0.30, []) is None

    def test_no_current_iv_returns_none(self):
        assert iv_rank(None, self.HISTORY) is None
