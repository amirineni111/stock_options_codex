"""
Forward testing: arming signals, resolving them, and the model store.

This is the half of the learning loop that makes the other half possible. If feature
vectors stop being logged, or outcomes stop linking back to the row that produced
them, nothing breaks visibly — the dashboard keeps working and the training set simply
never grows. That silence is why these are tested directly.
"""
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from options_screening.storage import Storage

ENTRY_TS = "2026-08-14T14:00:00+00:00"
NOW = datetime(2026, 8, 14, 18, 0, tzinfo=timezone.utc)


@pytest.fixture()
def storage(tmp_path) -> Storage:
    store = Storage(Path(tmp_path) / "test.sqlite3")
    store.initialize()
    return store


def _bars(prices, start="2026-08-14T14:15:00+00:00", minutes=15):
    """Bars whose high/low straddle each close, from a list of (low, high) pairs."""
    base = datetime.fromisoformat(start)
    return [
        {
            "timestamp": (base + timedelta(minutes=minutes * i)).isoformat(),
            "open": (low + high) / 2,
            "high": high,
            "low": low,
            "close": (low + high) / 2,
            "volume": 1000,
        }
        for i, (low, high) in enumerate(prices)
    ]


def _arm(storage, **overrides):
    fields = dict(
        lane="intraday",
        ticker="AAPL",
        signal="BUY_CANDIDATE",
        direction=1,
        entry=100.0,
        stop=97.5,
        target=103.75,
        entry_ts=ENTRY_TS,
        stop_dollars=2.5,
        target_dollars=3.75,
        cost_pct=0.08,
        features={"rsi_dir": 0.6, "adx14": 30.0},
        feature_version=1,
        total_score=62.0,
    )
    fields.update(overrides)
    return storage.record_tracked_signal(**fields)


class TestArming:
    def test_arming_returns_a_tracking_id(self, storage):
        assert _arm(storage) is not None

    def test_the_feature_vector_is_stored_at_arm_time(self, storage):
        """
        Load-bearing for the entire loop. The snapshot tables are replaced every
        scan, so a vector not stored now cannot be reconstructed when the trade
        resolves days later — the outcome row would be unlearnable.
        """
        _arm(storage)
        tracked = storage.load_tracked("intraday")
        assert tracked.iloc[0]["features_json"]
        assert tracked.iloc[0]["feature_version"] == 1

    def test_the_same_setup_is_not_armed_twice_while_open(self, storage):
        assert _arm(storage) is not None
        assert _arm(storage) is None

    def test_the_cooldown_blocks_immediate_re_entry_after_a_stop_out(self, storage):
        """
        Without this, every scan after a stop-out re-enters the same chop and racks
        up correlated losses that look like independent evidence to the model.
        """
        tracking_id = _arm(storage)
        storage.resolve_intraday_signals("AAPL", _bars([(96.0, 99.0)]), now=NOW)
        assert storage.load_tracked("intraday", status="closed").shape[0] == 1
        assert _arm(storage) is None, "re-armed inside the cooldown"

    def test_the_opposite_direction_is_a_different_trade(self, storage):
        assert _arm(storage, direction=1) is not None
        assert _arm(storage, direction=-1, stop=102.5, target=96.25) is not None

    def test_two_contracts_on_one_underlying_are_different_trades(self, storage):
        """The dedupe key is the instrument, not the ticker."""
        assert _arm(storage, lane="options", contract_ticker="O:AAPL_C125") is not None
        assert _arm(storage, lane="options", contract_ticker="O:AAPL_C130") is not None

    def test_lanes_do_not_collide(self, storage):
        assert _arm(storage, lane="intraday") is not None
        assert _arm(storage, lane="options", contract_ticker="O:AAPL_C125") is not None


class TestIntradayResolution:
    def test_a_target_touch_records_a_win(self, storage):
        _arm(storage)
        assert storage.resolve_intraday_signals("AAPL", _bars([(99.0, 104.0)]), now=NOW) == 1
        outcome = storage.load_outcomes("intraday").iloc[0]
        assert outcome["outcome"] == "WIN"
        assert outcome["exit_reason"] == "TARGET"

    def test_a_stop_touch_records_a_loss(self, storage):
        _arm(storage)
        storage.resolve_intraday_signals("AAPL", _bars([(97.0, 99.0)]), now=NOW)
        outcome = storage.load_outcomes("intraday").iloc[0]
        assert outcome["outcome"] == "LOSS"
        assert outcome["exit_reason"] == "STOP"

    def test_a_bar_spanning_both_levels_is_read_as_the_stop(self, storage):
        """
        There is no way to know which was touched first inside one bar. Assuming the
        target would inflate the win rate precisely on the most volatile bars, which
        is where the inflation does the most damage.
        """
        _arm(storage)
        storage.resolve_intraday_signals("AAPL", _bars([(97.0, 104.0)]), now=NOW)
        assert storage.load_outcomes("intraday").iloc[0]["outcome"] == "LOSS"

    def test_bars_before_entry_cannot_resolve_a_trade(self, storage):
        _arm(storage)
        stale = _bars([(90.0, 110.0)], start="2026-08-14T13:00:00+00:00")
        assert storage.resolve_intraday_signals("AAPL", stale, now=NOW) == 0

    def test_an_untouched_bracket_stays_open(self, storage):
        _arm(storage)
        assert storage.resolve_intraday_signals("AAPL", _bars([(99.5, 100.5)]), now=NOW) == 0
        assert storage.load_tracked("intraday", status="open").shape[0] == 1

    def test_a_stale_trade_times_out_at_the_last_close(self, storage):
        _arm(storage)
        much_later = NOW + timedelta(hours=20)
        resolved = storage.resolve_intraday_signals(
            "AAPL", _bars([(99.5, 100.5)]), max_hold_hours=8.0, now=much_later
        )
        assert resolved == 1
        assert storage.load_outcomes("intraday").iloc[0]["exit_reason"] == "TIMEOUT"

    def test_a_short_resolves_the_other_way(self, storage):
        _arm(storage, direction=-1, stop=102.5, target=96.25)
        storage.resolve_intraday_signals("AAPL", _bars([(95.0, 99.0)]), now=NOW)
        outcome = storage.load_outcomes("intraday").iloc[0]
        assert outcome["outcome"] == "WIN"


class TestAccounting:
    def test_r_is_computed_from_net_not_gross(self, storage):
        """
        Every fill pays the spread twice. A bracket that touches its printed target
        still cost something to enter and exit, so reporting gross R overstates every
        result by a consistent margin — the kind of error that survives review because
        it makes the numbers look plausible.
        """
        _arm(storage)
        storage.resolve_intraday_signals("AAPL", _bars([(99.0, 104.0)]), now=NOW)
        outcome = storage.load_outcomes("intraday").iloc[0]

        assert outcome["cost_dollars"] > 0
        assert outcome["net_dollars"] < outcome["gross_dollars"]
        assert outcome["r_multiple"] == pytest.approx(
            outcome["net_dollars"] / 2.5, abs=1e-4
        )

    def test_a_winner_still_falls_short_of_the_nominal_reward_ratio(self, storage):
        _arm(storage)
        storage.resolve_intraday_signals("AAPL", _bars([(99.0, 104.0)]), now=NOW)
        assert storage.load_outcomes("intraday").iloc[0]["r_multiple"] < 1.5

    def test_hold_time_measures_the_trade_not_the_scan_gap(self, storage):
        """
        Measured as (now - created_at) instead, this reports how long until a scan
        happened to look at the row — which in the forex sibling made wins and losses
        both average the same ~506 minutes and hid the difference entirely.
        """
        _arm(storage)
        bars = _bars([(99.5, 100.5), (99.5, 100.5), (99.0, 104.0)])
        storage.resolve_intraday_signals("AAPL", bars, now=NOW + timedelta(days=3))
        # Entry 14:00, resolving bar 14:45 -> 45 minutes, regardless of when we looked.
        assert storage.load_outcomes("intraday").iloc[0]["hold_minutes"] == 45


class TestOptionsResolution:
    def _arm_option(self, storage, **kw):
        fields = dict(
            lane="options",
            ticker="AAPL",
            contract_ticker="O:AAPL_C125",
            signal="BUY_CALL_CANDIDATE",
            direction=1,
            entry=4.00,
            stop=2.40,
            target=7.00,
            entry_ts=ENTRY_TS,
            stop_dollars=1.60,
            cost_pct=5.0,
            expiration_date="2026-09-18",
            features={"abs_delta": 0.45},
            feature_version=1,
        )
        fields.update(kw)
        return storage.record_tracked_signal(**fields)

    def test_the_premium_reaching_the_target_is_a_win(self, storage):
        self._arm_option(storage)
        resolved = storage.resolve_options_signals(
            {"O:AAPL_C125": 7.50}, today=date(2026, 8, 20), now=NOW
        )
        assert resolved == 1
        assert storage.load_outcomes("options").iloc[0]["outcome"] == "WIN"

    def test_the_premium_reaching_the_stop_is_a_loss(self, storage):
        self._arm_option(storage)
        storage.resolve_options_signals({"O:AAPL_C125": 2.00}, today=date(2026, 8, 20), now=NOW)
        assert storage.load_outcomes("options").iloc[0]["outcome"] == "LOSS"

    def test_a_premium_between_the_levels_stays_open(self, storage):
        self._arm_option(storage)
        assert storage.resolve_options_signals(
            {"O:AAPL_C125": 4.20}, today=date(2026, 8, 20), now=NOW
        ) == 0

    def test_a_contract_closes_the_day_before_expiry(self, storage):
        """The last session is gamma and decay noise, not a test of the thesis."""
        self._arm_option(storage)
        resolved = storage.resolve_options_signals(
            {"O:AAPL_C125": 4.20}, today=date(2026, 9, 17), now=NOW
        )
        assert resolved == 1
        assert storage.load_outcomes("options").iloc[0]["exit_reason"] == "EXPIRY"

    def test_a_missing_quote_leaves_the_trade_open(self, storage):
        """A contract we could not price is unresolved, not a loss."""
        self._arm_option(storage)
        assert storage.resolve_options_signals({}, today=date(2026, 8, 20), now=NOW) == 0

    def test_option_cost_is_charged_at_the_contracts_own_spread(self, storage):
        self._arm_option(storage)
        storage.resolve_options_signals({"O:AAPL_C125": 7.50}, today=date(2026, 8, 20), now=NOW)
        outcome = storage.load_outcomes("options").iloc[0]
        assert outcome["cost_dollars"] == pytest.approx(0.20)  # 5% of a $4 premium


class TestPerformanceAndTraining:
    def test_performance_is_recomputed_on_resolution(self, storage):
        _arm(storage)
        storage.resolve_intraday_signals("AAPL", _bars([(99.0, 104.0)]), now=NOW)
        stats = storage.load_performance("intraday")
        overall = stats[stats["scope"] == "overall"].iloc[0]
        assert overall["trades"] == 1 and overall["wins"] == 1
        assert overall["win_rate"] == 1.0

    def test_training_rows_carry_features_and_a_label(self, storage):
        _arm(storage)
        storage.resolve_intraday_signals("AAPL", _bars([(99.0, 104.0)]), now=NOW)
        rows = storage.load_training_rows("intraday", feature_version=1)
        assert len(rows) == 1
        assert rows[0]["features"] == {"rsi_dir": 0.6, "adx14": 30.0}
        assert rows[0]["outcome"] == "WIN"

    def test_rows_on_a_different_feature_version_are_excluded_not_imputed(self, storage):
        """
        Their inputs are unrecoverable. Inventing them would train the model on
        fiction, which is worse than training on less data.
        """
        _arm(storage)
        storage.resolve_intraday_signals("AAPL", _bars([(99.0, 104.0)]), now=NOW)
        assert storage.load_training_rows("intraday", feature_version=2) == []

    def test_rows_without_features_are_excluded(self, storage):
        _arm(storage, features=None, feature_version=None)
        storage.resolve_intraday_signals("AAPL", _bars([(99.0, 104.0)]), now=NOW)
        assert storage.load_training_rows("intraday", feature_version=1) == []

    def test_unresolved_trades_are_not_training_data(self, storage):
        _arm(storage)
        assert storage.load_training_rows("intraday", feature_version=1) == []


class TestModelStore:
    def test_a_candidate_saves_inactive(self, storage):
        """
        Training must never change live behaviour as a side effect. Promotion is a
        separate, deliberate act.
        """
        storage.save_model("intraday", '{"w": [1]}', feature_version=1)
        assert storage.load_model("intraday") is None
        assert storage.load_models("intraday").shape[0] == 1

    def test_activating_a_model_makes_it_the_one_that_serves(self, storage):
        model_id = storage.save_model("intraday", '{"w": [1]}', 1, activate=True)
        active = storage.load_model("intraday")
        assert active is not None and active["id"] == model_id

    def test_only_one_model_is_active_per_lane(self, storage):
        storage.save_model("intraday", '{"w": [1]}', 1, activate=True)
        second = storage.save_model("intraday", '{"w": [2]}', 1, activate=True)
        assert storage.load_model("intraday")["id"] == second
        models = storage.load_models("intraday")
        assert models["is_active"].sum() == 1

    def test_the_two_lanes_have_independent_models(self, storage):
        storage.save_model("intraday", '{"w": [1]}', 1, activate=True)
        storage.save_model("options", '{"w": [2]}', 1, activate=True)
        assert storage.load_model("intraday")["model_json"] == '{"w": [1]}'
        assert storage.load_model("options")["model_json"] == '{"w": [2]}'

    def test_a_model_can_shadow_without_gating(self, storage):
        model_id = storage.save_model("intraday", '{"w": [1]}', 1, shadow=True)
        assert storage.load_model("intraday", "is_active") is None
        assert storage.load_model("intraday", "is_shadow")["id"] == model_id

    def test_rollback_clears_the_active_model(self, storage):
        storage.save_model("intraday", '{"w": [1]}', 1, activate=True)
        storage.clear_model_flag("intraday", "is_active")
        assert storage.load_model("intraday") is None

    def test_an_unknown_flag_is_rejected_rather_than_interpolated(self, storage):
        """The only place a column name is interpolated into SQL."""
        with pytest.raises(ValueError):
            storage.load_model("intraday", "1=1; DROP TABLE trained_models--")
        with pytest.raises(ValueError):
            storage.set_model_flag("intraday", 1, "is_evil")
