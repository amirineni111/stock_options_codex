"""
The trained model and the promotion gate.

Two properties matter more than accuracy here. The model must **only ever veto** — it
cannot promote a setup the rules rejected — and it must **fail closed**: a corrupt or
stale-contract model degrades to rules-only rather than serving confident nonsense.
"""
import json

import numpy as np
import pytest

from options_screening.features import (
    INTRADAY_FEATURE_NAMES,
    INTRADAY_FEATURE_VERSION,
    OPTIONS_FEATURE_NAMES,
    to_vector,
)
from options_screening.model import (
    TrainedModel,
    brier_score,
    calibration_bins,
    fit,
    gated_performance,
    roc_auc,
    walk_forward,
)
from options_screening.storage import Storage
from options_screening.training import (
    evaluate_and_fit,
    gate_summary,
    load_serving_model,
    save_candidate,
)

NAMES = ("a", "b", "c")


def _separable(n=200, seed=0, noise=0.5):
    """A dataset with genuine signal in feature `a`."""
    rng = np.random.default_rng(seed)
    X = rng.normal(size=(n, 3))
    logits = 1.8 * X[:, 0] + noise * rng.normal(size=n)
    y = (logits > 0).astype(int)
    return X.tolist(), y.tolist()


def _noise(n=200, seed=1):
    """No relationship at all between features and label."""
    rng = np.random.default_rng(seed)
    return rng.normal(size=(n, 3)).tolist(), rng.integers(0, 2, size=n).tolist()


class TestFitting:
    def test_it_learns_a_real_signal(self):
        X, y = _separable()
        model = fit(X, y, NAMES, 1)
        assert model is not None
        assert roc_auc(y, model.predict_proba(X)) > 0.85

    def test_it_reports_no_skill_on_pure_noise(self):
        """A model that finds structure in noise is the one that loses money."""
        X, y = _noise()
        model = fit(X, y, NAMES, 1)
        assert model is not None
        assert 0.35 < roc_auc(y, model.predict_proba(X)) < 0.75

    def test_probabilities_stay_in_range(self):
        X, y = _separable()
        probabilities = fit(X, y, NAMES, 1).predict_proba(X)
        assert probabilities.min() >= 0.0 and probabilities.max() <= 1.0

    def test_extreme_inputs_do_not_overflow(self):
        """The naive sigmoid returns nan for large negative inputs."""
        X, y = _separable()
        model = fit(X, y, NAMES, 1)
        wild = model.predict_proba([[1e6, -1e6, 1e6], [-1e6, 1e6, -1e6]])
        assert np.all(np.isfinite(wild))

    def test_a_single_class_cannot_be_fitted(self):
        assert fit([[1.0, 2.0, 3.0]] * 20, [1] * 20, NAMES, 1) is None

    def test_too_few_rows_returns_none_rather_than_a_confident_model(self):
        assert fit([[1.0, 2.0, 3.0]] * 5, [0, 1, 0, 1, 0], NAMES, 1) is None

    def test_a_constant_feature_does_not_divide_by_zero(self):
        X, y = _separable()
        for row in X:
            row[2] = 7.0
        model = fit(X, y, NAMES, 1)
        assert model is not None and np.all(np.isfinite(model.predict_proba(X)))


class TestSerialisation:
    def test_a_model_round_trips_exactly(self):
        X, y = _separable()
        model = fit(X, y, NAMES, 1)
        restored = TrainedModel.from_json(model.to_json())
        assert np.allclose(model.predict_proba(X), restored.predict_proba(X))

    def test_the_feature_contract_travels_with_the_model(self):
        model = fit(*_separable(), NAMES, 7)
        restored = TrainedModel.from_json(model.to_json())
        assert restored.feature_version == 7
        assert restored.feature_names == list(NAMES)

    def test_corrupt_json_returns_none_rather_than_raising(self):
        """A bad model must degrade the scan to rules-only, not stop it."""
        assert TrainedModel.from_json("{not json") is None
        assert TrainedModel.from_json('{"feature_names": ["a"]}') is None


class TestMetrics:
    def test_auc_is_one_for_a_perfect_ranking(self):
        assert roc_auc([0, 0, 1, 1], [0.1, 0.2, 0.8, 0.9]) == 1.0

    def test_auc_is_half_for_all_ties(self):
        """Otherwise a model that returns one constant scores suspiciously well."""
        assert roc_auc([0, 1, 0, 1], [0.5, 0.5, 0.5, 0.5]) == 0.5

    def test_auc_is_none_when_a_class_is_missing(self):
        assert roc_auc([1, 1, 1], [0.2, 0.5, 0.9]) is None

    def test_brier_rewards_calibration_not_just_ranking(self):
        confident_right = brier_score([1, 0], [0.95, 0.05])
        hedged_right = brier_score([1, 0], [0.55, 0.45])
        assert confident_right < hedged_right

    def test_calibration_bins_compare_predicted_against_observed(self):
        bins = calibration_bins([1, 1, 0, 0], [0.9, 0.85, 0.1, 0.15], bins=5)
        assert any(b["observed"] == 1.0 for b in bins)
        assert any(b["observed"] == 0.0 for b in bins)


class TestWalkForward:
    def test_predictions_are_genuinely_out_of_sample(self):
        """
        A random split leaks: trades armed minutes apart share a market regime, so
        shuffling puts near-duplicates on both sides and reports skill that will not
        survive contact with tomorrow.
        """
        X, y = _separable(n=300)
        result = walk_forward(X, y, NAMES, 1, folds=4)
        assert result["ok"]
        assert result["n"] < len(y), "some rows must be reserved for training"
        assert result["auc"] > 0.8

    def test_noise_does_not_produce_out_of_sample_skill(self):
        X, y = _noise(n=300)
        result = walk_forward(X, y, NAMES, 1, folds=4)
        assert result["ok"]
        assert result["auc"] < 0.65

    def test_the_fold_spread_is_reported(self):
        """A wide spread means the model is fitting regimes, which a pooled AUC hides."""
        result = walk_forward(*_separable(n=300), NAMES, 1, folds=4)
        assert result["fold_auc_std"] is not None

    def test_too_little_data_is_refused_with_a_reason(self):
        result = walk_forward(*_separable(n=20), NAMES, 1, folds=4)
        assert not result["ok"] and "need" in result["reason"]


class TestGatedPerformance:
    def test_it_measures_only_the_trades_the_gate_would_allow(self):
        labels = [1, 1, 0, 0]
        scores = [0.9, 0.8, 0.4, 0.3]
        rows = gated_performance(labels, scores, [1.5, 1.5, -1.0, -1.0], thresholds=[0.5])
        assert rows[0]["trades"] == 2
        assert rows[0]["win_rate"] == 1.0
        assert rows[0]["expectancy_r"] == 1.5

    def test_a_threshold_that_keeps_nothing_reports_no_trades(self):
        rows = gated_performance([1, 0], [0.2, 0.1], [1.0, -1.0], thresholds=[0.9])
        assert rows[0]["trades"] == 0 and rows[0]["expectancy_r"] is None

    def test_expectancy_can_be_negative_while_auc_looks_fine(self):
        """
        Exactly why expectancy, not AUC, is the promotion criterion: ranking the
        losers correctly does not make the winners pay for them.
        """
        labels = [1, 0, 0, 0]
        scores = [0.9, 0.6, 0.5, 0.4]
        assert roc_auc(labels, scores) == 1.0
        rows = gated_performance(labels, scores, [1.5, -1.0, -1.0, -1.0], thresholds=[0.3])
        assert rows[0]["expectancy_r"] < 0


class TestServingModel:
    @pytest.fixture()
    def storage(self, tmp_path):
        store = Storage(tmp_path / "m.sqlite3")
        store.initialize()
        return store

    def _store_model(self, storage, version=INTRADAY_FEATURE_VERSION, **kw):
        model = fit(*_separable(), INTRADAY_FEATURE_NAMES[:3], version)
        return storage.save_model("intraday", model.to_json(), version, **kw)

    def test_an_active_model_is_served_for_gating(self, storage):
        self._store_model(storage, activate=True)
        model, mode = load_serving_model(storage, "intraday")
        assert model is not None and mode == "active"

    def test_a_shadow_model_is_served_only_when_nothing_gates(self, storage):
        """A shadow run must never be mistaken for a live one."""
        self._store_model(storage, shadow=True)
        _, mode = load_serving_model(storage, "intraday")
        assert mode == "shadow"

        self._store_model(storage, activate=True)
        _, mode = load_serving_model(storage, "intraday")
        assert mode == "active"

    def test_a_stale_feature_version_falls_back_to_rules_only(self, storage):
        """
        Coefficients are positional. A model built on a different contract would score
        every feature against the wrong weight and return confident nonsense, so the
        mismatch fails closed.
        """
        self._store_model(storage, version=INTRADAY_FEATURE_VERSION + 99, activate=True)
        model, mode = load_serving_model(storage, "intraday")
        assert model is None and mode is None

    def test_a_corrupt_model_does_not_crash_serving(self, storage):
        storage.save_model("intraday", "{not json", INTRADAY_FEATURE_VERSION, activate=True)
        model, _ = load_serving_model(storage, "intraday")
        assert model is None

    def test_no_model_means_no_model(self, storage):
        assert load_serving_model(storage, "intraday") == (None, None)


class TestPromotionGate:
    @pytest.fixture()
    def storage(self, tmp_path):
        store = Storage(tmp_path / "g.sqlite3")
        store.initialize()
        return store

    def _seed(self, storage, n=200, informative=True, seed=0):
        """Arm and resolve `n` trades whose features do (or do not) predict the outcome."""
        rng = np.random.default_rng(seed)
        for i in range(n):
            signal_strength = rng.normal()
            win = (signal_strength > 0) if informative else bool(rng.integers(0, 2))
            features = {name: 0.0 for name in INTRADAY_FEATURE_NAMES}
            features["rsi_dir"] = float(signal_strength)
            tracking_id = storage.record_tracked_signal(
                lane="intraday", ticker=f"T{i}", signal="BUY_CANDIDATE", direction=1,
                entry=100.0, stop=97.5, target=103.75,
                entry_ts=f"2026-01-01T{i % 24:02d}:00:00+00:00",
                stop_dollars=2.5, features=features,
                feature_version=INTRADAY_FEATURE_VERSION, cost_pct=0.0,
            )
            assert tracking_id is not None
            row = {"id": tracking_id, "entry_price": 100.0, "direction": 1,
                   "stop_dollars": 2.5, "cost_pct": 0.0, "ticker": f"T{i}",
                   "signal": "BUY_CANDIDATE", "contract_ticker": None,
                   "entry_ts": f"2026-01-01T{i % 24:02d}:00:00+00:00"}
            exit_price = 103.75 if win else 97.5
            storage._close_tracked(row, "intraday", exit_price,
                                   "2026-01-01T23:00:00+00:00",
                                   "TARGET" if win else "STOP",
                                   "WIN" if win else "LOSS")

    def test_not_enough_data_is_reported_as_such(self, storage):
        report = evaluate_and_fit(storage, "intraday")
        assert not report["passes"]
        assert "NOT ENOUGH DATA" in gate_summary(report)

    def test_an_informative_dataset_clears_the_gate(self, storage):
        self._seed(storage, n=200, informative=True)
        report = evaluate_and_fit(storage, "intraday", min_trades=120)
        assert report["n_resolved"] == 200
        assert report["auc"] > 0.7
        assert report["passes"], report["reason"]
        assert "CANDIDATE" in gate_summary(report)

    def test_a_noise_dataset_is_refused(self, storage):
        self._seed(storage, n=200, informative=False, seed=3)
        report = evaluate_and_fit(storage, "intraday", min_trades=120)
        assert not report["passes"]
        assert "DO NOT PROMOTE" in gate_summary(report)

    def test_a_passing_candidate_still_saves_inactive_by_default(self, storage):
        """Training must never change live behaviour as a side effect."""
        self._seed(storage, n=200, informative=True)
        report = evaluate_and_fit(storage, "intraday", min_trades=120)
        save_candidate(storage, "intraday", report)
        assert storage.load_model("intraday") is None
        assert storage.load_models("intraday").shape[0] == 1

    def test_a_failed_candidate_cannot_be_promoted_without_force(self, storage):
        self._seed(storage, n=200, informative=False, seed=3)
        report = evaluate_and_fit(storage, "intraday", min_trades=120)
        save_candidate(storage, "intraday", report, activate=True)
        assert storage.load_model("intraday") is None

    def test_force_makes_the_override_possible_and_visible(self, storage):
        self._seed(storage, n=200, informative=False, seed=3)
        report = evaluate_and_fit(storage, "intraday", min_trades=120)
        save_candidate(storage, "intraday", report, activate=True, force=True)
        assert storage.load_model("intraday") is not None

    def test_the_stored_metrics_record_why_it_passed_or_failed(self, storage):
        self._seed(storage, n=200, informative=True)
        report = evaluate_and_fit(storage, "intraday", min_trades=120)
        save_candidate(storage, "intraday", report)
        metrics = json.loads(storage.load_models("intraday").iloc[0]["metrics_json"])
        assert "auc" in metrics and "reason" in metrics and "best_gate" in metrics
