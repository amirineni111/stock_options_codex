"""
Model serving inside a real scan.

The property under test is the one the whole design rests on: **the rules propose and
the model disposes, and the two never swap roles.** A model that could promote would
turn a screening heuristic into an unaccountable oracle, and the failure would be
invisible — the dashboard would simply start showing setups nobody can explain.
"""
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pytest

from options_screening.config import AppSettings
from options_screening.features import INTRADAY_FEATURE_NAMES, INTRADAY_FEATURE_VERSION
from options_screening.intraday import IntradayScanRequest, run_intraday_scan
from options_screening.model import TrainedModel
from options_screening.storage import Storage
from test_intraday import MIDDAY, FakeClient


class ConstantModel(TrainedModel):
    """A model with a fixed opinion, so the gate's behaviour is unambiguous."""

    def __init__(self, probability: float, version: int = INTRADAY_FEATURE_VERSION):
        n = len(INTRADAY_FEATURE_NAMES)
        super().__init__(
            feature_names=list(INTRADAY_FEATURE_NAMES),
            feature_version=version,
            mean=[0.0] * n,
            scale=[1.0] * n,
            coefficients=[0.0] * n,
            # Coefficients are all zero, so every prediction is sigmoid(intercept).
            intercept=float(np.log(probability / (1 - probability))),
        )


@pytest.fixture()
def storage(tmp_path):
    store = Storage(Path(tmp_path) / "serve.sqlite3")
    store.initialize()
    return store


@pytest.fixture()
def scan_args():
    settings = AppSettings(polygon_api_key=None)
    request = IntradayScanRequest(tickers=["AAPL", "NVDA"], min_avg_dollar_volume=0.0)
    return settings, request


def _install(storage, probability, version=INTRADAY_FEATURE_VERSION, **flags):
    model = ConstantModel(probability, version)
    return storage.save_model("intraday", model.to_json(), version, **flags)


def _run(scan_args, storage):
    settings, request = scan_args
    return run_intraday_scan(settings, request, now=MIDDAY, client=FakeClient(), storage=storage)


class TestVetoOnly:
    def test_an_optimistic_model_cannot_manufacture_a_signal(self, storage, scan_args):
        """
        The model may not promote. A setup the rules scored below the actionable
        threshold must stay non-actionable however certain the model is.
        """
        rules_only, _, _ = _run(scan_args, storage)
        rules_signals = {r.ticker: r.trade_signal for r in rules_only}

        _install(storage, 0.99, activate=True)
        with_model, _, _ = _run(scan_args, storage)

        for result in with_model:
            was = rules_signals[result.ticker]
            if was in ("AVOID", "WATCH_ONLY"):
                assert result.trade_signal in ("AVOID", "WATCH_ONLY"), (
                    f"{result.ticker} was promoted from {was} to {result.trade_signal}"
                )

    def test_a_pessimistic_model_downgrades_but_never_deletes(self, storage, scan_args):
        _install(storage, 0.01, activate=True)
        results, _, _ = _run(scan_args, storage)

        assert results, "vetoed rows must still appear, with a reason"
        for result in results:
            assert result.trade_signal in ("AVOID", "WATCH_ONLY")
            assert result.signal_reason

    def test_the_probability_is_recorded_alongside_what_was_required(self, storage, scan_args):
        _install(storage, 0.42, activate=True)
        results, _, _ = _run(scan_args, storage)
        scored = [r for r in results if r.model_prob is not None]
        assert scored, "the model scored nothing"
        for result in scored:
            assert result.model_prob == pytest.approx(0.42, abs=0.01)
            assert result.required_prob is not None


class TestShadowMode:
    def test_a_shadow_model_scores_but_changes_nothing(self, storage, scan_args):
        """
        The only way to learn what a model would do to the trades it wants to block:
        once it is gating, those trades stop happening and stop being measurable.
        """
        rules_only, _, _ = _run(scan_args, storage)
        baseline = {r.ticker: (r.trade_signal, r.total_score) for r in rules_only}

        _install(storage, 0.01, shadow=True)
        shadowed, _, _ = _run(scan_args, storage)

        for result in shadowed:
            assert (result.trade_signal, result.total_score) == baseline[result.ticker]
            assert result.model_prob is not None, "shadow model did not log a probability"

    def test_gating_takes_precedence_over_shadowing(self, storage, scan_args):
        _install(storage, 0.5, shadow=True)
        _install(storage, 0.01, activate=True)
        results, _, _ = _run(scan_args, storage)
        scored = [r for r in results if r.model_prob is not None]
        assert scored
        assert all(r.model_prob == pytest.approx(0.01, abs=0.01) for r in scored)


class TestFailClosed:
    def test_a_stale_feature_version_serves_rules_only(self, storage, scan_args):
        rules_only, _, _ = _run(scan_args, storage)
        baseline = {r.ticker: r.total_score for r in rules_only}

        _install(storage, 0.01, version=INTRADAY_FEATURE_VERSION + 99, activate=True)
        results, _, _ = _run(scan_args, storage)

        for result in results:
            assert result.model_prob is None
            assert result.total_score == baseline[result.ticker]

    def test_a_corrupt_model_does_not_stop_the_scan(self, storage, scan_args):
        storage.save_model("intraday", "{not json", INTRADAY_FEATURE_VERSION, activate=True)
        results, summary, _ = _run(scan_args, storage)
        assert results
        assert summary.errors == 0

    def test_a_model_on_the_other_lane_is_not_consulted(self, storage, scan_args):
        storage.save_model(
            "options", ConstantModel(0.01).to_json(), INTRADAY_FEATURE_VERSION, activate=True
        )
        results, _, _ = _run(scan_args, storage)
        assert all(r.model_prob is None for r in results)
