"""The prediction scorecard: grading each call against what happened."""
import pandas as pd
import pytest

from options_screening import scorecard


def _predictions(rows):
    base = dict(
        ticker="AAPL", contract_ticker=None, signal="BUY_CANDIDATE", direction=1,
        total_score=55.0, model_prob=None, outcome="WIN", exit_reason="TARGET",
        r_multiple=1.4, resolved_at="2026-10-01 15:00:00",
    )
    return pd.DataFrame([{**base, **row} for row in rows])


def test_the_verdict_grades_the_prediction_not_the_pnl():
    assert scorecard.verdict("TARGET") == "SUCCESS"
    assert scorecard.verdict("STOP") == "FAILED"
    # A timeout that drifted into profit is a net WIN but not a correct call.
    assert scorecard.verdict("TIMEOUT") == "EXPIRED"
    assert scorecard.verdict("EXPIRY") == "EXPIRED"


def test_side_reads_options_from_the_contract_and_stocks_from_direction():
    assert scorecard.side("BUY_PUT_CANDIDATE", "O:AMD261120P00180000", 1) == "PUT"
    assert scorecard.side(None, "O:AMD261120C00180000", 1) == "CALL"
    assert scorecard.side("SHORT_CANDIDATE", None, -1) == "SHORT"
    assert scorecard.side("BUY_CANDIDATE", None, 1) == "LONG"


def test_accuracy_counts_only_the_decided_calls():
    frame = scorecard.annotate(_predictions([
        {"exit_reason": "TARGET"},
        {"exit_reason": "TARGET"},
        {"exit_reason": "STOP", "outcome": "LOSS", "r_multiple": -1.1},
        {"exit_reason": "TIMEOUT", "outcome": "WIN", "r_multiple": 0.2},
    ]))
    summary = scorecard.summarize(frame)
    assert (summary["success"], summary["failed"], summary["expired"]) == (2, 1, 1)
    assert summary["accuracy"] == pytest.approx(2 / 3)
    assert summary["net_win_rate"] == pytest.approx(3 / 4)
    assert summary["total_r"] == pytest.approx(1.9)


def test_an_empty_scorecard_has_no_accuracy_rather_than_zero():
    summary = scorecard.summarize(scorecard.annotate(_predictions([]).iloc[0:0]))
    assert summary["resolved"] == 0 and summary["accuracy"] is None


def test_breakdown_by_score_band_shows_whether_confidence_pays():
    frame = scorecard.annotate(_predictions([
        {"total_score": 48, "exit_reason": "STOP", "outcome": "LOSS"},
        {"total_score": 49, "exit_reason": "STOP", "outcome": "LOSS"},
        {"total_score": 75, "exit_reason": "TARGET"},
        {"total_score": 78, "exit_reason": "TARGET"},
        {"total_score": None, "exit_reason": "TARGET"},
    ]))
    table = scorecard.breakdown(frame, "score_band").set_index("score_band")
    assert table.loc["<50", "accuracy"] == 0.0
    assert table.loc["70-80", "accuracy"] == 1.0
    assert table.loc["unscored", "called"] == 1


def test_model_probability_bands_carry_the_average_prediction():
    frame = scorecard.annotate(_predictions([
        {"model_prob": 0.55, "exit_reason": "TARGET"},
        {"model_prob": 0.57, "exit_reason": "STOP", "outcome": "LOSS"},
        {"model_prob": None},
    ]))
    table = scorecard.breakdown(frame, "prob_band").set_index("prob_band")
    assert table.loc["50-60%", "avg_predicted"] == pytest.approx(0.56)
    assert table.loc["50-60%", "accuracy"] == 0.5
    assert "no model" in table.index


def test_week_is_the_monday_the_trade_resolved_in():
    frame = scorecard.annotate(_predictions([{"resolved_at": "2026-10-04 15:00:00"}]))  # a Sunday
    assert frame.iloc[0]["week"] == "2026-09-28"
