"""
The retrain pipeline and the quality gate, shared by the CLI and the dashboard.

Both entry points call ``evaluate_and_fit``, so a model promoted from the Model tab is
the same object, judged by the same numbers, as one promoted from
``scripts/train_model.py``. Splitting the logic across the two would let the reported
metrics drift apart, and the version a user actually promoted would be the one whose
numbers they had not seen.

The gate has two conditions and both must hold:

1. **Out-of-sample AUC clears ``min_auc``** — the model has some ranking skill on rows
   it never saw.
2. **The best gated expectancy is positive** — that skill survives contact with the
   cost model. AUC can look respectable while expectancy stays negative; ranking the
   losers correctly does not make the winners pay for them.

A candidate that fails is still *saved*, inactive, with its metrics. Being able to see
why something failed is worth more than a clean table.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Tuple

from .features import (
    INTRADAY_FEATURE_NAMES,
    INTRADAY_FEATURE_VERSION,
    OPTIONS_FEATURE_NAMES,
    OPTIONS_FEATURE_VERSION,
    to_vector,
)
from .model import (
    DEFAULT_L2,
    TrainedModel,
    fit,
    gated_performance,
    top_decile_precision,
    walk_forward,
)
from .signals import _PROB_MARGIN, _RR, breakeven_win_rate

# Below this many resolved trades the walk-forward folds are too small to mean
# anything. The siblings settled near 120; the same figure is used here because the
# effective sample is if anything smaller (a chain's contracts on one underlying are
# even more correlated than two stocks in the same sector).
MIN_RESOLVED_TRADES = 120

# Out-of-sample AUC a candidate must clear. Not far above chance on purpose: the
# expectancy condition is the one doing the real work, and demanding a high AUC on a
# few hundred correlated rows selects for overfitting rather than against it.
MIN_AUC = 0.55

LANES = {
    "intraday": (INTRADAY_FEATURE_NAMES, INTRADAY_FEATURE_VERSION),
    "options": (OPTIONS_FEATURE_NAMES, OPTIONS_FEATURE_VERSION),
}


def lane_contract(lane: str) -> Tuple[Sequence[str], int]:
    if lane not in LANES:
        raise ValueError(f"unknown lane: {lane}")
    return LANES[lane]


def evaluate_and_fit(
    storage,
    lane: str,
    l2: float = DEFAULT_L2,
    folds: int = 4,
    min_auc: float = MIN_AUC,
    min_trades: int = MIN_RESOLVED_TRADES,
) -> Dict[str, Any]:
    """
    Pull resolved trades, walk-forward evaluate, fit a final model, and gate it.

    Returns a report dict. ``report["model"]`` is the fitted model when there is one;
    ``report["passes"]`` is the gate verdict. Nothing here writes to the model store —
    saving and promoting are the caller's decisions, deliberately.
    """
    names, version = lane_contract(lane)
    rows = storage.load_training_rows(lane, version)

    report: Dict[str, Any] = {
        "lane": lane,
        "feature_version": version,
        "n_resolved": len(rows),
        "min_trades": min_trades,
        "passes": False,
        "model": None,
        "reason": "",
    }

    if len(rows) < min_trades:
        report["reason"] = (
            f"{len(rows)} resolved trades at feature version {version}; "
            f"need {min_trades} before the folds mean anything"
        )
        return report

    vectors = [to_vector(row["features"], names) for row in rows]
    labels = [1 if row["outcome"] == "WIN" else 0 for row in rows]
    r_multiples = [row.get("r_multiple") for row in rows]

    evaluation = walk_forward(vectors, labels, names, version, folds=folds, l2=l2)
    report["walk_forward"] = evaluation
    if not evaluation.get("ok"):
        report["reason"] = evaluation.get("reason", "walk-forward evaluation failed")
        return report

    # Expectancy is measured on the same out-of-sample predictions, so the threshold
    # sweep describes what serving would actually have done.
    oos_r = _align_r(r_multiples, len(evaluation["scores"]))
    gated = gated_performance(evaluation["labels"], evaluation["scores"], oos_r)
    report["gated"] = gated
    report["top_decile_precision"] = top_decile_precision(
        evaluation["labels"], evaluation["scores"]
    )

    usable = [g for g in gated if g["trades"] and g["trades"] >= 10]
    best = max(usable, key=lambda g: g["expectancy_r"]) if usable else None
    report["best_gate"] = best

    # The threshold serving would actually use, so the report answers the question
    # that matters rather than the most flattering one.
    report["serving_threshold"] = round(breakeven_win_rate(_RR, 0.0) + _PROB_MARGIN, 4)

    model = fit(vectors, labels, names, version, l2=l2)
    report["model"] = model
    if model is None:
        report["reason"] = "final fit did not converge"
        return report

    auc = evaluation.get("auc")
    auc_ok = auc is not None and auc >= min_auc
    expectancy_ok = best is not None and best["expectancy_r"] > 0

    report["auc"] = auc
    report["auc_ok"] = auc_ok
    report["expectancy_ok"] = expectancy_ok
    report["passes"] = bool(auc_ok and expectancy_ok)

    if report["passes"]:
        report["reason"] = (
            f"AUC {auc:.3f} >= {min_auc:.2f} and best gated expectancy "
            f"{best['expectancy_r']:+.3f}R at threshold {best['threshold']}"
        )
    elif not auc_ok:
        report["reason"] = f"out-of-sample AUC {auc if auc is None else round(auc, 3)} below {min_auc:.2f}"
    else:
        expectancy = best["expectancy_r"] if best else None
        report["reason"] = (
            f"no threshold turns a profit (best {expectancy:+.3f}R)"
            if expectancy is not None
            else "no threshold retained enough trades to judge"
        )
    return report


def _align_r(r_multiples: List[Optional[float]], n_oos: int) -> List[Optional[float]]:
    """
    The R values matching the walk-forward's out-of-sample rows.

    Those rows are the *tail* of the ordered set — the expanding window trains on the
    front and predicts forward — so the alignment is a suffix, not a prefix. Getting
    this backwards would pair each prediction with an unrelated trade's return and
    quietly produce a meaningless expectancy.
    """
    return r_multiples[-n_oos:] if n_oos <= len(r_multiples) else r_multiples


def save_candidate(
    storage,
    lane: str,
    report: Dict[str, Any],
    activate: bool = False,
    shadow: bool = False,
    force: bool = False,
    notes: Optional[str] = None,
) -> Optional[int]:
    """
    Persist the candidate from a report. Returns the stored model id.

    Promotion requires the gate to have passed, unless ``force`` is set — which exists
    so a deliberate override is possible and *visible*, not so the gate can be routed
    around casually.
    """
    model: Optional[TrainedModel] = report.get("model")
    if model is None:
        return None
    if (activate or shadow) and not report.get("passes") and not force:
        activate = shadow = False

    metrics = {
        key: report.get(key)
        for key in ("auc", "n_resolved", "best_gate", "top_decile_precision",
                    "serving_threshold", "reason", "passes")
    }
    walk = report.get("walk_forward") or {}
    metrics["brier"] = walk.get("brier")
    metrics["fold_auc_std"] = walk.get("fold_auc_std")
    metrics["calibration"] = walk.get("calibration")

    return storage.save_model(
        kind=lane,
        model_json=model.to_json(),
        feature_version=report["feature_version"],
        metrics=metrics,
        notes=notes,
        activate=activate,
        shadow=shadow,
    )


def load_serving_model(storage, lane: str) -> Tuple[Optional[TrainedModel], Optional[str]]:
    """
    The model that should score this lane's setups, and the mode it is serving in.

    Returns ``(model, "active")`` when one is gating, ``(model, "shadow")`` when one is
    only observing, and ``(None, None)`` otherwise.

    **A feature-version mismatch refuses the model.** Coefficients are positional, so
    a model built on a different contract would score every feature against the wrong
    weight and return confident nonsense. Failing closed to rules-only is the safe
    direction.

    Only one model gates and at most one shadows; whenever something is gating, the
    shadow lane is ignored — a shadow run must never be mistaken for a live one.
    """
    _, version = lane_contract(lane)
    for flag, mode in (("is_active", "active"), ("is_shadow", "shadow")):
        record = storage.load_model(lane, flag)
        if not record:
            continue
        model = TrainedModel.from_json(record["model_json"])
        if model is None:
            continue  # corrupt JSON must not crash a scan
        if model.feature_version != version:
            continue
        return model, mode
    return None, None


def gate_summary(report: Dict[str, Any]) -> str:
    """One-line verdict, shared by the CLI and the Model tab so they cannot disagree."""
    if report.get("n_resolved", 0) < report.get("min_trades", MIN_RESOLVED_TRADES):
        return f"NOT ENOUGH DATA - {report.get('reason', '')}"
    if report.get("passes"):
        return f"CANDIDATE - {report.get('reason', '')}"
    return f"DO NOT PROMOTE - {report.get('reason', '')}"
