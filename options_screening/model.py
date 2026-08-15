"""
A calibrated P(target before stop), and the metrics used to decide whether to trust it.

**Why L2 logistic regression rather than gradient boosting.** The effective sample is
far smaller than the row count. Equities move together intraday, so simultaneous longs
across AAPL, MSFT and NVDA are largely one beta bet recorded three times; the same is
true of a chain's worth of calls on one underlying. A few hundred rows of that is not
a few hundred independent observations, and a boosted tree will happily find structure
in the correlation.

The decision rule also needs *calibration*, not ranking: the serving gate compares a
probability against a cost-adjusted breakeven, so a model that ranks perfectly but
reports 0.9 where the truth is 0.5 is worse than useless. A linear model in log-odds
space is close to calibrated by construction and can be read coefficient by
coefficient, which matters when the alternative is trusting an opaque score with real
money behind it.

Swap this out when there are several thousand *decorrelated* resolved trades and the
calibration curve shows genuine non-linearity. Not before.

Everything is hand-rolled on numpy — no sklearn — matching the siblings and keeping
the dependency list short enough to audit.
"""
from __future__ import annotations

import json
import math
from typing import Any, Dict, List, Optional, Sequence, Tuple

import numpy as np

# Ridge strength on the standardised design matrix. Deliberately not tuned by
# cross-validation: with this sample size the tuning itself would overfit, and a fixed
# mild penalty is the honest choice.
DEFAULT_L2 = 1.0

# Newton/IRLS converges in a handful of steps on a problem this size; the cap is a
# guard against a separable dataset sending coefficients to infinity.
_MAX_ITER = 50
_TOLERANCE = 1e-8


class TrainedModel:
    """Standardise -> linear -> sigmoid, with the feature contract travelling with it."""

    def __init__(
        self,
        feature_names: Sequence[str],
        feature_version: int,
        mean: Sequence[float],
        scale: Sequence[float],
        coefficients: Sequence[float],
        intercept: float,
    ) -> None:
        self.feature_names = list(feature_names)
        self.feature_version = feature_version
        self.mean = np.asarray(mean, dtype=float)
        self.scale = np.asarray(scale, dtype=float)
        self.coefficients = np.asarray(coefficients, dtype=float)
        self.intercept = float(intercept)

    def predict_proba(self, vectors: Sequence[Sequence[float]]) -> np.ndarray:
        matrix = np.asarray(vectors, dtype=float)
        if matrix.ndim == 1:
            matrix = matrix.reshape(1, -1)
        standardised = (matrix - self.mean) / self.scale
        return _sigmoid(standardised @ self.coefficients + self.intercept)

    def predict_one(self, vector: Sequence[float]) -> float:
        return float(self.predict_proba([vector])[0])

    def to_json(self) -> str:
        return json.dumps(
            {
                "feature_names": self.feature_names,
                "feature_version": self.feature_version,
                "mean": self.mean.tolist(),
                "scale": self.scale.tolist(),
                "coefficients": self.coefficients.tolist(),
                "intercept": self.intercept,
            }
        )

    @classmethod
    def from_json(cls, payload: str) -> Optional["TrainedModel"]:
        """Returns ``None`` on anything malformed — a corrupt model must not crash a scan."""
        try:
            data = json.loads(payload)
            return cls(
                feature_names=data["feature_names"],
                feature_version=int(data["feature_version"]),
                mean=data["mean"],
                scale=data["scale"],
                coefficients=data["coefficients"],
                intercept=float(data["intercept"]),
            )
        except (TypeError, ValueError, KeyError):
            return None


def _sigmoid(z: np.ndarray) -> np.ndarray:
    # Split on the sign so neither branch overflows exp; the naive form warns and
    # returns nan for large negative z.
    out = np.empty_like(z, dtype=float)
    positive = z >= 0
    out[positive] = 1.0 / (1.0 + np.exp(-z[positive]))
    exp_z = np.exp(z[~positive])
    out[~positive] = exp_z / (1.0 + exp_z)
    return out


def fit(
    vectors: Sequence[Sequence[float]],
    labels: Sequence[int],
    feature_names: Sequence[str],
    feature_version: int,
    l2: float = DEFAULT_L2,
) -> Optional[TrainedModel]:
    """
    Fit by Newton-Raphson / IRLS. Returns ``None`` when the data cannot support a fit.

    The intercept is **not** penalised: shrinking it would drag the predicted base rate
    toward 0.5 regardless of the observed win rate, which is precisely the number the
    decision rule depends on being right.
    """
    X = np.asarray(vectors, dtype=float)
    y = np.asarray(labels, dtype=float)
    if X.ndim != 2 or X.shape[0] < 10 or len(set(y.tolist())) < 2:
        return None

    mean = X.mean(axis=0)
    scale = X.std(axis=0)
    # A constant column carries no information; setting its scale to 1 leaves it at
    # zero after centring rather than dividing by zero.
    scale[scale < 1e-12] = 1.0
    Z = (X - mean) / scale

    n_features = Z.shape[1]
    weights = np.zeros(n_features)
    intercept = 0.0
    # Penalty matrix with a zero in the intercept slot.
    design = np.hstack([Z, np.ones((Z.shape[0], 1))])
    penalty = np.eye(n_features + 1) * l2
    penalty[-1, -1] = 0.0

    beta = np.zeros(n_features + 1)
    for _ in range(_MAX_ITER):
        eta = design @ beta
        p = _sigmoid(eta)
        # Floor the variance so a confidently-separated point cannot zero the Hessian.
        w = np.clip(p * (1 - p), 1e-9, None)
        gradient = design.T @ (y - p) - penalty @ beta
        hessian = -(design.T * w) @ design - penalty
        try:
            step = np.linalg.solve(hessian, gradient)
        except np.linalg.LinAlgError:
            return None
        beta_new = beta - step
        if not np.all(np.isfinite(beta_new)):
            return None
        if np.max(np.abs(beta_new - beta)) < _TOLERANCE:
            beta = beta_new
            break
        beta = beta_new

    weights, intercept = beta[:-1], beta[-1]
    return TrainedModel(feature_names, feature_version, mean, scale, weights, intercept)


# ── Metrics ──────────────────────────────────────────────────────────────────


def roc_auc(labels: Sequence[int], scores: Sequence[float]) -> Optional[float]:
    """
    Area under the ROC curve, via the rank-sum identity, with ties averaged.

    Ties matter more here than usual: a model that returns the same probability for
    many setups would score a misleadingly high AUC if ties were broken arbitrarily.
    """
    y = np.asarray(labels, dtype=float)
    s = np.asarray(scores, dtype=float)
    positives = int(y.sum())
    negatives = len(y) - positives
    if positives == 0 or negatives == 0:
        return None

    order = np.argsort(s, kind="mergesort")
    ranks = np.empty(len(s), dtype=float)
    sorted_scores = s[order]
    i = 0
    while i < len(sorted_scores):
        j = i
        while j + 1 < len(sorted_scores) and sorted_scores[j + 1] == sorted_scores[i]:
            j += 1
        average_rank = (i + j) / 2.0 + 1.0
        ranks[order[i:j + 1]] = average_rank
        i = j + 1

    rank_sum = ranks[y == 1].sum()
    return float((rank_sum - positives * (positives + 1) / 2.0) / (positives * negatives))


def brier_score(labels: Sequence[int], scores: Sequence[float]) -> float:
    """Mean squared error of the probabilities. Lower is better; 0.25 is a coin flip."""
    y = np.asarray(labels, dtype=float)
    p = np.asarray(scores, dtype=float)
    return float(np.mean((p - y) ** 2))


def calibration_bins(
    labels: Sequence[int],
    scores: Sequence[float],
    bins: int = 5,
) -> List[Dict[str, Any]]:
    """
    Predicted vs observed frequency per probability bucket.

    The gate compares a probability against a breakeven threshold, so systematic
    over-confidence is a *decision* error, not just a scoring one. AUC cannot see it.
    """
    y = np.asarray(labels, dtype=float)
    p = np.asarray(scores, dtype=float)
    edges = np.linspace(0.0, 1.0, bins + 1)
    out = []
    for i in range(bins):
        low, high = edges[i], edges[i + 1]
        mask = (p >= low) & (p < high if i < bins - 1 else p <= high)
        count = int(mask.sum())
        if count == 0:
            continue
        out.append(
            {
                "bin": f"{low:.1f}-{high:.1f}",
                "count": count,
                "predicted": round(float(p[mask].mean()), 4),
                "observed": round(float(y[mask].mean()), 4),
            }
        )
    return out


def walk_forward(
    vectors: Sequence[Sequence[float]],
    labels: Sequence[int],
    feature_names: Sequence[str],
    feature_version: int,
    folds: int = 4,
    l2: float = DEFAULT_L2,
    min_train: int = 40,
) -> Dict[str, Any]:
    """
    Expanding-window evaluation, in the order the trades actually resolved.

    Every reported number comes from a prediction made by a model that never saw the
    row it is scoring. A random split would leak: trades armed minutes apart share a
    market regime, so shuffling puts near-duplicates on both sides of the split and
    reports a skill that will not survive contact with tomorrow.

    Returns out-of-sample predictions plus the per-fold AUC spread — a wide spread
    means the model is fitting regimes rather than setups, which a single pooled AUC
    hides.
    """
    X = np.asarray(vectors, dtype=float)
    y = np.asarray(labels, dtype=int)
    n = len(y)
    if n < min_train + folds:
        return {"ok": False, "reason": f"only {n} resolved trades; need {min_train + folds}"}

    fold_size = (n - min_train) // folds
    if fold_size < 1:
        return {"ok": False, "reason": "not enough rows per fold"}

    oos_labels: List[int] = []
    oos_scores: List[float] = []
    fold_aucs: List[float] = []

    for fold in range(folds):
        train_end = min_train + fold * fold_size
        test_end = train_end + fold_size if fold < folds - 1 else n
        model = fit(X[:train_end], y[:train_end], feature_names, feature_version, l2)
        if model is None:
            continue
        scores = model.predict_proba(X[train_end:test_end])
        oos_labels.extend(y[train_end:test_end].tolist())
        oos_scores.extend(scores.tolist())
        fold_auc = roc_auc(y[train_end:test_end], scores)
        if fold_auc is not None:
            fold_aucs.append(fold_auc)

    if not oos_scores:
        return {"ok": False, "reason": "no fold produced a usable model"}

    return {
        "ok": True,
        "n": len(oos_scores),
        "auc": roc_auc(oos_labels, oos_scores),
        "brier": round(brier_score(oos_labels, oos_scores), 6),
        "fold_auc_mean": round(float(np.mean(fold_aucs)), 4) if fold_aucs else None,
        "fold_auc_std": round(float(np.std(fold_aucs)), 4) if fold_aucs else None,
        "calibration": calibration_bins(oos_labels, oos_scores),
        "labels": oos_labels,
        "scores": oos_scores,
    }


def gated_performance(
    labels: Sequence[int],
    scores: Sequence[float],
    r_multiples: Sequence[Optional[float]],
    thresholds: Optional[Sequence[float]] = None,
) -> List[Dict[str, Any]]:
    """
    What actually happens to expectancy if the model gates at each threshold.

    This, not AUC, is the promotion criterion. AUC can look respectable while
    expectancy stays negative — ranking the losers correctly does not make the winners
    pay for them. Only trades the gate would have *allowed* are counted, which is the
    honest simulation of serving.
    """
    thresholds = thresholds or [round(0.30 + 0.05 * i, 2) for i in range(11)]
    y = np.asarray(labels, dtype=float)
    p = np.asarray(scores, dtype=float)
    r = np.asarray([0.0 if v is None else float(v) for v in r_multiples], dtype=float)

    out = []
    for threshold in thresholds:
        allowed = p >= threshold
        taken = int(allowed.sum())
        if taken == 0:
            out.append({"threshold": threshold, "trades": 0, "win_rate": None, "expectancy_r": None})
            continue
        out.append(
            {
                "threshold": threshold,
                "trades": taken,
                "win_rate": round(float(y[allowed].mean()), 4),
                "expectancy_r": round(float(r[allowed].mean()), 4),
                "total_r": round(float(r[allowed].sum()), 4),
            }
        )
    return out


def top_decile_precision(labels: Sequence[int], scores: Sequence[float]) -> Optional[float]:
    """Win rate among the model's most confident tenth — where it matters most."""
    y = np.asarray(labels, dtype=float)
    p = np.asarray(scores, dtype=float)
    if len(y) < 10:
        return None
    cutoff = max(1, len(y) // 10)
    top = np.argsort(p)[::-1][:cutoff]
    return round(float(y[top].mean()), 4)
