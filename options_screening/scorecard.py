"""
The prediction scorecard: what each signal *called*, against what then happened.

Every armed signal is a prediction with a precise meaning — "this price reaches the
target before it reaches the stop" — so it can be graded without interpretation:

* **SUCCESS** — the target was reached first. The call was right.
* **FAILED** — the stop was reached first. The call was wrong.
* **EXPIRED** — neither was reached inside the holding window (intraday timeout,
  option max-hold or expiry). The market never decided, so it counts toward neither
  side of the accuracy figure; its net P&L still counts in R.

This is deliberately a different question from the Performance tab's win rate. That
one asks "did the trade make money net of cost?" and so scores a timeout that drifted
into profit as a WIN. The scorecard asks "was the classification right?", and a
timeout is not evidence either way. Both are shown, because a system can be accurate
and still lose money (targets too close, costs too high) or the reverse.

Pure pandas, no Streamlit: ``ui_panels`` renders what this computes.
"""
from __future__ import annotations

import re
from typing import Any, Dict, Optional

import pandas as pd

SUCCESS = "SUCCESS"
FAILED = "FAILED"
EXPIRED = "EXPIRED"

# What the scorecard can be broken down by: label -> annotated column.
BREAKDOWNS = {
    "Direction source": "source",
    "Predicted signal": "signal",
    "Side": "side",
    "Score band": "score_band",
    "Model P(win)": "prob_band",
    "Ticker": "ticker",
    "Week resolved": "week",
    "Exit reason": "exit_reason",
}

# Bands are fixed rather than quantiles so a band means the same thing from one week
# to the next, and so the question "do higher scores succeed more often?" has a
# stable answer to compare against.
_SCORE_BINS = [float("-inf"), 50, 60, 70, 80, float("inf")]
_SCORE_LABELS = ["<50", "50-60", "60-70", "70-80", "80+"]
_PROB_BINS = [float("-inf"), 0.40, 0.50, 0.60, float("inf")]
_PROB_LABELS = ["<40%", "40-50%", "50-60%", "60%+"]

_OCC_TYPE = re.compile(r"\d{6}([CP])\d{8}$")


def verdict(exit_reason: Optional[str]) -> str:
    """SUCCESS / FAILED / EXPIRED for one resolved trade."""
    if exit_reason == "TARGET":
        return SUCCESS
    if exit_reason == "STOP":
        return FAILED
    return EXPIRED


def side(signal: Optional[str], contract_ticker: Optional[str], direction: Any) -> str:
    """CALL / PUT for an option, LONG / SHORT for a stock."""
    if contract_ticker:
        if signal and "CALL" in signal:
            return "CALL"
        if signal and "PUT" in signal:
            return "PUT"
        match = _OCC_TYPE.search(contract_ticker)
        if match:
            return "CALL" if match.group(1) == "C" else "PUT"
        return "OPTION"
    try:
        return "SHORT" if int(direction) < 0 else "LONG"
    except (TypeError, ValueError):
        return "LONG"


def source(signal: Optional[str], contract_ticker: Optional[str]) -> str:
    """Which directional read made the call — the comparison the options lane runs."""
    if signal and signal.startswith("ENGINE_"):
        return "daily engine"
    return "trend rule" if contract_ticker else "intraday engine"


def annotate(predictions: pd.DataFrame) -> pd.DataFrame:
    """``storage.load_predictions`` plus the verdict and the breakdown columns."""
    if predictions.empty:
        return predictions.assign(verdict=pd.Series(dtype=str))
    frame = predictions.copy()
    frame["verdict"] = frame["exit_reason"].apply(verdict)
    frame["side"] = [
        side(sig, contract, direction)
        for sig, contract, direction in zip(
            frame["signal"], frame["contract_ticker"], frame["direction"]
        )
    ]
    frame["source"] = [
        source(sig, contract) for sig, contract in zip(frame["signal"], frame["contract_ticker"])
    ]
    frame["score_band"] = pd.cut(
        pd.to_numeric(frame["total_score"], errors="coerce"),
        _SCORE_BINS, labels=_SCORE_LABELS, right=False,
    ).astype(str).replace("nan", "unscored")
    probs = pd.to_numeric(frame["model_prob"], errors="coerce")
    frame["prob_band"] = pd.cut(
        probs, _PROB_BINS, labels=_PROB_LABELS, right=False
    ).astype(str).replace("nan", "no model")
    resolved = pd.to_datetime(frame["resolved_at"], errors="coerce", utc=True)
    frame["week"] = (
        resolved.dt.tz_localize(None).dt.to_period("W-SUN").dt.start_time.dt.strftime("%Y-%m-%d")
    ).fillna("unknown")
    return frame


def summarize(annotated: pd.DataFrame) -> Dict[str, Any]:
    """Headline numbers for a set of resolved predictions."""
    if annotated.empty:
        return {
            "resolved": 0, "success": 0, "failed": 0, "expired": 0,
            "accuracy": None, "net_win_rate": None, "avg_r": None, "total_r": None,
        }
    counts = annotated["verdict"].value_counts()
    success = int(counts.get(SUCCESS, 0))
    failed = int(counts.get(FAILED, 0))
    r = pd.to_numeric(annotated["r_multiple"], errors="coerce")
    return {
        "resolved": int(len(annotated)),
        "success": success,
        "failed": failed,
        "expired": int(counts.get(EXPIRED, 0)),
        "accuracy": success / (success + failed) if success + failed else None,
        "net_win_rate": float((annotated["outcome"] == "WIN").mean()),
        "avg_r": float(r.mean()) if r.notna().any() else None,
        "total_r": float(r.sum()) if r.notna().any() else None,
    }


def breakdown(annotated: pd.DataFrame, column: str) -> pd.DataFrame:
    """One scorecard row per value of ``column``, most-called first."""
    if annotated.empty or column not in annotated.columns:
        return pd.DataFrame()
    rows = []
    for value, group in annotated.groupby(column, sort=False, dropna=False):
        stats = summarize(group)
        row = {
            column: value,
            "called": stats["resolved"],
            "success": stats["success"],
            "failed": stats["failed"],
            "expired": stats["expired"],
            "accuracy": _round(stats["accuracy"]),
            "avg_r": _round(stats["avg_r"]),
            "total_r": _round(stats["total_r"], 2),
        }
        if column == "prob_band":
            # Calibration on live trades: the model said this, the market did that.
            predicted = pd.to_numeric(group["model_prob"], errors="coerce")
            row["avg_predicted"] = _round(predicted.mean()) if predicted.notna().any() else None
        rows.append(row)
    return pd.DataFrame(rows).sort_values(["called", column], ascending=[False, True]).reset_index(drop=True)


def _round(value: Optional[float], digits: int = 3) -> Optional[float]:
    return None if value is None or pd.isna(value) else round(float(value), digits)
