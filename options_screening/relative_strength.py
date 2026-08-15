"""
Relative strength against SPY.

The equity analogue of the forex sibling's currency-strength matrix, and much
simpler: FX quotes are already relative (every pair is one currency against another),
so strength has to be reconstructed across the whole grid. A stock's day change is
absolute, and the market's own move is most of it, so the useful quantity is just the
difference.

``rs_bonus`` is fed back *into* the scorer rather than added to the finished total —
see ``signals.score_ticker``'s ``strength_bonus`` argument.
"""
from __future__ import annotations

from typing import Optional

# Percentage points of day change vs SPY before a name counts as genuinely leading or
# lagging. Below this, the difference is mostly beta and noise rather than information.
_RS_THRESHOLD = 1.0


def calculate_rs(day_change_pct: Optional[float], spy_change_pct: Optional[float]) -> Optional[float]:
    """Day change minus SPY's, in percentage points. ``None`` if either is missing."""
    if day_change_pct is None or spy_change_pct is None:
        return None
    return round(day_change_pct - spy_change_pct, 3)


def rs_assessment(rs: Optional[float]) -> Optional[str]:
    """OUTPERFORMING / UNDERPERFORMING / IN_LINE, or ``None`` when RS is unknown."""
    if rs is None:
        return None
    if rs >= _RS_THRESHOLD:
        return "OUTPERFORMING"
    if rs <= -_RS_THRESHOLD:
        return "UNDERPERFORMING"
    return "IN_LINE"


def rs_bonus(assessment: Optional[str], direction: Optional[str]) -> float:
    """
    Score adjustment for a candidate direction: reward alignment with relative
    strength, penalise fighting it more lightly than the reward.

    Keyed on the *direction* rather than on a finished ``trade_signal``, because this
    has to be computed before classification in order to be fed back into the score.
    Keying it on the signal forces it to be applied afterwards, which is exactly how
    the displayed score and the trained score drift apart.
    """
    if assessment is None or direction not in ("LONG", "SHORT"):
        return 0.0
    if assessment == "OUTPERFORMING":
        return 10.0 if direction == "LONG" else -5.0
    if assessment == "UNDERPERFORMING":
        return 10.0 if direction == "SHORT" else -5.0
    return 0.0
