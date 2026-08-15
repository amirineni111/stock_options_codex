"""
Judge a model on *live* resolved trades, rather than on its own training split.

    python scripts/model_report.py --lane intraday --mode shadow
    python scripts/model_report.py --lane options --mode any

The distinction between modes is the whole point of this script, and getting it wrong
is the easiest way to promote a model that does nothing:

* **shadow** — the model scored every directional setup the rules proposed and vetoed
  none of them, so winners and losers are both represented. This is the only sample
  that answers "what would it have done to the trades it wants to block".
* **active** — the model was gating, so only the trades it *allowed* have outcomes.
  That is a censored sample: it will look good almost regardless of whether the model
  has skill, because the trades that would have embarrassed it were never taken.
  Never promote on the strength of these numbers.
* **any** — includes rows armed before mode tracking existed. Useful for volume,
  misleading for judgement.

The two must never be pooled, which is why every tracking row records the mode that
produced its probability.
"""
from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from options_screening.config import get_settings  # noqa: E402
from options_screening.model import brier_score, roc_auc  # noqa: E402
from options_screening.storage import Storage  # noqa: E402
from options_screening.training import lane_contract  # noqa: E402

MIN_ROWS = 30


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lane", choices=("intraday", "options"), default="intraday")
    parser.add_argument("--mode", choices=("shadow", "active", "any"), default="shadow")
    parser.add_argument("--min-n", type=int, default=MIN_ROWS)
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    settings = get_settings()
    storage = Storage(settings.db_path)
    storage.initialize()

    _, version = lane_contract(args.lane)
    rows = [
        row
        for row in storage.load_training_rows(args.lane, version)
        if args.mode == "any" or (row.get("model_mode") == args.mode)
    ]

    print(f"\nLane: {args.lane}   mode: {args.mode}   feature version: {version}")
    print(f"Resolved trades with a logged probability: {len(rows)}")

    if args.mode == "active":
        print(
            "\nNOTE: these rows were produced while the model was gating, so only the\n"
            "trades it allowed have outcomes. The sample is censored and will flatter\n"
            "the model regardless of skill. Do not promote on this."
        )

    scored = [row for row in rows if row.get("model_prob") is not None]
    if len(scored) < args.min_n:
        print(f"\nVERDICT: NOT ENOUGH DATA - {len(scored)} scored rows, need {args.min_n}.")
        return 0

    labels = [1 if row["outcome"] == "WIN" else 0 for row in scored]
    probabilities = [float(row["model_prob"]) for row in scored]
    r_multiples = [row.get("r_multiple") or 0.0 for row in scored]

    auc = roc_auc(labels, probabilities)
    brier = brier_score(labels, probabilities)
    base_rate = sum(labels) / len(labels)

    print("\nMeasured on live outcomes:")
    print(f"  rows            {len(scored)}")
    print(f"  base win rate   {base_rate:.3f}")
    print(f"  AUC             {auc if auc is None else round(auc, 4)}")
    print(f"  Brier           {brier:.4f}")

    # What the model would have done at a range of thresholds, on real outcomes.
    print("\n  If it had gated at each threshold:")
    print(f"    {'thresh':>7} {'trades':>7} {'win rate':>9} {'exp R':>8}")
    for i in range(11):
        threshold = round(0.30 + 0.05 * i, 2)
        kept = [(l, r) for l, p, r in zip(labels, probabilities, r_multiples) if p >= threshold]
        if len(kept) < 5:
            continue
        wins = sum(l for l, _ in kept) / len(kept)
        expectancy = sum(r for _, r in kept) / len(kept)
        print(f"    {threshold:>7.2f} {len(kept):>7} {wins:>9.3f} {expectancy:>+8.3f}")

    by_signal = defaultdict(list)
    for row, r in zip(scored, r_multiples):
        by_signal[row.get("signal") or "?"].append(r)
    if len(by_signal) > 1:
        print("\n  By signal tier:")
        for signal, values in sorted(by_signal.items()):
            print(f"    {signal:<22} n={len(values):>4}  exp R {sum(values) / len(values):>+7.3f}")

    print(f"\nVERDICT: {_verdict(args.mode, auc, r_multiples)}")
    return 0


def _verdict(mode: str, auc, r_multiples) -> str:
    if mode == "active":
        return "CENSORED SAMPLE - informative only; judge a shadow run instead"
    if auc is None:
        return "NOT ENOUGH DATA - one outcome class is missing"
    expectancy = sum(r_multiples) / len(r_multiples)
    if auc < 0.5:
        return f"DO NOT PROMOTE - AUC {auc:.3f} is below chance"
    if expectancy <= 0:
        return f"DO NOT PROMOTE - live expectancy {expectancy:+.3f}R"
    return f"CANDIDATE - AUC {auc:.3f}, live expectancy {expectancy:+.3f}R"


if __name__ == "__main__":
    raise SystemExit(main())
