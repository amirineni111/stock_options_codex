"""
Retrain a lane's model from the CLI, with the same gate the dashboard applies.

Both entry points call ``training.evaluate_and_fit``, so a model promoted here is the
same object, judged by the same numbers, as one promoted from the Model tab.

    python scripts/train_model.py --lane intraday
    python scripts/train_model.py --lane options --activate
    python scripts/train_model.py --lane intraday --shadow

A candidate always saves **inactive** unless you ask for it. Promotion is a separate,
deliberate act, and `--force` exists so that overriding a failed gate is possible and
visible rather than casual.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Allow running as `python scripts/train_model.py` from the repo root.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from options_screening.config import get_settings  # noqa: E402
from options_screening.storage import Storage  # noqa: E402
from options_screening.training import (  # noqa: E402
    MIN_AUC,
    MIN_RESOLVED_TRADES,
    evaluate_and_fit,
    gate_summary,
    save_candidate,
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lane", choices=("intraday", "options"), default="intraday")
    parser.add_argument("--activate", action="store_true", help="promote if it clears the gate")
    parser.add_argument("--shadow", action="store_true", help="run it alongside, vetoing nothing")
    parser.add_argument("--force", action="store_true", help="promote even if the gate fails")
    parser.add_argument("--l2", type=float, default=1.0)
    parser.add_argument("--folds", type=int, default=4)
    parser.add_argument("--min-auc", type=float, default=MIN_AUC)
    parser.add_argument("--min-trades", type=int, default=MIN_RESOLVED_TRADES)
    parser.add_argument("--notes", default=None)
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    settings = get_settings()
    storage = Storage(settings.db_path)
    storage.initialize()

    report = evaluate_and_fit(
        storage,
        args.lane,
        l2=args.l2,
        folds=args.folds,
        min_auc=args.min_auc,
        min_trades=args.min_trades,
    )

    print(f"\nLane: {args.lane}   feature version {report['feature_version']}")
    print(f"Resolved trades: {report['n_resolved']} (need {report['min_trades']})")

    walk = report.get("walk_forward") or {}
    if walk.get("ok"):
        print("\nWalk-forward (every prediction from a model that never saw the row):")
        print(f"  out-of-sample rows  {walk['n']}")
        print(f"  AUC                 {_fmt(walk.get('auc'))}")
        print(f"  Brier               {_fmt(walk.get('brier'))}")
        print(f"  fold AUC mean/std   {_fmt(walk.get('fold_auc_mean'))} / {_fmt(walk.get('fold_auc_std'))}")
        print(f"  top-decile win rate {_fmt(report.get('top_decile_precision'))}")

        calibration = walk.get("calibration") or []
        if calibration:
            print("\n  Calibration (the gate compares a probability to a threshold, so")
            print("  systematic over-confidence is a decision error, not just a scoring one):")
            print(f"    {'bucket':<10} {'n':>5} {'predicted':>10} {'observed':>9}")
            for row in calibration:
                print(
                    f"    {row['bin']:<10} {row['count']:>5} "
                    f"{row['predicted']:>10.3f} {row['observed']:>9.3f}"
                )

    gated = report.get("gated") or []
    if gated:
        print("\n  Expectancy if the model gated at each threshold:")
        print(f"    {'thresh':>7} {'trades':>7} {'win rate':>9} {'exp R':>8}")
        for row in gated:
            if not row.get("trades"):
                continue
            print(
                f"    {row['threshold']:>7.2f} {row['trades']:>7} "
                f"{row['win_rate']:>9.3f} {row['expectancy_r']:>+8.3f}"
            )
        print(f"\n  Serving would gate at {_fmt(report.get('serving_threshold'))}")

    print(f"\nVERDICT: {gate_summary(report)}")

    if report.get("model") is None:
        print("No model was fitted, so nothing was saved.")
        return 1

    model_id = save_candidate(
        storage,
        args.lane,
        report,
        activate=args.activate,
        shadow=args.shadow,
        force=args.force,
        notes=args.notes,
    )
    state = "inactive"
    if args.activate and (report.get("passes") or args.force):
        state = "ACTIVE (gating)" + (" - forced past a failed gate" if not report.get("passes") else "")
    elif args.shadow and (report.get("passes") or args.force):
        state = "SHADOW (scoring, vetoing nothing)"
    print(f"Saved model #{model_id} as {state}.")

    if args.activate and not report.get("passes") and not args.force:
        print("Promotion refused: the gate did not pass. Re-run with --force to override.")
    return 0


def _fmt(value) -> str:
    return "n/a" if value is None else f"{value:.4f}"


if __name__ == "__main__":
    raise SystemExit(main())
