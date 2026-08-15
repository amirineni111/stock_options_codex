"""
The Performance and Model panels, shared by both lanes.

Written once and parameterised by lane rather than duplicated per page. The rest of
``app.py`` grew two near-identical copies of every block (one plain, one
``intraday_``-prefixed) and they had already drifted; there is no reason to repeat
that for the two most detail-heavy tabs in the app.

This is the one module under ``options_screening/`` that imports streamlit. The
package rule elsewhere is that the library holds no UI and the UI holds no strategy —
this is a view component, so it renders and reads, and every number it shows comes
from ``storage`` or from ``training``. It computes nothing itself.
"""
from __future__ import annotations

import json
from typing import Any, Dict, Optional

import pandas as pd
import streamlit as st

from .training import (
    MIN_RESOLVED_TRADES,
    evaluate_and_fit,
    gate_summary,
    lane_contract,
    load_serving_model,
    save_candidate,
)

LANE_LABELS = {"intraday": "intraday stock", "options": "option contract"}


# ── Performance ──────────────────────────────────────────────────────────────


def render_performance_tab(storage, lane: str) -> None:
    """Win rate, expectancy and the equity curve, from resolved forward tests."""
    label = LANE_LABELS.get(lane, lane)
    st.subheader("Forward-test performance")
    st.caption(
        f"Every actionable {label} signal is armed automatically and resolved against "
        "later data. These are measured outcomes, not a backtest — and every figure is "
        "**net of estimated transaction cost**, so R is what the trade would have "
        "returned rather than what the printed levels suggest."
    )

    stats = storage.load_performance(lane)
    outcomes = storage.load_outcomes(lane)
    open_rows = storage.load_tracked(lane, status="open")

    if outcomes.empty:
        st.info(
            "No trades have resolved yet. Signals are armed as they appear and resolve "
            "when price reaches the stop or the target, so this fills in over days, "
            "not minutes."
        )
        _render_open_positions(open_rows)
        return

    overall = stats[stats["scope"] == "overall"]
    row = overall.iloc[0] if not overall.empty else None

    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Resolved", int(row["trades"]) if row is not None else 0)
    col2.metric("Open", len(open_rows))
    col3.metric("Win rate", f"{row['win_rate']:.1%}" if row is not None and row["win_rate"] else "-")
    col4.metric(
        "Expectancy",
        f"{row['avg_r']:+.3f}R" if row is not None and row["avg_r"] is not None else "-",
        help="Average R per trade, net of cost. Positive is the whole point.",
    )
    col5.metric(
        "Total",
        f"{row['total_r']:+.1f}R" if row is not None and row["total_r"] is not None else "-",
    )

    # The single most useful sanity check on the whole system: cost drag is real and
    # visible rather than assumed away.
    if "gross_dollars" in outcomes.columns and "net_dollars" in outcomes.columns:
        gross = outcomes["gross_dollars"].sum()
        net = outcomes["net_dollars"].sum()
        st.caption(
            f"Estimated cost has taken ${gross - net:,.2f} out of ${gross:,.2f} gross "
            f"across {len(outcomes)} resolved trades."
        )

    equity = outcomes.sort_values("id")["r_multiple"].fillna(0).cumsum()
    if not equity.empty:
        st.line_chart(equity.reset_index(drop=True), height=220)
        st.caption("Cumulative R, oldest trade first.")

    by_signal = stats[stats["scope"] == "signal"]
    if not by_signal.empty:
        st.markdown("**By signal tier**")
        st.caption(
            "Worth reading separately: a tier that never outperforms is invisible "
            "inside the overall average, and that is exactly how a bad rule survives."
        )
        st.dataframe(
            by_signal[["scope_value", "trades", "wins", "losses", "win_rate", "avg_r", "total_r"]],
            use_container_width=True,
            hide_index=True,
        )

    _render_open_positions(open_rows)

    with st.expander("Resolved trades"):
        columns = [
            c
            for c in (
                "ticker", "contract_ticker", "signal", "outcome", "entry_price",
                "exit_price", "gross_dollars", "cost_dollars", "net_dollars",
                "r_multiple", "hold_minutes", "exit_reason", "exit_ts",
            )
            if c in outcomes.columns
        ]
        st.dataframe(outcomes[columns], use_container_width=True, hide_index=True)
        st.download_button(
            "Download resolved trades (CSV)",
            outcomes.to_csv(index=False).encode("utf-8"),
            file_name=f"{lane}_trade_outcomes.csv",
            mime="text/csv",
        )


def _render_open_positions(open_rows: pd.DataFrame) -> None:
    if open_rows.empty:
        return
    with st.expander(f"Open forward tests ({len(open_rows)})"):
        columns = [
            c
            for c in (
                "ticker", "contract_ticker", "signal", "entry_price", "stop_price",
                "target_price", "model_prob", "required_prob", "created_at",
            )
            if c in open_rows.columns
        ]
        st.dataframe(open_rows[columns], use_container_width=True, hide_index=True)


# ── Model ────────────────────────────────────────────────────────────────────


def render_model_tab(storage, lane: str) -> None:
    """Gate summary, walk-forward metrics, and deliberate promotion."""
    names, version = lane_contract(lane)
    st.subheader("Trained direction model")
    st.caption(
        "The rules **propose** a setup; a trained model **disposes**. The two never "
        "swap roles: the model can only downgrade a signal to WATCH_ONLY, never "
        "promote something the rules rejected."
    )

    model, mode = load_serving_model(storage, lane)
    col1, col2, col3 = st.columns(3)
    col1.metric("Serving", mode.upper() if mode else "RULES ONLY")
    col2.metric("Feature version", version)
    col3.metric("Features", len(names))
    if mode == "shadow":
        st.info(
            "A model is running in **shadow**: it scores every directional setup and "
            "logs the probability, but vetoes nothing. This is the only way to learn "
            "what it would do to the trades it wants to block — once it is gating, "
            "those trades stop happening and stop being measurable."
        )

    with st.expander("When to retrain, and how to read the result"):
        st.markdown(
            """
Read the numbers in this order. They are not equally important.

1. **Best gated expectancy** — the only one that decides anything. It is the average
   R per trade *among the trades the model would have allowed*, measured out of
   sample. If it is not positive, nothing else matters.
2. **Out-of-sample AUC** — does the model rank at all? Around 0.50 is a coin flip.
   Above roughly 0.60 on a few hundred correlated trades is already suspicious rather
   than impressive.
3. **Calibration** — the gate compares a probability against a breakeven threshold,
   so a model that ranks well but is systematically over-confident makes *decision*
   errors that AUC cannot see. Predicted and observed should track each other.
4. **Fold AUC spread** — a wide spread means the model is fitting market regimes
   rather than setups, and a single pooled AUC hides it.

The automatic gate checks 1 and 2. **It cannot check 3 or 4 for you** — those need a
human to look at the table.

Retrain when a few dozen new trades have resolved, or after changing anything in the
scoring engine. Editing the feature contract means bumping `FEATURE_VERSION` in
`options_screening/features.py`; the scanner refuses a model built on a different
contract and falls back to rules-only rather than serving misaligned probabilities.
            """
        )

    st.markdown("---")
    if st.button("Evaluate a new candidate", key=f"train_{lane}"):
        with st.spinner("Walk-forward evaluating..."):
            st.session_state[f"report_{lane}"] = evaluate_and_fit(storage, lane)

    report = st.session_state.get(f"report_{lane}")
    if report:
        _render_report(storage, lane, report)

    st.caption(
        f"Equivalent CLI, if you prefer it: `python scripts/train_model.py --lane {lane}`"
    )

    _render_model_history(storage, lane)


def _render_report(storage, lane: str, report: Dict[str, Any]) -> None:
    summary = gate_summary(report)
    if report.get("passes"):
        st.success(summary)
    elif report.get("n_resolved", 0) < report.get("min_trades", MIN_RESOLVED_TRADES):
        st.info(summary)
    else:
        st.warning(summary)

    walk = report.get("walk_forward") or {}
    if walk.get("ok"):
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("OOS rows", walk["n"])
        col2.metric("AUC", _fmt(walk.get("auc")))
        col3.metric("Brier", _fmt(walk.get("brier")), help="Lower is better; 0.25 is a coin flip.")
        col4.metric("Fold AUC sd", _fmt(walk.get("fold_auc_std")))

        calibration = walk.get("calibration") or []
        if calibration:
            st.markdown("**Calibration** — predicted vs observed. These should track.")
            st.dataframe(pd.DataFrame(calibration), use_container_width=True, hide_index=True)

    gated = report.get("gated") or []
    if gated:
        st.markdown("**Expectancy if the model gated at each threshold**")
        frame = pd.DataFrame([g for g in gated if g.get("trades")])
        st.dataframe(frame, use_container_width=True, hide_index=True)
        st.caption(f"Serving would gate at {_fmt(report.get('serving_threshold'))}.")

    if report.get("model") is None:
        return

    st.markdown("---")
    col1, col2 = st.columns([1, 2])
    with col1:
        save_only = st.button("Save (inactive)", key=f"save_{lane}")
        shadow = st.button("Save and shadow", key=f"shadow_{lane}")
    with col2:
        promote = st.button(
            "Promote to gating",
            key=f"promote_{lane}",
            type="primary",
            disabled=not report.get("passes") and not st.session_state.get(f"force_{lane}"),
        )
        if not report.get("passes"):
            # A deliberate second step, so overriding a failed gate cannot happen by
            # reflex.
            st.checkbox(
                "This candidate failed its gate. I understand, promote it anyway.",
                key=f"force_{lane}",
            )

    if save_only or shadow or promote:
        model_id = save_candidate(
            storage,
            lane,
            report,
            activate=promote,
            shadow=shadow,
            force=bool(st.session_state.get(f"force_{lane}")),
        )
        st.success(f"Saved model #{model_id}.")
        st.rerun()


def _render_model_history(storage, lane: str) -> None:
    models = storage.load_models(lane)
    if models.empty:
        return
    st.markdown("**Model history**")
    display = models.copy()
    display["auc"] = display["metrics_json"].apply(lambda raw: _metric(raw, "auc"))
    display["verdict"] = display["metrics_json"].apply(lambda raw: _metric(raw, "reason"))
    st.dataframe(
        display[["id", "created_at", "feature_version", "auc", "is_active", "is_shadow", "verdict"]],
        use_container_width=True,
        hide_index=True,
    )

    active = storage.load_model(lane, "is_active")
    if active and st.button("Stop gating (roll back to rules only)", key=f"rollback_{lane}"):
        storage.clear_model_flag(lane, "is_active")
        st.success("Rolled back. Signals are rules-only again.")
        st.rerun()


def _metric(raw: Optional[str], key: str):
    try:
        return json.loads(raw or "{}").get(key)
    except (TypeError, ValueError):
        return None


def _fmt(value) -> str:
    return "-" if value is None else f"{value:.4f}"
