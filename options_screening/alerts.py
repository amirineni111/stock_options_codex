"""
Push alerts: when a signal is armed, and when it resolves.

Two moments are worth a phone notification, and both are already recorded by the
forward-testing tables, so alerts are *derived* from those tables rather than passed
around in memory:

* **signal** — a row appears in ``signal_tracking``. That is the moment the system
  stands behind a setup, and the arming dedupe/cooldown already guarantees each setup
  is armed once, not on every scan it persists.
* **outcome** — a row appears in ``trade_outcomes``. The push says whether the call
  was right: SUCCESS (target first), FAILED (stop first) or EXPIRED (neither), with
  the lane's running record so every notification doubles as a scorecard.

``deliver_pending`` sweeps both tables for recent rows without an alert, claims each
one in the ``alerts`` table *before* pushing (so the dashboard and the headless
runner never push the same alert twice), groups an underlying's contracts into one
message, and records the delivery result. Sweeping rather than passing objects is
also what recovers an alert from a scan that was cut off between arming and
delivery — a Streamlit rerun stops the script mid-run.

Delivery is one webhook URL, shaped to the service it points at:

- ntfy (``ntfy.sh`` or self-hosted): plain-text body with Title/Priority/Tags
  headers — a free iPhone push with no account: install the ntfy app and subscribe
  to the topic;
- Discord / Slack incoming webhooks: their JSON message shapes;
- anything else: a generic JSON POST.

Standard library only, a short timeout, and failures are returned rather than
raised — a flaky push endpoint must never break a scan.

Ported from the Stocks_realtime_screening sibling, which alerts on arming only.
"""
from __future__ import annotations

import json
import re
import urllib.request
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence, Tuple
from urllib.error import URLError
from urllib.parse import urlparse

import pandas as pd

from . import scorecard
from .timeutil import to_eastern

_TIMEOUT_SECONDS = 5.0
# A burst beyond this is folded into one summary message rather than buzzing the
# phone a dozen times in a row.
_MAX_MESSAGES_PER_SWEEP = 8
# Contracts listed in one grouped option message; the rest are counted.
_CONTRACTS_PER_MESSAGE = 3

_LANE_TAG = {"options": "OPT", "intraday": "STK"}
_SIDE = {
    "BUY_CALL_CANDIDATE": "CALL", "BUY_PUT_CANDIDATE": "PUT",
    "ENGINE_BUY_CALL": "CALL", "ENGINE_BUY_PUT": "PUT",
    "STRONG_BUY": "LONG", "BUY_CANDIDATE": "LONG",
    "STRONG_SHORT": "SHORT", "SHORT_CANDIDATE": "SHORT",
}
_VERDICT_TAGS = {
    scorecard.SUCCESS: ("white_check_mark",),
    scorecard.FAILED: ("x",),
    scorecard.EXPIRED: ("hourglass",),
}
_VERDICT_TEXT = {
    scorecard.SUCCESS: "target hit",
    scorecard.FAILED: "stop hit",
    scorecard.EXPIRED: "no stop or target in time",
}
_OCC = re.compile(r"^O:([A-Z.]+?)(\d{2})(\d{2})(\d{2})([CP])(\d{8})$")


@dataclass
class Message:
    """One push. ``keys`` are the (kind, tracking_id) alerts it delivers."""

    title: str
    body: str
    priority: str = "default"
    tags: Tuple[str, ...] = ()
    keys: List[Tuple[str, int]] = field(default_factory=list)


@dataclass
class AlertReport:
    sent: List[Message] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)


# ── Formatting ───────────────────────────────────────────────────────────────


def _px(value: Optional[float]) -> str:
    return "?" if value is None or pd.isna(value) else f"{value:,.2f}"


def describe_contract(contract_ticker: Optional[str]) -> Optional[str]:
    """``O:AMD261120C00180000`` -> ``$180 CALL 11/20/26``."""
    match = _OCC.match(contract_ticker or "")
    if not match:
        return None
    _, yy, mm, dd, kind, strike = match.groups()
    return f"${int(strike) / 1000:g} {'CALL' if kind == 'C' else 'PUT'} {mm}/{dd}/{yy}"


def _instrument(row: Dict) -> str:
    """How a tracked row is named in a title: ticker, plus the contract for options."""
    contract = describe_contract(row.get("contract_ticker"))
    if contract:
        # The two options labels can call the same stock; the title says which spoke.
        source = " [engine]" if (row.get("signal") or "").startswith("ENGINE_") else ""
        return f"{row.get('ticker')} {contract}{source}"
    side = _SIDE.get(row.get("signal") or "", "LONG" if (row.get("direction") or 1) > 0 else "SHORT")
    return f"{row.get('ticker')} {side}"


def _held(minutes: Optional[float]) -> str:
    if minutes is None or pd.isna(minutes):
        return "?"
    minutes = int(minutes)
    days, rem = divmod(minutes, 1440)
    hours, mins = divmod(rem, 60)
    if days:
        return f"{days}d {hours}h"
    if hours:
        return f"{hours}h {mins}m"
    return f"{mins}m"


def _when(value) -> str:
    eastern = to_eastern(value)
    return f"{eastern:%m/%d %H:%M} ET" if eastern else "?"


def _model_line(row: Dict) -> str:
    prob = row.get("model_prob")
    if prob is None or pd.isna(prob):
        return ""
    mode = row.get("model_mode")
    return f" | model P(win) {prob:.0%}" + (f" ({mode})" if mode else "")


def signal_text(row: Dict) -> Tuple[str, str]:
    """(title, body) for one armed signal. The title is ASCII: it is an HTTP header."""
    lane = row.get("lane")
    tag = _LANE_TAG.get(lane, lane.upper() if lane else "")
    entry, stop, target = row.get("entry_price"), row.get("stop_price"), row.get("target_price")
    risk = (entry - stop) if entry is not None and stop is not None else None
    reward = (target - entry) if entry is not None and target is not None else None
    if row.get("direction") == -1 and risk is not None and reward is not None:
        risk, reward = -risk, -reward
    rr = f" ({reward / risk:.1f}:1)" if risk and reward and risk > 0 else ""
    score = row.get("total_score")
    score_txt = f"score {score:.0f}" if score is not None and not pd.isna(score) else ""

    if row.get("contract_ticker"):
        title = f"{tag} {_instrument(row)} @ {_px(entry)}"
        lines = [f"Premium stop {_px(stop)} | target {_px(target)}{rr}"]
        if row.get("underlying_stop") is not None:
            lines.append(_underlying_line(row))
        lines.append(f"{row.get('signal')} | {score_txt}{_model_line(row)}")
        lines.append("Option data is 15-min delayed - check the live quote before trading.")
    else:
        title = f"{tag} {_instrument(row)} @ {_px(entry)} ({row.get('signal')})"
        lines = [
            f"Stop {_px(stop)} | Target {_px(target)}{rr} | {score_txt}{_model_line(row)}",
            f"bar {_when(row.get('entry_ts'))}",
        ]
    return _ascii(title), "\n".join(lines)


def signal_message(rows: Sequence[Dict]) -> Message:
    """One push for a group of armed rows (same lane, ticker and signal)."""
    keys = [("signal", int(row["id"])) for row in rows]
    if len(rows) == 1:
        title, body = signal_text(rows[0])
        return Message(title, body, _signal_priority(rows[0]), _signal_tags(rows[0]), keys)

    ranked = sorted(rows, key=lambda r: r.get("total_score") or 0.0, reverse=True)
    best = ranked[0]
    tag = _LANE_TAG.get(best.get("lane"), "")
    side = _SIDE.get(best.get("signal") or "", "")
    source = " [engine]" if (best.get("signal") or "").startswith("ENGINE_") else ""
    title = f"{tag} {best.get('ticker')} {side}{source} x{len(rows)} (best {describe_contract(best.get('contract_ticker')) or ''} @ {_px(best.get('entry_price'))})"
    lines = [
        f"{describe_contract(r.get('contract_ticker')) or r.get('contract_ticker')} @ {_px(r.get('entry_price'))}"
        f" | stop {_px(r.get('stop_price'))} | tgt {_px(r.get('target_price'))}"
        for r in ranked[:_CONTRACTS_PER_MESSAGE]
    ]
    if len(ranked) > _CONTRACTS_PER_MESSAGE:
        lines.append(f"+{len(ranked) - _CONTRACTS_PER_MESSAGE} more contracts")
    if best.get("underlying_stop") is not None:
        lines.append(_underlying_line(best))
    lines.append(f"{best.get('signal')} | option data is 15-min delayed")
    return Message(_ascii(title), "\n".join(lines), _signal_priority(best), _signal_tags(best), keys)


def outcome_text(row: Dict, record: Optional[Dict] = None) -> Tuple[str, str, str]:
    """(title, body, verdict) for one resolved trade."""
    result = scorecard.verdict(row.get("exit_reason"))
    tag = _LANE_TAG.get(row.get("lane"), "")
    r_multiple = row.get("r_multiple")
    r_txt = f"{r_multiple:+.2f}R" if r_multiple is not None and not pd.isna(r_multiple) else "?R"
    title = f"{result} {tag} {_instrument(row)}: {_VERDICT_TEXT[result]} ({r_txt})"

    exit_pct = row.get("exit_pct")
    pct_txt = f"net {exit_pct:+.1f}% " if exit_pct is not None and not pd.isna(exit_pct) else ""
    lines = [
        f"Entry {_px(row.get('entry_price'))} -> exit {_px(row.get('exit_price'))} | "
        f"{pct_txt}({r_txt}) | held {_held(row.get('hold_minutes'))}",
        f"Called {row.get('signal')} {_when(row.get('entry_ts') or row.get('armed_at'))}",
    ]
    if record and record.get("resolved"):
        accuracy = record.get("accuracy")
        lines.append(
            f"{tag} record: {record['success']} success / {record['failed']} failed"
            + (f" ({accuracy:.0%})" if accuracy is not None else "")
            + (f", {record['expired']} expired" if record.get("expired") else "")
            + (f", total {record['total_r']:+.1f}R" if record.get("total_r") is not None else "")
        )
    return _ascii(title), "\n".join(lines), result


def _underlying_line(row: Dict) -> str:
    """Where to act on the stock: the position is managed there, the P&L is premium."""
    falls = "PUT" not in (describe_contract(row.get("contract_ticker")) or "")
    return (
        f"{row.get('ticker')} at {_px(row.get('underlying_price'))}: exit if it "
        f"{'falls' if falls else 'rises'} to {_px(row.get('underlying_stop'))}, "
        f"take profit at {_px(row.get('underlying_target'))}"
    )


def _signal_priority(row: Dict) -> str:
    return "high" if (row.get("signal") or "").startswith("STRONG") else "default"


def _signal_tags(row: Dict) -> Tuple[str, ...]:
    side = _SIDE.get(row.get("signal") or "")
    bullish = side in ("CALL", "LONG") if side else (row.get("direction") or 1) > 0
    return ("chart_with_upwards_trend",) if bullish else ("chart_with_downwards_trend",)


def _ascii(text: str) -> str:
    return text.encode("ascii", "replace").decode()


# ── Transport ────────────────────────────────────────────────────────────────


def _request(url: str, message: Message) -> urllib.request.Request:
    host = urlparse(url).netloc.lower()
    if "discord.com" in host or "discordapp.com" in host:
        payload = json.dumps({"content": f"**{message.title}**\n{message.body}"}).encode()
        return urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
    if "hooks.slack.com" in host:
        payload = json.dumps({"text": f"*{message.title}*\n{message.body}"}).encode()
        return urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
    if "ntfy" in host:
        headers = {"Title": _ascii(message.title), "Priority": message.priority}
        if message.tags:
            headers["Tags"] = ",".join(message.tags)
        return urllib.request.Request(url, data=message.body.encode("utf-8"), headers=headers)
    payload = json.dumps({"title": message.title, "body": message.body}).encode()
    return urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})


def send(url: str, message: Message) -> Optional[str]:
    """POST one message. Returns None on success, else a short error string."""
    try:
        with urllib.request.urlopen(_request(url, message), timeout=_TIMEOUT_SECONDS) as resp:
            if resp.status >= 300:
                return f"HTTP {resp.status}"
    except (URLError, OSError, ValueError) as exc:
        return str(getattr(exc, "reason", exc))
    return None


def send_test(url: str) -> Optional[str]:
    return send(url, Message(
        "Options screener test alert",
        "If you can read this on your phone, push alerts are working.",
    ))


def channel_name(url: str) -> Optional[str]:
    """Which service ``url`` points at, for the alert log. None when no URL is set."""
    if not url:
        return None
    host = urlparse(url).netloc.lower()
    for key, name in (("ntfy", "ntfy"), ("discord", "discord"), ("hooks.slack.com", "slack")):
        if key in host:
            return name
    return "webhook"


# ── The sweep ────────────────────────────────────────────────────────────────


def deliver_pending(storage, url: str, source: str) -> AlertReport:
    """
    Alert every recent signal and outcome that has not been alerted yet.

    With no ``url`` the alerts are still claimed and logged — the Alerts tab and the
    dashboard toasts show them — they are just not pushed anywhere. A process that
    should not alert at all must not call this: claiming is what stops the other
    process from pushing.
    """
    channel = channel_name(url)
    messages: List[Message] = []

    groups: "OrderedDict[Tuple, List[Dict]]" = OrderedDict()
    for row in _safe(storage.load_pending_signal_alerts):
        title, body = signal_text(row)
        if _claim(storage, "signal", row, int(row["id"]), title, body, source, channel):
            groups.setdefault((row.get("lane"), row.get("ticker"), row.get("signal")), []).append(row)
    messages.extend(signal_message(rows) for rows in groups.values())

    records: Dict[str, Dict] = {}
    for row in _safe(storage.load_pending_outcome_alerts):
        lane = row.get("lane")
        if lane not in records:
            try:
                records[lane] = scorecard.summarize(scorecard.annotate(storage.load_predictions(lane)))
            except Exception:
                records[lane] = {}
        title, body, result = outcome_text(row, records[lane])
        if _claim(storage, "outcome", row, int(row["tracking_id"]), title, body, source, channel):
            messages.append(Message(
                title, body, "default", _VERDICT_TAGS[result], [("outcome", int(row["tracking_id"]))]
            ))

    messages = _fold(messages)
    report = AlertReport(sent=messages)
    for message in messages:
        error = send(url, message) if url else None
        if error:
            report.errors.append(f"{message.title}: {error}")
        if channel is None:
            continue
        for kind, tracking_id in message.keys:
            try:
                storage.set_alert_delivery(kind, tracking_id, error)
            except Exception:
                pass
    return report


def _fold(messages: List[Message]) -> List[Message]:
    """Cap a burst: past the limit, the rest share one summary message."""
    if len(messages) <= _MAX_MESSAGES_PER_SWEEP:
        return messages
    head = messages[: _MAX_MESSAGES_PER_SWEEP - 1]
    rest = messages[_MAX_MESSAGES_PER_SWEEP - 1:]
    summary = Message(
        title=f"+{len(rest)} more alerts",
        body="\n".join(m.title for m in rest),
        keys=[key for m in rest for key in m.keys],
    )
    return head + [summary]


def _claim(storage, kind: str, row: Dict, tracking_id: int, title: str, body: str,
           source: str, channel: Optional[str]) -> bool:
    try:
        return storage.claim_alert(
            kind, row.get("lane"), tracking_id, title, body, source, channel,
            ticker=row.get("ticker"), contract_ticker=row.get("contract_ticker"),
            signal=row.get("signal"),
        )
    except Exception:
        return True  # cannot log it - still better to push than to drop it


def _safe(loader) -> List[Dict]:
    try:
        return loader()
    except Exception:
        return []
