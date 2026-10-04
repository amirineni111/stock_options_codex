"""Alert formatting, per-service request shapes, and the claim-then-push sweep."""
import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from options_screening import alerts
from options_screening.alerts import (
    Message, _fold, _request, channel_name, deliver_pending, describe_contract,
    outcome_text, send, signal_message, signal_text,
)
from options_screening.storage import Storage

CALL = "O:AMD261120C00180000"
PUT = "O:AMD261120P00182500"
DEAD_URL = "http://127.0.0.1:9/ntfy-down"   # nothing listens on port 9


@pytest.fixture()
def storage(tmp_path) -> Storage:
    store = Storage(Path(tmp_path) / "test.sqlite3")
    store.initialize()
    return store


def _option_row(**kw):
    row = dict(
        id=1, lane="options", ticker="AMD", contract_ticker=CALL, signal="BUY_CALL_CANDIDATE",
        direction=1, entry_price=4.35, stop_price=2.10, target_price=8.90, total_score=71.0,
        underlying_price=172.40, underlying_stop=165.20, underlying_target=183.20,
    )
    row.update(kw)
    return row


def _arm(storage, **kw):
    fields = dict(
        lane="options", ticker="AMD", contract_ticker=CALL, signal="BUY_CALL_CANDIDATE",
        direction=1, entry=4.35, stop=2.10, target=8.90,
        entry_ts=datetime.now(timezone.utc).isoformat(), stop_dollars=2.25,
        cost_pct=5.0, expiration_date=(date.today() + timedelta(days=40)).isoformat(),
        total_score=71.0, underlying_price=172.4, underlying_stop=165.2, underlying_target=183.2,
    )
    fields.update(kw)
    return storage.record_tracked_signal(**fields)


# ── Formatting ───────────────────────────────────────────────────────────────


def test_occ_symbols_read_as_strike_side_and_expiry():
    assert describe_contract(CALL) == "$180 CALL 11/20/26"
    assert describe_contract(PUT) == "$182.5 PUT 11/20/26"
    assert describe_contract("AAPL") is None


def test_an_option_alert_says_where_to_act_on_the_stock():
    title, body = signal_text(_option_row())
    assert title == "OPT AMD $180 CALL 11/20/26 @ 4.35"
    assert "Premium stop 2.10 | target 8.90 (2.0:1)" in body
    assert "AMD at 172.40: exit if it falls to 165.20, take profit at 183.20" in body
    assert "15-min delayed" in body
    assert title.isascii()


def test_a_put_alert_exits_when_the_stock_rises():
    _, body = signal_text(_option_row(contract_ticker=PUT, signal="BUY_PUT_CANDIDATE",
                                      underlying_stop=179.0, underlying_target=161.0))
    assert "exit if it rises to 179.00" in body


def test_a_stock_alert_carries_side_levels_and_reward_to_risk():
    title, body = signal_text(dict(
        id=2, lane="intraday", ticker="NVDA", signal="SHORT_CANDIDATE", direction=-1,
        entry_price=100.0, stop_price=102.5, target_price=96.25, total_score=52.0,
        entry_ts="2026-09-24T18:30:00+00:00",
    ))
    assert title == "STK NVDA SHORT @ 100.00 (SHORT_CANDIDATE)"
    assert "(1.5:1)" in body and "bar 09/24 14:30 ET" in body


def test_several_contracts_on_one_underlying_are_one_push():
    rows = [_option_row(id=i, total_score=60 + i, entry_price=1.0 + i) for i in range(5)]
    message = signal_message(rows)
    assert message.title.startswith("OPT AMD CALL x5")
    assert "+2 more contracts" in message.body
    assert {key for key in message.keys} == {("signal", i) for i in range(5)}
    # Best-scored contract first.
    assert message.body.splitlines()[0].endswith("tgt 8.90") and "@ 5.00" in message.body.splitlines()[0]


def test_an_outcome_alert_grades_the_call_and_carries_the_record():
    row = dict(
        tracking_id=3, lane="options", ticker="AMD", contract_ticker=CALL,
        signal="BUY_CALL_CANDIDATE", entry_price=4.35, exit_price=8.90, exit_pct=98.2,
        r_multiple=1.86, exit_reason="TARGET", hold_minutes=3060,
        entry_ts="2026-10-02T14:45:00+00:00",
    )
    record = {"resolved": 12, "success": 7, "failed": 5, "expired": 0, "accuracy": 7 / 12, "total_r": 3.1}
    title, body, verdict = outcome_text(row, record)
    assert verdict == "SUCCESS"
    assert title == "SUCCESS OPT AMD $180 CALL 11/20/26: target hit (+1.86R)"
    assert "held 2d 3h" in body
    assert "Called BUY_CALL_CANDIDATE 10/02 10:45 ET" in body
    assert "OPT record: 7 success / 5 failed (58%), total +3.1R" in body


def test_a_stopped_trade_is_graded_failed():
    title, _, verdict = outcome_text(dict(
        tracking_id=4, lane="intraday", ticker="TSLA", signal="BUY_CANDIDATE", direction=1,
        entry_price=250.0, exit_price=245.0, r_multiple=-1.05, exit_reason="STOP",
    ))
    assert verdict == "FAILED"
    assert title == "FAILED STK TSLA LONG: stop hit (-1.05R)"


# ── Transport ────────────────────────────────────────────────────────────────


def test_ntfy_gets_plain_text_with_title_priority_and_tags():
    message = Message("T", "body text", "high", ("white_check_mark",))
    request = _request("https://ntfy.sh/my-topic", message)
    assert request.get_header("Title") == "T"
    assert request.get_header("Priority") == "high"
    assert request.get_header("Tags") == "white_check_mark"
    assert request.data == b"body text"


def test_discord_slack_and_generic_hooks_get_their_shapes():
    message = Message("T", "B")
    assert json.loads(_request("https://discord.com/api/webhooks/1/x", message).data) == {"content": "**T**\nB"}
    assert json.loads(_request("https://hooks.slack.com/services/x", message).data) == {"text": "*T*\nB"}
    assert json.loads(_request("https://example.com/hook", message).data) == {"title": "T", "body": "B"}


def test_channel_name_matches_request_routing():
    assert channel_name("https://ntfy.sh/x") == "ntfy"
    assert channel_name("https://discord.com/api/webhooks/1/x") == "discord"
    assert channel_name("https://example.com/hook") == "webhook"
    assert channel_name("") is None


def test_an_unreachable_endpoint_reports_instead_of_raising():
    assert send(DEAD_URL, Message("T", "B")) is not None


def test_a_burst_is_folded_into_one_summary():
    messages = [Message(f"m{i}", "b", keys=[("signal", i)]) for i in range(12)]
    folded = _fold(messages)
    assert len(folded) == alerts._MAX_MESSAGES_PER_SWEEP
    assert folded[-1].title == "+5 more alerts"
    assert len(folded[-1].keys) == 5


# ── The sweep ────────────────────────────────────────────────────────────────


def test_a_new_signal_is_claimed_logged_and_pushed_once(storage):
    _arm(storage)
    report = deliver_pending(storage, DEAD_URL, "runner")
    assert len(report.sent) == 1 and len(report.errors) == 1
    logged = storage.load_alerts()
    assert list(logged["kind"]) == ["signal"]
    assert logged.iloc[0]["delivered"] == 0 and logged.iloc[0]["delivery_error"]

    # The dashboard sweeping a moment later must not push it again.
    assert deliver_pending(storage, DEAD_URL, "dashboard").sent == []


def test_without_a_url_alerts_are_logged_but_not_pushed(storage):
    _arm(storage)
    report = deliver_pending(storage, "", "dashboard")
    assert len(report.sent) == 1 and report.errors == []
    row = storage.load_alerts().iloc[0]
    assert row["channel"] is None and row["delivered"] == 0


def test_contracts_armed_together_on_one_underlying_share_a_push(storage):
    _arm(storage, contract_ticker="O:AMD261120C00180000")
    _arm(storage, contract_ticker="O:AMD261120C00185000")
    _arm(storage, ticker="INTC", contract_ticker="O:INTC261120C00030000")
    report = deliver_pending(storage, "", "dashboard")
    assert sorted(m.title.split()[1] for m in report.sent) == ["AMD", "INTC"]
    assert len(storage.load_alerts()) == 3  # one logged row per contract


def test_a_resolved_trade_raises_an_outcome_alert(storage):
    _arm(storage)
    deliver_pending(storage, "", "dashboard")          # the signal alert
    storage.resolve_options_signals({CALL: 9.10})       # target reached
    report = deliver_pending(storage, "", "dashboard")
    assert len(report.sent) == 1
    assert report.sent[0].title.startswith("SUCCESS OPT AMD $180 CALL")
    assert report.sent[0].tags == ("white_check_mark",)
    assert "OPT record: 1 success / 0 failed (100%)" in report.sent[0].body

    alerts_df = storage.load_alerts()
    signal_row = alerts_df[alerts_df["kind"] == "signal"].iloc[0]
    assert signal_row["trade_status"] == "closed" and signal_row["exit_reason"] == "TARGET"
