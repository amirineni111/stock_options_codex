"""
Headless scanner: scan every 15 minutes and push new signals and outcomes to a phone.

Wakes a short lag after each quarter-hour, runs the same scan the dashboard runs, with
the dashboard's saved settings, then pushes anything newly armed *and* anything newly
resolved (SUCCESS / FAILED / EXPIRED, with the running record). Keeps forward tests
resolving while no browser tab is open, which the dashboard cannot do.

Always run from the project root:

    python scripts/run_alerts.py                       # options lane, every 15 min
    python scripts/run_alerts.py --lanes options intraday
    python scripts/run_alerts.py --once                # one cycle now, then exit
    python scripts/run_alerts.py --test-push           # send a test notification
    python scripts/run_alerts.py --sample-alert        # a made-up signal + outcome, real format

Safe to run alongside the dashboard: both write to the same database, arming is
deduplicated, and every alert is claimed before it is pushed, so nothing is pushed
twice.
"""
from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from options_screening.alerts import (  # noqa: E402
    Message, channel_name, deliver_pending, outcome_text, send, send_test, signal_text,
)
from options_screening.config import get_settings  # noqa: E402
from options_screening.intraday import run_intraday_scan  # noqa: E402
from options_screening.runner import (  # noqa: E402
    alert_url, eastern_stamp, in_scan_window, intraday_request, intraday_tickers,
    load_preferences, next_wake, options_request, options_tickers,
)
from options_screening.scanner import run_scan  # noqa: E402
from options_screening.storage import Storage  # noqa: E402

LANES = ("options", "intraday")


def _cycle(settings, storage, lanes, url: str, offhours: bool, quiet: bool) -> None:
    # Settings are re-read every cycle, so a change made in the dashboard applies to
    # the next scan without restarting the runner.
    prefs = load_preferences()
    now = datetime.now(timezone.utc)
    for lane in lanes:
        if not offhours and not in_scan_window(now, lane):
            continue
        try:
            if lane == "options":
                if not settings.polygon_api_key:
                    print("  options skipped: POLYGON_API_KEY is not set", flush=True)
                    continue
                tickers, note = options_tickers(prefs)
                if note:
                    print(f"  {note}", flush=True)
                summary = run_scan(settings, storage, options_request(prefs, tickers))
                line = (f"options: {len(tickers)} tickers, {summary.accepted} ranked, "
                        f"{summary.armed} new signals, {summary.resolved} resolved, "
                        f"{summary.errors} errors")
                busy = summary.armed or summary.resolved
            else:
                tickers, note = intraday_tickers(prefs)
                results, summary, logs = run_intraday_scan(
                    settings, intraday_request(prefs, tickers), storage=storage
                )
                storage.save_intraday_scan(results, logs)
                line = (f"intraday: {len(tickers)} tickers, {summary.accepted} candidates, "
                        f"{summary.errors} errors")
                busy = summary.accepted
            if not quiet or busy:
                print(f"[{eastern_stamp()}] {line}", flush=True)
        except Exception as exc:  # keep the loop alive through network blips
            print(f"[{eastern_stamp()}] {lane} scan failed: {exc}", flush=True)

    report = deliver_pending(storage, url, "runner")
    for message in report.sent:
        print(f"  ALERT {message.title}", flush=True)
    for error in report.errors:
        print(f"  push failed - {error}", flush=True)


def _sample(url: str) -> int:
    signal = {
        "id": 0, "lane": "options", "ticker": "SAMPLE", "contract_ticker": "O:SAMPLE261120C00180000",
        "signal": "BUY_CALL_CANDIDATE", "direction": 1, "entry_price": 4.35, "stop_price": 2.10,
        "target_price": 8.90, "total_score": 71.0, "underlying_price": 172.40,
        "underlying_stop": 165.20, "underlying_target": 183.20,
    }
    outcome = {
        "tracking_id": 0, "lane": "options", "ticker": "SAMPLE",
        "contract_ticker": "O:SAMPLE261120C00180000", "signal": "BUY_CALL_CANDIDATE",
        "entry_price": 4.35, "exit_price": 8.90, "exit_pct": 98.2, "r_multiple": 1.86,
        "exit_reason": "TARGET", "hold_minutes": 3060, "entry_ts": datetime.now(timezone.utc).isoformat(),
    }
    record = {"resolved": 12, "success": 7, "failed": 5, "expired": 0, "accuracy": 7 / 12, "total_r": 3.1}
    title, body = signal_text(signal)
    errors = [send(url, Message("SAMPLE - " + title, body, tags=("chart_with_upwards_trend",)))]
    title, body, _ = outcome_text(outcome, record)
    errors.append(send(url, Message("SAMPLE - " + title, body, tags=("white_check_mark",))))
    errors = [e for e in errors if e]
    print("sent 2 sample alerts" if not errors else f"failed: {errors}")
    return 0 if not errors else 1


def main() -> int:
    ap = argparse.ArgumentParser(description="Scan every 15 minutes and push new signals and outcomes")
    ap.add_argument("--lanes", nargs="+", choices=LANES, default=["options"],
                    help="which pages to scan (default: options)")
    ap.add_argument("--lag", type=float, default=60.0,
                    help="seconds after each quarter-hour to scan (default 60)")
    ap.add_argument("--offhours", action="store_true", help="also scan outside market hours")
    ap.add_argument("--once", action="store_true", help="run one cycle now and exit")
    ap.add_argument("--test-push", action="store_true", help="send a test notification and exit")
    ap.add_argument("--sample-alert", action="store_true",
                    help="send a made-up signal and outcome in the real format, then exit")
    ap.add_argument("--quiet", action="store_true", help="only print cycles that armed or resolved something")
    args = ap.parse_args()

    settings = get_settings()
    url = alert_url(settings, load_preferences())

    if args.test_push or args.sample_alert:
        if not url:
            print("No push URL: set OPTIONS_ALERT_WEBHOOK_URL in .env (see .env.example).")
            return 1
        if args.sample_alert:
            return _sample(url)
        error = send_test(url)
        print("sent" if error is None else f"failed: {error}")
        return 0 if error is None else 1

    storage = Storage(settings.db_path)
    storage.initialize()
    push = f"push -> {channel_name(url)}" if url else "log only (no OPTIONS_ALERT_WEBHOOK_URL)"
    print(f"Scanning {', '.join(args.lanes)} every 15 min; {push}. Ctrl+C to stop.", flush=True)

    if args.once:
        _cycle(settings, storage, args.lanes, url, offhours=True, quiet=False)
        return 0

    try:
        while True:
            now = datetime.now(timezone.utc)
            wake = next_wake(now, args.lag)
            time.sleep(max(0.0, (wake - now).total_seconds()))
            _cycle(settings, storage, args.lanes, url, args.offhours, args.quiet)
    except KeyboardInterrupt:
        print("stopped")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
