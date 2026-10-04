from __future__ import annotations

import json
import sqlite3
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import pandas as pd

from .intraday import IntradayResult
from .models import RejectedContract, ScoredContract
from .scoring import days_to_expiration
from .timeutil import exchange_date, minutes_between, parse_ts, utc_now

SQLITE_TIMEOUT_SECONDS = 30.0
SQLITE_BUSY_TIMEOUT_MS = 30000
SCAN_RESULT_EXTRA_COLUMNS = {
    "underlying_last_price": "REAL",
    "sma20": "REAL",
    "sma50": "REAL",
    "trend_signal": "TEXT",
    "trend_aligned": "INTEGER",
    "earnings_date": "TEXT",
    "earnings_warning": "TEXT",
    "breakeven_distance_pct": "REAL",
    "expected_move_pct": "REAL",
    "expected_move_to_breakeven_ok": "INTEGER",
    "favorable_2pct_value": "REAL",
    "favorable_2pct_pnl": "REAL",
    "adverse_2pct_value": "REAL",
    "adverse_2pct_pnl": "REAL",
    "decision_checklist": "TEXT",
    "trade_signal": "TEXT",
    "signal_reason": "TEXT",
    # Greeks-derived columns. Theta and vega were fetched and stored from the start
    # but never read by any calculation until the decay model landed.
    "underlying_atr14": "REAL",
    "theta_per_premium": "REAL",
    "gamma_leverage": "REAL",
    "iv_rank": "REAL",
    "premium_entry": "REAL",
    "premium_stop": "REAL",
    "premium_target": "REAL",
    "underlying_stop": "REAL",
    "underlying_target": "REAL",
    "risk_dollars": "REAL",
    "reward_dollars": "REAL",
    "premium_rr": "REAL",
    "target_hold_days": "REAL",
    "decay_at_target": "REAL",
    "model_prob": "REAL",
    "required_prob": "REAL",
    "engine_signal": "TEXT",
    "engine_reason": "TEXT",
    "engine_score": "REAL",
}
# The single source of truth for the intraday results table: column name -> SQL type,
# in insert order. The DDL, the INSERT column list, and the value tuple are all
# generated from this.
#
# They used to be three hand-maintained parallel lists (plus a fourth in app.py for
# display), which is a standing invitation for drift — adding a scoring field meant
# editing four places and the failure mode was a silent column shift, not an error.
INTRADAY_COLUMNS = {
    "rank": "INTEGER",
    "ticker": "TEXT",
    "last_price": "REAL",
    "day_change_pct": "REAL",
    "volume": "INTEGER",
    "relative_volume": "REAL",
    "avg_dollar_volume": "REAL",
    "open": "REAL",
    "high": "REAL",
    "low": "REAL",
    "prev_close": "REAL",
    "rsi14": "REAL",
    "ema9": "REAL",
    "ema20": "REAL",
    "macd": "REAL",
    "macd_signal": "REAL",
    "macd_histogram": "REAL",
    "atr14": "REAL",
    "adx14": "REAL",
    "vwap": "REAL",
    "spread_pct": "REAL",
    "regime": "TEXT",
    "dominant": "TEXT",
    "momentum_score": "REAL",
    "reversion_score": "REAL",
    "breakout_score": "REAL",
    "mtf_score": "REAL",
    "mtf_confluence": "TEXT",
    "sr_score": "REAL",
    "total_score": "REAL",
    "at_key_level": "INTEGER",
    "blocked_ahead": "INTEGER",
    "nearest_support": "REAL",
    "nearest_resistance": "REAL",
    "extension_atr": "REAL",
    "rs_vs_spy": "REAL",
    "rs_assessment": "TEXT",
    "suggested_entry": "REAL",
    "suggested_stop": "REAL",
    "suggested_target": "REAL",
    "stop_dollars": "REAL",
    "target_dollars": "REAL",
    "stop_pct": "REAL",
    "target_pct": "REAL",
    "rr_ratio": "REAL",
    "cost_pct": "REAL",
    "cost_ratio": "REAL",
    "model_prob": "REAL",
    "required_prob": "REAL",
    "trade_signal": "TEXT",
    "signal_reason": "TEXT",
    "risk_notes": "TEXT",
    "market_phase": "TEXT",
    "bar_timestamp": "TEXT",
    "as_of": "TEXT",
}

# Added to signal_tracking after it shipped. An option position is managed on the
# underlying while its P&L is in premium, so the alert has to say where on the stock
# the stop and target sit — and by resolution time the scan row holding them is gone.
TRACKING_EXTRA_COLUMNS = {
    "underlying_price": "REAL",
    "underlying_stop": "REAL",
    "underlying_target": "REAL",
}

# Columns stored as 0/1 rather than as SQLite booleans.
_INTRADAY_BOOL_COLUMNS = {"at_key_level", "blocked_ahead"}

# Number of columns the options INSERT lists explicitly, before the generated tail.
_BASE_RESULT_COLUMN_COUNT = 49

# The greeks-derived fields, appended to the options INSERT from one list so the
# column names, the placeholders and the value tuple cannot drift apart.
_EXTRA_RESULT_FIELDS = (
    "underlying_atr14",
    "theta_per_premium",
    "gamma_leverage",
    "iv_rank",
    "premium_entry",
    "premium_stop",
    "premium_target",
    "underlying_stop",
    "underlying_target",
    "risk_dollars",
    "reward_dollars",
    "premium_rr",
    "target_hold_days",
    "decay_at_target",
    "model_prob",
    "required_prob",
    "engine_signal",
    "engine_reason",
    "engine_score",
)


def _extra_result_columns_sql() -> str:
    return "".join(f", {name}" for name in _EXTRA_RESULT_FIELDS)


def _result_placeholders() -> str:
    return ", ".join("?" * (_BASE_RESULT_COLUMN_COUNT + len(_EXTRA_RESULT_FIELDS)))


class Storage:
    def __init__(self, db_path: Path) -> None:
        self.db_path = Path(db_path)
        self.current_scan_id = None

    def initialize(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS scan_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    started_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    finished_at TEXT,
                    request_json TEXT NOT NULL,
                    summary_json TEXT
                );
                CREATE TABLE IF NOT EXISTS scan_results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    scan_id INTEGER NOT NULL,
                    rank INTEGER,
                    underlying TEXT,
                    contract_ticker TEXT,
                    contract_type TEXT,
                    expiration_date TEXT,
                    strike_price REAL,
                    bid REAL,
                    ask REAL,
                    last_price REAL,
                    mid_price REAL,
                    spread_pct REAL,
                    delta REAL,
                    gamma REAL,
                    theta REAL,
                    vega REAL,
                    implied_volatility REAL,
                    open_interest INTEGER,
                    volume INTEGER,
                    underlying_price REAL,
                    days_to_expiration INTEGER,
                    max_contracts_by_risk INTEGER,
                    premium_at_risk REAL,
                    breakeven REAL,
                    score REAL,
                    score_liquidity REAL,
                    score_spread REAL,
                    score_delta REAL,
                    score_expiration REAL,
                    score_iv REAL,
                    underlying_last_price REAL,
                    sma20 REAL,
                    sma50 REAL,
                    trend_signal TEXT,
                    trend_aligned INTEGER,
                    earnings_date TEXT,
                    earnings_warning TEXT,
                    breakeven_distance_pct REAL,
                    expected_move_pct REAL,
                    expected_move_to_breakeven_ok INTEGER,
                    favorable_2pct_value REAL,
                    favorable_2pct_pnl REAL,
                    adverse_2pct_value REAL,
                    adverse_2pct_pnl REAL,
                    decision_checklist TEXT,
                    trade_signal TEXT,
                    signal_reason TEXT,
                    reason TEXT,
                    as_of TEXT
                );
                CREATE TABLE IF NOT EXISTS rejected_contracts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    scan_id INTEGER NOT NULL,
                    underlying TEXT,
                    contract_ticker TEXT,
                    contract_type TEXT,
                    reason TEXT,
                    as_of TEXT
                );
                CREATE TABLE IF NOT EXISTS scan_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    scan_id INTEGER,
                    ticker TEXT,
                    accepted INTEGER,
                    rejected INTEGER,
                    error TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS watched_contracts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    contract_ticker TEXT NOT NULL,
                    underlying TEXT,
                    contract_type TEXT,
                    entry_price REAL,
                    target_price REAL,
                    stop_price REAL,
                    notes TEXT,
                    status TEXT DEFAULT 'watching',
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    closed_at TEXT
                );
                CREATE TABLE IF NOT EXISTS intraday_scan_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ticker TEXT,
                    signal TEXT,
                    error TEXT,
                    provider TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                );
                -- ── Forward testing ──────────────────────────────────────────
                -- Both lanes share one schema, distinguished by `lane`, rather than
                -- two parallel sets of tables and two copies of every method.
                CREATE TABLE IF NOT EXISTS signal_tracking (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    lane TEXT NOT NULL,
                    ticker TEXT NOT NULL,
                    contract_ticker TEXT,
                    signal TEXT,
                    direction INTEGER NOT NULL,
                    entry_price REAL,
                    stop_price REAL,
                    target_price REAL,
                    stop_dollars REAL,
                    target_dollars REAL,
                    atr14 REAL,
                    entry_ts TEXT,
                    expiration_date TEXT,
                    -- The feature vector as it was AT ARM TIME. Recomputing it later
                    -- is impossible: the snapshot tables are replaced every scan, so
                    -- by the time a trade resolves its inputs are gone and the
                    -- outcome row is unlearnable.
                    features_json TEXT,
                    feature_version INTEGER,
                    model_prob REAL,
                    required_prob REAL,
                    -- Which serving mode produced model_prob. Rows scored by a
                    -- gating model are a censored sample and must never be pooled
                    -- with shadow rows when judging that model.
                    model_mode TEXT,
                    cost_pct REAL,
                    cost_ratio REAL,
                    total_score REAL,
                    status TEXT DEFAULT 'open',
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS trade_outcomes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    lane TEXT NOT NULL,
                    tracking_id INTEGER,
                    ticker TEXT,
                    contract_ticker TEXT,
                    signal TEXT,
                    entry_price REAL,
                    exit_price REAL,
                    gross_dollars REAL,
                    cost_dollars REAL,
                    net_dollars REAL,
                    exit_pct REAL,
                    r_multiple REAL,
                    outcome TEXT,
                    hold_minutes INTEGER,
                    exit_ts TEXT,
                    exit_reason TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS performance_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    lane TEXT NOT NULL,
                    scope TEXT NOT NULL,
                    scope_value TEXT,
                    trades INTEGER,
                    wins INTEGER,
                    losses INTEGER,
                    win_rate REAL,
                    avg_r REAL,
                    total_r REAL,
                    avg_hold_minutes REAL,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS trained_models (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    kind TEXT NOT NULL,
                    model_json TEXT NOT NULL,
                    feature_version INTEGER,
                    metrics_json TEXT,
                    notes TEXT,
                    is_active INTEGER DEFAULT 0,
                    is_shadow INTEGER DEFAULT 0,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                );
                -- One row per alert raised: a newly armed signal ('signal') or a
                -- resolved one ('outcome'), whether or not a push went out. Delivery
                -- status lives here so a dead webhook is visible in the Alerts tab
                -- instead of silently losing alerts. UNIQUE is the dedupe: the
                -- dashboard and the headless runner can both see the same armed
                -- signal, and only the process that inserts first may push it.
                CREATE TABLE IF NOT EXISTS alerts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    kind TEXT NOT NULL,
                    lane TEXT NOT NULL,
                    tracking_id INTEGER NOT NULL,
                    ticker TEXT,
                    contract_ticker TEXT,
                    signal TEXT,
                    title TEXT,
                    body TEXT,
                    source TEXT,
                    channel TEXT,
                    delivered INTEGER DEFAULT 0,
                    delivery_error TEXT,
                    UNIQUE(kind, tracking_id)
                );
                CREATE INDEX IF NOT EXISTS idx_alerts_created ON alerts(created_at);
                -- One implied-volatility reading per underlying per trading day, for
                -- the IV rank. It cannot live in scan_results: that table keeps only
                -- the last 10 scans, which on a 15-minute cadence is 2.5 hours, so the
                -- "rank" compared today's IV with this afternoon's.
                CREATE TABLE IF NOT EXISTS iv_daily (
                    underlying TEXT NOT NULL,
                    day TEXT NOT NULL,
                    iv REAL NOT NULL,
                    observations INTEGER NOT NULL DEFAULT 1,
                    PRIMARY KEY (underlying, day)
                );
                CREATE TABLE IF NOT EXISTS intraday_watchlist (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ticker TEXT NOT NULL,
                    signal TEXT,
                    entry_price REAL,
                    target_price REAL,
                    stop_price REAL,
                    notes TEXT,
                    status TEXT DEFAULT 'watching',
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    closed_at TEXT
                );
                """
            )
            # Generated from the one column spec, so the table can never disagree
            # with the INSERT or with the model it stores.
            columns_ddl = ",\n                    ".join(
                f"{name} {sql_type}" for name, sql_type in INTRADAY_COLUMNS.items()
            )
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS intraday_results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    {columns_ddl}
                )
                """
            )
            self._ensure_columns(conn, "scan_results", SCAN_RESULT_EXTRA_COLUMNS)
            # Additive and idempotent: an existing table from an older build gains the
            # new scoring columns rather than needing a drop.
            self._ensure_columns(conn, "intraday_results", INTRADAY_COLUMNS)
            self._ensure_columns(conn, "intraday_scan_logs", {"provider": "TEXT"})
            self._ensure_columns(conn, "signal_tracking", TRACKING_EXTRA_COLUMNS)

    def start_scan(self, request: Dict) -> int:
        with self._connect() as conn:
            cursor = conn.execute("INSERT INTO scan_runs (request_json) VALUES (?)", (json.dumps(request),))
            self.current_scan_id = cursor.lastrowid
            conn.execute("DELETE FROM scan_results WHERE scan_id NOT IN (SELECT id FROM scan_runs ORDER BY id DESC LIMIT 10)")
            conn.execute("DELETE FROM rejected_contracts WHERE scan_id NOT IN (SELECT id FROM scan_runs ORDER BY id DESC LIMIT 10)")
            return self.current_scan_id

    def finish_scan(self, summary: Dict) -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE scan_runs SET finished_at = CURRENT_TIMESTAMP, summary_json = ? WHERE id = ?",
                (json.dumps(summary), self.current_scan_id),
            )

    def save_results(self, results: Iterable[ScoredContract]) -> None:
        rows = []
        for index, result in enumerate(results, start=1):
            c = result.contract
            rows.append(
                (
                    self.current_scan_id,
                    index,
                    c.underlying,
                    c.contract_ticker,
                    c.contract_type,
                    c.expiration_date.isoformat(),
                    c.strike_price,
                    c.bid,
                    c.ask,
                    c.last_price,
                    c.mid_price,
                    c.spread_pct,
                    c.delta,
                    c.gamma,
                    c.theta,
                    c.vega,
                    c.implied_volatility,
                    c.open_interest,
                    c.volume,
                    c.underlying_price,
                    # Same exchange-date basis the scorer used. Deriving it here from
                    # `as_of.date()` (a UTC date) put a different DTE in this column
                    # than the one the contract was actually filtered and ranked on,
                    # for every scan run after 20:00 ET.
                    days_to_expiration(c),
                    result.max_contracts_by_risk,
                    result.premium_at_risk,
                    result.breakeven,
                    result.score,
                    result.score_components.get("liquidity"),
                    result.score_components.get("spread"),
                    result.score_components.get("delta"),
                    result.score_components.get("expiration"),
                    result.score_components.get("iv"),
                    result.underlying_last_price,
                    result.sma20,
                    result.sma50,
                    result.trend_signal,
                    _bool_to_int(result.trend_aligned),
                    result.earnings_date.isoformat() if result.earnings_date else None,
                    result.earnings_warning,
                    result.breakeven_distance_pct,
                    result.expected_move_pct,
                    _bool_to_int(result.expected_move_to_breakeven_ok),
                    result.favorable_2pct_value,
                    result.favorable_2pct_pnl,
                    result.adverse_2pct_value,
                    result.adverse_2pct_pnl,
                    result.decision_checklist,
                    result.trade_signal,
                    result.signal_reason,
                    result.reason,
                    c.as_of.isoformat(),
                    # Greeks-derived tail, in the same order as _EXTRA_RESULT_FIELDS.
                    *(getattr(result, name) for name in _EXTRA_RESULT_FIELDS),
                )
            )
        if not rows:
            return
        with self._connect() as conn:
            conn.executemany(
                f"""
                INSERT INTO scan_results (
                    scan_id, rank, underlying, contract_ticker, contract_type, expiration_date,
                    strike_price, bid, ask, last_price, mid_price, spread_pct, delta, gamma,
                    theta, vega, implied_volatility, open_interest, volume, underlying_price,
                    days_to_expiration, max_contracts_by_risk, premium_at_risk, breakeven,
                    score, score_liquidity, score_spread, score_delta, score_expiration,
                    score_iv, underlying_last_price, sma20, sma50, trend_signal, trend_aligned,
                    earnings_date, earnings_warning, breakeven_distance_pct, expected_move_pct,
                    expected_move_to_breakeven_ok, favorable_2pct_value, favorable_2pct_pnl,
                    adverse_2pct_value, adverse_2pct_pnl, decision_checklist, trade_signal,
                    signal_reason, reason, as_of{_extra_result_columns_sql()}
                ) VALUES ({_result_placeholders()})
                """,
                rows,
            )

    def save_rejections(self, rejections: Iterable[RejectedContract]) -> None:
        rows = [
            (self.current_scan_id, r.underlying, r.contract_ticker, r.contract_type, r.reason, r.as_of.isoformat())
            for r in rejections
        ]
        if not rows:
            return
        with self._connect() as conn:
            conn.executemany(
                """
                INSERT INTO rejected_contracts (scan_id, underlying, contract_ticker, contract_type, reason, as_of)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                rows,
            )

    def log_ticker(self, ticker: str, accepted: int, rejected: int, error: str = None) -> None:
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO scan_logs (scan_id, ticker, accepted, rejected, error) VALUES (?, ?, ?, ?, ?)",
                (self.current_scan_id, ticker, accepted, rejected, error),
            )

    def load_latest_results(self) -> pd.DataFrame:
        return self._read_latest("scan_results", "ORDER BY score DESC")

    def load_latest_rejections(self) -> pd.DataFrame:
        return self._read_latest("rejected_contracts", "ORDER BY underlying, contract_ticker")

    def load_scan_logs(self) -> pd.DataFrame:
        with self._connect() as conn:
            return pd.read_sql_query(
                "SELECT * FROM scan_logs ORDER BY id DESC LIMIT 500",
                conn,
            )

    def add_watch_contract(
        self,
        contract_ticker: str,
        underlying: str = None,
        contract_type: str = None,
        entry_price: float = None,
        target_price: float = None,
        stop_price: float = None,
        notes: str = None,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO watched_contracts (
                    contract_ticker, underlying, contract_type, entry_price, target_price, stop_price, notes
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (contract_ticker, underlying, contract_type, entry_price, target_price, stop_price, notes),
            )

    def close_watch_contract(self, watch_id: int) -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE watched_contracts SET status = 'closed', closed_at = CURRENT_TIMESTAMP WHERE id = ?",
                (watch_id,),
            )

    def load_watchlist(self) -> pd.DataFrame:
        with self._connect() as conn:
            return pd.read_sql_query("SELECT * FROM watched_contracts ORDER BY id DESC", conn)

    def save_intraday_scan(self, results: Iterable[IntradayResult], logs: Iterable[Dict]) -> None:
        """
        Replace the stored intraday scan with this one.

        Only the latest scan is kept: the results are a live view of the tape, and the
        durable record of what was *decided* lives in the tracking and outcome tables
        rather than here.
        """
        names = list(INTRADAY_COLUMNS)
        result_rows = [tuple(_intraday_value(r, name) for name in names) for r in results]
        # `provider` is carried on the log dicts and was previously dropped on the way
        # in, so the UI could never show which data source produced a row.
        log_rows = [
            (row.get("ticker"), row.get("signal"), row.get("error"),
             row.get("provider"), row.get("created_at"))
            for row in logs
        ]
        placeholders = ", ".join("?" for _ in names)
        with self._connect() as conn:
            conn.execute("DELETE FROM intraday_results")
            conn.execute("DELETE FROM intraday_scan_logs")
            if result_rows:
                conn.executemany(
                    f"INSERT INTO intraday_results ({', '.join(names)}) VALUES ({placeholders})",
                    result_rows,
                )
            if log_rows:
                conn.executemany(
                    "INSERT INTO intraday_scan_logs (ticker, signal, error, provider, created_at)"
                    " VALUES (?, ?, ?, ?, ?)",
                    log_rows,
                )

    def load_intraday_results(self) -> pd.DataFrame:
        with self._connect() as conn:
            return pd.read_sql_query("SELECT * FROM intraday_results ORDER BY total_score DESC", conn)

    def load_intraday_logs(self) -> pd.DataFrame:
        with self._connect() as conn:
            return pd.read_sql_query("SELECT * FROM intraday_scan_logs ORDER BY id DESC LIMIT 500", conn)

    def add_intraday_watch(
        self,
        ticker: str,
        signal: str = None,
        entry_price: float = None,
        target_price: float = None,
        stop_price: float = None,
        notes: str = None,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO intraday_watchlist (ticker, signal, entry_price, target_price, stop_price, notes)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (ticker, signal, entry_price, target_price, stop_price, notes),
            )

    def close_intraday_watch(self, watch_id: int) -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE intraday_watchlist SET status = 'closed', closed_at = CURRENT_TIMESTAMP WHERE id = ?",
                (watch_id,),
            )

    def load_intraday_watchlist(self) -> pd.DataFrame:
        with self._connect() as conn:
            return pd.read_sql_query("SELECT * FROM intraday_watchlist ORDER BY id DESC", conn)

    def record_iv(self, underlying: str, day: date, iv: Optional[float]) -> None:
        """
        Fold one scan's IV reading into the underlying's reading for ``day``.

        A running mean across the day's scans, so a day scanned 26 times and a day
        scanned once each count as one observation in the rank.
        """
        if iv is None or iv <= 0:
            return
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO iv_daily (underlying, day, iv, observations) VALUES (?, ?, ?, 1)
                ON CONFLICT(underlying, day) DO UPDATE SET
                    iv = (iv * observations + excluded.iv) / (observations + 1),
                    observations = observations + 1
                """,
                (underlying.upper(), day.isoformat(), float(iv)),
            )

    def load_iv_history(self, underlying: str, limit: int = 252) -> List[float]:
        """
        Daily implied-volatility readings for one underlying, newest first — about a
        year by default, which is the window an IV rank is conventionally quoted over.

        The rank needs 10 days before it reports anything (``greeks.iv_rank``), so a
        new install shows it blank for its first two weeks rather than ranking against
        a handful of intraday readings.
        """
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT iv FROM iv_daily WHERE underlying = ? ORDER BY day DESC LIMIT ?",
                (underlying.upper(), limit),
            ).fetchall()
        return [row[0] for row in rows if row[0] is not None]

    # ── Forward testing ──────────────────────────────────────────────────────

    # Without a cooldown, every scan after a stop-out re-enters the same chop and
    # racks up correlated losses that look like independent evidence to the model.
    REARM_COOLDOWN_MINUTES = 45

    def record_tracked_signal(
        self,
        lane: str,
        ticker: str,
        signal: str,
        direction: int,
        entry: float,
        stop: float,
        target: float,
        entry_ts: str,
        contract_ticker: Optional[str] = None,
        stop_dollars: Optional[float] = None,
        target_dollars: Optional[float] = None,
        atr14: Optional[float] = None,
        expiration_date: Optional[str] = None,
        features: Optional[Dict] = None,
        feature_version: Optional[int] = None,
        model_prob: Optional[float] = None,
        required_prob: Optional[float] = None,
        model_mode: Optional[str] = None,
        cost_pct: Optional[float] = None,
        cost_ratio: Optional[float] = None,
        total_score: Optional[float] = None,
        underlying_price: Optional[float] = None,
        underlying_stop: Optional[float] = None,
        underlying_target: Optional[float] = None,
        one_per_underlying: bool = False,
    ) -> Optional[int]:
        """
        Arm an actionable signal for hands-off forward evaluation.

        Returns the new tracking id, or ``None`` when the signal was suppressed
        because an identical one is already open or was armed inside the cooldown.

        ``features`` is captured **here, at arm time**, and this is the load-bearing
        detail of the whole learning loop: the snapshot tables are replaced on every
        scan, so if the vector is not stored now it cannot be reconstructed when the
        trade resolves days later, and the outcome row is unlearnable.
        """
        # Dedupe key: the specific instrument, falling back to the ticker for the
        # equity lane. With ``one_per_underlying`` it is the underlying and the signal
        # instead: five AMD calls armed together are one bet on AMD rising, and
        # tracking all five would grade one stock move as five predictions — the
        # scorecard's accuracy would swing on whichever name had the most strikes, and
        # the model would train on duplicates. Keying on the signal keeps the two
        # options labels independent, so each can hold its own AMD call.
        cooldown = f"-{self.REARM_COOLDOWN_MINUTES} minutes"
        with self._connect() as conn:
            if one_per_underlying:
                existing = conn.execute(
                    "SELECT 1 FROM signal_tracking "
                    "WHERE lane=? AND ticker=? AND signal=? "
                    "AND (status='open' OR created_at >= datetime('now', ?)) LIMIT 1",
                    (lane, ticker, signal, cooldown),
                ).fetchone()
            else:
                existing = conn.execute(
                    "SELECT 1 FROM signal_tracking "
                    "WHERE lane=? AND COALESCE(contract_ticker, ticker)=? AND direction=? "
                    "AND (status='open' OR created_at >= datetime('now', ?)) LIMIT 1",
                    (lane, contract_ticker or ticker, direction, cooldown),
                ).fetchone()
            if existing:
                return None
            cursor = conn.execute(
                """
                INSERT INTO signal_tracking (
                    lane, ticker, contract_ticker, signal, direction, entry_price,
                    stop_price, target_price, stop_dollars, target_dollars, atr14,
                    entry_ts, expiration_date, features_json, feature_version,
                    model_prob, required_prob, model_mode, cost_pct, cost_ratio,
                    total_score, underlying_price, underlying_stop, underlying_target
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    lane, ticker, contract_ticker, signal, direction, entry,
                    stop, target, stop_dollars, target_dollars, atr14,
                    entry_ts, expiration_date,
                    json.dumps(features) if features else None, feature_version,
                    model_prob, required_prob, model_mode, cost_pct, cost_ratio,
                    total_score, underlying_price, underlying_stop, underlying_target,
                ),
            )
            return cursor.lastrowid

    def load_open_tracked(self, lane: str, ticker: Optional[str] = None) -> List[Dict]:
        query = "SELECT * FROM signal_tracking WHERE lane=? AND status='open'"
        params: List = [lane]
        if ticker:
            query += " AND ticker=?"
            params.append(ticker)
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            return [dict(row) for row in conn.execute(query, params).fetchall()]

    def resolve_intraday_signals(
        self,
        ticker: str,
        bars: List[Dict],
        max_hold_hours: float = 8.0,
        now: Optional[datetime] = None,
    ) -> int:
        """
        Resolve open equity signals against forward bars. Returns the number closed.

        The stop is checked **before** the target within each bar. When a single bar
        spans both levels there is no way to know which was touched first, so the
        conservative reading is taken — assuming the target every time would inflate
        the win rate exactly on the most volatile bars.

        Forward bars are selected by string comparison, which is valid only because
        every writer emits the one UTC ISO-8601 format ``timeutil.to_iso`` produces.
        """
        open_rows = self.load_open_tracked("intraday", ticker)
        if not open_rows:
            return 0

        now = now or utc_now()
        resolved = 0
        for row in open_rows:
            entry_ts = row.get("entry_ts") or ""
            direction = row.get("direction") or 1
            stop = row.get("stop_price") or 0.0
            target = row.get("target_price") or 0.0
            forward = [b for b in bars if (b.get("timestamp") or "") > entry_ts]

            exit_price = outcome = exit_ts = exit_reason = None
            for bar in forward:
                high, low = bar.get("high"), bar.get("low")
                if high is None or low is None:
                    continue
                if direction == 1:
                    if low <= stop:
                        exit_price, outcome, exit_reason = stop, "LOSS", "STOP"
                    elif high >= target:
                        exit_price, outcome, exit_reason = target, "WIN", "TARGET"
                else:
                    if high >= stop:
                        exit_price, outcome, exit_reason = stop, "LOSS", "STOP"
                    elif low <= target:
                        exit_price, outcome, exit_reason = target, "WIN", "TARGET"
                if outcome:
                    exit_ts = bar.get("timestamp")
                    break

            if outcome is None:
                age = _age_seconds(row, now)
                aged_out = age is not None and age > max_hold_hours * 3600
                if aged_out and forward:
                    exit_price = forward[-1].get("close")
                    exit_ts = forward[-1].get("timestamp")
                    exit_reason = "TIMEOUT"
                else:
                    continue  # still live

            self._close_tracked(row, "intraday", exit_price, exit_ts, exit_reason, outcome)
            resolved += 1

        if resolved:
            self.compute_and_save_performance("intraday")
        return resolved

    def resolve_options_signals(
        self,
        quotes: Dict[str, Optional[float]],
        today: Optional[date] = None,
        now: Optional[datetime] = None,
        max_hold_days: float = 30.0,
        bars: Optional[Dict[str, List[Dict]]] = None,
    ) -> int:
        """
        Resolve open option signals against each contract's own price path.

        ``bars`` maps contract ticker to its intraday bars (15-minute, from Polygon
        aggregates). When a contract has them, every bar after entry is walked in
        order, stop before target within a bar, exactly as the equity lane does — so a
        level touched and retraced between two scans is still seen. A bar that *opens*
        through the stop fills at that open, not at the stop: options gap overnight,
        and crediting the stop price on a gap would understate the loss. A gap through
        the target is credited only at the target, for the same reason in reverse.

        ``quotes`` maps contract ticker to its current mid, and is the fallback for a
        contract whose bars could not be fetched: a point-in-time check that misses
        anything touched and retraced between scans. Checking the stop first keeps
        that bias conservative.

        Also closes anything at or past expiry, and anything held past
        ``max_hold_days``.
        """
        open_rows = self.load_open_tracked("options")
        if not open_rows:
            return 0

        now = now or utc_now()
        today = today or exchange_date(now)
        bars = bars or {}
        resolved = 0

        for row in open_rows:
            contract = row.get("contract_ticker")
            mid = quotes.get(contract) if contract else None
            stop = row.get("stop_price") or 0.0
            target = row.get("target_price") or 0.0
            exit_price = outcome = exit_reason = None
            exit_ts = now.isoformat()

            if contract in bars:
                entered = parse_ts(row.get("entry_ts") or row.get("created_at"))
                forward = [
                    b for b in bars[contract]
                    if entered is not None and (_bar_time(b) or entered) > entered
                ]
                for bar in forward:
                    high, low, bar_open = bar.get("high"), bar.get("low"), bar.get("open")
                    if high is None or low is None:
                        continue
                    if low <= stop:
                        fill = bar_open if bar_open is not None and bar_open < stop else stop
                        exit_price, outcome, exit_reason = fill, "LOSS", "STOP"
                    elif high >= target:
                        exit_price, outcome, exit_reason = target, "WIN", "TARGET"
                    if outcome:
                        exit_ts = bar.get("timestamp") or exit_ts
                        break
                if mid is None and forward:
                    mid = forward[-1].get("close")
            elif mid is not None:
                # A long option: premium falling to the stop is the loss, rising to
                # the target is the win. Direction is already baked into the bracket.
                if mid <= stop:
                    exit_price, outcome, exit_reason = mid, "LOSS", "STOP"
                elif mid >= target:
                    exit_price, outcome, exit_reason = mid, "WIN", "TARGET"

            if outcome is None:
                expiry = row.get("expiration_date")
                expiry_date = date.fromisoformat(expiry) if expiry else None
                age = _age_seconds(row, now)
                aged_out = age is not None and age > max_hold_days * 86400
                # Close a day before expiry: the last session is gamma/decay noise,
                # not a test of the thesis.
                expiring = expiry_date is not None and today >= expiry_date - timedelta(days=1)
                if (expiring or aged_out) and mid is not None:
                    exit_price = mid
                    exit_reason = "EXPIRY" if expiring else "TIMEOUT"
                else:
                    continue

            self._close_tracked(row, "options", exit_price, exit_ts, exit_reason, outcome)
            resolved += 1

        if resolved:
            self.compute_and_save_performance("options")
        return resolved

    def _close_tracked(
        self,
        row: Dict,
        lane: str,
        exit_price: Optional[float],
        exit_ts: Optional[str],
        exit_reason: Optional[str],
        outcome: Optional[str],
    ) -> None:
        """
        Write the outcome row and mark the tracking row closed.

        **R is computed from net, not gross.** Every fill costs the spread twice, and
        a bracket that touches its printed target still paid to get in and out.
        Reporting gross R overstates every result by a consistent margin, which is
        exactly the kind of error that survives a review because it makes the numbers
        look plausible.
        """
        entry_price = row.get("entry_price") or 0.0
        direction = row.get("direction") or 1
        stop_dollars = row.get("stop_dollars") or 0.0
        exit_price = exit_price or 0.0

        gross_dollars = round((exit_price - entry_price) * direction, 4) if entry_price else 0.0
        cost_dollars = (
            round((row.get("cost_pct") or 0.0) / 100.0 * entry_price, 4) if entry_price else 0.0
        )
        net_dollars = round(gross_dollars - cost_dollars, 4)
        if exit_reason in ("TIMEOUT", "EXPIRY"):
            outcome = "WIN" if net_dollars > 0 else ("LOSS" if net_dollars < 0 else "BREAKEVEN")
        exit_pct = round(net_dollars / entry_price * 100, 4) if entry_price else 0.0
        r_multiple = round(net_dollars / stop_dollars, 4) if stop_dollars else None

        # Real trade duration: entry bar to resolving bar. Measuring it as
        # (now - created_at) reports how long until a scan happened to look, which in
        # the forex sibling made wins and losses both average the same ~506 minutes.
        hold_minutes = minutes_between(row.get("entry_ts") or row.get("created_at"), exit_ts)

        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO trade_outcomes (
                    lane, tracking_id, ticker, contract_ticker, signal, entry_price,
                    exit_price, gross_dollars, cost_dollars, net_dollars, exit_pct,
                    r_multiple, outcome, hold_minutes, exit_ts, exit_reason
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    lane, row["id"], row.get("ticker"), row.get("contract_ticker"),
                    row.get("signal"), entry_price, exit_price, gross_dollars,
                    cost_dollars, net_dollars, exit_pct, r_multiple, outcome,
                    int(hold_minutes) if hold_minutes is not None else None,
                    exit_ts, exit_reason,
                ),
            )
            conn.execute("UPDATE signal_tracking SET status='closed' WHERE id=?", (row["id"],))

    def compute_and_save_performance(self, lane: str) -> None:
        """Recompute the aggregate stats a lane's Performance tab reads."""
        with self._connect() as conn:
            conn.execute("DELETE FROM performance_stats WHERE lane=?", (lane,))
            conn.execute(
                """
                INSERT INTO performance_stats (
                    lane, scope, scope_value, trades, wins, losses, win_rate,
                    avg_r, total_r, avg_hold_minutes
                )
                SELECT ?, 'overall', 'ALL',
                       COUNT(*),
                       SUM(outcome='WIN'), SUM(outcome='LOSS'),
                       1.0 * SUM(outcome='WIN') / COUNT(*),
                       AVG(r_multiple), SUM(r_multiple), AVG(hold_minutes)
                FROM trade_outcomes WHERE lane=?
                """,
                (lane, lane),
            )
            # Per-signal breakdown, so a tier that never outperforms is visible as
            # such rather than hidden inside the overall average.
            conn.execute(
                """
                INSERT INTO performance_stats (
                    lane, scope, scope_value, trades, wins, losses, win_rate,
                    avg_r, total_r, avg_hold_minutes
                )
                SELECT ?, 'signal', signal,
                       COUNT(*),
                       SUM(outcome='WIN'), SUM(outcome='LOSS'),
                       1.0 * SUM(outcome='WIN') / COUNT(*),
                       AVG(r_multiple), SUM(r_multiple), AVG(hold_minutes)
                FROM trade_outcomes WHERE lane=? GROUP BY signal
                """,
                (lane, lane),
            )

    def load_performance(self, lane: str) -> pd.DataFrame:
        with self._connect() as conn:
            return pd.read_sql_query(
                "SELECT * FROM performance_stats WHERE lane=? ORDER BY scope, scope_value",
                conn,
                params=(lane,),
            )

    def load_outcomes(self, lane: str, limit: int = 1000) -> pd.DataFrame:
        with self._connect() as conn:
            return pd.read_sql_query(
                "SELECT * FROM trade_outcomes WHERE lane=? ORDER BY id DESC LIMIT ?",
                conn,
                params=(lane, limit),
            )

    def load_tracked(self, lane: str, status: Optional[str] = None) -> pd.DataFrame:
        query = "SELECT * FROM signal_tracking WHERE lane=?"
        params: List = [lane]
        if status:
            query += " AND status=?"
            params.append(status)
        with self._connect() as conn:
            return pd.read_sql_query(query + " ORDER BY id DESC", conn, params=params)

    def load_predictions(self, lane: str) -> pd.DataFrame:
        """
        Every resolved trade beside what was predicted for it at arm time.

        The outcome table alone says what happened; the tracking row says what was
        *called* — the signal tier, the score, the model's probability, the direction.
        Joining them is what turns a list of results into a scorecard of the
        classifier.
        """
        with self._connect() as conn:
            return pd.read_sql_query(
                """
                SELECT o.id, o.tracking_id, o.ticker, o.contract_ticker, o.signal,
                       o.entry_price, o.exit_price, o.net_dollars, o.exit_pct,
                       o.r_multiple, o.outcome, o.exit_reason, o.hold_minutes,
                       o.exit_ts, o.created_at AS resolved_at,
                       t.direction, t.stop_price, t.target_price, t.total_score,
                       t.model_prob, t.required_prob, t.model_mode, t.entry_ts,
                       t.expiration_date, t.created_at AS armed_at
                FROM trade_outcomes o
                JOIN signal_tracking t ON t.id = o.tracking_id
                WHERE o.lane = ?
                ORDER BY o.id ASC
                """,
                conn,
                params=(lane,),
            )

    # ── Alerts ───────────────────────────────────────────────────────────────

    # How far back a sweep looks for signals or outcomes that never got an alert.
    # Long enough to survive a skipped 15-minute cycle or a dashboard rerun cutting a
    # scan off between arming and delivery; short enough that switching alerts on
    # does not push a day's worth of history to the phone.
    ALERT_SWEEP_MINUTES = 90

    def load_pending_signal_alerts(self, max_age_minutes: Optional[int] = None) -> List[Dict]:
        """Recently armed tracking rows that have no 'signal' alert yet."""
        age = self.ALERT_SWEEP_MINUTES if max_age_minutes is None else max_age_minutes
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT t.* FROM signal_tracking t "
                "LEFT JOIN alerts a ON a.kind = 'signal' AND a.tracking_id = t.id "
                "WHERE a.id IS NULL AND t.created_at >= datetime('now', ?) "
                "ORDER BY t.id",
                (f"-{int(age)} minutes",),
            ).fetchall()
        return [dict(row) for row in rows]

    def load_pending_outcome_alerts(self, max_age_minutes: Optional[int] = None) -> List[Dict]:
        """Recently resolved trades that have no 'outcome' alert yet, with their call."""
        age = self.ALERT_SWEEP_MINUTES if max_age_minutes is None else max_age_minutes
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT o.*, t.direction, t.stop_price, t.target_price, t.total_score, "
                "       t.model_prob, t.entry_ts, t.expiration_date, "
                "       t.created_at AS armed_at "
                "FROM trade_outcomes o "
                "JOIN signal_tracking t ON t.id = o.tracking_id "
                "LEFT JOIN alerts a ON a.kind = 'outcome' AND a.tracking_id = o.tracking_id "
                "WHERE a.id IS NULL AND o.created_at >= datetime('now', ?) "
                "ORDER BY o.id",
                (f"-{int(age)} minutes",),
            ).fetchall()
        return [dict(row) for row in rows]

    def claim_alert(
        self,
        kind: str,
        lane: str,
        tracking_id: int,
        title: str,
        body: str,
        source: str,
        channel: Optional[str],
        ticker: Optional[str] = None,
        contract_ticker: Optional[str] = None,
        signal: Optional[str] = None,
    ) -> bool:
        """
        Log one alert before it is pushed. Returns False when it was already claimed
        — by an earlier sweep, or by the other process racing this one.
        """
        with self._connect() as conn:
            cursor = conn.execute(
                "INSERT OR IGNORE INTO alerts (kind, lane, tracking_id, ticker, "
                "contract_ticker, signal, title, body, source, channel) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (kind, lane, tracking_id, ticker, contract_ticker, signal, title, body,
                 source, channel),
            )
            return cursor.rowcount == 1

    def set_alert_delivery(self, kind: str, tracking_id: int, error: Optional[str]) -> None:
        """Record the push outcome of a claimed alert."""
        with self._connect() as conn:
            conn.execute(
                "UPDATE alerts SET delivered = ?, delivery_error = ? "
                "WHERE kind = ? AND tracking_id = ?",
                (int(error is None), error, kind, tracking_id),
            )

    def load_alerts(
        self,
        lane: Optional[str] = None,
        limit: int = 500,
        since_minutes: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Most recent alerts first, each with the current state of the trade it is
        about: ``trade_status`` ('open' | 'closed') and, once closed, the outcome.
        """
        sql = (
            "SELECT a.*, t.status AS trade_status, t.entry_price, t.stop_price, "
            "       t.target_price, o.outcome, o.exit_reason, o.exit_price, "
            "       o.r_multiple, o.exit_ts "
            "FROM alerts a "
            "LEFT JOIN signal_tracking t ON t.id = a.tracking_id "
            "LEFT JOIN trade_outcomes o ON o.id = ("
            "  SELECT MAX(id) FROM trade_outcomes WHERE tracking_id = a.tracking_id"
            ") "
            "WHERE 1 = 1"
        )
        params: List = []
        if lane:
            sql += " AND a.lane = ?"
            params.append(lane)
        if since_minutes is not None:
            sql += " AND a.created_at >= datetime('now', ?)"
            params.append(f"-{int(since_minutes)} minutes")
        sql += " ORDER BY a.created_at DESC, a.id DESC LIMIT ?"
        params.append(limit)
        with self._connect() as conn:
            return pd.read_sql_query(sql, conn, params=params)

    def load_training_rows(self, lane: str, feature_version: int) -> List[Dict]:
        """
        Resolved trades that carry a feature vector at the current contract version.

        Rows armed before feature logging existed, or under a different feature
        contract, are **excluded rather than imputed**: their inputs are unrecoverable
        and inventing them would train the model on fiction.
        """
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT t.features_json, t.model_mode, t.model_prob, t.required_prob,
                       t.signal, t.total_score,
                       o.outcome, o.r_multiple, o.net_dollars, o.exit_reason
                FROM trade_outcomes o
                JOIN signal_tracking t ON t.id = o.tracking_id
                WHERE o.lane = ?
                  AND t.features_json IS NOT NULL
                  AND t.feature_version = ?
                  AND o.outcome IN ('WIN', 'LOSS')
                ORDER BY o.id ASC
                """,
                (lane, feature_version),
            ).fetchall()
        out = []
        for row in rows:
            record = dict(row)
            try:
                record["features"] = json.loads(record.pop("features_json"))
            except (TypeError, ValueError):
                continue
            out.append(record)
        return out

    # ── Model store ──────────────────────────────────────────────────────────

    def save_model(
        self,
        kind: str,
        model_json: str,
        feature_version: int,
        metrics: Optional[Dict] = None,
        notes: Optional[str] = None,
        activate: bool = False,
        shadow: bool = False,
    ) -> int:
        """
        Store a trained candidate. It saves **inactive** unless explicitly activated.

        Training must never change live behaviour as a side effect: a model is
        reviewed against its own metrics first, and promoting it is a separate,
        deliberate act that can be rolled back.
        """
        with self._connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO trained_models (
                    kind, model_json, feature_version, metrics_json, notes,
                    is_active, is_shadow
                ) VALUES (?, ?, ?, ?, ?, 0, 0)
                """,
                (kind, model_json, feature_version, json.dumps(metrics or {}), notes),
            )
            model_id = cursor.lastrowid
        if activate:
            self.set_model_flag(kind, model_id, "is_active")
        elif shadow:
            self.set_model_flag(kind, model_id, "is_shadow")
        return model_id

    def set_model_flag(self, kind: str, model_id: int, flag: str) -> None:
        """Make one model the active (or shadow) one for a lane, clearing the rest."""
        # The only place a column name is interpolated, so it is whitelisted rather
        # than trusted.
        if flag not in ("is_active", "is_shadow"):
            raise ValueError(f"unknown model flag: {flag}")
        with self._connect() as conn:
            conn.execute(f"UPDATE trained_models SET {flag}=0 WHERE kind=?", (kind,))
            conn.execute(f"UPDATE trained_models SET {flag}=1 WHERE id=? AND kind=?", (model_id, kind))

    def clear_model_flag(self, kind: str, flag: str) -> None:
        if flag not in ("is_active", "is_shadow"):
            raise ValueError(f"unknown model flag: {flag}")
        with self._connect() as conn:
            conn.execute(f"UPDATE trained_models SET {flag}=0 WHERE kind=?", (kind,))

    def load_model(self, kind: str, flag: str = "is_active") -> Optional[Dict]:
        if flag not in ("is_active", "is_shadow"):
            raise ValueError(f"unknown model flag: {flag}")
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                f"SELECT * FROM trained_models WHERE kind=? AND {flag}=1 "
                "ORDER BY id DESC LIMIT 1",
                (kind,),
            ).fetchone()
        return dict(row) if row else None

    def load_models(self, kind: str) -> pd.DataFrame:
        with self._connect() as conn:
            return pd.read_sql_query(
                "SELECT id, kind, feature_version, metrics_json, notes, is_active, "
                "is_shadow, created_at FROM trained_models WHERE kind=? ORDER BY id DESC",
                conn,
                params=(kind,),
            )

    def _read_latest(self, table: str, order_by: str) -> pd.DataFrame:
        with self._connect() as conn:
            latest = conn.execute("SELECT MAX(id) FROM scan_runs WHERE finished_at IS NOT NULL").fetchone()[0]
            if latest is None:
                return pd.DataFrame()
            return pd.read_sql_query(f"SELECT * FROM {table} WHERE scan_id = ? {order_by}", conn, params=(latest,))

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=SQLITE_TIMEOUT_SECONDS)
        conn.execute(f"PRAGMA busy_timeout = {SQLITE_BUSY_TIMEOUT_MS}")
        return conn

    def _ensure_columns(self, conn: sqlite3.Connection, table: str, columns: Dict[str, str]) -> None:
        existing = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
        for column, column_type in columns.items():
            if column not in existing:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {column_type}")


def _age_seconds(row: Dict, now: datetime) -> Optional[float]:
    """
    How long a tracked trade has been open, measured from its entry bar.

    Not from ``created_at``: that is SQLite's wall clock, and subtracting it from an
    injected ``now`` mixes two clocks — the timeout test passed the week it was
    written and failed every week after.
    """
    started = parse_ts(row.get("entry_ts") or row.get("created_at"))
    if started is None:
        return None
    return (now - started).total_seconds()


def _bar_time(bar: Dict) -> Optional[datetime]:
    return parse_ts(bar.get("timestamp"))


def _intraday_value(result: IntradayResult, column: str):
    """One column's storable value, pulled off the model by name."""
    value = getattr(result, column, None)
    if column in _INTRADAY_BOOL_COLUMNS:
        return _bool_to_int(value)
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _bool_to_int(value) -> Optional[int]:
    if value is None:
        return None
    return 1 if value else 0
