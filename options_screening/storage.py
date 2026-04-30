import json
import sqlite3
from pathlib import Path
from typing import Dict, Iterable, List

import pandas as pd

from .intraday import IntradayResult
from .models import RejectedContract, ScoredContract

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
}


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
                CREATE TABLE IF NOT EXISTS intraday_results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    rank INTEGER,
                    ticker TEXT,
                    last_price REAL,
                    day_change_pct REAL,
                    volume INTEGER,
                    relative_volume REAL,
                    open REAL,
                    high REAL,
                    low REAL,
                    prev_close REAL,
                    minute_price REAL,
                    spread_pct REAL,
                    signal_mode TEXT,
                    momentum_score REAL,
                    mean_reversion_score REAL,
                    total_score REAL,
                    trade_signal TEXT,
                    signal_reason TEXT,
                    risk_notes TEXT,
                    as_of TEXT
                );
                CREATE TABLE IF NOT EXISTS intraday_scan_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ticker TEXT,
                    signal TEXT,
                    error TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
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
            self._ensure_columns(conn, "scan_results", SCAN_RESULT_EXTRA_COLUMNS)

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
                    (c.expiration_date - c.as_of.date()).days,
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
                )
            )
        if not rows:
            return
        with self._connect() as conn:
            conn.executemany(
                """
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
                    signal_reason, reason, as_of
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        result_rows = [
            (
                r.rank,
                r.ticker,
                r.last_price,
                r.day_change_pct,
                r.volume,
                r.relative_volume,
                r.open,
                r.high,
                r.low,
                r.prev_close,
                r.minute_price,
                r.spread_pct,
                r.signal_mode,
                r.momentum_score,
                r.mean_reversion_score,
                r.total_score,
                r.trade_signal,
                r.signal_reason,
                r.risk_notes,
                r.as_of.isoformat(),
            )
            for r in results
        ]
        log_rows = [(row.get("ticker"), row.get("signal"), row.get("error"), row.get("created_at")) for row in logs]
        with self._connect() as conn:
            conn.execute("DELETE FROM intraday_results")
            conn.execute("DELETE FROM intraday_scan_logs")
            if result_rows:
                conn.executemany(
                    """
                    INSERT INTO intraday_results (
                        rank, ticker, last_price, day_change_pct, volume, relative_volume,
                        open, high, low, prev_close, minute_price, spread_pct, signal_mode,
                        momentum_score, mean_reversion_score, total_score, trade_signal,
                        signal_reason, risk_notes, as_of
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    result_rows,
                )
            if log_rows:
                conn.executemany(
                    "INSERT INTO intraday_scan_logs (ticker, signal, error, created_at) VALUES (?, ?, ?, ?)",
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


def _bool_to_int(value) -> int:
    if value is None:
        return None
    return 1 if value else 0
