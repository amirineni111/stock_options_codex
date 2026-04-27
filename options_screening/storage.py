import json
import sqlite3
from pathlib import Path
from typing import Dict, Iterable, List

import pandas as pd

from .models import RejectedContract, ScoredContract

SQLITE_TIMEOUT_SECONDS = 30.0
SQLITE_BUSY_TIMEOUT_MS = 30000


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
                """
            )

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
                    score_iv, reason, as_of
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
