"""
Schema migration from the pre-parity build.

This repo has a live database with real scan history in it. Migrations are additive and
idempotent — run on every startup, tolerant of already having been applied — so an
existing install gains the new columns instead of needing a drop. Worth a test because
the failure mode is losing a user's accumulated history, which is not recoverable.
"""
import sqlite3

import pytest

from options_screening.storage import Storage

# The intraday_results schema exactly as it shipped before this work.
LEGACY_SCHEMA = """
CREATE TABLE intraday_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    rank INTEGER, ticker TEXT, last_price REAL, day_change_pct REAL,
    volume INTEGER, relative_volume REAL, open REAL, high REAL, low REAL,
    prev_close REAL, minute_price REAL, rsi14 REAL, ema9 REAL, ema20 REAL,
    macd REAL, macd_signal REAL, macd_histogram REAL, vwap REAL,
    spread_pct REAL, signal_mode TEXT, momentum_score REAL,
    mean_reversion_score REAL, total_score REAL, trade_signal TEXT,
    signal_reason TEXT, risk_notes TEXT, as_of TEXT
);
CREATE TABLE intraday_scan_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT, ticker TEXT, signal TEXT,
    error TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP
);
"""


@pytest.fixture()
def legacy_db(tmp_path):
    """An old-schema database with one row of history in it."""
    path = tmp_path / "legacy.sqlite3"
    con = sqlite3.connect(path)
    con.executescript(LEGACY_SCHEMA)
    con.execute(
        "INSERT INTO intraday_results (rank, ticker, last_price, signal_mode,"
        " total_score, trade_signal) VALUES (1, 'AAPL', 180.0, 'Momentum', 55.0,"
        " 'BUY_CANDIDATE')"
    )
    con.commit()
    con.close()
    return path


def _columns(path, table):
    con = sqlite3.connect(path)
    try:
        return {row[1] for row in con.execute(f"PRAGMA table_info({table})")}
    finally:
        con.close()


class TestAdditiveMigration:
    def test_an_old_database_gains_the_new_scoring_columns(self, legacy_db):
        Storage(legacy_db).initialize()
        columns = _columns(legacy_db, "intraday_results")
        for expected in ("atr14", "adx14", "regime", "dominant", "suggested_stop",
                         "cost_ratio", "model_prob", "mtf_confluence", "rs_vs_spy"):
            assert expected in columns, f"missing {expected}"

    def test_existing_rows_survive(self, legacy_db):
        """The failure mode here is losing real accumulated history."""
        Storage(legacy_db).initialize()
        con = sqlite3.connect(legacy_db)
        rows = con.execute("SELECT ticker, total_score FROM intraday_results").fetchall()
        con.close()
        assert rows == [("AAPL", 55.0)]

    def test_superseded_columns_are_left_alone_rather_than_dropped(self, legacy_db):
        """
        Dropping is what turns an additive migration into a destructive one. The dead
        columns cost nothing; the code simply stops writing them.
        """
        Storage(legacy_db).initialize()
        assert "minute_price" in _columns(legacy_db, "intraday_results")
        assert "signal_mode" in _columns(legacy_db, "intraday_results")

    def test_the_log_table_gains_provider(self, legacy_db):
        """`provider` was carried on the log dicts and dropped on the way in."""
        Storage(legacy_db).initialize()
        assert "provider" in _columns(legacy_db, "intraday_scan_logs")

    def test_initialize_is_idempotent(self, legacy_db):
        """It runs on every startup, so re-application must be a no-op."""
        storage = Storage(legacy_db)
        storage.initialize()
        storage.initialize()
        storage.initialize()
        assert "atr14" in _columns(legacy_db, "intraday_results")

    def test_the_forward_testing_tables_are_created(self, legacy_db):
        Storage(legacy_db).initialize()
        con = sqlite3.connect(legacy_db)
        tables = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        con.close()
        assert {"signal_tracking", "trade_outcomes", "performance_stats",
                "trained_models"} <= tables

    def test_the_options_table_gains_the_greeks_columns(self, tmp_path):
        storage = Storage(tmp_path / "fresh.sqlite3")
        storage.initialize()
        columns = _columns(tmp_path / "fresh.sqlite3", "scan_results")
        for expected in ("theta_per_premium", "iv_rank", "premium_stop", "premium_rr",
                         "decay_at_target", "underlying_atr14"):
            assert expected in columns, f"missing {expected}"

    def test_a_migrated_database_can_be_written_to(self, legacy_db):
        """Columns existing is not the same as an INSERT matching them."""
        storage = Storage(legacy_db)
        storage.initialize()
        tracking_id = storage.record_tracked_signal(
            lane="intraday", ticker="MSFT", signal="BUY_CANDIDATE", direction=1,
            entry=100.0, stop=97.5, target=103.75,
            entry_ts="2026-08-14T14:00:00+00:00", stop_dollars=2.5,
            features={"a": 1.0}, feature_version=1,
        )
        assert tracking_id is not None
        assert len(storage.load_open_tracked("intraday")) == 1
