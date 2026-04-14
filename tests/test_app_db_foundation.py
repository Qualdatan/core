# SPDX-License-Identifier: AGPL-3.0-only
"""Tests fuer das AppDB-Fundament (Schema, Migrations, Connection)."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from qualdatan_core.app_db import AppDB, default_app_db_path, open_app_db

EXPECTED_TABLES = {
    "projects",
    "runs",
    "run_materials",
    "run_facets",
    "codings",
    "codebook_entries",
    "cache_llm",
    "cache_pdf",
    "app_state",
}


class TestOpen:
    def test_memory(self):
        db = open_app_db(":memory:")
        assert db.schema_version == 1
        db.close()

    def test_file(self, tmp_path: Path):
        p = tmp_path / "nested" / "app.db"
        db = open_app_db(p)
        assert p.exists()
        db.close()

    def test_context_manager(self, tmp_path: Path):
        with open_app_db(tmp_path / "app.db") as db:
            assert db.schema_version == 1
        # zweite Oeffnung persistiert Version
        with open_app_db(tmp_path / "app.db") as db:
            assert db.schema_version == 1

    def test_env_override(self, tmp_path: Path, monkeypatch):
        target = tmp_path / "custom.db"
        monkeypatch.setenv("QUALDATAN_APP_DB", str(target))
        assert default_app_db_path() == target


class TestSchema:
    def test_tables_exist(self):
        with open_app_db(":memory:") as db:
            with db.connection() as conn:
                rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            names = {r[0] for r in rows}
        assert EXPECTED_TABLES.issubset(names)

    def test_fk_enforced(self):
        with open_app_db(":memory:") as db:
            with db.connection() as conn:
                with pytest.raises(sqlite3.IntegrityError):
                    conn.execute(
                        "INSERT INTO runs(project_id, run_dir) VALUES (?, ?)",
                        (999, "/tmp/x"),
                    )

    def test_indexes_exist(self):
        with open_app_db(":memory:") as db:
            with db.connection() as conn:
                rows = conn.execute("SELECT name FROM sqlite_master WHERE type='index'").fetchall()
            names = {r[0] for r in rows}
        assert "idx_runs_project_status" in names
        assert "idx_codings_project_code" in names


class TestTransactions:
    def test_commit(self):
        with open_app_db(":memory:") as db:
            with db.transaction() as conn:
                conn.execute("INSERT INTO projects(name) VALUES ('p1')")
            with db.connection() as conn:
                row = conn.execute("SELECT name FROM projects").fetchone()
            assert row["name"] == "p1"

    def test_rollback(self):
        with open_app_db(":memory:") as db:
            with pytest.raises(RuntimeError):
                with db.transaction() as conn:
                    conn.execute("INSERT INTO projects(name) VALUES ('p1')")
                    raise RuntimeError("boom")
            with db.connection() as conn:
                row = conn.execute("SELECT COUNT(*) FROM projects").fetchone()
            assert row[0] == 0


class TestClose:
    def test_close_is_idempotent(self):
        db = open_app_db(":memory:")
        db.close()
        db.close()
        with pytest.raises(RuntimeError):
            with db.connection():
                pass
