# SPDX-License-Identifier: AGPL-3.0-only
"""Tests fuer :mod:`qualdatan_core.app_db.migrate`."""

from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path

import pytest

from qualdatan_core.app_db import open_app_db
from qualdatan_core.app_db.migrate import (
    MigrationReport,
    _normalize_status,
    _parse_block_range,
    migrate_legacy_output,
)

# ---------------------------------------------------------------------------
# Legacy-Fixture-Builder
# ---------------------------------------------------------------------------
_LEGACY_SCHEMA = """
CREATE TABLE run_state (key TEXT PRIMARY KEY, value TEXT NOT NULL);

CREATE TABLE companies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL,
    source_dir TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE pdf_documents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project TEXT NOT NULL,
    filename TEXT NOT NULL,
    relative_path TEXT NOT NULL UNIQUE,
    path TEXT NOT NULL,
    company_id INTEGER
);

CREATE TABLE interview_documents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    company_id INTEGER NOT NULL,
    filename TEXT NOT NULL,
    path TEXT NOT NULL
);

CREATE TABLE codings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pdf_id INTEGER NOT NULL,
    page INTEGER NOT NULL,
    block_id TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT 'text',
    description TEXT DEFAULT '',
    ganzer_block INTEGER DEFAULT 1,
    begruendung TEXT DEFAULT ''
);

CREATE TABLE coding_codes (
    coding_id INTEGER NOT NULL,
    code_id TEXT NOT NULL,
    PRIMARY KEY (coding_id, code_id)
);
"""


def _make_legacy_db(
    path: Path,
    *,
    status: str = "COMPLETED",
    companies: list[str] | None = None,
    pdfs: list[tuple[str, int | None]] | None = None,
    interviews: list[tuple[str, int]] | None = None,
    codings: list[tuple[int, str, str, list[str]]] | None = None,
    include_companies_table: bool = True,
    include_run_state: bool = True,
) -> None:
    """Schreibt eine synthetische Legacy-``pipeline.db``.

    Args:
        path: Ziel-Pfad.
        status: Wert fuer ``run_state.status``.
        companies: Namen der Companies, Index entspricht id-1.
        pdfs: Liste von ``(filename, company_id)``.
        interviews: Liste von ``(filename, company_id)``.
        codings: Liste von ``(pdf_idx_1based, block_id, description, code_ids)``.
        include_companies_table: Wenn False, wird die companies-Tabelle
            nachtraeglich entfernt.
        include_run_state: Wenn False, wird run_state entfernt.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    try:
        conn.executescript(_LEGACY_SCHEMA)
        if include_run_state:
            now = datetime.now().isoformat()
            conn.execute(
                "INSERT INTO run_state(key, value) VALUES ('status', ?)",
                (status,),
            )
            conn.execute(
                "INSERT INTO run_state(key, value) VALUES ('started_at', ?)",
                (now,),
            )
            conn.execute(
                "INSERT INTO run_state(key, value) VALUES ('updated_at', ?)",
                (now,),
            )
        for name in companies or []:
            conn.execute(
                "INSERT INTO companies(name, source_dir) VALUES (?, ?)",
                (name, f"/legacy/{name}"),
            )
        for fn, cid in pdfs or []:
            conn.execute(
                """
                INSERT INTO pdf_documents(project, filename, relative_path,
                                          path, company_id)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    "proj",
                    fn,
                    f"proj/{fn}",
                    f"/abs/proj/{fn}",
                    cid,
                ),
            )
        for fn, cid in interviews or []:
            conn.execute(
                """
                INSERT INTO interview_documents(company_id, filename, path)
                VALUES (?, ?, ?)
                """,
                (cid, fn, f"/abs/interviews/{fn}"),
            )
        for pdf_idx, block_id, desc, code_ids in codings or []:
            cur = conn.execute(
                """
                INSERT INTO codings(pdf_id, page, block_id, source,
                                    description, begruendung)
                VALUES (?, 1, ?, 'text', ?, 'test justification')
                """,
                (pdf_idx, block_id, desc),
            )
            coding_id = cur.lastrowid
            for code in code_ids:
                conn.execute(
                    "INSERT INTO coding_codes(coding_id, code_id) VALUES (?, ?)",
                    (coding_id, code),
                )
        conn.commit()
        if not include_companies_table:
            conn.execute("DROP TABLE companies")
            conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Unit-level helpers
# ---------------------------------------------------------------------------
class TestNormalizeStatus:
    def test_completed(self):
        assert _normalize_status("COMPLETED") == "completed"

    def test_running_becomes_failed(self):
        assert _normalize_status("RUNNING") == "failed"

    def test_unknown_becomes_failed(self):
        assert _normalize_status("PARTIAL") == "failed"

    def test_none_becomes_failed(self):
        assert _normalize_status(None) == "failed"

    def test_case_insensitive(self):
        assert _normalize_status("completed") == "completed"


class TestParseBlockRange:
    def test_numeric_range(self):
        assert _parse_block_range("100-200") == (100, 200)

    def test_non_numeric_returns_nulls(self):
        assert _parse_block_range("p1_b3") == (None, None)

    def test_empty(self):
        assert _parse_block_range("") == (None, None)
        assert _parse_block_range(None) == (None, None)

    def test_reversed_order_is_swapped(self):
        assert _parse_block_range("200-100") == (100, 200)


# ---------------------------------------------------------------------------
# End-to-end
# ---------------------------------------------------------------------------
class TestMigrateLegacyOutput:
    def test_full_roundtrip(self, tmp_path: Path):
        output = tmp_path / "output"
        run_a = output / "run_A"
        _make_legacy_db(
            run_a / "pipeline.db",
            status="COMPLETED",
            companies=["HKS"],
            pdfs=[("doc1.pdf", 1), ("doc2.pdf", 1)],
            interviews=[("iv1.docx", 1)],
            codings=[
                (1, "p1_b1", "some text", ["PROC-EXEC"]),
                (2, "100-200", "other text", ["ROLE-PM", "PROC-EXEC"]),
            ],
        )

        with open_app_db(":memory:") as db:
            report = migrate_legacy_output(db, output)
            assert isinstance(report, MigrationReport)
            assert report.run_dirs_scanned == 1
            assert report.run_dirs_migrated == 1
            assert report.projects_created == 1
            assert report.codings_imported == 3  # 1 + 2 codes
            assert report.materials_imported == 3  # 2 PDFs + 1 interview

            with db.connection() as conn:
                proj = conn.execute("SELECT id, name FROM projects").fetchone()
                assert proj["name"] == "HKS"

                run = conn.execute("SELECT * FROM runs").fetchone()
                assert run["status"] == "completed"
                assert run["run_dir"] == str(run_a.resolve())

                codings = conn.execute(
                    "SELECT code_id, segment_start, segment_end, justification "
                    "FROM codings ORDER BY id"
                ).fetchall()
                assert len(codings) == 3
                codes = {c["code_id"] for c in codings}
                assert codes == {"PROC-EXEC", "ROLE-PM"}
                # 100-200 -> parsed segment
                ranged = [c for c in codings if c["segment_start"] is not None]
                assert any(c["segment_start"] == 100 and c["segment_end"] == 200 for c in ranged)

    def test_status_mapping_running(self, tmp_path: Path):
        output = tmp_path / "output"
        _make_legacy_db(
            output / "run_X" / "pipeline.db",
            status="RUNNING",
            companies=["HKS"],
        )
        with open_app_db(":memory:") as db:
            migrate_legacy_output(db, output)
            with db.connection() as conn:
                row = conn.execute("SELECT status FROM runs").fetchone()
                assert row["status"] == "failed"

    def test_idempotent(self, tmp_path: Path):
        output = tmp_path / "output"
        _make_legacy_db(
            output / "run_A" / "pipeline.db",
            companies=["HKS"],
            pdfs=[("doc.pdf", 1)],
            codings=[(1, "b", "t", ["C1", "C2"])],
        )
        with open_app_db(":memory:") as db:
            r1 = migrate_legacy_output(db, output)
            assert r1.run_dirs_migrated == 1
            assert r1.codings_imported == 2

            r2 = migrate_legacy_output(db, output)
            assert r2.run_dirs_migrated == 0
            assert r2.run_dirs_skipped == 1
            assert r2.codings_imported == 0

            with db.connection() as conn:
                (n,) = conn.execute("SELECT COUNT(*) FROM codings").fetchone()
                (rn,) = conn.execute("SELECT COUNT(*) FROM runs").fetchone()
            assert n == 2
            assert rn == 1

    def test_dry_run_writes_nothing(self, tmp_path: Path):
        output = tmp_path / "output"
        _make_legacy_db(
            output / "run_A" / "pipeline.db",
            companies=["HKS"],
            pdfs=[("doc.pdf", 1)],
            codings=[(1, "b", "t", ["C1"])],
        )
        with open_app_db(":memory:") as db:
            report = migrate_legacy_output(db, output, dry_run=True)
            assert report.run_dirs_scanned == 1
            assert report.run_dirs_migrated == 0
            # dry-run zaehlt aber trotzdem die Codings
            assert report.codings_imported >= 1
            with db.connection() as conn:
                (n,) = conn.execute("SELECT COUNT(*) FROM runs").fetchone()
                (p,) = conn.execute("SELECT COUNT(*) FROM projects").fetchone()
            assert n == 0
            assert p == 0

    def test_no_companies_synthesizes_legacy_project(self, tmp_path: Path):
        output = tmp_path / "output"
        _make_legacy_db(
            output / "run_solo" / "pipeline.db",
            companies=[],
            include_companies_table=False,
            pdfs=[("doc.pdf", None)],
            codings=[(1, "b", "t", ["C1"])],
        )
        with open_app_db(":memory:") as db:
            report = migrate_legacy_output(db, output)
            assert report.run_dirs_migrated == 1
            assert report.projects_created == 1
            with db.connection() as conn:
                row = conn.execute("SELECT name FROM projects").fetchone()
            assert row["name"] == "legacy-run_solo"

    def test_skip_run_without_pipeline_db(self, tmp_path: Path):
        output = tmp_path / "output"
        (output / "run_empty").mkdir(parents=True)
        with open_app_db(":memory:") as db:
            report = migrate_legacy_output(db, output)
            # run_empty wird als run_*-Dir erkannt, aber ohne pipeline.db
            # als skipped gezaehlt (keine Writes).
            assert report.run_dirs_scanned == 1
            assert report.run_dirs_skipped == 1
            assert report.run_dirs_migrated == 0

    def test_malformed_pipeline_db_warns_and_continues(self, tmp_path: Path):
        output = tmp_path / "output"
        run_bad = output / "run_bad"
        run_bad.mkdir(parents=True)
        (run_bad / "pipeline.db").write_bytes(b"this is not a sqlite file")

        run_good = output / "run_good"
        _make_legacy_db(
            run_good / "pipeline.db",
            companies=["HKS"],
            pdfs=[("x.pdf", 1)],
        )

        with open_app_db(":memory:") as db:
            report = migrate_legacy_output(db, output)
            assert report.run_dirs_scanned == 2
            assert report.run_dirs_migrated == 1
            assert report.run_dirs_skipped == 1
            assert any("nicht lesbar" in w for w in report.warnings)

    def test_output_root_is_single_run(self, tmp_path: Path):
        run = tmp_path / "run_single"
        _make_legacy_db(
            run / "pipeline.db",
            companies=["HKS"],
            pdfs=[("a.pdf", 1)],
            codings=[(1, "b", "t", ["C1"])],
        )
        with open_app_db(":memory:") as db:
            report = migrate_legacy_output(db, run)
            assert report.run_dirs_scanned == 1
            assert report.run_dirs_migrated == 1

    def test_multiple_companies_create_separate_projects(self, tmp_path: Path):
        output = tmp_path / "output"
        run = output / "run_multi"
        _make_legacy_db(
            run / "pipeline.db",
            companies=["HKS", "PBN"],
            pdfs=[("a.pdf", 1), ("b.pdf", 2)],
            codings=[
                (1, "b", "HKS-text", ["CODE-A"]),
                (2, "b", "PBN-text", ["CODE-B"]),
            ],
        )
        with open_app_db(":memory:") as db:
            report = migrate_legacy_output(db, output)
            assert report.projects_created == 2
            assert report.run_dirs_migrated == 1  # ein run_dir, 2 runs
            with db.connection() as conn:
                rows = conn.execute("SELECT name FROM projects ORDER BY name").fetchall()
                names = [r["name"] for r in rows]
                (rn,) = conn.execute("SELECT COUNT(*) FROM runs").fetchone()
            assert names == ["HKS", "PBN"]
            assert rn == 2

    def test_non_existing_output_root(self, tmp_path: Path):
        with open_app_db(":memory:") as db:
            report = migrate_legacy_output(db, tmp_path / "does_not_exist")
            assert report.run_dirs_scanned == 0
            assert report.run_dirs_migrated == 0

    def test_codings_without_coding_codes_still_emits_row(self, tmp_path: Path):
        """Ein Coding ohne Codes soll nicht verloren gehen (code_id='')."""
        output = tmp_path / "output"
        run = output / "run_edge"
        _make_legacy_db(
            run / "pipeline.db",
            companies=["HKS"],
            pdfs=[("a.pdf", 1)],
            codings=[(1, "b", "text", [])],  # kein Code
        )
        with open_app_db(":memory:") as db:
            report = migrate_legacy_output(db, output)
            assert report.codings_imported == 1
            with db.connection() as conn:
                row = conn.execute("SELECT code_id FROM codings").fetchone()
                assert row["code_id"] == ""

    def test_materials_kinds(self, tmp_path: Path):
        output = tmp_path / "output"
        _make_legacy_db(
            output / "run_mat" / "pipeline.db",
            companies=["HKS"],
            pdfs=[("d.pdf", 1)],
            interviews=[("iv.docx", 1)],
        )
        with open_app_db(":memory:") as db:
            migrate_legacy_output(db, output)
            with db.connection() as conn:
                rows = conn.execute(
                    "SELECT material_kind FROM run_materials ORDER BY material_kind"
                ).fetchall()
                kinds = [r["material_kind"] for r in rows]
            assert kinds == ["pdf_text", "transcript"]
