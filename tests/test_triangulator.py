"""Tests fuer src/triangulator.py — Persistente Triangulations-DB (Phase 6).

Das Testsetup erstellt ``pipeline.db``-Fakes per Raw-SQL, um unabhaengig
von Phase 2's ``_init_schema`` zu sein.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from qualdatan_core.triangulation import (
    LEGACY_COMPANY_NAME,
    TriangulationDB,
    list_run_dirs,
    open_triangulation_db,
    rebuild_from_all_runs,
    update_from_run,
)


# ---------------------------------------------------------------------------
# Helpers — Fake pipeline.db creators
# ---------------------------------------------------------------------------

_LEGACY_PIPELINE_SCHEMA = """
CREATE TABLE pdf_documents (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  project TEXT NOT NULL,
  filename TEXT NOT NULL,
  relative_path TEXT NOT NULL UNIQUE,
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

CREATE TABLE visual_detail (
  pdf_id INTEGER NOT NULL,
  page INTEGER NOT NULL,
  description TEXT DEFAULT '',
  elements_json TEXT DEFAULT '[]',
  annotations_json TEXT DEFAULT '[]',
  cross_refs_json TEXT DEFAULT '[]',
  PRIMARY KEY (pdf_id, page)
);
"""


_PHASE2_PIPELINE_SCHEMA = """
CREATE TABLE companies (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT UNIQUE NOT NULL,
  source_dir TEXT,
  created_at TEXT
);

CREATE TABLE projects (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  company_id INTEGER NOT NULL,
  folder_name TEXT NOT NULL,
  code TEXT,
  name TEXT,
  source_dir TEXT,
  UNIQUE(company_id, folder_name)
);

CREATE TABLE interview_documents (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  company_id INTEGER NOT NULL,
  filename TEXT NOT NULL,
  path TEXT NOT NULL
);

CREATE TABLE pdf_documents (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  project TEXT NOT NULL,
  filename TEXT NOT NULL,
  relative_path TEXT NOT NULL UNIQUE,
  path TEXT NOT NULL,
  project_id INTEGER,
  company_id INTEGER,
  source_kind TEXT
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

CREATE TABLE visual_detail (
  pdf_id INTEGER NOT NULL,
  page INTEGER NOT NULL,
  description TEXT DEFAULT '',
  elements_json TEXT DEFAULT '[]',
  annotations_json TEXT DEFAULT '[]',
  cross_refs_json TEXT DEFAULT '[]',
  PRIMARY KEY (pdf_id, page)
);
"""


def _make_legacy_pipeline_db(run_dir: Path) -> Path:
    """Erstellt eine pipeline.db ohne companies/projects (pre-Phase-2)."""
    run_dir.mkdir(parents=True, exist_ok=True)
    db_path = run_dir / "pipeline.db"
    con = sqlite3.connect(str(db_path))
    con.executescript(_LEGACY_PIPELINE_SCHEMA)
    # Zwei pseudo-Projekte (als project-string), je 1 PDF
    con.execute(
        "INSERT INTO pdf_documents (id, project, filename, relative_path, path) "
        "VALUES (1, 'HKS_Projekt_A', 'Aufgabenstellung.pdf', 'HKS_Projekt_A/Aufgabenstellung.pdf', '/x/a.pdf')"
    )
    con.execute(
        "INSERT INTO pdf_documents (id, project, filename, relative_path, path) "
        "VALUES (2, 'HKS_Projekt_B', 'Vertrag.pdf', 'HKS_Projekt_B/Vertrag.pdf', '/x/b.pdf')"
    )
    # Codings mit AwF-Codes
    con.execute(
        "INSERT INTO codings (id, pdf_id, page, block_id, source) "
        "VALUES (1, 1, 1, 'p1b1', 'text')"
    )
    con.execute(
        "INSERT INTO codings (id, pdf_id, page, block_id, source) "
        "VALUES (2, 1, 2, 'p2b1', 'text')"
    )
    con.execute(
        "INSERT INTO codings (id, pdf_id, page, block_id, source) "
        "VALUES (3, 2, 1, 'p1b1', 'text')"
    )
    con.execute("INSERT INTO coding_codes (coding_id, code_id) VALUES (1, 'C-01')")
    con.execute("INSERT INTO coding_codes (coding_id, code_id) VALUES (1, 'A-05')")
    con.execute("INSERT INTO coding_codes (coding_id, code_id) VALUES (2, 'C-01')")
    con.execute("INSERT INTO coding_codes (coding_id, code_id) VALUES (3, 'C-03')")
    # Visual detail mit Elementen
    elements_a = json.dumps([
        {
            "ifc_class": "IfcWall",
            "log_achieved": "LOG 200",
            "description": "Aussenwand EG",
            "awf_code": "C-01",
            "loi_attributes": {"material": "KS", "thickness_cm": 24},
        },
        {
            "ifc_class": "IfcSlab",
            "log_achieved": "LOG 100",
        },
    ])
    con.execute(
        "INSERT INTO visual_detail (pdf_id, page, description, elements_json) "
        "VALUES (1, 1, 'Grundriss EG', ?)",
        (elements_a,),
    )
    con.commit()
    con.close()
    return db_path


def _make_phase2_pipeline_db(
    run_dir: Path,
    company_name: str = "HKS",
    project_folder: str = "Projekt - PBN - Mehrfamilienhaus",
    project_code: str = "PBN",
    project_name: str = "Mehrfamilienhaus",
) -> Path:
    """Erstellt eine pipeline.db MIT companies/projects (Phase-2)."""
    run_dir.mkdir(parents=True, exist_ok=True)
    db_path = run_dir / "pipeline.db"
    con = sqlite3.connect(str(db_path))
    con.executescript(_PHASE2_PIPELINE_SCHEMA)
    con.execute(
        "INSERT INTO companies (id, name, source_dir) VALUES (1, ?, ?)",
        (company_name, f"/input/companies/{company_name}"),
    )
    con.execute(
        """
        INSERT INTO projects (id, company_id, folder_name, code, name, source_dir)
        VALUES (1, 1, ?, ?, ?, ?)
        """,
        (project_folder, project_code, project_name, f"/input/companies/{company_name}/{project_folder}"),
    )
    con.execute(
        """
        INSERT INTO pdf_documents (id, project, filename, relative_path, path, project_id, company_id, source_kind)
        VALUES (1, ?, 'Aufgabenstellung.pdf', ?, '/x/a.pdf', 1, 1, 'project')
        """,
        (project_folder, f"{project_folder}/Aufgabenstellung.pdf"),
    )
    con.execute(
        """
        INSERT INTO pdf_documents (id, project, filename, relative_path, path, project_id, company_id, source_kind)
        VALUES (2, ?, 'Vertrag.pdf', ?, '/x/b.pdf', 1, 1, 'project')
        """,
        (project_folder, f"{project_folder}/Vertrag.pdf"),
    )
    con.execute(
        "INSERT INTO codings (id, pdf_id, page, block_id, source) VALUES (1, 1, 1, 'p1b1', 'text')"
    )
    con.execute(
        "INSERT INTO codings (id, pdf_id, page, block_id, source) VALUES (2, 2, 1, 'p1b1', 'text')"
    )
    con.execute("INSERT INTO coding_codes (coding_id, code_id) VALUES (1, 'C-02')")
    con.execute("INSERT INTO coding_codes (coding_id, code_id) VALUES (1, 'I-01')")
    con.execute("INSERT INTO coding_codes (coding_id, code_id) VALUES (2, 'C-02')")
    elements = json.dumps([
        {"ifc_class": "IfcDoor", "log_achieved": "LOG 200", "awf_code": "C-02"},
    ])
    con.execute(
        "INSERT INTO visual_detail (pdf_id, page, elements_json) VALUES (1, 3, ?)",
        (elements,),
    )
    con.commit()
    con.close()
    return db_path


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestSchema:
    def test_schema_creates_all_tables(self, tmp_path):
        db = TriangulationDB(tmp_path / "tri.db")
        try:
            rows = db.conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            ).fetchall()
            names = {r["name"] for r in rows}
            expected = {
                "companies",
                "projects",
                "runs",
                "project_facts",
                "project_lph",
                "project_awf",
                "project_elements",
                "project_interfaces",
            }
            assert expected.issubset(names)

            # Indizes
            idx_rows = db.conn.execute(
                "SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_%'"
            ).fetchall()
            idx_names = {r["name"] for r in idx_rows}
            assert "idx_facts_project_key" in idx_names
            assert "idx_awf_project" in idx_names
            assert "idx_elements_project" in idx_names
        finally:
            db.close()

    def test_open_triangulation_db_factory(self, tmp_path):
        db = open_triangulation_db(tmp_path / "tri.db")
        assert db.list_companies() == []
        db.close()


class TestUpserts:
    def test_upsert_company_idempotent(self, tmp_path):
        with TriangulationDB(tmp_path / "tri.db") as db:
            cid1 = db.upsert_company("HKS", "/path/HKS")
            cid2 = db.upsert_company("HKS", "/path/HKS")
            assert cid1 == cid2
            companies = db.list_companies()
            assert len(companies) == 1
            assert companies[0]["name"] == "HKS"
            assert companies[0]["first_seen"] is not None
            assert companies[0]["last_seen"] is not None

    def test_upsert_project_unique_per_company_folder(self, tmp_path):
        with TriangulationDB(tmp_path / "tri.db") as db:
            hks = db.upsert_company("HKS")
            other = db.upsert_company("OtherCo")
            p1 = db.upsert_project(hks, "Projekt - PBN - MFH", "PBN", "MFH")
            p1_again = db.upsert_project(hks, "Projekt - PBN - MFH", "PBN", "MFH")
            p2 = db.upsert_project(other, "Projekt - PBN - MFH", "PBN", "MFH")
            assert p1 == p1_again
            assert p1 != p2
            rows = db.list_projects()
            assert len(rows) == 2
            hks_projects = db.list_projects("HKS")
            assert len(hks_projects) == 1
            assert hks_projects[0]["company_name"] == "HKS"

    def test_add_fact_multiple_per_project(self, tmp_path):
        with TriangulationDB(tmp_path / "tri.db") as db:
            cid = db.upsert_company("HKS")
            pid = db.upsert_project(cid, "Projekt A")
            db.add_fact(pid, "bauvolumen_eur", "3200000", source_type="interview")
            db.add_fact(pid, "gebaeudetyp", "MFH", source_type="pdf", confidence=0.9)
            db.add_fact(pid, "bauvolumen_eur", "3500000", source_type="pdf")
            overview = db.project_overview(pid)
            assert len(overview["facts"]) == 3
            keys = [f["key"] for f in overview["facts"]]
            assert keys.count("bauvolumen_eur") == 2
            assert "gebaeudetyp" in keys

    def test_upsert_awf_idempotent(self, tmp_path):
        with TriangulationDB(tmp_path / "tri.db") as db:
            cid = db.upsert_company("HKS")
            pid = db.upsert_project(cid, "Projekt A")
            a1 = db.upsert_awf(pid, "C-01", awf_name="Planen", kategorie="C", evidence_count=3)
            a2 = db.upsert_awf(pid, "C-01", evidence_count=2)
            assert a1 == a2
            rows = db.conn.execute(
                "SELECT * FROM project_awf WHERE project_id = ?", (pid,)
            ).fetchall()
            assert len(rows) == 1
            assert rows[0]["evidence_count"] == 5
            assert rows[0]["awf_name"] == "Planen"


class TestUpdateFromRun:
    def test_update_from_run_with_legacy_pipeline_db(self, tmp_path):
        run_dir = tmp_path / "runs" / "2026-04-09_legacy"
        _make_legacy_pipeline_db(run_dir)
        tri_path = tmp_path / "tri.db"
        stats = update_from_run(run_dir, db_path=tri_path, mode="legacy")

        assert stats["legacy_mode"] is True
        assert stats["companies"] >= 1
        assert stats["projects"] == 2

        with TriangulationDB(tri_path) as db:
            companies = db.list_companies()
            names = {c["name"] for c in companies}
            assert LEGACY_COMPANY_NAME in names
            projects = db.list_projects(LEGACY_COMPANY_NAME)
            folder_names = {p["folder_name"] for p in projects}
            assert folder_names == {"HKS_Projekt_A", "HKS_Projekt_B"}

    def test_update_from_run_with_phase2_pipeline_db(self, tmp_path):
        run_dir = tmp_path / "runs" / "2026-04-09_phase2"
        _make_phase2_pipeline_db(run_dir)
        tri_path = tmp_path / "tri.db"
        stats = update_from_run(run_dir, db_path=tri_path, mode="company")

        assert stats["legacy_mode"] is False
        assert stats["companies"] == 1
        assert stats["projects"] == 1

        with TriangulationDB(tri_path) as db:
            companies = db.list_companies()
            assert len(companies) == 1
            assert companies[0]["name"] == "HKS"
            projects = db.list_projects("HKS")
            assert len(projects) == 1
            assert projects[0]["code"] == "PBN"
            assert projects[0]["last_run_id"] is not None

            runs = db.conn.execute("SELECT * FROM runs").fetchall()
            assert len(runs) == 1
            assert runs[0]["mode"] == "company"

    def test_update_from_run_extracts_awf_codes(self, tmp_path):
        run_dir = tmp_path / "runs" / "2026-04-09_awf"
        _make_legacy_pipeline_db(run_dir)
        tri_path = tmp_path / "tri.db"
        update_from_run(run_dir, db_path=tri_path)

        with TriangulationDB(tri_path) as db:
            # project HKS_Projekt_A has 2 codings with C-01 → evidence_count = 2
            # project HKS_Projekt_B has 1 coding with C-03 → evidence_count = 1
            proj_a = db.conn.execute(
                """SELECT p.id FROM projects p JOIN companies c ON p.company_id=c.id
                   WHERE c.name = ? AND p.folder_name = ?""",
                (LEGACY_COMPANY_NAME, "HKS_Projekt_A"),
            ).fetchone()
            assert proj_a is not None
            awfs_a = db.conn.execute(
                "SELECT awf_code, evidence_count FROM project_awf WHERE project_id = ?",
                (proj_a["id"],),
            ).fetchall()
            awf_map = {r["awf_code"]: r["evidence_count"] for r in awfs_a}
            assert awf_map == {"C-01": 2}
            # A-05 darf NICHT als AwF landen (nur C-* Prefix)
            assert "A-05" not in awf_map

            proj_b = db.conn.execute(
                """SELECT p.id FROM projects p JOIN companies c ON p.company_id=c.id
                   WHERE c.name = ? AND p.folder_name = ?""",
                (LEGACY_COMPANY_NAME, "HKS_Projekt_B"),
            ).fetchone()
            assert proj_b is not None
            awfs_b = db.conn.execute(
                "SELECT awf_code, evidence_count FROM project_awf WHERE project_id = ?",
                (proj_b["id"],),
            ).fetchall()
            awf_map_b = {r["awf_code"]: r["evidence_count"] for r in awfs_b}
            assert awf_map_b == {"C-03": 1}

    def test_update_from_run_extracts_visual_elements(self, tmp_path):
        run_dir = tmp_path / "runs" / "2026-04-09_elems"
        _make_legacy_pipeline_db(run_dir)
        tri_path = tmp_path / "tri.db"
        stats = update_from_run(run_dir, db_path=tri_path)

        assert stats["elements"] == 2

        with TriangulationDB(tri_path) as db:
            elements = db.conn.execute(
                "SELECT * FROM project_elements ORDER BY id"
            ).fetchall()
            assert len(elements) == 2
            wall = elements[0]
            assert wall["ifc_class"] == "IfcWall"
            assert wall["log_value"] == "LOG 200"
            assert wall["awf_code"] == "C-01"
            assert wall["evidence_page"] == 1
            assert wall["evidence_doc"] == "Aufgabenstellung.pdf"
            loi = json.loads(wall["loi_attributes_json"])
            assert loi["material"] == "KS"
            slab = elements[1]
            assert slab["ifc_class"] == "IfcSlab"
            assert slab["loi_attributes_json"] is None


class TestRebuild:
    def test_rebuild_from_all_runs_dedupes(self, tmp_path):
        # Zwei Runs (Phase-2) mit identischer company+project → sollen dedupet werden
        output_root = tmp_path / "output"
        run1 = output_root / "2026-04-09_a"
        run2 = output_root / "2026-04-09_b"
        _make_phase2_pipeline_db(run1)
        _make_phase2_pipeline_db(run2)

        tri_path = tmp_path / "tri.db"

        # Erstmal normal importieren
        update_from_run(run1, db_path=tri_path)
        update_from_run(run2, db_path=tri_path)

        with TriangulationDB(tri_path) as db:
            companies = db.list_companies()
            assert len(companies) == 1
            projects = db.list_projects("HKS")
            assert len(projects) == 1  # dedupet per UNIQUE(company, folder_name)

        # list_run_dirs sollte beide Runs finden
        found = list_run_dirs(output_root)
        assert len(found) == 2
        assert run1 in found
        assert run2 in found

        # Rebuild: sollte alles droppen und erneut importieren
        total = rebuild_from_all_runs(db_path=tri_path, output_root=output_root)
        assert total["runs_imported"] == 2

        with TriangulationDB(tri_path) as db:
            companies = db.list_companies()
            assert len(companies) == 1  # still deduped
            projects = db.list_projects("HKS")
            assert len(projects) == 1
            runs = db.conn.execute("SELECT * FROM runs").fetchall()
            assert len(runs) == 2  # both runs recorded
