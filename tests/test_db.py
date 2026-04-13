"""Tests fuer die Pipeline-Datenbank (src/db.py)."""

import json
import threading
import pytest

from qualdatan_core.db import PipelineDB


@pytest.fixture
def db(tmp_path):
    """Erstellt eine temporaere Datenbank."""
    return PipelineDB(tmp_path / "test.db")


# ---------------------------------------------------------------------------
# Schema + Grundlagen
# ---------------------------------------------------------------------------

class TestDBBasics:
    def test_create_db(self, tmp_path):
        db = PipelineDB(tmp_path / "test.db")
        assert (tmp_path / "test.db").exists()

    def test_wal_mode(self, db):
        conn = db._get_conn()
        mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
        assert mode == "wal"

    def test_foreign_keys_enabled(self, db):
        conn = db._get_conn()
        fk = conn.execute("PRAGMA foreign_keys").fetchone()[0]
        assert fk == 1


# ---------------------------------------------------------------------------
# Run State
# ---------------------------------------------------------------------------

class TestRunState:
    def test_set_and_get_state(self, db):
        db.set_state("status", "running")
        assert db.get_state("status") == "running"

    def test_get_state_default(self, db):
        assert db.get_state("missing", "default") == "default"

    def test_set_complex_value(self, db):
        db.set_state("transcripts", ["a.docx", "b.docx"])
        result = db.get_state("transcripts")
        assert result == ["a.docx", "b.docx"]

    def test_get_all_state(self, db):
        db.set_state("status", "running")
        db.set_state("recipe", "mayring")
        state = db.get_all_state()
        assert state["status"] == "running"
        assert state["recipe"] == "mayring"

    def test_overwrite_state(self, db):
        db.set_state("status", "running")
        db.set_state("status", "completed")
        assert db.get_state("status") == "completed"


# ---------------------------------------------------------------------------
# PDF Documents
# ---------------------------------------------------------------------------

class TestPDFDocuments:
    def test_upsert_pdf(self, db):
        pid = db.upsert_pdf("Projekt_A", "plan.pdf", "Projekt_A/plan.pdf",
                            "/path/to/plan.pdf", file_size_kb=100)
        assert pid > 0

    def test_upsert_returns_same_id(self, db):
        pid1 = db.upsert_pdf("P", "a.pdf", "P/a.pdf", "/p/a.pdf")
        pid2 = db.upsert_pdf("P", "a.pdf", "P/a.pdf", "/p/a.pdf")
        assert pid1 == pid2

    def test_get_pdf_id(self, db):
        db.upsert_pdf("P", "a.pdf", "P/a.pdf", "/p/a.pdf")
        assert db.get_pdf_id("P/a.pdf") is not None
        assert db.get_pdf_id("P/missing.pdf") is None

    def test_get_pdf(self, db):
        pid = db.upsert_pdf("P", "a.pdf", "P/a.pdf", "/p/a.pdf", file_size_kb=50)
        pdf = db.get_pdf(pid)
        assert pdf["filename"] == "a.pdf"
        assert pdf["project"] == "P"
        assert pdf["file_size_kb"] == 50

    def test_get_all_pdfs(self, db):
        db.upsert_pdf("A", "1.pdf", "A/1.pdf", "/a/1.pdf")
        db.upsert_pdf("B", "2.pdf", "B/2.pdf", "/b/2.pdf")
        pdfs = db.get_all_pdfs()
        assert len(pdfs) == 2

    def test_update_classification(self, db):
        pid = db.upsert_pdf("P", "a.pdf", "P/a.pdf", "/p/a.pdf")
        db.update_pdf_classification(pid, "plan", 0.85)
        pdf = db.get_pdf(pid)
        assert pdf["document_type"] == "plan"
        assert pdf["confidence"] == 0.85


# ---------------------------------------------------------------------------
# Pipeline Status
# ---------------------------------------------------------------------------

class TestPipelineStatus:
    def test_set_and_check_status(self, db):
        pid = db.upsert_pdf("P", "a.pdf", "P/a.pdf", "/p/a.pdf")
        assert not db.is_step_done(pid, "extraction")
        db.set_step_status(pid, "extraction", "done")
        assert db.is_step_done(pid, "extraction")

    def test_running_status(self, db):
        pid = db.upsert_pdf("P", "a.pdf", "P/a.pdf", "/p/a.pdf")
        db.set_step_status(pid, "extraction", "running")
        assert not db.is_step_done(pid, "extraction")

    def test_error_status(self, db):
        pid = db.upsert_pdf("P", "a.pdf", "P/a.pdf", "/p/a.pdf")
        db.set_step_status(pid, "extraction", "error", "file not found")
        assert not db.is_step_done(pid, "extraction")

    def test_get_pending_pdfs(self, db):
        p1 = db.upsert_pdf("P", "a.pdf", "P/a.pdf", "/p/a.pdf")
        p2 = db.upsert_pdf("P", "b.pdf", "P/b.pdf", "/p/b.pdf")
        db.set_step_status(p1, "extraction", "done")
        pending = db.get_pending_pdfs("extraction")
        assert p2 in pending
        assert p1 not in pending

    def test_get_step_summary(self, db):
        p1 = db.upsert_pdf("P", "a.pdf", "P/a.pdf", "/p/a.pdf")
        p2 = db.upsert_pdf("P", "b.pdf", "P/b.pdf", "/p/b.pdf")
        db.set_step_status(p1, "extraction", "done")
        db.set_step_status(p2, "extraction", "done")
        db.set_step_status(p1, "coding", "done")
        db.set_step_status(p2, "coding", "error")

        summary = db.get_step_summary()
        assert summary["extraction"]["done"] == 2
        assert summary["coding"]["done"] == 1
        assert summary["coding"]["error"] == 1


# ---------------------------------------------------------------------------
# Extractions
# ---------------------------------------------------------------------------

class TestExtractions:
    def test_save_and_load(self, db):
        pid = db.upsert_pdf("P", "a.pdf", "P/a.pdf", "/p/a.pdf")
        data = {
            "file": "a.pdf",
            "pages": [
                {"page": 1, "blocks": [{"id": "p1_b0", "type": "text", "text": "Hello"}]}
            ],
            "metadata": {"page_count": 1},
        }
        db.save_extraction(pid, data)
        loaded = db.load_extraction(pid)
        assert loaded["file"] == "a.pdf"
        assert len(loaded["pages"]) == 1
        assert loaded["pages"][0]["blocks"][0]["text"] == "Hello"

    def test_has_extraction(self, db):
        pid = db.upsert_pdf("P", "a.pdf", "P/a.pdf", "/p/a.pdf")
        assert not db.has_extraction(pid)
        db.save_extraction(pid, {"file": "a.pdf", "pages": [], "metadata": {}})
        assert db.has_extraction(pid)

    def test_load_missing(self, db):
        assert db.load_extraction(999) is None


# ---------------------------------------------------------------------------
# Page Metrics + Classifications
# ---------------------------------------------------------------------------

class TestMetricsAndClassifications:
    def test_save_page_metrics(self, db):
        pid = db.upsert_pdf("P", "a.pdf", "P/a.pdf", "/p/a.pdf")
        metrics = {
            "text_coverage": 0.5, "image_coverage": 0.1,
            "text_char_count": 500, "drawing_count": 10,
            "aspect_ratio": 1.41, "is_landscape": True,
            "page_format": "A3", "page_width": 1190, "page_height": 842,
        }
        db.save_page_metrics(pid, 1, metrics)

        conn = db._get_conn()
        row = conn.execute(
            "SELECT * FROM page_metrics WHERE pdf_id = ? AND page = ?",
            (pid, 1),
        ).fetchone()
        assert dict(row)["text_coverage"] == 0.5
        assert dict(row)["page_format"] == "A3"

    def test_save_classification(self, db):
        pid = db.upsert_pdf("P", "a.pdf", "P/a.pdf", "/p/a.pdf")
        db.save_classification(pid, 1, "plan", 0.85, "floor_plan",
                               has_title_block=True,
                               title_block={"massstab": "1:100"})
        result = db.get_classifications(pid)
        assert len(result) == 1
        assert result[0]["page_type"] == "plan"
        assert result[0]["has_title_block"] == 1
        tb = json.loads(result[0]["title_block_json"])
        assert tb["massstab"] == "1:100"


# ---------------------------------------------------------------------------
# Codings
# ---------------------------------------------------------------------------

class TestCodings:
    def test_save_coding(self, db):
        pid = db.upsert_pdf("P", "a.pdf", "P/a.pdf", "/p/a.pdf")
        cid = db.save_coding(pid, 1, "p1_b0", ["A-01", "B-02"],
                             source="text", begruendung="Test")
        assert cid > 0

    def test_get_codings_for_pdf(self, db):
        pid = db.upsert_pdf("P", "a.pdf", "P/a.pdf", "/p/a.pdf")
        db.save_coding(pid, 1, "p1_b0", ["A-01"])
        db.save_coding(pid, 2, "p2_b0", ["B-01", "C-01"])

        codings = db.get_codings_for_pdf(pid)
        assert len(codings) == 2
        assert "A-01" in codings[0]["codes"]
        assert len(codings[1]["codes"]) == 2

    def test_get_all_codings_by_code(self, db):
        p1 = db.upsert_pdf("P", "a.pdf", "P/a.pdf", "/p/a.pdf")
        p2 = db.upsert_pdf("P", "b.pdf", "P/b.pdf", "/p/b.pdf")
        db.save_coding(p1, 1, "p1_b0", ["A-01"])
        db.save_coding(p2, 1, "p1_b0", ["A-01"])
        db.save_coding(p2, 2, "p2_b0", ["B-01"])

        results = db.get_all_codings_by_code("A-01")
        assert len(results) == 2

    def test_save_neue_codes(self, db):
        pid = db.upsert_pdf("P", "a.pdf", "P/a.pdf", "/p/a.pdf")
        db.save_neue_codes([
            {"code_id": "L-01", "code_name": "Test", "hauptkategorie": "L"},
        ], pdf_id=pid)

        conn = db._get_conn()
        row = conn.execute("SELECT * FROM neue_codes WHERE code_id = 'L-01'").fetchone()
        assert dict(row)["code_name"] == "Test"

    def test_get_coding_summary(self, db):
        pid = db.upsert_pdf("P", "a.pdf", "P/a.pdf", "/p/a.pdf")
        db.save_coding(pid, 1, "p1_b0", ["A-01"], source="text")
        db.save_coding(pid, 1, "p1_v0", ["A-01", "O-01"], source="visual_triage")

        summary = db.get_coding_summary()
        assert "A-01" in summary
        assert summary["A-01"]["text"] == 1
        assert summary["A-01"]["visual_triage"] == 1


# ---------------------------------------------------------------------------
# Visual Triage + Detail
# ---------------------------------------------------------------------------

class TestVisual:
    def test_save_visual_triage(self, db):
        pid = db.upsert_pdf("P", "a.pdf", "P/a.pdf", "/p/a.pdf")
        db.save_visual_triage(pid, 1, {
            "page_type": "floor_plan",
            "priority": "high",
            "estimated_log": "LOG-03",
            "building_elements": ["Waende", "Tueren"],
            "description": "EG Grundriss",
            "confidence": 0.9,
        })

        results = db.get_visual_triage(pid)
        assert len(results) == 1
        assert results[0]["page_type"] == "floor_plan"
        assert results[0]["priority"] == "high"
        assert "Waende" in results[0]["building_elements"]

    def test_save_visual_detail(self, db):
        pid = db.upsert_pdf("P", "a.pdf", "P/a.pdf", "/p/a.pdf")
        db.save_visual_detail(pid, 1, {
            "description": "Grundriss",
            "building_elements": [
                {"element_type": "Wand", "ifc_class": "IfcWall", "log_achieved": "LOG-03"}
            ],
            "annotations": ["Bemassung"],
            "cross_references": ["Schnitt A-A"],
        })

        results = db.get_visual_detail(pid)
        assert len(results) == 1
        assert results[0]["building_elements"][0]["ifc_class"] == "IfcWall"
        assert "Bemassung" in results[0]["annotations"]

    def test_has_visual(self, db):
        pid = db.upsert_pdf("P", "a.pdf", "P/a.pdf", "/p/a.pdf")
        assert not db.has_visual_triage(pid)
        db.save_visual_triage(pid, 1, {"page_type": "plan"})
        assert db.has_visual_triage(pid)


# ---------------------------------------------------------------------------
# Aggregation (Toolkit-Export)
# ---------------------------------------------------------------------------

class TestAggregation:
    def test_get_all_building_elements(self, db):
        pid = db.upsert_pdf("P", "a.pdf", "P/a.pdf", "/p/a.pdf")
        db.save_visual_detail(pid, 1, {
            "building_elements": [
                {"element_type": "Wand", "ifc_class": "IfcWall"},
                {"element_type": "Tuer", "ifc_class": "IfcDoor"},
            ],
        })

        elements = db.get_all_building_elements()
        assert len(elements) == 2
        assert elements[0]["project"] == "P"
        assert elements[1]["ifc_class"] == "IfcDoor"

    def test_get_documents_by_type(self, db):
        p1 = db.upsert_pdf("P", "a.pdf", "P/a.pdf", "/p/a.pdf")
        p2 = db.upsert_pdf("P", "b.pdf", "P/b.pdf", "/p/b.pdf")
        db.update_pdf_classification(p1, "plan", 0.9)
        db.update_pdf_classification(p2, "text", 0.85)

        plans = db.get_documents_by_type("plan")
        assert len(plans) == 1
        assert plans[0]["filename"] == "a.pdf"

    def test_get_log_evidence_summary(self, db):
        pid = db.upsert_pdf("P", "a.pdf", "P/a.pdf", "/p/a.pdf")
        db.save_visual_triage(pid, 1, {
            "estimated_log": "LOG-03", "page_type": "floor_plan",
        })
        db.save_visual_triage(pid, 2, {
            "estimated_log": "LOG-02", "page_type": "section",
        })

        summary = db.get_log_evidence_summary()
        assert len(summary) == 2
        logs = {r["estimated_log"] for r in summary}
        assert "LOG-03" in logs
        assert "LOG-02" in logs


# ---------------------------------------------------------------------------
# Companies / Projects / Interview Docs (Phase 2)
# ---------------------------------------------------------------------------


class TestCompanyTables:
    def test_upsert_company_creates_and_returns_id(self, db):
        cid = db.upsert_company("HKS", "/data/companies/HKS")
        assert cid > 0
        conn = db._get_conn()
        row = conn.execute(
            "SELECT name, source_dir FROM companies WHERE id = ?", (cid,)
        ).fetchone()
        assert row["name"] == "HKS"
        assert row["source_dir"] == "/data/companies/HKS"

    def test_upsert_company_idempotent(self, db):
        cid1 = db.upsert_company("HKS", "/a")
        cid2 = db.upsert_company("HKS", "/b")  # zweiter Aufruf -> gleiche ID
        assert cid1 == cid2
        # Nur ein Eintrag insgesamt
        conn = db._get_conn()
        count = conn.execute(
            "SELECT COUNT(*) as c FROM companies"
        ).fetchone()["c"]
        assert count == 1

    def test_upsert_project_unique_per_company_folder(self, db):
        c1 = db.upsert_company("HKS", "/x")
        c2 = db.upsert_company("MiniCo", "/y")

        p1 = db.upsert_project(c1, "Projekt - PBN - Neustadt", "PBN", "Neustadt", "/x/PBN")
        p2 = db.upsert_project(c1, "Projekt - PBN - Neustadt", "PBN", "Neustadt", "/x/PBN")
        assert p1 == p2  # gleiches Projekt -> gleiche id

        # Andere Company, gleicher folder_name -> neuer Eintrag
        p3 = db.upsert_project(c2, "Projekt - PBN - Neustadt", "PBN", "Neustadt", "/y/PBN")
        assert p3 != p1

        # Anderer folder_name in c1 -> neuer Eintrag
        p4 = db.upsert_project(c1, "Projekt - WGN - Nord", "WGN", "Nord", "/x/WGN")
        assert p4 != p1

    def test_pdf_documents_company_id_nullable(self, db):
        """pdf_documents soll ohne company_id/project_id einsetzbar bleiben
        (die neuen Spalten sind nullable)."""
        pid = db.upsert_pdf("Legacy", "a.pdf", "Legacy/a.pdf", "/l/a.pdf")
        pdf = db.get_pdf(pid)
        # Neue Spalten existieren, sind aber NULL
        assert "company_id" in pdf
        assert "project_id" in pdf
        assert "source_kind" in pdf
        assert pdf["company_id"] is None
        assert pdf["project_id"] is None
        assert pdf["source_kind"] is None

    def test_alter_columns_idempotent(self, tmp_path):
        """Zweifache Initialisierung darf nicht crashen — ALTER ADD COLUMN
        wuerde sonst 'duplicate column name' werfen."""
        from src.db import PipelineDB
        db_path = tmp_path / "twice.db"
        db1 = PipelineDB(db_path)
        db1.upsert_pdf("P", "a.pdf", "P/a.pdf", "/p/a.pdf")
        db1.close()

        # Zweite Instanz auf dem gleichen File -> _init_db wird
        # erneut ausgefuehrt, darf aber nicht crashen
        db2 = PipelineDB(db_path)
        pdfs = db2.get_all_pdfs()
        assert len(pdfs) == 1
        # Und upsert_company funktioniert immer noch
        cid = db2.upsert_company("X", "/x")
        assert cid > 0

    def test_upsert_interview_doc_unique(self, db):
        cid = db.upsert_company("HKS", "/x")
        i1 = db.upsert_interview_doc(cid, "Beyer.docx", "/x/Interviews/Beyer.docx")
        i2 = db.upsert_interview_doc(cid, "Beyer.docx", "/x/Interviews/Beyer.docx")
        assert i1 == i2
        i3 = db.upsert_interview_doc(cid, "Mueller.docx", "/x/Interviews/Mueller.docx")
        assert i3 != i1

    def test_list_companies_in_db_with_project_counts(self, db):
        c1 = db.upsert_company("HKS", "/x")
        c2 = db.upsert_company("MiniCo", "/y")
        db.upsert_project(c1, "Projekt - A - A", "A", "A", "")
        db.upsert_project(c1, "Projekt - B - B", "B", "B", "")
        db.upsert_project(c2, "Projekt - C - C", "C", "C", "")

        companies = db.list_companies_in_db()
        assert len(companies) == 2
        by_name = {c["name"]: c for c in companies}
        assert by_name["HKS"]["project_count"] == 2
        assert by_name["MiniCo"]["project_count"] == 1


# ---------------------------------------------------------------------------
# Thread Safety
# ---------------------------------------------------------------------------

class TestThreadSafety:
    def test_concurrent_writes(self, db):
        """Mehrere Threads schreiben gleichzeitig."""
        errors = []

        def write_pdf(i):
            try:
                db.upsert_pdf("P", f"pdf_{i}.pdf", f"P/pdf_{i}.pdf", f"/p/pdf_{i}.pdf")
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=write_pdf, args=(i,)) for i in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        assert len(db.get_all_pdfs()) == 20

    def test_concurrent_read_write(self, db):
        """Lesen und Schreiben gleichzeitig."""
        pid = db.upsert_pdf("P", "a.pdf", "P/a.pdf", "/p/a.pdf")
        errors = []

        def writer():
            try:
                for i in range(50):
                    db.save_coding(pid, 1, f"p1_b{i}", [f"A-{i:02d}"])
            except Exception as e:
                errors.append(e)

        def reader():
            try:
                for _ in range(50):
                    db.get_codings_for_pdf(pid)
            except Exception as e:
                errors.append(e)

        t1 = threading.Thread(target=writer)
        t2 = threading.Thread(target=reader)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        assert len(errors) == 0
