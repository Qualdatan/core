# SPDX-License-Identifier: AGPL-3.0-only
"""Tests fuer ``qualdatan_core.app_db.codings`` (Phase D)."""

from __future__ import annotations

import sqlite3

import pytest

from qualdatan_core.app_db import open_app_db
from qualdatan_core.app_db.codings import (
    CodeFrequency,
    CodingRecord,
    add_coded_segment,
    add_coding,
    add_codings_bulk,
    code_frequencies,
    codings_by_document,
    count_codings,
    delete_codings_for_run,
    get_coding,
    list_codings,
    unique_codes_for_project,
)
from qualdatan_core.models import CodedSegment


# ---------------------------------------------------------------------------
# Fixtures — eigene Minimal-Setups, um nicht auf projects.py-Agent zu warten.
# ---------------------------------------------------------------------------
def _make_project(db, name: str = "P1") -> int:
    with db.transaction() as conn:
        cur = conn.execute("INSERT INTO projects(name) VALUES (?)", (name,))
    return int(cur.lastrowid)


def _make_run(db, project_id: int, run_dir: str) -> int:
    with db.transaction() as conn:
        cur = conn.execute(
            "INSERT INTO runs(project_id, run_dir) VALUES (?, ?)",
            (project_id, run_dir),
        )
    return int(cur.lastrowid)


@pytest.fixture
def db():
    d = open_app_db(":memory:")
    try:
        yield d
    finally:
        d.close()


@pytest.fixture
def project_id(db):
    return _make_project(db, "P1")


@pytest.fixture
def run_id(db, project_id):
    return _make_run(db, project_id, "/tmp/run1")


@pytest.fixture
def second_run(db, project_id):
    return _make_run(db, project_id, "/tmp/run2")


# ---------------------------------------------------------------------------
# add_coding / get_coding
# ---------------------------------------------------------------------------
class TestAddGet:
    def test_roundtrip(self, db, project_id, run_id):
        rec = add_coding(
            db,
            run_id=run_id,
            project_id=project_id,
            document="i1.txt",
            code_id="PROC-EXEC",
            segment_start=10,
            segment_end=25,
            text="Ausfuehrung",
            confidence=0.87,
            justification="weil",
            facet_id="mayring",
        )
        assert isinstance(rec, CodingRecord)
        assert rec.id > 0
        assert rec.run_id == run_id
        assert rec.project_id == project_id
        assert rec.document == "i1.txt"
        assert rec.code_id == "PROC-EXEC"
        assert rec.segment_start == 10
        assert rec.segment_end == 25
        assert rec.text == "Ausfuehrung"
        assert rec.confidence == pytest.approx(0.87)
        assert rec.justification == "weil"
        assert rec.facet_id == "mayring"
        assert rec.created_at  # SQL-Default gesetzt

        fetched = get_coding(db, rec.id)
        assert fetched == rec

    def test_get_missing_returns_none(self, db):
        assert get_coding(db, 999) is None

    def test_visual_coding_nullable_segments(self, db, project_id, run_id):
        rec = add_coding(
            db,
            run_id=run_id,
            project_id=project_id,
            document="plan.pdf",
            code_id="BIM-LOG",
            bbox_json='{"x":1,"y":2}',
        )
        assert rec.segment_start is None
        assert rec.segment_end is None
        assert rec.bbox_json == '{"x":1,"y":2}'

    def test_fk_violation_unknown_run(self, db, project_id):
        with pytest.raises(sqlite3.IntegrityError):
            add_coding(
                db,
                run_id=999,
                project_id=project_id,
                document="x",
                code_id="C",
            )

    def test_fk_violation_unknown_project(self, db, run_id):
        with pytest.raises(sqlite3.IntegrityError):
            add_coding(
                db,
                run_id=run_id,
                project_id=999,
                document="x",
                code_id="C",
            )


# ---------------------------------------------------------------------------
# add_coded_segment
# ---------------------------------------------------------------------------
class TestAddCodedSegment:
    def test_mapping(self, db, project_id, run_id):
        seg = CodedSegment(
            code_id="A-01",
            code_name="Rolle",
            hauptkategorie="A",
            text="Projektleiter",
            char_start=5,
            char_end=18,
            document="i2.txt",
            kodierdefinition="def",
            ankerbeispiel="anker",
            abgrenzungsregel="abgrenz",
        )
        rec = add_coded_segment(
            db,
            run_id=run_id,
            project_id=project_id,
            segment=seg,
            facet_id="mayring",
            confidence=0.5,
        )
        assert rec.document == "i2.txt"
        assert rec.code_id == "A-01"
        assert rec.segment_start == 5
        assert rec.segment_end == 18
        assert rec.text == "Projektleiter"
        assert rec.facet_id == "mayring"
        assert rec.confidence == pytest.approx(0.5)


# ---------------------------------------------------------------------------
# Bulk
# ---------------------------------------------------------------------------
class TestBulk:
    def test_100_rows(self, db, project_id, run_id):
        rows = [
            {
                "run_id": run_id,
                "project_id": project_id,
                "document": f"d{i % 5}.txt",
                "code_id": f"C-{i % 7}",
                "segment_start": i,
                "segment_end": i + 3,
                "text": f"t{i}",
            }
            for i in range(100)
        ]
        n = add_codings_bulk(db, rows)
        assert n == 100
        assert count_codings(db) == 100

    def test_missing_required_raises(self, db, project_id, run_id):
        bad = [{"run_id": run_id, "project_id": project_id, "document": "d"}]
        with pytest.raises(KeyError):
            add_codings_bulk(db, bad)

    def test_bulk_fk_rollback(self, db, project_id, run_id):
        rows = [
            {
                "run_id": run_id,
                "project_id": project_id,
                "document": "ok",
                "code_id": "C1",
            },
            {
                "run_id": 999,
                "project_id": project_id,
                "document": "bad",
                "code_id": "C2",
            },
        ]
        with pytest.raises(sqlite3.IntegrityError):
            add_codings_bulk(db, rows)
        # Transaktion hat alles zurueckgerollt.
        assert count_codings(db) == 0

    def test_empty_bulk(self, db):
        assert add_codings_bulk(db, []) == 0


# ---------------------------------------------------------------------------
# list / count filters + pagination
# ---------------------------------------------------------------------------
@pytest.fixture
def seeded(db, project_id, run_id, second_run):
    add_coding(
        db,
        run_id=run_id,
        project_id=project_id,
        document="a.txt",
        code_id="C1",
        facet_id="F1",
    )
    add_coding(
        db,
        run_id=run_id,
        project_id=project_id,
        document="a.txt",
        code_id="C2",
        facet_id="F2",
    )
    add_coding(
        db,
        run_id=second_run,
        project_id=project_id,
        document="b.txt",
        code_id="C1",
        facet_id="F1",
    )
    add_coding(
        db,
        run_id=second_run,
        project_id=project_id,
        document="b.txt",
        code_id="C3",
        facet_id="F2",
    )
    return {"run_id": run_id, "second_run": second_run, "project_id": project_id}


class TestListCount:
    def test_list_all(self, db, seeded):
        assert len(list_codings(db)) == 4

    def test_filter_run(self, db, seeded):
        rows = list_codings(db, run_id=seeded["run_id"])
        assert len(rows) == 2
        assert {r.run_id for r in rows} == {seeded["run_id"]}

    def test_filter_project(self, db, seeded):
        rows = list_codings(db, project_id=seeded["project_id"])
        assert len(rows) == 4

    def test_filter_code_id(self, db, seeded):
        rows = list_codings(db, code_id="C1")
        assert len(rows) == 2
        assert all(r.code_id == "C1" for r in rows)

    def test_filter_document(self, db, seeded):
        rows = list_codings(db, document="a.txt")
        assert len(rows) == 2

    def test_filter_facet(self, db, seeded):
        rows = list_codings(db, facet_id="F2")
        assert len(rows) == 2
        assert all(r.facet_id == "F2" for r in rows)

    def test_pagination(self, db, seeded):
        page1 = list_codings(db, limit=2, offset=0)
        page2 = list_codings(db, limit=2, offset=2)
        assert len(page1) == 2
        assert len(page2) == 2
        assert {r.id for r in page1}.isdisjoint({r.id for r in page2})

    def test_count_matches_list(self, db, seeded):
        assert count_codings(db) == len(list_codings(db))
        assert count_codings(db, code_id="C1") == len(list_codings(db, code_id="C1"))
        assert count_codings(db, document="b.txt", facet_id="F1") == 1


# ---------------------------------------------------------------------------
# delete_codings_for_run
# ---------------------------------------------------------------------------
class TestDelete:
    def test_delete_only_that_run(self, db, seeded):
        n = delete_codings_for_run(db, seeded["run_id"])
        assert n == 2
        remaining = list_codings(db)
        assert len(remaining) == 2
        assert all(r.run_id == seeded["second_run"] for r in remaining)

    def test_delete_unknown_run_returns_zero(self, db, seeded):
        assert delete_codings_for_run(db, 9999) == 0


# ---------------------------------------------------------------------------
# Cross-run summaries
# ---------------------------------------------------------------------------
class TestFrequencies:
    def test_two_runs_three_codes(self, db, project_id, run_id, second_run):
        # C1: 2 runs, 2 docs, 3 codings
        add_coding(db, run_id=run_id, project_id=project_id, document="a.txt", code_id="C1")
        add_coding(db, run_id=run_id, project_id=project_id, document="a.txt", code_id="C1")
        add_coding(db, run_id=second_run, project_id=project_id, document="b.txt", code_id="C1")
        # C2: 1 run, 1 doc, 2 codings
        add_coding(db, run_id=run_id, project_id=project_id, document="a.txt", code_id="C2")
        add_coding(db, run_id=run_id, project_id=project_id, document="a.txt", code_id="C2")
        # C3: 1 run, 1 doc, 1 coding
        add_coding(db, run_id=second_run, project_id=project_id, document="b.txt", code_id="C3")

        freqs = code_frequencies(db, project_id=project_id)
        by_code = {f.code_id: f for f in freqs}
        assert by_code["C1"] == CodeFrequency("C1", 2, 2, 3)
        assert by_code["C2"] == CodeFrequency("C2", 1, 1, 2)
        assert by_code["C3"] == CodeFrequency("C3", 1, 1, 1)
        # Sortierung: coding_count DESC
        assert [f.code_id for f in freqs] == ["C1", "C2", "C3"]

    def test_filter_code_ids(self, db, project_id, run_id):
        add_coding(db, run_id=run_id, project_id=project_id, document="a.txt", code_id="C1")
        add_coding(db, run_id=run_id, project_id=project_id, document="a.txt", code_id="C2")
        freqs = code_frequencies(db, project_id=project_id, code_ids=["C2"])
        assert len(freqs) == 1
        assert freqs[0].code_id == "C2"

    def test_empty_code_ids_returns_empty(self, db, project_id):
        assert code_frequencies(db, project_id=project_id, code_ids=[]) == []

    def test_isolates_by_project(self, db, project_id, run_id):
        other_p = _make_project(db, "P2")
        other_r = _make_run(db, other_p, "/tmp/other")
        add_coding(db, run_id=run_id, project_id=project_id, document="a.txt", code_id="C1")
        add_coding(db, run_id=other_r, project_id=other_p, document="z.txt", code_id="C1")
        freqs = code_frequencies(db, project_id=project_id)
        assert len(freqs) == 1
        assert freqs[0].coding_count == 1


class TestCodingsByDocument:
    def test_returns_only_document(self, db, seeded):
        rows = codings_by_document(db, project_id=seeded["project_id"], document="a.txt")
        assert len(rows) == 2
        assert all(r.document == "a.txt" for r in rows)

    def test_empty_when_unknown_document(self, db, seeded):
        rows = codings_by_document(db, project_id=seeded["project_id"], document="missing.txt")
        assert rows == []


class TestUniqueCodes:
    def test_distinct_and_sorted(self, db, seeded):
        codes = unique_codes_for_project(db, seeded["project_id"])
        assert codes == ["C1", "C2", "C3"]

    def test_isolated_per_project(self, db, project_id, run_id):
        other_p = _make_project(db, "P2")
        other_r = _make_run(db, other_p, "/tmp/other")
        add_coding(db, run_id=run_id, project_id=project_id, document="a.txt", code_id="C-ONLY-P1")
        add_coding(db, run_id=other_r, project_id=other_p, document="z.txt", code_id="C-ONLY-P2")
        assert unique_codes_for_project(db, project_id) == ["C-ONLY-P1"]
        assert unique_codes_for_project(db, other_p) == ["C-ONLY-P2"]
