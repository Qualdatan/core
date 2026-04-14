# SPDX-License-Identifier: AGPL-3.0-only
"""Tests fuer projects/runs/materials/facets DAOs (App-DB Phase D)."""

from __future__ import annotations

import sqlite3

import pytest

from qualdatan_core.app_db import open_app_db
from qualdatan_core.app_db.projects import (
    VALID_RUN_STATUSES,
    Project,
    Run,
    RunFacet,
    RunMaterial,
    add_run_facet,
    add_run_material,
    create_project,
    create_run,
    delete_project,
    delete_run,
    get_latest_run,
    get_project,
    get_project_by_name,
    get_run,
    list_projects,
    list_run_facets,
    list_run_materials,
    list_runs,
    update_project,
    update_run_status,
)


@pytest.fixture
def db():
    handle = open_app_db(":memory:")
    try:
        yield handle
    finally:
        handle.close()


# ---------------------------------------------------------------------------
# Projects
# ---------------------------------------------------------------------------
class TestProjects:
    def test_create_and_get_roundtrip(self, db):
        p = create_project(db, name="HKS", description="BIM-capable", preset_id="bim.v1")
        assert isinstance(p, Project)
        assert p.id > 0
        assert p.name == "HKS"
        assert p.description == "BIM-capable"
        assert p.preset_id == "bim.v1"
        assert p.created_at

        same = get_project(db, p.id)
        assert same == p

    def test_get_by_name(self, db):
        p = create_project(db, name="PBN")
        assert get_project_by_name(db, "PBN") == p
        assert get_project_by_name(db, "unknown") is None

    def test_get_unknown_returns_none(self, db):
        assert get_project(db, 9999) is None

    def test_list_sorted(self, db):
        create_project(db, name="Charlie")
        create_project(db, name="Alpha")
        create_project(db, name="Bravo")
        names = [p.name for p in list_projects(db)]
        assert names == ["Alpha", "Bravo", "Charlie"]

    def test_duplicate_name_rejected(self, db):
        create_project(db, name="dup")
        with pytest.raises(sqlite3.IntegrityError):
            create_project(db, name="dup")

    def test_update_partial(self, db):
        p = create_project(db, name="x", description="old")
        upd = update_project(db, p.id, description="new", preset_id="p.v2")
        assert upd.name == "x"
        assert upd.description == "new"
        assert upd.preset_id == "p.v2"

    def test_update_noop_returns_current(self, db):
        p = create_project(db, name="x")
        upd = update_project(db, p.id)
        assert upd == p

    def test_update_missing_raises(self, db):
        with pytest.raises(LookupError):
            update_project(db, 424242, name="foo")

    def test_delete_project(self, db):
        p = create_project(db, name="tbd")
        delete_project(db, p.id)
        assert get_project(db, p.id) is None


# ---------------------------------------------------------------------------
# Runs
# ---------------------------------------------------------------------------
class TestRuns:
    def test_create_and_get(self, db):
        p = create_project(db, name="HKS")
        r = create_run(db, project_id=p.id, run_dir="/runs/2026-01")
        assert isinstance(r, Run)
        assert r.project_id == p.id
        assert r.run_dir == "/runs/2026-01"
        assert r.status == "pending"
        assert r.finished_at is None
        assert get_run(db, r.id) == r

    def test_create_invalid_status(self, db):
        p = create_project(db, name="HKS")
        with pytest.raises(ValueError):
            create_run(db, project_id=p.id, run_dir="/runs/x", status="nope")

    def test_create_unknown_project_raises(self, db):
        with pytest.raises(sqlite3.IntegrityError):
            create_run(db, project_id=999, run_dir="/r")

    def test_unique_run_dir_per_project(self, db):
        p = create_project(db, name="HKS")
        create_run(db, project_id=p.id, run_dir="/runs/a")
        with pytest.raises(sqlite3.IntegrityError):
            create_run(db, project_id=p.id, run_dir="/runs/a")

    def test_list_runs_filter_by_project(self, db):
        a = create_project(db, name="A")
        b = create_project(db, name="B")
        create_run(db, project_id=a.id, run_dir="/a/1")
        create_run(db, project_id=a.id, run_dir="/a/2")
        create_run(db, project_id=b.id, run_dir="/b/1")
        assert len(list_runs(db, project_id=a.id)) == 2
        assert len(list_runs(db, project_id=b.id)) == 1

    def test_list_runs_filter_by_status(self, db):
        p = create_project(db, name="HKS")
        r1 = create_run(db, project_id=p.id, run_dir="/1")
        create_run(db, project_id=p.id, run_dir="/2")
        update_run_status(db, r1.id, "completed")
        done = list_runs(db, status="completed")
        assert len(done) == 1 and done[0].id == r1.id

    def test_list_runs_limit_and_order(self, db):
        p = create_project(db, name="HKS")
        ids = [create_run(db, project_id=p.id, run_dir=f"/r{i}").id for i in range(5)]
        rows = list_runs(db, project_id=p.id, limit=3)
        assert [r.id for r in rows] == list(reversed(ids))[:3]

    def test_list_runs_invalid_status(self, db):
        with pytest.raises(ValueError):
            list_runs(db, status="xxx")

    def test_update_status_running_preserves_finished_at(self, db):
        p = create_project(db, name="HKS")
        r = create_run(db, project_id=p.id, run_dir="/r")
        upd = update_run_status(db, r.id, "running")
        assert upd.status == "running"
        assert upd.finished_at is None

    def test_update_status_completed_sets_finished_at(self, db):
        p = create_project(db, name="HKS")
        r = create_run(db, project_id=p.id, run_dir="/r")
        upd = update_run_status(db, r.id, "completed")
        assert upd.status == "completed"
        assert upd.finished_at is not None
        # Idempotenz: erneut completed ueberschreibt nicht
        again = update_run_status(db, r.id, "completed")
        assert again.finished_at == upd.finished_at

    def test_update_status_failed_sets_finished_at(self, db):
        p = create_project(db, name="HKS")
        r = create_run(db, project_id=p.id, run_dir="/r")
        upd = update_run_status(db, r.id, "failed")
        assert upd.finished_at is not None

    def test_update_status_explicit_finished_at(self, db):
        p = create_project(db, name="HKS")
        r = create_run(db, project_id=p.id, run_dir="/r")
        upd = update_run_status(db, r.id, "completed", finished_at="2026-04-13T10:00:00")
        assert upd.finished_at == "2026-04-13T10:00:00"

    def test_update_status_invalid_raises(self, db):
        p = create_project(db, name="HKS")
        r = create_run(db, project_id=p.id, run_dir="/r")
        with pytest.raises(ValueError):
            update_run_status(db, r.id, "bogus")

    def test_update_status_missing_run(self, db):
        with pytest.raises(LookupError):
            update_run_status(db, 12345, "completed")

    def test_get_latest_run(self, db):
        p = create_project(db, name="HKS")
        assert get_latest_run(db, p.id) is None
        create_run(db, project_id=p.id, run_dir="/1")
        r2 = create_run(db, project_id=p.id, run_dir="/2")
        latest = get_latest_run(db, p.id)
        assert latest is not None and latest.id == r2.id

    def test_delete_run(self, db):
        p = create_project(db, name="HKS")
        r = create_run(db, project_id=p.id, run_dir="/r")
        delete_run(db, r.id)
        assert get_run(db, r.id) is None


# ---------------------------------------------------------------------------
# Materials + Facets
# ---------------------------------------------------------------------------
class TestMaterialsFacets:
    def test_materials_roundtrip_and_order(self, db):
        p = create_project(db, name="HKS")
        r = create_run(db, project_id=p.id, run_dir="/r")
        m1 = add_run_material(db, r.id, material_kind="transcript", path="/t/1.txt")
        m2 = add_run_material(
            db,
            r.id,
            material_kind="pdf_text",
            path="/t/2.pdf",
            relative_path="docs/2.pdf",
            source_label="Vertrag",
        )
        assert isinstance(m1, RunMaterial)
        rows = list_run_materials(db, r.id)
        assert [m.id for m in rows] == [m1.id, m2.id]
        assert rows[1].source_label == "Vertrag"
        assert rows[1].relative_path == "docs/2.pdf"

    def test_add_material_unknown_run(self, db):
        with pytest.raises(sqlite3.IntegrityError):
            add_run_material(db, 999, material_kind="transcript", path="/x")

    def test_facets_roundtrip_and_order(self, db):
        p = create_project(db, name="HKS")
        r = create_run(db, project_id=p.id, run_dir="/r")
        f1 = add_run_facet(db, r.id, facet_id="mayring", bundle_id="b.v1")
        f2 = add_run_facet(db, r.id, facet_id="prisma", params_json='{"k":1}')
        assert isinstance(f1, RunFacet)
        rows = list_run_facets(db, r.id)
        assert [f.id for f in rows] == [f1.id, f2.id]
        assert rows[0].bundle_id == "b.v1"
        assert rows[1].params_json == '{"k":1}'

    def test_add_facet_unknown_run(self, db):
        with pytest.raises(sqlite3.IntegrityError):
            add_run_facet(db, 999, facet_id="x")


# ---------------------------------------------------------------------------
# Cascades + FK
# ---------------------------------------------------------------------------
class TestCascades:
    def test_delete_project_cascades(self, db):
        p = create_project(db, name="HKS")
        r = create_run(db, project_id=p.id, run_dir="/r")
        add_run_material(db, r.id, material_kind="transcript", path="/t")
        add_run_facet(db, r.id, facet_id="mayring")
        delete_project(db, p.id)
        assert get_run(db, r.id) is None
        assert list_run_materials(db, r.id) == []
        assert list_run_facets(db, r.id) == []

    def test_delete_run_cascades_to_children(self, db):
        p = create_project(db, name="HKS")
        r = create_run(db, project_id=p.id, run_dir="/r")
        add_run_material(db, r.id, material_kind="transcript", path="/t")
        add_run_facet(db, r.id, facet_id="mayring")
        delete_run(db, r.id)
        assert list_run_materials(db, r.id) == []
        assert list_run_facets(db, r.id) == []


def test_valid_statuses_exposed():
    assert VALID_RUN_STATUSES == frozenset({"pending", "running", "completed", "failed"})
