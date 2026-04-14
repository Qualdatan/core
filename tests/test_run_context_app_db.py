# SPDX-License-Identifier: AGPL-3.0-only
"""Tests fuer die App-DB-Anbindung in :mod:`qualdatan_core.run_context` (Phase D.2).

Decken ab:
    * :meth:`RunContext.attach_to_app_db` (Neu-/Wiederverwendung, Idempotenz)
    * :meth:`RunContext.register_material` und :meth:`register_facet`
    * App-DB-Mirror in ``init_state`` / ``mark_completed`` / ``mark_failed``
    * Best-effort-Verhalten bei geschlossener App-DB
    * Freie Funktion :func:`create_run` mit/ohne App-DB
    * Rueckwaertskompatibilitaet fuer nicht-attached RunContexts
"""

from __future__ import annotations

import json

import pytest

from qualdatan_core.app_db import (
    create_project,
    get_project_by_name,
    get_run,
    list_run_facets,
    list_run_materials,
    list_runs,
    open_app_db,
)
from qualdatan_core.run_context import RunContext, RunStatus, create_run


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
@pytest.fixture()
def app_db():
    """Frische In-Memory App-DB fuer jeden Test, sauber geschlossen."""
    db = open_app_db(":memory:")
    try:
        yield db
    finally:
        try:
            db.close()
        except Exception:
            pass


@pytest.fixture()
def run_dir(tmp_path):
    d = tmp_path / "run-2026-04-13"
    d.mkdir()
    return d


# ---------------------------------------------------------------------------
# TestAttachToAppDB
# ---------------------------------------------------------------------------
class TestAttachToAppDB:
    def test_attach_on_fresh_db_creates_project_and_run(self, app_db, run_dir):
        ctx = RunContext(run_dir)
        run_id = ctx.attach_to_app_db(app_db, "demo", description="Demo-Proj")

        assert isinstance(run_id, int)
        assert ctx.app_run_id == run_id
        assert ctx.app_db is app_db

        project = get_project_by_name(app_db, "demo")
        assert project is not None
        assert ctx.app_project_id == project.id
        assert project.description == "Demo-Proj"

        run = get_run(app_db, run_id)
        assert run is not None
        assert run.run_dir == str(run_dir)
        assert run.project_id == project.id

    def test_existing_project_is_reused(self, app_db, run_dir, tmp_path):
        existing = create_project(app_db, name="reuse", description="old")
        ctx = RunContext(run_dir)
        ctx.attach_to_app_db(app_db, "reuse")

        assert ctx.app_project_id == existing.id
        # 2. Run in anderem Verzeichnis -> gleicher project_id
        run_dir2 = tmp_path / "second-run"
        run_dir2.mkdir()
        ctx2 = RunContext(run_dir2)
        ctx2.attach_to_app_db(app_db, "reuse")
        assert ctx2.app_project_id == existing.id
        assert ctx.app_run_id != ctx2.app_run_id

    def test_attach_is_idempotent(self, app_db, run_dir):
        ctx = RunContext(run_dir)
        first = ctx.attach_to_app_db(app_db, "idem")
        second = ctx.attach_to_app_db(app_db, "idem")
        assert first == second
        assert ctx.app_run_id == first

        project = get_project_by_name(app_db, "idem")
        runs = list_runs(app_db, project_id=project.id)
        assert len(runs) == 1

    def test_config_is_stored_as_json(self, app_db, run_dir):
        ctx = RunContext(run_dir)
        cfg = {"recipe": "mayring", "nested": {"k": 1}}
        run_id = ctx.attach_to_app_db(app_db, "cfgproj", config=cfg)
        run = get_run(app_db, run_id)
        assert run is not None
        assert json.loads(run.config_json) == cfg


# ---------------------------------------------------------------------------
# TestRegister
# ---------------------------------------------------------------------------
class TestRegister:
    def test_register_material_without_attach_returns_none(self, run_dir):
        ctx = RunContext(run_dir)
        result = ctx.register_material("transcript", "/tmp/a.txt")
        assert result is None

    def test_register_material_with_attach(self, app_db, run_dir):
        ctx = RunContext(run_dir)
        ctx.attach_to_app_db(app_db, "matproj")
        mid = ctx.register_material(
            "transcript",
            "/abs/path.txt",
            relative_path="sub/path.txt",
            source_label="HKS",
        )
        assert isinstance(mid, int)
        materials = list_run_materials(app_db, ctx.app_run_id)
        assert len(materials) == 1
        m = materials[0]
        assert m.material_kind == "transcript"
        assert m.path == "/abs/path.txt"
        assert m.relative_path == "sub/path.txt"
        assert m.source_label == "HKS"

    def test_register_facet_without_attach_returns_none(self, run_dir):
        ctx = RunContext(run_dir)
        assert ctx.register_facet("facet.x") is None

    def test_register_facet_stores_params_json(self, app_db, run_dir):
        ctx = RunContext(run_dir)
        ctx.attach_to_app_db(app_db, "facetproj")
        fid = ctx.register_facet("facet.code", bundle_id="bundle.core", params={"k": 1})
        assert isinstance(fid, int)
        facets = list_run_facets(app_db, ctx.app_run_id)
        assert len(facets) == 1
        f = facets[0]
        assert f.facet_id == "facet.code"
        assert f.bundle_id == "bundle.core"
        assert "k" in f.params_json
        assert json.loads(f.params_json) == {"k": 1}


# ---------------------------------------------------------------------------
# TestStatusMirror
# ---------------------------------------------------------------------------
class TestStatusMirror:
    def test_init_state_mirrors_running(self, app_db, run_dir):
        ctx = RunContext(run_dir)
        ctx.attach_to_app_db(app_db, "statproj")
        ctx.init_state(recipe_id="r1", transcripts=["a.txt"])

        run = get_run(app_db, ctx.app_run_id)
        assert run.status == "running"

    def test_mark_completed_mirrors_completed(self, app_db, run_dir):
        ctx = RunContext(run_dir)
        ctx.attach_to_app_db(app_db, "compproj")
        ctx.init_state(recipe_id="r1")
        ctx.mark_completed()

        run = get_run(app_db, ctx.app_run_id)
        assert run.status == "completed"
        assert run.finished_at is not None and run.finished_at != ""

    def test_mark_failed_mirrors_failed_and_interrupts_pipeline(self, app_db, run_dir):
        ctx = RunContext(run_dir)
        ctx.attach_to_app_db(app_db, "failproj")
        ctx.init_state(recipe_id="r1")
        ctx.mark_failed(reason="boom")

        run = get_run(app_db, ctx.app_run_id)
        assert run.status == "failed"
        assert ctx.db.get_state("status") == RunStatus.INTERRUPTED
        assert ctx.db.get_state("error_reason") == "boom"

    def test_mark_completed_is_best_effort_when_db_closed(self, app_db, run_dir):
        ctx = RunContext(run_dir)
        ctx.attach_to_app_db(app_db, "besteffort")
        ctx.init_state(recipe_id="r1")

        # App-DB schliessen -> Mirror muss fehlertolerant sein
        app_db.close()

        # Darf nicht crashen
        ctx.mark_completed()

        # Pipeline.db-Flow blieb intakt
        assert ctx.db.get_state("status") == RunStatus.COMPLETED
        assert ctx.db.get_state("finished_at")

    def test_mark_failed_is_best_effort_when_db_closed(self, app_db, run_dir):
        ctx = RunContext(run_dir)
        ctx.attach_to_app_db(app_db, "besteffort2")
        ctx.init_state(recipe_id="r1")
        app_db.close()

        ctx.mark_failed(reason="shutdown")
        assert ctx.db.get_state("status") == RunStatus.INTERRUPTED
        assert ctx.db.get_state("error_reason") == "shutdown"


# ---------------------------------------------------------------------------
# TestCreateRunWithAppDB
# ---------------------------------------------------------------------------
class TestCreateRunWithAppDB:
    def test_create_run_without_params_keeps_legacy_behavior(self, monkeypatch, tmp_path):
        monkeypatch.setattr("qualdatan_core.run_context.OUTPUT_ROOT", tmp_path)
        ctx = create_run()
        assert ctx.app_db is None
        assert ctx.app_project_id is None
        assert ctx.app_run_id is None
        assert ctx.run_dir.parent == tmp_path

    def test_create_run_with_app_db_attaches(self, monkeypatch, tmp_path, app_db):
        monkeypatch.setattr("qualdatan_core.run_context.OUTPUT_ROOT", tmp_path)
        ctx = create_run(
            app_db=app_db,
            project_name="demo",
            preset_id="bundle.core",
            config={"foo": "bar"},
        )
        assert ctx.app_db is app_db
        assert ctx.app_run_id is not None

        project = get_project_by_name(app_db, "demo")
        assert project is not None
        assert project.preset_id == "bundle.core"
        run = get_run(app_db, ctx.app_run_id)
        assert run.run_dir == str(ctx.run_dir)
        assert json.loads(run.config_json) == {"foo": "bar"}


# ---------------------------------------------------------------------------
# TestBackwardsCompat
# ---------------------------------------------------------------------------
class TestBackwardsCompat:
    def test_unattached_context_has_none_fields(self, run_dir):
        ctx = RunContext(run_dir)
        assert ctx.app_db is None
        assert ctx.app_project_id is None
        assert ctx.app_run_id is None

    def test_init_state_without_app_db_works(self, run_dir):
        ctx = RunContext(run_dir)
        ctx.init_state(
            recipe_id="r",
            codebase_name="cb",
            transcripts=["a.txt"],
            mode="interviews",
        )
        assert ctx.db.get_state("status") == RunStatus.RUNNING
        assert ctx.db.get_state("recipe_id") == "r"
        assert ctx.db.get_state("transcripts") == ["a.txt"]

    def test_mark_completed_without_app_db_works(self, run_dir):
        ctx = RunContext(run_dir)
        ctx.init_state(recipe_id="r")
        ctx.mark_completed()
        assert ctx.db.get_state("status") == RunStatus.COMPLETED
        assert ctx.db.get_state("finished_at")

    def test_mark_failed_without_app_db_works(self, run_dir):
        ctx = RunContext(run_dir)
        ctx.init_state(recipe_id="r")
        ctx.mark_failed(reason="x")
        assert ctx.db.get_state("status") == RunStatus.INTERRUPTED
        assert ctx.db.get_state("error_reason") == "x"
