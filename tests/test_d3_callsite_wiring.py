# SPDX-License-Identifier: AGPL-3.0-only
"""Tests fuer Phase D.3: Callsite-Wiring der App-DB-Materials.

Stellt sicher, dass die drei Haupt-Callsites (Interview-Analyse, PDF-Coder,
Curation-Bootstrap) ihre Materials in die App-DB spiegeln, wenn der
RunContext an eine App-DB angebunden ist — und ohne Attach bit-identisch
mit dem Legacy-Verhalten bleiben (kein Crash, keine Writes).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from qualdatan_core.app_db import list_run_materials, open_app_db
from qualdatan_core.curation.bootstrap import _mirror_samples
from qualdatan_core.pdf_coder import _mirror_pdfs
from qualdatan_core.run_context import RunContext
from qualdatan_core.steps.step1_analyze import _mirror_transcripts


@pytest.fixture()
def app_db():
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
    d = tmp_path / "d3-run"
    d.mkdir()
    return d


@pytest.fixture()
def attached_ctx(app_db, run_dir):
    ctx = RunContext(run_dir)
    ctx.attach_to_app_db(app_db, "d3-proj")
    return ctx


# ---------------------------------------------------------------------------
# step1_analyze._mirror_transcripts
# ---------------------------------------------------------------------------
class TestMirrorTranscripts:
    def test_registers_transcripts_into_app_db(self, attached_ctx, app_db):
        attached_ctx.init_state(transcripts=["a.docx", "b.docx"])
        n = _mirror_transcripts(attached_ctx)
        assert n == 2

        materials = list_run_materials(app_db, attached_ctx.app_run_id)
        kinds = {m.material_kind for m in materials}
        paths = {m.path for m in materials}
        assert kinds == {"transcript"}
        assert paths == {"a.docx", "b.docx"}

    def test_empty_state_is_noop(self, attached_ctx, app_db):
        attached_ctx.init_state(transcripts=[])
        assert _mirror_transcripts(attached_ctx) == 0
        assert list_run_materials(app_db, attached_ctx.app_run_id) == []

    def test_unattached_is_noop_and_no_crash(self, run_dir):
        ctx = RunContext(run_dir)
        ctx.init_state(transcripts=["x.docx"])
        # Darf nicht crashen und keine Materials schreiben.
        assert _mirror_transcripts(ctx) == 0


# ---------------------------------------------------------------------------
# pdf_coder._mirror_pdfs
# ---------------------------------------------------------------------------
class TestMirrorPdfs:
    def test_registers_pdfs_with_relative_and_source(self, attached_ctx, app_db):
        pdfs = [
            {
                "path": "/abs/HKS/BOE/plan.pdf",
                "relative_path": "BOE/plan.pdf",
                "project": "BOE",
                "filename": "plan.pdf",
            },
            {
                "path": "/abs/HKS/BOE/spec.pdf",
                "relative_path": "BOE/spec.pdf",
                "project": "BOE",
                "filename": "spec.pdf",
            },
        ]
        n = _mirror_pdfs(pdfs, attached_ctx)
        assert n == 2

        materials = list_run_materials(app_db, attached_ctx.app_run_id)
        assert len(materials) == 2
        m0 = next(m for m in materials if m.path.endswith("plan.pdf"))
        assert m0.material_kind == "pdf_text"
        assert m0.relative_path == "BOE/plan.pdf"
        assert m0.source_label == "BOE"

    def test_unattached_is_noop(self, run_dir):
        ctx = RunContext(run_dir)
        pdfs = [{"path": "/x.pdf", "relative_path": "x.pdf", "project": "P", "filename": "x.pdf"}]
        assert _mirror_pdfs(pdfs, ctx) == 0


# ---------------------------------------------------------------------------
# curation/bootstrap._mirror_samples
# ---------------------------------------------------------------------------
class TestMirrorSamples:
    def test_registers_samples(self, attached_ctx, app_db, tmp_path):
        f1 = tmp_path / "sample1.docx"
        f2 = tmp_path / "sub" / "sample2.docx"
        f2.parent.mkdir()
        f1.write_text("x")
        f2.write_text("y")

        n = _mirror_samples(attached_ctx, [f1, f2])
        assert n == 2

        materials = list_run_materials(app_db, attached_ctx.app_run_id)
        assert {m.material_kind for m in materials} == {"transcript_sample"}
        labels = {m.source_label for m in materials}
        assert labels == {"sample1.docx", "sample2.docx"}
        paths = {m.path for m in materials}
        assert str(f1) in paths and str(f2) in paths

    def test_unattached_is_noop(self, run_dir, tmp_path):
        ctx = RunContext(run_dir)
        f1 = tmp_path / "s.docx"
        f1.write_text("x")
        assert _mirror_samples(ctx, [f1]) == 0
