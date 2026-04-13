# SPDX-License-Identifier: AGPL-3.0-only
"""Tests fuer Codebook-Overrides DAO (App-DB Phase E)."""

from __future__ import annotations

import pytest

from qualdatan_core.app_db import open_app_db
from qualdatan_core.app_db.codebook import (
    CodebookEntry,
    get_codebook_entry,
    list_codebook_entries,
    reset_codebook_entry,
    upsert_codebook_entry,
)
from qualdatan_core.app_db.projects import create_project


@pytest.fixture
def db():
    handle = open_app_db(":memory:")
    try:
        yield handle
    finally:
        handle.close()


@pytest.fixture
def project(db):
    return create_project(db, name="proj-a")


@pytest.fixture
def project_b(db):
    return create_project(db, name="proj-b")


class TestUpsert:
    def test_upsert_insert_creates_entry(self, db, project):
        entry = upsert_codebook_entry(
            db, project.id, "A-01",
            label_override="Label A",
            color_override="#112233",
            definition_override="Definition A",
            examples_override=["ex1", "ex2"],
        )
        assert isinstance(entry, CodebookEntry)
        assert entry.project_id == project.id
        assert entry.code_id == "A-01"
        assert entry.label_override == "Label A"
        assert entry.color_override == "#112233"
        assert entry.definition_override == "Definition A"
        assert entry.examples_override == ["ex1", "ex2"]

    def test_upsert_update_overwrites_fields(self, db, project):
        upsert_codebook_entry(
            db, project.id, "A-01",
            label_override="Old",
            color_override="#000000",
        )
        entry = upsert_codebook_entry(
            db, project.id, "A-01",
            label_override="New",
            color_override="#FFFFFF",
        )
        assert entry.label_override == "New"
        assert entry.color_override == "#FFFFFF"
        # Nur ein Eintrag pro (project,code)
        rows = list_codebook_entries(db, project.id)
        assert len(rows) == 1

    def test_partial_update_preserves_untouched_fields(self, db, project):
        upsert_codebook_entry(
            db, project.id, "A-01",
            label_override="Label",
            color_override="#ABCDEF",
            definition_override="Def",
            examples_override=["a", "b"],
        )
        # Nur label_override updaten:
        entry = upsert_codebook_entry(
            db, project.id, "A-01", label_override="New Label"
        )
        assert entry.label_override == "New Label"
        assert entry.color_override == "#ABCDEF"
        assert entry.definition_override == "Def"
        assert entry.examples_override == ["a", "b"]

    def test_upsert_explicit_none_clears_field(self, db, project):
        upsert_codebook_entry(
            db, project.id, "A-01",
            label_override="Label",
            color_override="#ABCDEF",
        )
        entry = upsert_codebook_entry(
            db, project.id, "A-01", color_override=None
        )
        assert entry.color_override is None
        assert entry.label_override == "Label"


class TestGet:
    def test_get_returns_none_when_missing(self, db, project):
        assert get_codebook_entry(db, project.id, "X-99") is None

    def test_examples_roundtrip(self, db, project):
        upsert_codebook_entry(
            db, project.id, "A-01",
            examples_override=["eins", "zwei", "drei"],
        )
        entry = get_codebook_entry(db, project.id, "A-01")
        assert entry is not None
        assert entry.examples_override == ["eins", "zwei", "drei"]

    def test_examples_none_when_not_set(self, db, project):
        upsert_codebook_entry(
            db, project.id, "A-01", label_override="Nur Label"
        )
        entry = get_codebook_entry(db, project.id, "A-01")
        assert entry is not None
        assert entry.examples_override is None


class TestList:
    def test_list_filters_by_project(self, db, project, project_b):
        upsert_codebook_entry(db, project.id, "A-01", label_override="a1")
        upsert_codebook_entry(db, project.id, "A-02", label_override="a2")
        upsert_codebook_entry(db, project_b.id, "B-01", label_override="b1")
        rows_a = list_codebook_entries(db, project.id)
        rows_b = list_codebook_entries(db, project_b.id)
        assert {r.code_id for r in rows_a} == {"A-01", "A-02"}
        assert {r.code_id for r in rows_b} == {"B-01"}


class TestReset:
    def test_reset_returns_false_if_absent(self, db, project):
        assert reset_codebook_entry(db, project.id, "X-99") is False

    def test_reset_returns_true_and_deletes(self, db, project):
        upsert_codebook_entry(db, project.id, "A-01", label_override="x")
        assert reset_codebook_entry(db, project.id, "A-01") is True
        assert get_codebook_entry(db, project.id, "A-01") is None
        # Idempotent: zweiter Reset false
        assert reset_codebook_entry(db, project.id, "A-01") is False
