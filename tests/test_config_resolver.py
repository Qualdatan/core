# SPDX-License-Identifier: AGPL-3.0-only
"""Tests fuer den 3-Ebenen-Resolver (Phase E Config-Resolver)."""

from __future__ import annotations

import pytest

from qualdatan_core.app_db import open_app_db
from qualdatan_core.app_db.codebook import upsert_codebook_entry
from qualdatan_core.app_db.projects import create_project
from qualdatan_core.coding.colors import CodeColorMap
from qualdatan_core.config_resolver import (
    resolve_color,
    resolve_definition,
    resolve_examples,
    resolve_label,
)


@pytest.fixture
def db():
    handle = open_app_db(":memory:")
    try:
        yield handle
    finally:
        handle.close()


@pytest.fixture
def project(db):
    return create_project(db, name="proj-r")


# ---------------------------------------------------------------------------
# resolve_color
# ---------------------------------------------------------------------------
class TestResolveColor:
    def test_run_config_wins(self, db, project):
        upsert_codebook_entry(
            db, project.id, "A-01", color_override="#AAAAAA"
        )
        run_config = {"codes": {"A-01": {"color": "#111111"}}}
        result = resolve_color(
            "A-01",
            project_id=project.id,
            run_config=run_config,
            app_db=db,
            bundle_default="#222222",
        )
        assert result == "#111111"

    def test_db_override_wins_over_bundle(self, db, project):
        upsert_codebook_entry(
            db, project.id, "A-01", color_override="#AAAAAA"
        )
        result = resolve_color(
            "A-01",
            project_id=project.id,
            app_db=db,
            bundle_default="#222222",
        )
        assert result == "#AAAAAA"

    def test_bundle_default_when_no_override(self, db, project):
        result = resolve_color(
            "A-01",
            project_id=project.id,
            app_db=db,
            bundle_default="#222222",
        )
        assert result == "#222222"

    def test_generator_fallback_when_nothing_set(self):
        result = resolve_color("A-01")
        expected = CodeColorMap(codes=["A-01"]).get_hex("A-01")
        assert result == expected
        assert result.startswith("#") and len(result) == 7


# ---------------------------------------------------------------------------
# resolve_label
# ---------------------------------------------------------------------------
class TestResolveLabel:
    def test_run_config_wins(self, db, project):
        upsert_codebook_entry(
            db, project.id, "A-01", label_override="DB Label"
        )
        run_config = {"codes": {"A-01": {"label": "Run Label"}}}
        result = resolve_label(
            "A-01",
            project_id=project.id,
            run_config=run_config,
            app_db=db,
            bundle_default="Bundle Label",
        )
        assert result == "Run Label"

    def test_db_override_wins_over_bundle(self, db, project):
        upsert_codebook_entry(
            db, project.id, "A-01", label_override="DB Label"
        )
        result = resolve_label(
            "A-01",
            project_id=project.id,
            app_db=db,
            bundle_default="Bundle Label",
        )
        assert result == "DB Label"

    def test_bundle_default_when_no_override(self, db, project):
        result = resolve_label(
            "A-01",
            project_id=project.id,
            app_db=db,
            bundle_default="Bundle Label",
        )
        assert result == "Bundle Label"

    def test_empty_fallback_when_nothing_set(self):
        assert resolve_label("A-01") == ""


# ---------------------------------------------------------------------------
# resolve_definition
# ---------------------------------------------------------------------------
class TestResolveDefinition:
    def test_run_config_wins(self, db, project):
        upsert_codebook_entry(
            db, project.id, "A-01", definition_override="DB Def"
        )
        run_config = {"codes": {"A-01": {"definition": "Run Def"}}}
        result = resolve_definition(
            "A-01",
            project_id=project.id,
            run_config=run_config,
            app_db=db,
            bundle_default="Bundle Def",
        )
        assert result == "Run Def"

    def test_db_override_wins_over_bundle(self, db, project):
        upsert_codebook_entry(
            db, project.id, "A-01", definition_override="DB Def"
        )
        result = resolve_definition(
            "A-01",
            project_id=project.id,
            app_db=db,
            bundle_default="Bundle Def",
        )
        assert result == "DB Def"

    def test_bundle_default_when_no_override(self, db, project):
        result = resolve_definition(
            "A-01",
            project_id=project.id,
            app_db=db,
            bundle_default="Bundle Def",
        )
        assert result == "Bundle Def"

    def test_empty_fallback_when_nothing_set(self):
        assert resolve_definition("A-01") == ""


# ---------------------------------------------------------------------------
# resolve_examples
# ---------------------------------------------------------------------------
class TestResolveExamples:
    def test_run_config_wins(self, db, project):
        upsert_codebook_entry(
            db, project.id, "A-01", examples_override=["db1", "db2"]
        )
        run_config = {"codes": {"A-01": {"examples": ["run1"]}}}
        result = resolve_examples(
            "A-01",
            project_id=project.id,
            run_config=run_config,
            app_db=db,
            bundle_default=["bundle1"],
        )
        assert result == ["run1"]

    def test_db_override_wins_over_bundle(self, db, project):
        upsert_codebook_entry(
            db, project.id, "A-01", examples_override=["db1", "db2"]
        )
        result = resolve_examples(
            "A-01",
            project_id=project.id,
            app_db=db,
            bundle_default=["bundle1"],
        )
        assert result == ["db1", "db2"]

    def test_bundle_default_when_no_override(self, db, project):
        result = resolve_examples(
            "A-01",
            project_id=project.id,
            app_db=db,
            bundle_default=["bundle1", "bundle2"],
        )
        assert result == ["bundle1", "bundle2"]

    def test_empty_fallback_when_nothing_set(self):
        assert resolve_examples("A-01") == []


# ---------------------------------------------------------------------------
# Skip-Logic
# ---------------------------------------------------------------------------
class TestSkipLogic:
    def test_project_id_none_skips_db_lookup(self, db, project):
        # DB has override — but project_id=None must skip it.
        upsert_codebook_entry(
            db, project.id, "A-01", label_override="DB Label"
        )
        result = resolve_label(
            "A-01",
            project_id=None,
            app_db=db,
            bundle_default="Bundle Label",
        )
        assert result == "Bundle Label"

    def test_app_db_none_skips_db_lookup(self, db, project):
        upsert_codebook_entry(
            db, project.id, "A-01", label_override="DB Label"
        )
        result = resolve_label(
            "A-01",
            project_id=project.id,
            app_db=None,
            bundle_default="Bundle Label",
        )
        assert result == "Bundle Label"

    def test_run_config_none_skips_run_lookup(self, db, project):
        upsert_codebook_entry(
            db, project.id, "A-01", label_override="DB Label"
        )
        result = resolve_label(
            "A-01",
            project_id=project.id,
            run_config=None,
            app_db=db,
            bundle_default="Bundle Label",
        )
        assert result == "DB Label"
