# SPDX-License-Identifier: AGPL-3.0-only
"""Tests for the qualdatan_core.facets foundation (Phase B.1)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from qualdatan_core.facets import (
    ActorRoleFacet,
    CodeContribution,
    EvidenceFacet,
    Facet,
    FacetContext,
    FacetLoadError,
    FreeCodingFacet,
    Material,
    ProcessStepFacet,
    TaxonomyFacet,
    clear_registry,
    discovered_facet_types,
    get_facet,
    list_facets,
    load_facet_from_dict,
    load_facet_from_yaml,
    load_facets_from_dir,
    register_facet,
    unregister_facet,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
@pytest.fixture(autouse=True)
def _clean_registry():
    clear_registry()
    yield
    clear_registry()


def _ifc_facet() -> TaxonomyFacet:
    return TaxonomyFacet(
        id="ifc-elements",
        label="IFC-Bauelemente",
        input_kinds=(Material.PDF_VISUAL,),
        codebook_contribution=(
            CodeContribution(id="IFC-WALL", label="Wand"),
            CodeContribution(id="IFC-SLAB", label="Decke"),
        ),
    )


def _log_facet() -> EvidenceFacet:
    return EvidenceFacet(
        id="log-evidence",
        label="LOG-Evidenz",
        input_kinds=(Material.PDF_VISUAL,),
        codebook_contribution=(
            CodeContribution(id="LOG-01", label="ungenau"),
            CodeContribution(id="LOG-02", label="schematisch"),
            CodeContribution(id="LOG-03", label="parametrisch"),
        ),
    )


# ---------------------------------------------------------------------------
# Protocol conformance + Registry
# ---------------------------------------------------------------------------
class TestProtocolConformance:
    def test_taxonomy_facet_satisfies_protocol(self):
        assert isinstance(_ifc_facet(), Facet)

    def test_all_builtin_types_satisfy_protocol(self):
        instances = [
            _ifc_facet(),
            _log_facet(),
            ActorRoleFacet(
                id="roles",
                label="Rollen",
                input_kinds=(Material.TEXT,),
                codebook_contribution=(CodeContribution("ARCH", "Architekt"),),
            ),
            ProcessStepFacet(
                id="steps",
                label="Schritte",
                input_kinds=(Material.TEXT,),
                codebook_contribution=(CodeContribution("PLAN", "Planung"),),
            ),
            FreeCodingFacet(id="open", label="Open Coding", input_kinds=(Material.TEXT,)),
        ]
        for f in instances:
            assert isinstance(f, Facet), f"{type(f).__name__} verletzt Facet-Protocol"


class TestRegistry:
    def test_register_and_get(self):
        f = _ifc_facet()
        register_facet(f)
        assert get_facet("ifc-elements") is f

    def test_register_duplicate_raises(self):
        register_facet(_ifc_facet())
        with pytest.raises(ValueError, match="already registered"):
            register_facet(_ifc_facet())

    def test_register_non_facet_raises(self):
        with pytest.raises(TypeError):
            register_facet(object())  # type: ignore[arg-type]

    def test_list_facets_is_sorted(self):
        register_facet(_log_facet())
        register_facet(_ifc_facet())
        assert [f.id for f in list_facets()] == ["ifc-elements", "log-evidence"]

    def test_unregister_is_idempotent(self):
        register_facet(_ifc_facet())
        unregister_facet("ifc-elements")
        unregister_facet("ifc-elements")  # second call no-op

    def test_discovered_types_includes_builtins(self):
        types = discovered_facet_types()
        for name in ("taxonomy", "evidence", "actor_role", "process_step", "free_coding"):
            assert name in types


# ---------------------------------------------------------------------------
# Prompt building
# ---------------------------------------------------------------------------
class TestBuildPrompt:
    def test_taxonomy_includes_codes_and_material(self):
        f = _ifc_facet()
        ctx = FacetContext(material="Floor plan with walls.", material_kind=Material.TEXT)
        prompt = f.build_prompt(ctx)
        assert "IFC-WALL" in prompt
        assert "IFC-SLAB" in prompt
        assert "Floor plan with walls." in prompt
        assert f.label in prompt

    def test_evidence_orders_scale(self):
        f = _log_facet()
        ctx = FacetContext(material="...", material_kind=Material.TEXT)
        prompt = f.build_prompt(ctx)
        # Skala muss aufsteigend sortiert dargestellt werden
        assert prompt.index("LOG-01") < prompt.index("LOG-02") < prompt.index("LOG-03")

    def test_free_coding_handles_no_seed_codes(self):
        f = FreeCodingFacet(id="open", label="Open", input_kinds=(Material.TEXT,))
        prompt = f.build_prompt(FacetContext(material="x", material_kind=Material.TEXT))
        assert "(keine)" in prompt


# ---------------------------------------------------------------------------
# Response parsing
# ---------------------------------------------------------------------------
class TestParseResponse:
    def test_taxonomy_parses_strict(self):
        f = _ifc_facet()
        ctx = FacetContext(material="...", material_kind=Material.TEXT, source_label="page-1")
        raw = json.dumps(
            [
                {"code_id": "IFC-WALL", "text": "Wand 1", "char_start": 0, "char_end": 6},
                {"code_id": "IFC-SLAB", "text": "Decke", "char_start": 10, "char_end": 15},
            ]
        )
        out = f.parse_response(raw, ctx)
        assert [s.code_id for s in out] == ["IFC-WALL", "IFC-SLAB"]
        assert out[0].document == "page-1"
        assert out[0].code_name == "Wand"

    def test_taxonomy_rejects_unknown_code(self):
        f = _ifc_facet()
        ctx = FacetContext(material="...", material_kind=Material.TEXT)
        bad = json.dumps([{"code_id": "IFC-FOO", "text": "x", "char_start": 0, "char_end": 1}])
        with pytest.raises(ValueError, match="unbekannte code_id"):
            f.parse_response(bad, ctx)

    def test_free_coding_accepts_unknown_code(self):
        f = FreeCodingFacet(id="open", label="Open", input_kinds=(Material.TEXT,))
        ctx = FacetContext(material="...", material_kind=Material.TEXT)
        raw = json.dumps(
            [
                {
                    "code_id": "EMERGENT-1",
                    "code_label": "Emerging Concept",
                    "text": "x",
                    "char_start": 0,
                    "char_end": 1,
                }
            ]
        )
        out = f.parse_response(raw, ctx)
        assert out[0].code_id == "EMERGENT-1"
        assert out[0].code_name == "Emerging Concept"

    def test_handles_json_in_code_fence(self):
        f = _ifc_facet()
        ctx = FacetContext(material="...", material_kind=Material.TEXT)
        raw = '```json\n[{"code_id": "IFC-WALL", "text": "x", "char_start": 0, "char_end": 1}]\n```'
        out = f.parse_response(raw, ctx)
        assert out[0].code_id == "IFC-WALL"


# ---------------------------------------------------------------------------
# YAML loader
# ---------------------------------------------------------------------------
class TestYamlLoader:
    def test_load_taxonomy_from_dict(self):
        data = {
            "id": "ifc-elements",
            "type": "taxonomy",
            "label": "IFC",
            "input_kinds": ["pdf_visual"],
            "codes": [
                {"id": "IFC-WALL", "label": "Wand"},
                "IFC-SLAB",
            ],
        }
        f = load_facet_from_dict(data)
        assert isinstance(f, TaxonomyFacet)
        assert f.id == "ifc-elements"
        assert [c.id for c in f.codebook_contribution] == ["IFC-WALL", "IFC-SLAB"]
        assert f.codebook_contribution[1].label == "IFC-SLAB"  # default label = id

    def test_load_evidence_uses_scale_key(self):
        data = {
            "id": "log",
            "type": "evidence",
            "label": "LOG",
            "scale": ["LOG-01", "LOG-02", "LOG-03"],
        }
        f = load_facet_from_dict(data)
        assert isinstance(f, EvidenceFacet)
        assert [c.id for c in f.codebook_contribution] == ["LOG-01", "LOG-02", "LOG-03"]

    def test_unknown_type_raises(self):
        with pytest.raises(FacetLoadError, match="Unbekannter Facet-Typ"):
            load_facet_from_dict({"id": "x", "type": "no-such-type"})

    def test_missing_id_raises(self):
        with pytest.raises(FacetLoadError, match="fehlt 'id'"):
            load_facet_from_dict({"type": "taxonomy"})

    def test_missing_type_raises(self):
        with pytest.raises(FacetLoadError, match="fehlt 'type'"):
            load_facet_from_dict({"id": "x"})

    def test_load_from_yaml_file(self, tmp_path: Path):
        yml = tmp_path / "ifc.yaml"
        yml.write_text(
            "id: ifc-elements\n"
            "type: taxonomy\n"
            "label: IFC\n"
            "input_kinds: [pdf_visual]\n"
            "codes:\n"
            "  - { id: IFC-WALL, label: Wand }\n",
            encoding="utf-8",
        )
        f = load_facet_from_yaml(yml)
        assert isinstance(f, TaxonomyFacet)
        assert f.codebook_contribution[0].label == "Wand"

    def test_load_from_dir_recursive(self, tmp_path: Path):
        sub = tmp_path / "sub"
        sub.mkdir()
        (sub / "a.yaml").write_text("id: a\ntype: free_coding\nlabel: A\n", encoding="utf-8")
        (tmp_path / "b.yml").write_text("id: b\ntype: free_coding\nlabel: B\n", encoding="utf-8")
        # Non-YAML wird ignoriert
        (tmp_path / "readme.txt").write_text("ignore me", encoding="utf-8")
        out = load_facets_from_dir(tmp_path)
        ids = sorted(f.id for f in out)
        assert ids == ["a", "b"]

    def test_load_from_missing_dir_returns_empty(self, tmp_path: Path):
        assert load_facets_from_dir(tmp_path / "does-not-exist") == []
