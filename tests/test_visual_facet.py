# SPDX-License-Identifier: AGPL-3.0-only
"""Tests for qualdatan_core.coding.visual_facet (Phase B.7).

Spiegelbild von test_facets.py, aber fuer die bildorientierten Varianten
(VisualTaxonomyFacet / VisualEvidenceFacet). Auch der End-to-End-Load aus
dem bim-basic-Bundle wird hier abgedeckt.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from qualdatan_core.coding.visual_facet import (
    VisualEvidenceFacet,
    VisualTaxonomyFacet,
)
from qualdatan_core.facets import (
    CodeContribution,
    Facet,
    FacetContext,
    Material,
    discovered_facet_types,
    load_facet_from_dict,
    load_facets_from_dir,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
def _ifc() -> VisualTaxonomyFacet:
    return VisualTaxonomyFacet(
        id="ifc-elements",
        label="IFC-Bauelemente",
        input_kinds=(Material.PDF_VISUAL, Material.IMAGE),
        codebook_contribution=(
            CodeContribution("IFC-WALL", "Wand (IfcWall)"),
            CodeContribution("IFC-SLAB", "Decke (IfcSlab)"),
            CodeContribution("IFC-COLUMN", "Stuetze (IfcColumn)"),
        ),
    )


def _log() -> VisualEvidenceFacet:
    return VisualEvidenceFacet(
        id="log-evidence",
        label="LOG-Evidenz",
        input_kinds=(Material.PDF_VISUAL, Material.IMAGE),
        codebook_contribution=(
            CodeContribution("LOG-01", "symbolisch"),
            CodeContribution("LOG-02", "vereinfacht"),
            CodeContribution("LOG-03", "detailliert"),
        ),
    )


# ---------------------------------------------------------------------------
# Protocol + Registry
# ---------------------------------------------------------------------------
class TestProtocolConformance:
    def test_visual_taxonomy_satisfies_protocol(self):
        assert isinstance(_ifc(), Facet)

    def test_visual_evidence_satisfies_protocol(self):
        assert isinstance(_log(), Facet)

    def test_discovered_types_include_visual(self):
        types = discovered_facet_types()
        assert "visual_taxonomy" in types
        assert "visual_evidence" in types


# ---------------------------------------------------------------------------
# Prompt building
# ---------------------------------------------------------------------------
class TestBuildPrompt:
    def test_visual_taxonomy_prompt_mentions_codes_and_image(self):
        f = _ifc()
        ctx = FacetContext(
            material=b"(png-bytes)", material_kind=Material.PDF_VISUAL,
            source_label="plan.pdf#page=3",
        )
        prompt = f.build_prompt(ctx)
        assert "IFC-WALL" in prompt
        assert "IFC-SLAB" in prompt
        assert "Bild" in prompt
        assert f.label in prompt

    def test_visual_evidence_prompt_shows_scale(self):
        f = _log()
        ctx = FacetContext(
            material=b"(png-bytes)", material_kind=Material.PDF_VISUAL,
        )
        prompt = f.build_prompt(ctx)
        assert prompt.index("LOG-01") < prompt.index("LOG-02") < prompt.index("LOG-03")


# ---------------------------------------------------------------------------
# Response parsing
# ---------------------------------------------------------------------------
class TestParseResponse:
    def test_visual_taxonomy_parses_strict(self):
        f = _ifc()
        ctx = FacetContext(
            material=b"...", material_kind=Material.PDF_VISUAL,
            source_label="plan.pdf#p3",
        )
        raw = json.dumps([
            {"code_id": "IFC-WALL", "bbox": [0.1, 0.2, 0.3, 0.4], "confidence": 0.9},
            {"code_id": "IFC-COLUMN", "bbox": [0.5, 0.1, 0.05, 0.5], "confidence": 0.7},
        ])
        out = f.parse_response(raw, ctx)
        assert [s.code_id for s in out] == ["IFC-WALL", "IFC-COLUMN"]
        assert out[0].document == "plan.pdf#p3"

    def test_visual_taxonomy_rejects_unknown_code(self):
        f = _ifc()
        ctx = FacetContext(material=b"...", material_kind=Material.PDF_VISUAL)
        bad = json.dumps([{"code_id": "IFC-FOO", "bbox": [0, 0, 1, 1]}])
        with pytest.raises(ValueError, match="unbekannte code_id"):
            f.parse_response(bad, ctx)

    def test_visual_evidence_parses_single_object(self):
        f = _log()
        ctx = FacetContext(material=b"...", material_kind=Material.PDF_VISUAL)
        raw = json.dumps({"code_id": "LOG-02", "justification": "Schichten fehlen"})
        out = f.parse_response(raw, ctx)
        assert len(out) == 1
        assert out[0].code_id == "LOG-02"


# ---------------------------------------------------------------------------
# YAML loader + bim-basic bundle
# ---------------------------------------------------------------------------
class TestYamlLoader:
    def test_load_visual_taxonomy_from_dict(self):
        data = {
            "id": "ifc-elements",
            "type": "visual_taxonomy",
            "label": "IFC",
            "codes": [
                {"id": "IFC-WALL", "label": "Wand"},
                "IFC-SLAB",
            ],
        }
        f = load_facet_from_dict(data)
        assert isinstance(f, VisualTaxonomyFacet)
        assert Material.PDF_VISUAL in f.input_kinds  # default fallback
        assert [c.id for c in f.codebook_contribution] == ["IFC-WALL", "IFC-SLAB"]

    def test_load_visual_evidence_with_scale_key(self):
        data = {
            "id": "log",
            "type": "visual_evidence",
            "label": "LOG",
            "scale": ["LOG-01", "LOG-02", "LOG-03", "LOG-04", "LOG-05"],
        }
        f = load_facet_from_dict(data)
        assert isinstance(f, VisualEvidenceFacet)
        assert len(f.codebook_contribution) == 5

    def test_bim_basic_bundle_loads(self):
        """End-to-end: das mitgelieferte bim-basic-Bundle laedt sauber."""
        # Drei Fallback-Pfade: im uv-Workspace via Submodule (repos/core nach
        # Umbrella), im lokalen Clone des Org-Repos (Umbrella als Geschwister),
        # oder Umbrella-Zielpfad fuer direkte Entwicklung.
        here = Path(__file__).resolve()
        candidates = [
            here.parents[3] / "bundles" / "bim-basic" / "facets",         # Umbrella/repos/core/tests -> Umbrella
            here.parents[4] / "bundles" / "bim-basic" / "facets",         # tiefere Nesting-Variante
            Path("/mnt/d/ai/transcript/bundles/bim-basic/facets"),
        ]
        facets_dir = next((p for p in candidates if p.exists()), None)
        if facets_dir is None:
            pytest.skip(f"bim-basic bundle nicht gefunden (probiert: {candidates})")

        loaded = load_facets_from_dir(facets_dir)
        ids = sorted(f.id for f in loaded)
        assert ids == ["ifc-elements", "log-evidence"]

        ifc = next(f for f in loaded if f.id == "ifc-elements")
        log = next(f for f in loaded if f.id == "log-evidence")
        assert isinstance(ifc, VisualTaxonomyFacet)
        assert isinstance(log, VisualEvidenceFacet)
        assert len(ifc.codebook_contribution) == 15  # 15 IFC-Klassen im Bundle
        assert len(log.codebook_contribution) == 5   # LOG-01..05
