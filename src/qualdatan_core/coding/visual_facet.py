# SPDX-License-Identifier: AGPL-3.0-only
"""Visuelle Facet-Typen fuer bildbasiertes Material.

Diese Typen sind die bildorientierten Zwillinge von :class:`TaxonomyFacet`
und :class:`EvidenceFacet`: gleiche Codebook-Logik, aber der erzeugte
Prompt adressiert ein **Vision-Modell** (Claude mit Image-Input,
OpenAI GPT-4o-Vision, etc.), nicht einen reinen Text-Prompt.

Auf der Parser-Seite wird zusaetzlich eine ``bbox`` pro Kodierung
akzeptiert (relative Koordinaten in Seitenprozent) — die landet im
``CodedSegment`` als char_start/char_end-Surrogat derzeit nicht weiter,
ist aber fuer zukuenftige QDPX-Selections (PDFSelection) vorbereitet.

Registrierung bei ``builtin_facet_types`` als ``visual_taxonomy`` und
``visual_evidence`` (siehe :mod:`qualdatan_core.facets.types`).
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from ..facets.base import CodeContribution, FacetContext, Material
from ..facets.types import (
    _normalise_codes,
    _normalise_kinds,
    _parse_segments_json,
)
from ..models import CodedSegment

_VISUAL_TAXONOMY_PROMPT = (
    "Analysiere das beiliegende Bild und identifiziere sichtbare Elemente "
    "aus der Taxonomie '{label}'.\n"
    "Erlaubte Codes: {codes_csv}\n\n"
    "Antworte als JSON-Liste von Objekten "
    '{{"code_id": "…", "bbox": [x, y, w, h], "confidence": 0..1, '
    '"rationale": "…"}}. '
    "Die Bounding-Box ist optional, Koordinaten in Seitenprozent (0..1)."
)


_VISUAL_EVIDENCE_PROMPT = (
    "Bewerte das beiliegende Bild auf der Skala '{label}' (niedrig -> hoch).\n"
    "Stufen: {codes_list}\n\n"
    "Antworte als JSON-Objekt "
    '{{"code_id": "…", "justification": "…", "confidence": 0..1}}.'
)


@dataclass
class VisualTaxonomyFacet:
    """Taxonomie-Klassifikation fuer Bildmaterial (Plaene, Fotos)."""

    id: str
    label: str
    input_kinds: tuple[Material, ...]
    codebook_contribution: tuple[CodeContribution, ...]
    description: str = ""
    prompt_template: str = _VISUAL_TAXONOMY_PROMPT

    @classmethod
    def from_yaml(cls, data: Mapping[str, Any]) -> VisualTaxonomyFacet:
        return cls(
            id=data["id"],
            label=data.get("label", data["id"]),
            description=data.get("description", ""),
            input_kinds=_normalise_kinds(data.get("input_kinds", ["pdf_visual", "image"])),
            codebook_contribution=_normalise_codes(data.get("codes", [])),
            prompt_template=data.get("prompt_template", _VISUAL_TAXONOMY_PROMPT),
        )

    def build_prompt(self, ctx: FacetContext) -> str:
        return self.prompt_template.format(
            label=self.label,
            codes_csv=", ".join(c.id for c in self.codebook_contribution),
        )

    def parse_response(self, raw: str | Mapping[str, Any], ctx: FacetContext) -> list[CodedSegment]:
        return _parse_segments_json(raw, ctx, self.codebook_contribution)


@dataclass
class VisualEvidenceFacet:
    """Ordinale Evidenz-Skala fuer Bildmaterial (z.B. LOG-01..05)."""

    id: str
    label: str
    input_kinds: tuple[Material, ...]
    codebook_contribution: tuple[CodeContribution, ...]  # Reihenfolge = Skala
    description: str = ""
    prompt_template: str = _VISUAL_EVIDENCE_PROMPT

    @classmethod
    def from_yaml(cls, data: Mapping[str, Any]) -> VisualEvidenceFacet:
        return cls(
            id=data["id"],
            label=data.get("label", data["id"]),
            description=data.get("description", ""),
            input_kinds=_normalise_kinds(data.get("input_kinds", ["pdf_visual", "image"])),
            codebook_contribution=_normalise_codes(data.get("scale", data.get("codes", []))),
            prompt_template=data.get("prompt_template", _VISUAL_EVIDENCE_PROMPT),
        )

    def build_prompt(self, ctx: FacetContext) -> str:
        return self.prompt_template.format(
            label=self.label,
            codes_list=" -> ".join(c.id for c in self.codebook_contribution),
        )

    def parse_response(self, raw: str | Mapping[str, Any], ctx: FacetContext) -> list[CodedSegment]:
        return _parse_segments_json(raw, ctx, self.codebook_contribution)


__all__ = [
    "VisualTaxonomyFacet",
    "VisualEvidenceFacet",
]
