# SPDX-License-Identifier: AGPL-3.0-only
"""Facet protocol — domain-agnostic building block for qualdatan analyses.

Ein Facet ist ein konfigurierbarer Analyse-Baustein: es bekommt Material
(Text / PDF-Text / PDF-Visual / Image), produziert ein Prompt fuer einen
LLM und parsed die Antwort zu einer Liste von Kodierungen.

Facets sind selbst inhaltsfrei — die konkrete Taxonomie / Evidenz-Skala /
Codeliste kommt aus dem Bundle, das den Facet instanziiert. So funktioniert
derselbe Facet-Code fuer "IFC-Bauelemente" wie fuer "ICD-10-Diagnosen"
oder "BPMN-Akteursrollen".
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Mapping, Protocol, runtime_checkable

from ..models import CodedSegment


class Material(str, Enum):
    """Materialart, die ein Facet konsumieren kann."""

    TEXT = "text"             # Roher Text (z.B. Interview-Transkript)
    PDF_TEXT = "pdf_text"     # Aus PDF extrahierter Text + Block-Koordinaten
    PDF_VISUAL = "pdf_visual" # PDF-Seite als Bild (Vision-Modelle)
    IMAGE = "image"           # Standalone Bild


@dataclass(frozen=True)
class CodeContribution:
    """Ein Code, den ein Facet ins Codebook einbringt.

    `id` ist die ID innerhalb des Bundle-Namespaces (z.B. ``IFC-WALL``).
    Die Bundle-ID + Facet-ID praefixiert das Code-Owner-System bei Bedarf.
    """

    id: str
    label: str
    description: str = ""
    color_hint: str | None = None  # optionaler Hex-RGB-Vorschlag


@dataclass
class FacetContext:
    """Kontext, den der Orchestrator beim Aufruf eines Facets liefert."""

    material: Any                                 # Roh-Material (str / dict / bytes)
    material_kind: Material
    source_label: str = ""                        # Anzeigetext (Dateiname, Page-Spec)
    metadata: Mapping[str, Any] = field(default_factory=dict)
    # Modell-/Token-Defaults; ein Facet kann eigene Werte vorschlagen.
    model: str | None = None
    max_tokens: int | None = None


@runtime_checkable
class Facet(Protocol):
    """Statisches Protocol fuer Facet-Implementierungen.

    Concrete Facets koennen Python-Klassen (z.B. plugins) oder
    deklarativ aus YAML geladene Objekte sein (siehe ``types.py``).
    """

    id: str
    label: str
    input_kinds: tuple[Material, ...]
    codebook_contribution: tuple[CodeContribution, ...]

    def build_prompt(self, ctx: FacetContext) -> str:
        """Erzeugt den LLM-Prompt fuer das uebergebene Material."""
        ...

    def parse_response(self, raw: str | Mapping[str, Any], ctx: FacetContext) -> list[CodedSegment]:
        """Wandelt die LLM-Antwort in CodedSegments um."""
        ...


__all__ = [
    "Facet",
    "FacetContext",
    "CodeContribution",
    "Material",
]
