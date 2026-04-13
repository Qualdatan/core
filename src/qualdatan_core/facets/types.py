# SPDX-License-Identifier: AGPL-3.0-only
"""Built-in Facet types (declarative, configured via YAML).

Diese Klassen sind das, was Bundles benutzen, um eigene Domaenen zu
beschreiben — z.B. ``IFC-Bauelemente`` als ``TaxonomyFacet`` mit der
IFC-Klassen-Liste als Choices, ``LOG-Evidenz`` als ``EvidenceFacet`` mit
einer 5-stufigen Skala.

Alle Typen erfuellen das :class:`Facet`-Protocol aus ``base.py``. Sie sind
mit Absicht *spezifisch genug*, um wirklich nuetzlich zu sein, aber
*generisch genug*, um in beliebigen Domaenen zu funktionieren.

Der Code hier liefert nur die generischen Schemata und einen sehr einfachen,
heuristischen Default fuer ``build_prompt`` / ``parse_response``. Bundles
koennen Prompt-Templates ueberschreiben (Feld ``prompt_template`` im YAML),
und Phase-D wird die Rohbausteine durch eine richtige LLM-Wrapper-Schicht
in ``qualdatan_core/llm`` ergaenzen.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any, Callable, Mapping

from ..models import CodedSegment
from .base import CodeContribution, Facet, FacetContext, Material


# ---------------------------------------------------------------------------
# Gemeinsame Helfer
# ---------------------------------------------------------------------------
def _normalise_kinds(raw: Any) -> tuple[Material, ...]:
    if raw is None:
        return ()
    if isinstance(raw, str):
        raw = [raw]
    return tuple(Material(k) for k in raw)


def _normalise_codes(raw: Any) -> tuple[CodeContribution, ...]:
    if not raw:
        return ()
    out: list[CodeContribution] = []
    for entry in raw:
        if isinstance(entry, CodeContribution):
            out.append(entry)
            continue
        if isinstance(entry, str):
            out.append(CodeContribution(id=entry, label=entry))
            continue
        out.append(
            CodeContribution(
                id=entry["id"],
                label=entry.get("label", entry["id"]),
                description=entry.get("description", ""),
                color_hint=entry.get("color_hint"),
            )
        )
    return tuple(out)


def _strip_text(material: Any) -> str:
    if isinstance(material, str):
        return material
    if isinstance(material, Mapping) and "text" in material:
        return str(material["text"])
    return str(material)


# ---------------------------------------------------------------------------
# TaxonomyFacet — pick categories from a controlled vocabulary
# ---------------------------------------------------------------------------
@dataclass
class TaxonomyFacet:
    """Wandelt Material in Kodierungen anhand einer Taxonomie.

    Beispiel: IFC-Klassen, ICD-10-Diagnosen, BPMN-Aktivitaetstypen.
    """

    id: str
    label: str
    input_kinds: tuple[Material, ...]
    codebook_contribution: tuple[CodeContribution, ...]
    description: str = ""
    prompt_template: str = (
        "Klassifiziere folgendes Material gegen die Taxonomie '{label}'.\n"
        "Erlaubte Codes: {codes_csv}\n\n"
        "Material:\n{material}\n\n"
        "Antworte als JSON-Liste von Objekten "
        "{{\"code_id\": \"…\", \"text\": \"…\", \"char_start\": int, \"char_end\": int}}."
    )

    @classmethod
    def from_yaml(cls, data: Mapping[str, Any]) -> "TaxonomyFacet":
        return cls(
            id=data["id"],
            label=data.get("label", data["id"]),
            description=data.get("description", ""),
            input_kinds=_normalise_kinds(data.get("input_kinds", ["text"])),
            codebook_contribution=_normalise_codes(data.get("codes", [])),
            prompt_template=data.get("prompt_template", cls.__dataclass_fields__["prompt_template"].default),
        )

    def build_prompt(self, ctx: FacetContext) -> str:
        return self.prompt_template.format(
            label=self.label,
            codes_csv=", ".join(c.id for c in self.codebook_contribution),
            material=_strip_text(ctx.material),
        )

    def parse_response(self, raw: str | Mapping[str, Any], ctx: FacetContext) -> list[CodedSegment]:
        return _parse_segments_json(raw, ctx, self.codebook_contribution)


# ---------------------------------------------------------------------------
# EvidenceFacet — assign one of several ordinal evidence levels
# ---------------------------------------------------------------------------
@dataclass
class EvidenceFacet:
    """Stuft Material auf einer ordinalen Skala ein (z.B. LOG-01..05)."""

    id: str
    label: str
    input_kinds: tuple[Material, ...]
    codebook_contribution: tuple[CodeContribution, ...]   # Reihenfolge = Skala
    description: str = ""
    prompt_template: str = (
        "Bewerte Material auf der Skala '{label}'.\n"
        "Stufen (von niedrig nach hoch): {codes_list}\n\n"
        "Material:\n{material}\n\n"
        "Antworte als JSON: {{\"code_id\": \"…\", \"justification\": \"…\"}}."
    )

    @classmethod
    def from_yaml(cls, data: Mapping[str, Any]) -> "EvidenceFacet":
        return cls(
            id=data["id"],
            label=data.get("label", data["id"]),
            description=data.get("description", ""),
            input_kinds=_normalise_kinds(data.get("input_kinds", ["text"])),
            codebook_contribution=_normalise_codes(data.get("scale", data.get("codes", []))),
            prompt_template=data.get("prompt_template", cls.__dataclass_fields__["prompt_template"].default),
        )

    def build_prompt(self, ctx: FacetContext) -> str:
        return self.prompt_template.format(
            label=self.label,
            codes_list=" -> ".join(c.id for c in self.codebook_contribution),
            material=_strip_text(ctx.material),
        )

    def parse_response(self, raw: str | Mapping[str, Any], ctx: FacetContext) -> list[CodedSegment]:
        return _parse_segments_json(raw, ctx, self.codebook_contribution)


# ---------------------------------------------------------------------------
# ActorRoleFacet — extract actor mentions and assign roles
# ---------------------------------------------------------------------------
@dataclass
class ActorRoleFacet:
    """Findet Akteure im Material und ordnet sie Rollen zu."""

    id: str
    label: str
    input_kinds: tuple[Material, ...]
    codebook_contribution: tuple[CodeContribution, ...]   # die moeglichen Rollen
    description: str = ""
    prompt_template: str = (
        "Identifiziere Akteure und ordne jedem eine Rolle aus '{label}' zu.\n"
        "Verfuegbare Rollen: {codes_csv}\n\n"
        "Material:\n{material}\n\n"
        "Antworte als JSON-Liste {{\"code_id\": \"<rolle>\", \"text\": \"<akteur>\", "
        "\"char_start\": int, \"char_end\": int}}."
    )

    @classmethod
    def from_yaml(cls, data: Mapping[str, Any]) -> "ActorRoleFacet":
        return cls(
            id=data["id"],
            label=data.get("label", data["id"]),
            description=data.get("description", ""),
            input_kinds=_normalise_kinds(data.get("input_kinds", ["text"])),
            codebook_contribution=_normalise_codes(data.get("roles", data.get("codes", []))),
            prompt_template=data.get("prompt_template", cls.__dataclass_fields__["prompt_template"].default),
        )

    def build_prompt(self, ctx: FacetContext) -> str:
        return self.prompt_template.format(
            label=self.label,
            codes_csv=", ".join(c.id for c in self.codebook_contribution),
            material=_strip_text(ctx.material),
        )

    def parse_response(self, raw: str | Mapping[str, Any], ctx: FacetContext) -> list[CodedSegment]:
        return _parse_segments_json(raw, ctx, self.codebook_contribution)


# ---------------------------------------------------------------------------
# ProcessStepFacet — identify ordered process steps
# ---------------------------------------------------------------------------
@dataclass
class ProcessStepFacet:
    """Zerlegt Material in eine Sequenz definierter Prozessschritte."""

    id: str
    label: str
    input_kinds: tuple[Material, ...]
    codebook_contribution: tuple[CodeContribution, ...]   # die moeglichen Schritte
    description: str = ""
    prompt_template: str = (
        "Identifiziere Prozessschritte aus '{label}' im Material und gib sie "
        "in der Reihenfolge ihres Auftretens zurueck.\n"
        "Erlaubte Schritte: {codes_csv}\n\n"
        "Material:\n{material}\n\n"
        "Antworte als JSON-Liste {{\"code_id\": \"…\", \"text\": \"<auszug>\", "
        "\"char_start\": int, \"char_end\": int, \"sequence\": int}}."
    )

    @classmethod
    def from_yaml(cls, data: Mapping[str, Any]) -> "ProcessStepFacet":
        return cls(
            id=data["id"],
            label=data.get("label", data["id"]),
            description=data.get("description", ""),
            input_kinds=_normalise_kinds(data.get("input_kinds", ["text"])),
            codebook_contribution=_normalise_codes(data.get("steps", data.get("codes", []))),
            prompt_template=data.get("prompt_template", cls.__dataclass_fields__["prompt_template"].default),
        )

    def build_prompt(self, ctx: FacetContext) -> str:
        return self.prompt_template.format(
            label=self.label,
            codes_csv=", ".join(c.id for c in self.codebook_contribution),
            material=_strip_text(ctx.material),
        )

    def parse_response(self, raw: str | Mapping[str, Any], ctx: FacetContext) -> list[CodedSegment]:
        return _parse_segments_json(raw, ctx, self.codebook_contribution)


# ---------------------------------------------------------------------------
# FreeCodingFacet — open coding without a fixed taxonomy
# ---------------------------------------------------------------------------
@dataclass
class FreeCodingFacet:
    """Offene Kodierung — Modell darf eigene Codes vergeben.

    Optional kann ein Set an *Seed-Codes* mitgegeben werden, an dem das
    Modell sich orientieren soll.
    """

    id: str
    label: str
    input_kinds: tuple[Material, ...]
    codebook_contribution: tuple[CodeContribution, ...] = ()
    description: str = ""
    prompt_template: str = (
        "Fuehre offene Kodierung am Material durch (Methode '{label}').\n"
        "Bekannte Seed-Codes (optional, du darfst neue erfinden): {codes_csv}\n\n"
        "Material:\n{material}\n\n"
        "Antworte als JSON-Liste {{\"code_id\": \"…\", \"code_label\": \"…\", "
        "\"text\": \"…\", \"char_start\": int, \"char_end\": int}}."
    )

    @classmethod
    def from_yaml(cls, data: Mapping[str, Any]) -> "FreeCodingFacet":
        return cls(
            id=data["id"],
            label=data.get("label", data["id"]),
            description=data.get("description", ""),
            input_kinds=_normalise_kinds(data.get("input_kinds", ["text"])),
            codebook_contribution=_normalise_codes(data.get("seed_codes", data.get("codes", []))),
            prompt_template=data.get("prompt_template", cls.__dataclass_fields__["prompt_template"].default),
        )

    def build_prompt(self, ctx: FacetContext) -> str:
        return self.prompt_template.format(
            label=self.label,
            codes_csv=", ".join(c.id for c in self.codebook_contribution) or "(keine)",
            material=_strip_text(ctx.material),
        )

    def parse_response(self, raw: str | Mapping[str, Any], ctx: FacetContext) -> list[CodedSegment]:
        # FreeCoding erlaubt unbekannte Code-IDs; wir validieren nicht gegen
        # codebook_contribution.
        return _parse_segments_json(raw, ctx, self.codebook_contribution, strict=False)


# ---------------------------------------------------------------------------
# Built-in registry helper
# ---------------------------------------------------------------------------
def builtin_facet_types() -> dict[str, Callable[..., Facet]]:
    """Map ``type``-Strings aus YAML auf die zugehoerigen Built-in-Klassen."""

    # Lokal importieren, um Zirkularitaet (coding.visual_facet -> ..facets.types)
    # zu vermeiden.
    from ..coding.visual_facet import VisualEvidenceFacet, VisualTaxonomyFacet

    return {
        "taxonomy":        TaxonomyFacet.from_yaml,
        "evidence":        EvidenceFacet.from_yaml,
        "actor_role":      ActorRoleFacet.from_yaml,
        "process_step":    ProcessStepFacet.from_yaml,
        "free_coding":     FreeCodingFacet.from_yaml,
        "visual_taxonomy": VisualTaxonomyFacet.from_yaml,
        "visual_evidence": VisualEvidenceFacet.from_yaml,
    }


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------
_JSON_FENCE_RE = re.compile(r"```(?:json)?\s*(.*?)\s*```", re.DOTALL | re.IGNORECASE)


def _coerce_json(raw: str | Mapping[str, Any]) -> Any:
    if isinstance(raw, Mapping) or isinstance(raw, list):
        return raw
    text = str(raw).strip()
    fence = _JSON_FENCE_RE.search(text)
    if fence:
        text = fence.group(1).strip()
    return json.loads(text)


def _parse_segments_json(
    raw: str | Mapping[str, Any],
    ctx: FacetContext,
    codes: tuple[CodeContribution, ...],
    *,
    strict: bool = True,
) -> list[CodedSegment]:
    payload = _coerce_json(raw)
    if isinstance(payload, Mapping):
        payload = [payload]
    if not isinstance(payload, list):
        raise ValueError(f"Erwartete JSON-Liste oder -Objekt, bekam {type(payload).__name__}")
    code_ids = {c.id for c in codes}
    code_labels = {c.id: c.label for c in codes}
    out: list[CodedSegment] = []
    for entry in payload:
        cid = entry["code_id"]
        if strict and code_ids and cid not in code_ids:
            raise ValueError(
                f"Facet '{ctx.source_label or 'unknown'}' lieferte unbekannte "
                f"code_id='{cid}'; erwartet eines von {sorted(code_ids)}"
            )
        out.append(
            CodedSegment(
                code_id=cid,
                code_name=entry.get("code_label") or code_labels.get(cid, cid),
                hauptkategorie=entry.get("hauptkategorie", cid),
                text=entry.get("text", ""),
                char_start=int(entry.get("char_start", 0)),
                char_end=int(entry.get("char_end", 0)),
                document=ctx.source_label,
            )
        )
    return out


__all__ = [
    "TaxonomyFacet",
    "EvidenceFacet",
    "ActorRoleFacet",
    "ProcessStepFacet",
    "FreeCodingFacet",
    "builtin_facet_types",
]
