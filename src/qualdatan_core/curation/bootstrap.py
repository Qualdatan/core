"""Codebook-Curation (Phase 4): Bootstrap fuer ein Draft-Codebook.

Das Vorgehen:

1. Ein kleines **Sample** von Interview- oder Dokument-Dateien wird durch die
   normale Pipeline gejagt (``run_analysis`` fuer Interviews,
   ``pdf_coder.run_coding`` fuer PDFs). Die entstandenen Codes landen entweder
   in ``AnalysisResult.codes`` oder in der ``pipeline.db`` des Runs.
2. Die Codes werden zusammen mit einer optionalen **Seed-Codebase** (aus
   ``CODEBASES_DIR``) **gemergt**, inklusive Herkunfts-Meta
   (``source: provided|inductive``).
3. Fuer jeden Code werden Frequenz, kuerzestes Beispielzitat und eine
   Begruendung/Definition aggregiert.
4. Das Ergebnis landet als ``draft_codebook.yml`` im Run-Verzeichnis, im
   gleichen Schema wie ``input/codebases/codebook.yml`` (mit einem
   zusaetzlichen ``_meta`` Block pro Code zur Review-Hilfe).

Der CLI-Entry-Point liegt in ``main.py`` (``cmd_curate``).
"""

from __future__ import annotations

import datetime as _dt
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from ..config import CODEBASES_DIR
from ..recipe import Recipe
from ..run_context import RunContext

# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------


@dataclass
class _CodeAgg:
    """Internes Aggregat fuer einen einzelnen Code."""

    code_id: str
    name: str = ""
    hauptkategorie: str = ""
    definition: str = ""
    shortest_example: str = ""
    first_reason: str = ""
    frequency: int = 0
    source: str = "inductive"  # 'provided' | 'inductive'
    first_seen: str = ""

    def add_occurrence(
        self,
        example: str = "",
        reason: str = "",
        name: str = "",
        definition: str = "",
        hauptkategorie: str = "",
    ) -> None:
        self.frequency += 1
        if name and not self.name:
            self.name = name
        if definition and not self.definition:
            self.definition = definition
        if hauptkategorie and not self.hauptkategorie:
            self.hauptkategorie = hauptkategorie
        # Kuerzestes sinnvolles Beispielzitat gewinnt (aber nicht leer).
        if example:
            example = example.strip()
            if example and (not self.shortest_example or len(example) < len(self.shortest_example)):
                self.shortest_example = example
        if reason and not self.first_reason:
            self.first_reason = reason.strip()


@dataclass
class _Aggregate:
    """Sammelt alle Codes nach code_id und liefert das YAML-Grundgeruest."""

    codes: dict[str, _CodeAgg] = field(default_factory=dict)

    def get_or_create(self, code_id: str, source: str = "inductive") -> _CodeAgg:
        if code_id not in self.codes:
            self.codes[code_id] = _CodeAgg(
                code_id=code_id,
                source=source,
                first_seen=_today_iso(),
            )
        return self.codes[code_id]


def _today_iso() -> str:
    return _dt.date.today().isoformat()


def _hauptkategorie_from(code_id: str) -> str:
    """Leitet die Hauptkategorie aus der ``X-NN`` Code-ID ab."""
    if "-" in code_id:
        return code_id.split("-", 1)[0]
    return code_id or "Z"


# ---------------------------------------------------------------------------
# Seed-Codebase laden
# ---------------------------------------------------------------------------


def _load_seed_codebook(name: str) -> dict[str, Any]:
    """Laedt eine Seed-Codebase (``CODEBASES_DIR/<name>.yml``).

    Akzeptiert ``.yml``/``.yaml`` Endungen und auch den exakten
    Dateinamen. Das Schema entspricht ``codebook.yml``
    (Top-Key ``kategorien``).
    """
    candidates = [
        CODEBASES_DIR / f"{name}.yml",
        CODEBASES_DIR / f"{name}.yaml",
        CODEBASES_DIR / name,
    ]
    for path in candidates:
        if path.exists() and path.is_file():
            with open(path, encoding="utf-8") as fh:
                data = yaml.safe_load(fh) or {}
            if not isinstance(data, dict):
                raise ValueError(f"Seed-Codebase {path} muss ein YAML-Mapping sein.")
            return data
    raise FileNotFoundError(
        f"Seed-Codebase '{name}' nicht gefunden in {CODEBASES_DIR} "
        f"(versucht: .yml, .yaml, exakter Name)."
    )


def _seed_to_aggregate(seed: dict[str, Any], agg: _Aggregate) -> dict[str, str]:
    """Nimmt alle Seed-Codes in das Aggregat auf (``source='provided'``).

    Returns:
        Mapping code_id -> kategorie-id, damit die spaetere YAML-Ausgabe
        die originale Kategorie-Sortierung verwenden kann.
    """
    category_map: dict[str, str] = {}
    kategorien = seed.get("kategorien", []) or []
    for kat in kategorien:
        kat_id = str(kat.get("id") or "").strip() or "Z"
        for code in kat.get("codes", []) or []:
            code_id = str(code.get("id") or "").strip()
            if not code_id:
                continue
            entry = agg.get_or_create(code_id, source="provided")
            entry.source = "provided"
            entry.hauptkategorie = kat_id
            if not entry.name:
                entry.name = str(code.get("name") or "")
            if not entry.definition:
                entry.definition = str(code.get("definition") or "")
            if not entry.shortest_example:
                entry.shortest_example = str(code.get("ankerbeispiel") or "")
            category_map[code_id] = kat_id
    return category_map


def _seed_category_names(seed: dict[str, Any]) -> dict[str, str]:
    """Extrahiert die Kategorie-Namen aus einer Seed-Codebase."""
    out: dict[str, str] = {}
    for kat in seed.get("kategorien", []) or []:
        kat_id = str(kat.get("id") or "").strip()
        if not kat_id:
            continue
        out[kat_id] = str(kat.get("name") or "")
    return out


# ---------------------------------------------------------------------------
# Codes aus Run-Daten sammeln
# ---------------------------------------------------------------------------


def _ingest_interview_codes(agg: _Aggregate, analysis_result) -> int:
    """Traegt Codes aus einem AnalysisResult in das Aggregat ein.

    Zahlt die Frequenz aus ``result.segments``, nicht aus ``result.codes``
    (``codes[...]["count"]`` ist nur der Per-Datei-Zaehler und waere
    bei mehreren Sample-Files falsch). Gibt die Zahl der verarbeiteten
    Segmente zurueck.
    """
    if analysis_result is None:
        return 0
    n = 0
    # Erst registry durchlaufen, damit Name/Definition auch ohne Segments kommen
    registry = getattr(analysis_result, "codes", {}) or {}
    segments = getattr(analysis_result, "segments", []) or []

    for code_id, meta in registry.items():
        entry = agg.get_or_create(code_id)
        entry.name = entry.name or str(meta.get("name", ""))
        entry.hauptkategorie = (
            entry.hauptkategorie
            or str(meta.get("hauptkategorie", ""))
            or _hauptkategorie_from(code_id)
        )
        entry.definition = entry.definition or str(meta.get("kodierdefinition", ""))
        if not entry.shortest_example:
            entry.shortest_example = str(meta.get("ankerbeispiel", "") or "")

    for seg in segments:
        code_id = getattr(seg, "code_id", None) or (
            seg.get("code_id") if isinstance(seg, dict) else None
        )
        if not code_id:
            continue
        text = getattr(seg, "text", "") or (seg.get("text", "") if isinstance(seg, dict) else "")
        reason = getattr(seg, "abgrenzungsregel", "") or (
            seg.get("abgrenzungsregel", "") if isinstance(seg, dict) else ""
        )
        name = getattr(seg, "code_name", "") or (
            seg.get("code_name", "") if isinstance(seg, dict) else ""
        )
        definition = getattr(seg, "kodierdefinition", "") or (
            seg.get("kodierdefinition", "") if isinstance(seg, dict) else ""
        )
        hauptkategorie = getattr(seg, "hauptkategorie", "") or (
            seg.get("hauptkategorie", "") if isinstance(seg, dict) else ""
        )
        agg.get_or_create(code_id).add_occurrence(
            example=text,
            reason=reason,
            name=name,
            definition=definition,
            hauptkategorie=hauptkategorie or _hauptkategorie_from(code_id),
        )
        n += 1
    return n


def _ingest_db_codes(agg: _Aggregate, ctx: RunContext) -> int:
    """Liest ``codings``/``coding_codes``/``neue_codes`` aus der Run-DB.

    Aggregiert Frequenz (anzahl ``coding_codes``-Eintraege), nimmt den
    kuerzesten Block-Text als Beispiel und den ``begruendung``-String als
    erste Begruendung. ``neue_codes``-Eintraege liefern Name/Definition.
    """
    conn = ctx.db._get_conn()
    n = 0

    # Name/Definition aus neue_codes
    name_map: dict[str, dict[str, str]] = {}
    for r in conn.execute(
        "SELECT code_id, code_name, hauptkategorie, kodierdefinition FROM neue_codes"
    ):
        name_map[r["code_id"]] = {
            "name": r["code_name"] or "",
            "hauptkategorie": r["hauptkategorie"] or "",
            "definition": r["kodierdefinition"] or "",
        }

    # Codings + Code-IDs joinen
    rows = conn.execute(
        """
        SELECT cc.code_id AS code_id,
               c.begruendung AS begruendung,
               c.description AS description,
               c.block_id    AS block_id,
               c.pdf_id      AS pdf_id,
               c.page        AS page
        FROM coding_codes cc
        JOIN codings c ON c.id = cc.coding_id
        """
    ).fetchall()

    for r in rows:
        code_id = r["code_id"]
        if not code_id:
            continue
        entry = agg.get_or_create(code_id)
        meta = name_map.get(code_id, {})
        # Beispiel: wir haben hier keinen Text, nur block_id.
        # Begruendung als Pseudo-Example — fuer den Draft reicht das.
        example = r["description"] or ""
        reason = r["begruendung"] or ""
        entry.add_occurrence(
            example=example,
            reason=reason,
            name=meta.get("name", ""),
            definition=meta.get("definition", ""),
            hauptkategorie=(meta.get("hauptkategorie", "") or _hauptkategorie_from(code_id)),
        )
        n += 1

    # Codes aus neue_codes, die noch keine Kodierung gesehen haben
    for code_id, meta in name_map.items():
        entry = agg.get_or_create(code_id)
        if not entry.name and meta.get("name"):
            entry.name = meta["name"]
        if not entry.definition and meta.get("definition"):
            entry.definition = meta["definition"]
        if not entry.hauptkategorie:
            entry.hauptkategorie = meta.get("hauptkategorie") or _hauptkategorie_from(code_id)

    return n


# ---------------------------------------------------------------------------
# YAML-Ausgabe
# ---------------------------------------------------------------------------


def _build_yaml_struct(agg: _Aggregate, category_names: dict[str, str] | None = None) -> dict:
    """Baut das codebook.yml-kompatible Dict aus dem Aggregat."""
    category_names = category_names or {}

    # Codes nach Hauptkategorie gruppieren
    by_cat: dict[str, list[_CodeAgg]] = defaultdict(list)
    for code in agg.codes.values():
        cat = code.hauptkategorie or _hauptkategorie_from(code.code_id) or "Z"
        by_cat[cat].append(code)

    # Kategorien sortieren (alphabetisch, Z zum Schluss)
    def _cat_key(c: str) -> tuple:
        return (c == "Z", c)

    kategorien = []
    for cat_id in sorted(by_cat.keys(), key=_cat_key):
        codes = sorted(by_cat[cat_id], key=lambda e: e.code_id)
        kat_entry: dict[str, Any] = {
            "id": cat_id,
            "name": category_names.get(cat_id, "") or ("Sonstiges" if cat_id == "Z" else cat_id),
            "codes": [],
        }
        for code in codes:
            code_entry: dict[str, Any] = {
                "id": code.code_id,
                "name": code.name or code.code_id,
                "definition": code.definition or "",
                "ankerbeispiel": code.shortest_example or code.first_reason or "",
                "_meta": {
                    "frequency": code.frequency,
                    "source": code.source,
                    "first_seen": code.first_seen or _today_iso(),
                },
            }
            kat_entry["codes"].append(code_entry)
        kategorien.append(kat_entry)

    return {"kategorien": kategorien}


def _write_draft_yaml(path: Path, struct: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    header = (
        "# =============================================================\n"
        "# Draft-Codebook — automatisch erzeugt durch `main.py curate`\n"
        "# Review + manuell editieren, dann nach input/codebases/ kopieren.\n"
        "# =============================================================\n"
    )
    body = yaml.safe_dump(struct, allow_unicode=True, sort_keys=False, width=100)
    path.write_text(header + body, encoding="utf-8")


# ---------------------------------------------------------------------------
# Statistik (fuer CLI-Ausgabe)
# ---------------------------------------------------------------------------


@dataclass
class CurationStats:
    """Kleine Zusammenfassung fuer die CLI-Ausgabe."""

    total_codes: int = 0
    provided_codes: int = 0
    inductive_codes: int = 0
    unused_provided: int = 0
    single_use: int = 0
    path: Path | None = None

    @classmethod
    def from_aggregate(cls, agg: _Aggregate, path: Path) -> CurationStats:
        total = len(agg.codes)
        provided = sum(1 for c in agg.codes.values() if c.source == "provided")
        inductive = sum(1 for c in agg.codes.values() if c.source == "inductive")
        unused_provided = sum(
            1 for c in agg.codes.values() if c.source == "provided" and c.frequency == 0
        )
        single_use = sum(1 for c in agg.codes.values() if c.frequency == 1)
        return cls(
            total_codes=total,
            provided_codes=provided,
            inductive_codes=inductive,
            unused_provided=unused_provided,
            single_use=single_use,
            path=path,
        )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def _mirror_samples(ctx: RunContext, sample_files: list[Path]) -> int:
    """Spiegelt Curation-Sample-Materials in die App-DB (no-op ohne Attach).

    Jedes Sample-File wird als ``material_kind="transcript_sample"`` mit
    absolutem Pfad registriert. Der Dateiname wird als ``source_label``
    uebergeben.

    Args:
        ctx: Aktueller RunContext.
        sample_files: Liste der Sample-Pfade, die fuer das Curation-
            Bootstrapping herangezogen werden.

    Returns:
        Anzahl erfolgreich registrierter Materials.
    """
    count = 0
    for p in sample_files:
        mid = ctx.register_material(
            "transcript_sample",
            str(p),
            source_label=p.name,
        )
        if mid is not None:
            count += 1
    return count


def bootstrap_codebook(
    ctx: RunContext,
    recipe: Recipe,
    sample_files: list[Path],
    codebase_seed: str | None = None,
    analysis_result=None,
) -> tuple[Path, CurationStats]:
    """Bootstrappt ein Draft-Codebook aus einem Sample.

    Args:
        ctx: RunContext des aktuellen Laufs (wird fuer DB-Zugriff + Pfade
            verwendet).
        recipe: Aktives Recipe — nur fuer Metadaten (``coding_strategy``)
            und als Parameter-Passthrough an Tests/Aufrufer. Diese Funktion
            ruft das LLM **nicht** selbst auf; der Caller muss vorher die
            Pipeline fuer ``sample_files`` durchlaufen lassen.
        sample_files: Liste der Sample-Dateien (nur fuer Logging /
            Dokumentation; die eigentlichen Codes werden aus
            ``analysis_result`` und/oder ``ctx.db`` gelesen).
        codebase_seed: Name einer Seed-Codebase in ``CODEBASES_DIR``
            (optional). Wenn gesetzt, werden die Seed-Codes als
            ``source='provided'`` uebernommen und mit induktiven Codes
            gemergt.
        analysis_result: Optional ein :class:`AnalysisResult` (Interview-
            Flow). Wenn ``None``, wird ausschliesslich die Run-DB gelesen
            (Dokument-Flow).

    Returns:
        Tuple ``(Path, CurationStats)``. Der Path zeigt auf
        ``<run>/draft_codebook.yml``.
    """
    # D.3: Sample-Materials in App-DB spiegeln (no-op wenn nicht attached)
    _mirror_samples(ctx, sample_files)

    agg = _Aggregate()
    category_names: dict[str, str] = {}

    # 1. Seed einspielen
    if codebase_seed:
        seed = _load_seed_codebook(codebase_seed)
        _seed_to_aggregate(seed, agg)
        category_names.update(_seed_category_names(seed))

    # 2. Codes aus dem Interview-Flow einsammeln
    if analysis_result is not None:
        _ingest_interview_codes(agg, analysis_result)

    # 3. Codes aus der Run-DB (Dokumenten-Flow)
    try:
        _ingest_db_codes(agg, ctx)
    except Exception as e:  # pragma: no cover - defensive
        print(f"  WARN: Konnte Codes nicht aus pipeline.db lesen: {e}")

    # 4. Kategorie-Namen aus Recipe ergaenzen, falls Seed sie nicht liefert
    recipe_cats = getattr(recipe, "categories", {}) or {}
    for cat_id, cat_name in recipe_cats.items():
        category_names.setdefault(cat_id, cat_name)

    if not agg.codes:
        # Sauberer Fallback: leeres Codebook mit Catch-All-Kategorie
        print(
            "  HINWEIS: Keine Codes aus Sample extrahiert — schreibe leeres "
            "Draft-Codebook (Z = Catch-All)."
        )

    struct = _build_yaml_struct(agg, category_names=category_names)
    out_path = ctx.run_dir / "draft_codebook.yml"
    _write_draft_yaml(out_path, struct)

    stats = CurationStats.from_aggregate(agg, out_path)

    # Log
    _sample_names = ", ".join(f.name for f in sample_files[:4])
    if len(sample_files) > 4:
        _sample_names += f", ... (+{len(sample_files) - 4})"
    print(f"  Sample ({len(sample_files)}): {_sample_names or '(keine Dateien)'}")
    return out_path, stats
