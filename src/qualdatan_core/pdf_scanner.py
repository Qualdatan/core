"""PDF-Scanner: Rekursiver Scan von Projekt-Ordnern nach PDFs.

Erzeugt ein Manifest mit allen gefundenen PDFs und Metadaten.
Office-Dateien (.docx/.xlsx) werden optional zu PDF konvertiert.
"""

import json
import re
from pathlib import Path

from .config import PROJECTS_DIR


# Regex-Patterns die per Default erkennen, ob eine PDF ein Plan ist.
# Greifen auf den vollstaendigen relative_path (also inkl. Ordnernamen).
DEFAULT_PLAN_PATTERNS = [
    re.compile(r"(?i)(^|/)(pl[aä]ene|plans|pl[aä]ne)(/|$)"),  # Ordner "Plaene"/"Pläne"
    re.compile(r"(?i)(grundriss|schnittplan|schnitt(?!stelle)|ansicht(?!skarte)|"
               r"lageplan|detailplan|fassadenplan|aufriss)"),
]


def filter_pdfs(pdfs: list[dict],
                skip_plans: bool = False,
                skip_patterns: list[str] | None = None,
                only_patterns: list[str] | None = None) -> tuple[list[dict], list[dict]]:
    """Filtert die PDF-Liste nach Plan-Status oder beliebigen Regex.

    Args:
        pdfs: Liste aus scan_projects()
        skip_plans: Wenn True, werden Plaene anhand DEFAULT_PLAN_PATTERNS rausgefiltert.
        skip_patterns: Zusaetzliche Regex-Patterns; PDFs deren relative_path matcht
            werden rausgefiltert.
        only_patterns: Wenn gesetzt, werden NUR PDFs behalten deren relative_path
            mindestens eines dieser Patterns matcht.

    Returns:
        (kept, removed) — Tuple zweier Listen, damit der Aufrufer sehen kann
        welche PDFs warum gefiltert wurden.
    """
    skip_compiled = []
    if skip_plans:
        skip_compiled.extend(DEFAULT_PLAN_PATTERNS)
    if skip_patterns:
        for p in skip_patterns:
            skip_compiled.append(re.compile(p))

    only_compiled = [re.compile(p) for p in (only_patterns or [])]

    kept = []
    removed = []
    for pdf in pdfs:
        rel = pdf["relative_path"]

        # only_patterns: muss matchen
        if only_compiled and not any(p.search(rel) for p in only_compiled):
            removed.append({**pdf, "_filter_reason": "not in only_patterns"})
            continue

        # skip_patterns: darf nicht matchen
        matched_skip = next((p for p in skip_compiled if p.search(rel)), None)
        if matched_skip:
            removed.append({**pdf, "_filter_reason": f"skip pattern: {matched_skip.pattern}"})
            continue

        kept.append(pdf)

    return kept, removed


def scan_projects(projects_dir: Path = None,
                  project_filter: str = None,
                  convert_office: bool = False,
                  convert_cache_dir: Path = None) -> list[dict]:
    """Scannt Projekt-Ordner rekursiv nach PDFs.

    Args:
        projects_dir: Basisverzeichnis (default: input/projects/)
        project_filter: Optional — nur dieses Projekt scannen
        convert_office: Wenn True, werden .docx/.xlsx Dateien zu PDF
            konvertiert (via MS Office COM oder LibreOffice headless)
            und in das Ergebnis aufgenommen.
        convert_cache_dir: Zielverzeichnis fuer konvertierte PDFs.
            Pflicht wenn convert_office=True.

    Returns:
        Liste von Eintraegen mit Metadaten:
        [{"path": str, "project": str, "relative_path": str, "filename": str,
          "size_kb": int, "source_path": str (optional, falls konvertiert),
          "source_format": str (optional: 'docx'|'xlsx'|...)}]
    """
    base = projects_dir or PROJECTS_DIR
    if not base.exists():
        print(f"  Verzeichnis nicht gefunden: {base}")
        return []

    pdfs = []
    for pdf_path in sorted(base.rglob("*.pdf")):
        # Projekt = erster Unterordner unter projects/
        rel = pdf_path.relative_to(base)
        parts = rel.parts
        if len(parts) < 2:
            # PDF liegt direkt in projects/, kein Projekt-Ordner
            project = "_ohne_projekt"
        else:
            project = parts[0]

        if project_filter and project != project_filter:
            continue

        pdfs.append({
            "path": str(pdf_path),
            "project": project,
            "relative_path": str(rel),
            "filename": pdf_path.name,
            "size_kb": pdf_path.stat().st_size // 1024,
        })

    # Office-Dateien konvertieren und mit aufnehmen
    if convert_office:
        if convert_cache_dir is None:
            raise ValueError("convert_cache_dir muss gesetzt sein wenn convert_office=True")
        pdfs.extend(_scan_office_files(base, project_filter, Path(convert_cache_dir)))

    return pdfs


def _scan_office_files(base: Path, project_filter: str | None,
                       cache_dir: Path) -> list[dict]:
    """Sucht docx/xlsx, konvertiert sie und gibt PDF-Eintraege zurueck."""
    from .office_converter import (
        find_office_files, convert_to_pdf,
        OfficeConverterUnavailable, detect_backend,
    )

    office_files = find_office_files(base, project_filter)
    if not office_files:
        return []

    backend = detect_backend()
    if backend is None:
        print(f"  WARN: {len(office_files)} Office-Dateien gefunden, aber kein "
              f"Konverter verfuegbar. Installiere LibreOffice oder MS Office.")
        return []

    print(f"  Konvertiere {len(office_files)} Office-Dateien via {backend}...")

    converted = []
    for src in office_files:
        rel = src.relative_to(base)
        parts = rel.parts
        project = parts[0] if len(parts) >= 2 else "_ohne_projekt"

        # Cache-Pfad: <cache>/<rel mit .pdf>
        dst = cache_dir / rel.with_suffix(".pdf")

        try:
            convert_to_pdf(src, dst)
        except (OfficeConverterUnavailable, RuntimeError, ValueError) as e:
            print(f"    FEHLER bei {rel}: {e}")
            continue

        # Eintrag sieht aus wie eine normale PDF, mit Hinweis auf Original
        converted.append({
            "path": str(dst),
            "project": project,
            "relative_path": str(rel.with_suffix(".pdf")),
            "filename": dst.name,
            "size_kb": dst.stat().st_size // 1024,
            "source_path": str(src),
            "source_format": src.suffix.lower().lstrip("."),
        })

    print(f"  Konvertiert: {len(converted)}/{len(office_files)} Office-Dateien")
    return converted


def build_manifest(pdfs: list[dict]) -> dict:
    """Baut ein Manifest aus der PDF-Liste.

    Returns:
        {"projects": {name: [files]}, "total_pdfs": N, "total_size_kb": N}
    """
    projects = {}
    total_size = 0
    for pdf in pdfs:
        proj = pdf["project"]
        projects.setdefault(proj, []).append({
            "relative_path": pdf["relative_path"],
            "filename": pdf["filename"],
            "size_kb": pdf["size_kb"],
        })
        total_size += pdf["size_kb"]

    return {
        "projects": projects,
        "total_pdfs": len(pdfs),
        "total_size_kb": total_size,
    }


def save_manifest(manifest: dict, path: Path):
    """Speichert das Manifest als JSON."""
    path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2),
                    encoding="utf-8")


def print_manifest_summary(manifest: dict):
    """Gibt eine Übersicht des Manifests aus."""
    print(f"\n  Gefunden: {manifest['total_pdfs']} PDFs "
          f"({manifest['total_size_kb']} KB)")
    for proj, files in manifest["projects"].items():
        print(f"    {proj}: {len(files)} Dateien")
