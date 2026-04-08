"""PDF-Scanner: Rekursiver Scan von Projekt-Ordnern nach PDFs.

Erzeugt ein Manifest mit allen gefundenen PDFs und Metadaten.
"""

import json
from pathlib import Path

from .config import PROJECTS_DIR


def scan_projects(projects_dir: Path = None,
                  project_filter: str = None) -> list[dict]:
    """Scannt Projekt-Ordner rekursiv nach PDFs.

    Args:
        projects_dir: Basisverzeichnis (default: input/projects/)
        project_filter: Optional — nur dieses Projekt scannen

    Returns:
        Liste von PDF-Einträgen mit Metadaten:
        [{"path": Path, "project": str, "relative_path": str, "size_kb": int}]
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

    return pdfs


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
