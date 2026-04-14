# SPDX-License-Identifier: AGPL-3.0-only
"""Configurable folder-tree scanner for "subject" datasets.

In Forschungs- und Praxis-Projekten gibt es haeufig eine Mehrebenen-
Ordnerstruktur, die ein qualitativen Sample beschreibt:

    <BASE>/<Subject>/
    |-- <Interview-Subdir>/                 <- Interview-Material
    |   |-- ...docx
    |-- <Projekt-Praefix> - <Code> - Name/  <- Sub-Sammlungen (z.B. Projekte)
    |-- <Notes-Subdir>/                     <- Sonstige Materialien
    `-- ...

Welche Praefixe, Subdirs, Regex und Datei-Extensions gelten, ist
**Bundle-Sache** — Default-Werte unten orientieren sich am ersten
Use-Case (BIM-Bauprojekte mit "Projekt - <CODE> - <NAME>"-Ordnern), aber
jedes Feld ist via :class:`FolderLayout` konfigurierbar. Bundles laden
ihre Layouts ueblicherweise aus YAML (siehe :func:`FolderLayout.from_dict`).
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any


# ---------------------------------------------------------------------------
# Datenmodelle
# ---------------------------------------------------------------------------
@dataclass
class SubjectFolder:
    """Eine Sub-Sammlung innerhalb eines Subjects (z.B. ein Projekt)."""

    folder_name: str
    code: str | None
    name: str
    path: Path
    pdf_count: int = 0
    office_count: int = 0


@dataclass
class Subject:
    """Top-Level-Eintrag im Sample (frueher 'Company')."""

    name: str
    path: Path
    interviews: list[Path] = field(default_factory=list)
    folders: list[SubjectFolder] = field(default_factory=list)
    notes_path: Path | None = None
    notes_files: list[Path] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Layout-Definition
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class FolderLayout:
    """Konfiguration fuer den Subject-Scanner.

    Defaults entsprechen dem BIM-Research-Layout (HKS/PBN). Andere Domaenen
    konfigurieren ueber ``from_dict`` / Bundle-YAML.
    """

    # Welche Praefix-Worte einen Sub-Sammlungs-Ordner einleiten (case-insensitive).
    folder_prefix: str = "projekt"

    # Regex zum Parsen von folder_prefix - <CODE> - <NAME>. Named groups
    # ``code`` und ``name`` werden ausgelesen.
    folder_pattern: str = r"^Projekt\s*[-\u2013]\s*(?P<code>[\w\d]+)\s*[-\u2013]\s*(?P<name>.+)$"

    # Subdir-Namen (case-insensitive). ``None`` deaktiviert.
    interviews_subdir: str | None = "interviews"
    notes_subdir: str | None = "sonstiges"

    # Datei-Klassifikation
    interview_exts: tuple[str, ...] = (".docx",)
    pdf_ext: str = ".pdf"
    office_exts: tuple[str, ...] = (".docx", ".doc", ".xlsx", ".xls")

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> FolderLayout:
        """Instanziiert ein Layout aus einem (z.B. aus YAML geparsten) Dict.

        Unbekannte Felder werden ignoriert (Forward-Compat).
        """

        kwargs: dict[str, Any] = {}
        for key in (
            "folder_prefix",
            "folder_pattern",
            "interviews_subdir",
            "notes_subdir",
            "pdf_ext",
        ):
            if key in data:
                kwargs[key] = data[key]
        for key in ("interview_exts", "office_exts"):
            if key in data:
                kwargs[key] = tuple(data[key])
        return cls(**kwargs)

    def with_overrides(self, **overrides: Any) -> FolderLayout:
        """Liefert eine Kopie mit ueberschriebenen Feldern."""

        return replace(self, **overrides)

    # --- compiled regex (cached) ----------------------------------------
    @property
    def _compiled_pattern(self) -> re.Pattern[str]:
        # Re-kompiliert pro Layout-Instanz; FolderLayout ist frozen, also
        # billig genug, das jedes Mal nachzuschlagen.
        return re.compile(self.folder_pattern)


DEFAULT_LAYOUT = FolderLayout()


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------
def _is_hidden(p: Path) -> bool:
    return p.name.startswith(".")


def _iter_visible_dirs(base: Path) -> Iterable[Path]:
    if not base.exists():
        return iter(())
    return (p for p in sorted(base.iterdir()) if p.is_dir() and not _is_hidden(p))


def _find_subdir_case_insensitive(base: Path, name: str | None) -> Path | None:
    if name is None:
        return None
    target = name.lower()
    for child in base.iterdir():
        if child.is_dir() and child.name.lower() == target:
            return child
    return None


def _count_files_by_ext(path: Path, exts: set[str]) -> int:
    if not path.exists():
        return 0
    n = 0
    for p in path.rglob("*"):
        if p.is_file() and p.suffix.lower() in exts:
            n += 1
    return n


def _list_files_recursive(path: Path | None) -> list[Path]:
    if path is None or not path.exists():
        return []
    out: list[Path] = []
    for p in sorted(path.rglob("*")):
        if p.is_file() and not _is_hidden(p) and not p.name.startswith("~$"):
            out.append(p)
    return out


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
def parse_folder(folder: Path, layout: FolderLayout = DEFAULT_LAYOUT) -> SubjectFolder:
    """Parst einen Sub-Sammlungs-Ordner ueber das Layout-Regex.

    Faellt nicht um wenn der Praefix fehlt: in dem Fall gilt ``code=None``
    und ``name=folder_name``.
    """

    folder = Path(folder)
    folder_name = folder.name
    match = layout._compiled_pattern.match(folder_name)
    if match:
        code = match.group("code").strip()
        name = match.group("name").strip()
    else:
        code = None
        name = folder_name

    return SubjectFolder(
        folder_name=folder_name,
        code=code,
        name=name,
        path=folder,
        pdf_count=_count_files_by_ext(folder, {layout.pdf_ext}),
        office_count=_count_files_by_ext(folder, set(layout.office_exts)),
    )


def list_subjects(base: Path) -> list[str]:
    """Top-Level-Subject-Ordner unter ``base``. Leere Liste wenn ``base``
    nicht existiert.
    """

    base = Path(base)
    if not base.exists():
        return []
    return sorted(c.name for c in _iter_visible_dirs(base))


def scan_subject(
    name: str,
    base: Path,
    layout: FolderLayout = DEFAULT_LAYOUT,
) -> Subject:
    """Scannt eine einzelne Subject-Struktur und sammelt Metadaten.

    Wirft ``FileNotFoundError`` wenn der Subject-Ordner nicht existiert.
    """

    base = Path(base)
    subject_path = base / name
    if not subject_path.exists() or not subject_path.is_dir():
        raise FileNotFoundError(f"Subject '{name}' nicht gefunden unter {base}")

    # Interviews: nicht rekursiv, nur konfigurierte Extensions direkt drin.
    interviews: list[Path] = []
    interviews_dir = _find_subdir_case_insensitive(subject_path, layout.interviews_subdir)
    if interviews_dir is not None:
        valid_exts = {ext.lower() for ext in layout.interview_exts}
        for p in sorted(interviews_dir.iterdir()):
            if not p.is_file():
                continue
            if p.name.startswith("~$") or p.name.startswith("."):
                continue
            if p.suffix.lower() in valid_exts:
                interviews.append(p)

    # Notes/Sonstiges: rekursiv.
    notes_dir = _find_subdir_case_insensitive(subject_path, layout.notes_subdir)
    notes_files = _list_files_recursive(notes_dir) if notes_dir else []

    # Sub-Sammlungen (Projekte): Top-Level-Ordner mit dem konfigurierten Praefix.
    skip_names: set[str] = set()
    if interviews_dir is not None:
        skip_names.add(interviews_dir.name)
    if notes_dir is not None:
        skip_names.add(notes_dir.name)

    folders: list[SubjectFolder] = []
    prefix_lower = layout.folder_prefix.lower() if layout.folder_prefix else ""
    for child in _iter_visible_dirs(subject_path):
        if child.name in skip_names:
            continue
        if prefix_lower and not child.name.lower().startswith(prefix_lower):
            continue
        folders.append(parse_folder(child, layout=layout))

    folders.sort(key=lambda p: p.folder_name)

    return Subject(
        name=name,
        path=subject_path,
        interviews=interviews,
        folders=folders,
        notes_path=notes_dir,
        notes_files=notes_files,
    )


# ---------------------------------------------------------------------------
# Backward-compat (HKS/PBN-Vokabular)
# ---------------------------------------------------------------------------
# Bis Phase B.x in TUI/Desktop alle Verweise auf die generischen Namen
# umgestellt sind, exponieren wir die alten Namen weiter. Property-Aliase auf
# Subject machen ``.projects`` / ``.sonstiges_path`` / ``.sonstiges_files``
# transparent verfuegbar.
def _projects_property(self: Subject) -> list[SubjectFolder]:
    return self.folders


def _sonstiges_path_property(self: Subject) -> Path | None:
    return self.notes_path


def _sonstiges_files_property(self: Subject) -> list[Path]:
    return self.notes_files


Subject.projects = property(_projects_property)  # type: ignore[attr-defined]
Subject.sonstiges_path = property(_sonstiges_path_property)  # type: ignore[attr-defined]
Subject.sonstiges_files = property(_sonstiges_files_property)  # type: ignore[attr-defined]

Company = Subject
CompanyProject = SubjectFolder
list_companies = list_subjects
scan_company = scan_subject
parse_project_folder = parse_folder


__all__ = [
    "FolderLayout",
    "DEFAULT_LAYOUT",
    "Subject",
    "SubjectFolder",
    "list_subjects",
    "scan_subject",
    "parse_folder",
    # Backward-compat:
    "Company",
    "CompanyProject",
    "list_companies",
    "scan_company",
    "parse_project_folder",
]
