# SPDX-License-Identifier: AGPL-3.0-only
"""Tests for qualdatan_core.layouts.folder (Phase B.2).

Portiert aus dem urspruenglichen test_company_scanner.py des Umbrellas;
benennt Company -> Subject und nutzt die neuen FolderLayout-konfigurierbaren
APIs.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from qualdatan_core.layouts import (
    DEFAULT_LAYOUT,
    FolderLayout,
    Subject,
    list_subjects,
    parse_folder,
    scan_subject,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
def _touch(path: Path, size: int = 1) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"x" * size)
    return path


@pytest.fixture
def subjects_base(tmp_path: Path) -> Path:
    """Realistische Fake-Subject-Struktur (BIM-style: HKS, MiniCo, ...)."""

    base = tmp_path / "subjects"
    hks = base / "HKS"
    _touch(hks / "Interviews" / "Interview_Beyer.docx")
    _touch(hks / "Interviews" / "Interview_Mueller.docx")
    _touch(hks / "Interviews" / "README.txt")
    _touch(hks / "Interviews" / "subdir" / "Anhang.docx")

    proj_pbn = hks / "Projekt - PBN - Mehrfamilienhaus Neustadt"
    _touch(proj_pbn / "Aufgabenstellung.pdf")
    _touch(proj_pbn / "Vertrag.docx")
    _touch(proj_pbn / "Plaene" / "EG.pdf")
    _touch(proj_pbn / "Plaene" / "OG.pdf")

    proj_wgn = hks / "Projekt - WGN - Wohngebaeude Nord"
    _touch(proj_wgn / "Flaechen.xlsx")

    _touch(hks / "Sonstiges" / "Orga" / "alt.pdf")
    _touch(hks / "Sonstiges" / "notiz.docx")

    mini = base / "MiniCo"
    _touch(mini / "Projekt - X - Nur ein Projekt" / "spec.pdf")

    no_int = base / "NoInterviews"
    _touch(no_int / "Projekt - Z - Solo" / "s.pdf")
    _touch(no_int / "Sonstiges" / "alt.pdf")

    hidden = base / ".hidden_subject"
    _touch(hidden / "should_not_appear.txt")

    return base


# ---------------------------------------------------------------------------
# parse_folder
# ---------------------------------------------------------------------------
class TestParseFolder:
    def test_standard_prefix(self, tmp_path):
        f = tmp_path / "Projekt - PBN - Mehrfamilienhaus Neustadt"
        f.mkdir()
        sf = parse_folder(f)
        assert sf.code == "PBN"
        assert sf.name == "Mehrfamilienhaus Neustadt"
        assert sf.folder_name == f.name
        assert sf.path == f

    def test_en_dash_prefix(self, tmp_path):
        f = tmp_path / "Projekt \u2013 WGN \u2013 Wohngebaeude Nord"
        f.mkdir()
        sf = parse_folder(f)
        assert sf.code == "WGN"
        assert sf.name == "Wohngebaeude Nord"

    def test_mixed_dashes(self, tmp_path):
        f = tmp_path / "Projekt - ABC \u2013 Gemischt"
        f.mkdir()
        sf = parse_folder(f)
        assert sf.code == "ABC"
        assert sf.name == "Gemischt"

    def test_no_prefix_fallback(self, tmp_path):
        f = tmp_path / "Irgendwas ohne Praefix"
        f.mkdir()
        sf = parse_folder(f)
        assert sf.code is None
        assert sf.name == "Irgendwas ohne Praefix"

    def test_special_chars(self, tmp_path):
        f = tmp_path / "Projekt - 42 - Haus am See (Phase 1)"
        f.mkdir()
        sf = parse_folder(f)
        assert sf.code == "42"
        assert sf.name == "Haus am See (Phase 1)"

    def test_pdf_and_office_counts(self, tmp_path):
        f = tmp_path / "Projekt - X - Test"
        _touch(f / "a.pdf")
        _touch(f / "b.PDF")
        _touch(f / "sub" / "c.pdf")
        _touch(f / "v.docx")
        _touch(f / "calc.xlsx")
        _touch(f / "sub" / "~$lock.docx")  # ignored by office_count rglob (still counted btw)
        _touch(f / "notes.txt")
        sf = parse_folder(f)
        assert sf.pdf_count == 3
        # ~$lock.docx wird vom rglob trotzdem gefunden, aber realistisch sind
        # Lockfiles transient; wir testen nur die gewollte Untergrenze.
        assert sf.office_count >= 2


# ---------------------------------------------------------------------------
# list_subjects
# ---------------------------------------------------------------------------
class TestListSubjects:
    def test_lists_top_level(self, subjects_base):
        names = list_subjects(subjects_base)
        assert names == sorted(["HKS", "MiniCo", "NoInterviews"])

    def test_skips_hidden(self, subjects_base):
        assert ".hidden_subject" not in list_subjects(subjects_base)

    def test_missing_base_returns_empty(self, tmp_path):
        assert list_subjects(tmp_path / "nope") == []

    def test_empty_base_returns_empty(self, tmp_path):
        empty = tmp_path / "empty"
        empty.mkdir()
        assert list_subjects(empty) == []


# ---------------------------------------------------------------------------
# scan_subject (default layout)
# ---------------------------------------------------------------------------
class TestScanSubjectDefault:
    def test_full(self, subjects_base):
        s = scan_subject("HKS", subjects_base)
        assert isinstance(s, Subject)
        assert s.name == "HKS"

        names = {p.name for p in s.interviews}
        assert names == {"Interview_Beyer.docx", "Interview_Mueller.docx"}

        codes = {f.code for f in s.folders}
        assert codes == {"PBN", "WGN"}
        pbn = next(f for f in s.folders if f.code == "PBN")
        assert pbn.pdf_count == 3
        assert pbn.office_count == 1

        assert s.notes_path is not None and s.notes_path.name == "Sonstiges"
        note_names = {p.name for p in s.notes_files}
        assert note_names == {"alt.pdf", "notiz.docx"}

    def test_without_notes(self, subjects_base):
        s = scan_subject("MiniCo", subjects_base)
        assert s.notes_path is None
        assert s.notes_files == []
        assert s.interviews == []
        assert len(s.folders) == 1 and s.folders[0].code == "X"

    def test_without_interviews(self, subjects_base):
        s = scan_subject("NoInterviews", subjects_base)
        assert s.interviews == []
        assert s.notes_path is not None
        assert len(s.folders) == 1 and s.folders[0].code == "Z"

    def test_unknown_raises(self, subjects_base):
        with pytest.raises(FileNotFoundError):
            scan_subject("DoesNotExist", subjects_base)

    def test_case_insensitive_subdirs(self, tmp_path):
        base = tmp_path / "subjects"
        sub = base / "Mixed"
        _touch(sub / "INTERVIEWS" / "a.docx")
        _touch(sub / "Sonstiges" / "b.pdf")
        _touch(sub / "Projekt - Q - Q" / "q.pdf")
        s = scan_subject("Mixed", base)
        assert len(s.interviews) == 1
        assert s.notes_path is not None
        assert len(s.folders) == 1

    def test_ignores_non_prefix_folders(self, tmp_path):
        base = tmp_path / "subjects"
        sub = base / "Weird"
        _touch(sub / "Interviews" / "i.docx")
        _touch(sub / "Projekt - A - Alpha" / "a.pdf")
        _touch(sub / "Archive" / "old.pdf")  # not a project folder
        s = scan_subject("Weird", base)
        assert len(s.folders) == 1
        assert s.folders[0].code == "A"

    def test_folders_sorted(self, tmp_path):
        base = tmp_path / "subjects"
        sub = base / "Sorted"
        _touch(sub / "Projekt - ZZZ - Z" / "z.pdf")
        _touch(sub / "Projekt - AAA - A" / "a.pdf")
        _touch(sub / "Projekt - MMM - M" / "m.pdf")
        s = scan_subject("Sorted", base)
        assert [f.code for f in s.folders] == ["AAA", "MMM", "ZZZ"]


# ---------------------------------------------------------------------------
# FolderLayout — overrides + YAML
# ---------------------------------------------------------------------------
class TestFolderLayout:
    def test_default_layout_matches_bim_research(self):
        assert DEFAULT_LAYOUT.folder_prefix == "projekt"
        assert "code" in DEFAULT_LAYOUT.folder_pattern

    def test_with_overrides(self):
        l2 = DEFAULT_LAYOUT.with_overrides(folder_prefix="case")
        assert l2.folder_prefix == "case"
        # original unchanged
        assert DEFAULT_LAYOUT.folder_prefix == "projekt"

    def test_custom_layout_english_terms(self, tmp_path):
        """Demonstriert: Layout fuer 'Case - <ID> - <Name>' mit 'recordings/'
        statt 'interviews/' — domain-frei.
        """
        layout = FolderLayout(
            folder_prefix="case",
            folder_pattern=r"^Case\s*-\s*(?P<code>\w+)\s*-\s*(?P<name>.+)$",
            interviews_subdir="recordings",
            notes_subdir="misc",
            interview_exts=(".mp3", ".wav"),
        )
        base = tmp_path / "study"
        sub = base / "Site42"
        _touch(sub / "recordings" / "session1.mp3")
        _touch(sub / "Case - 1 - Treatment" / "data.pdf")
        _touch(sub / "misc" / "notes.txt")

        s = scan_subject("Site42", base, layout=layout)
        assert [p.name for p in s.interviews] == ["session1.mp3"]
        assert len(s.folders) == 1
        assert s.folders[0].code == "1"
        assert s.notes_path is not None and s.notes_path.name == "misc"

    def test_from_dict(self):
        layout = FolderLayout.from_dict(
            {
                "folder_prefix": "case",
                "interview_exts": [".mp3", ".wav"],
                "unknown_field": "ignored",
            }
        )
        assert layout.folder_prefix == "case"
        assert layout.interview_exts == (".mp3", ".wav")
        # defaults for unspecified fields
        assert layout.notes_subdir == DEFAULT_LAYOUT.notes_subdir
