"""Tests fuer den PDF-Scanner: scan_projects, filter_pdfs."""

from pathlib import Path

import pytest

from qualdatan_core.pdf.scanner import (
    DEFAULT_PLAN_PATTERNS,
    filter_pdfs,
    scan_projects,
)


@pytest.fixture
def sample_pdfs():
    """Liste typischer BOE-PDFs."""
    return [
        {
            "path": "/x/HKS/BOE/Aufgabenstellung/Teilleistungen.pdf",
            "project": "HKS",
            "filename": "Teilleistungen.pdf",
            "relative_path": "HKS/BOE/Aufgabenstellung/Teilleistungen.pdf",
            "size_kb": 14,
        },
        {
            "path": "/x/HKS/BOE/Pläne/LP 4/Arc-4-1_00 Grundriss EG.pdf",
            "project": "HKS",
            "filename": "Arc-4-1_00 Grundriss EG.pdf",
            "relative_path": "HKS/BOE/Pläne/LP 4/Arc-4-1_00 Grundriss EG.pdf",
            "size_kb": 517,
        },
        {
            "path": "/x/HKS/BOE/Pläne/LP 4/Arc-4-2_01 Schnittplan.pdf",
            "project": "HKS",
            "filename": "Arc-4-2_01 Schnittplan.pdf",
            "relative_path": "HKS/BOE/Pläne/LP 4/Arc-4-2_01 Schnittplan.pdf",
            "size_kb": 576,
        },
        {
            "path": "/x/HKS/BOE/Pläne/LP 4/Arc-4-3_01 Ansichten Haus 1.pdf",
            "project": "HKS",
            "filename": "Arc-4-3_01 Ansichten Haus 1.pdf",
            "relative_path": "HKS/BOE/Pläne/LP 4/Arc-4-3_01 Ansichten Haus 1.pdf",
            "size_kb": 1377,
        },
        {
            "path": "/x/HKS/BOE/Pläne/LP 4/Ffl-4-0 Lageplan.pdf",
            "project": "HKS",
            "filename": "Ffl-4-0 Lageplan.pdf",
            "relative_path": "HKS/BOE/Pläne/LP 4/Ffl-4-0 Lageplan.pdf",
            "size_kb": 4480,
        },
        {
            "path": "/x/HKS/BOE/Vertrag.pdf",
            "project": "HKS",
            "filename": "Vertrag.pdf",
            "relative_path": "HKS/BOE/Vertrag.pdf",
            "size_kb": 845,
        },
        {
            "path": "/x/HKS/BOE/Termine/Terminplan.pdf",
            "project": "HKS",
            "filename": "Terminplan.pdf",
            "relative_path": "HKS/BOE/Termine/Terminplan.pdf",
            "size_kb": 38,
        },
    ]


class TestFilterPdfs:
    def test_no_filter_keeps_all(self, sample_pdfs):
        kept, removed = filter_pdfs(sample_pdfs)
        assert len(kept) == len(sample_pdfs)
        assert len(removed) == 0

    def test_skip_plans_removes_plaene_folder(self, sample_pdfs):
        kept, removed = filter_pdfs(sample_pdfs, skip_plans=True)
        # Alle 4 Plaene aus dem "Pläne"-Ordner sollten weg sein
        plan_filenames = [r["filename"] for r in removed]
        assert "Arc-4-1_00 Grundriss EG.pdf" in plan_filenames
        assert "Arc-4-2_01 Schnittplan.pdf" in plan_filenames
        assert "Arc-4-3_01 Ansichten Haus 1.pdf" in plan_filenames
        assert "Ffl-4-0 Lageplan.pdf" in plan_filenames
        # Vertrag und Terminplan bleiben
        assert "Vertrag.pdf" in [k["filename"] for k in kept]
        assert "Terminplan.pdf" in [k["filename"] for k in kept]

    def test_skip_plans_does_not_match_termin(self, sample_pdfs):
        # "Terminplan" enthaelt "plan", darf aber NICHT als Plan klassifiziert
        # werden — der Default-Filter trifft "lageplan|detailplan|fassadenplan"
        # explizit, nicht "plan" allgemein.
        kept, removed = filter_pdfs(sample_pdfs, skip_plans=True)
        kept_names = [k["filename"] for k in kept]
        assert "Terminplan.pdf" in kept_names

    def test_skip_pattern_custom_regex(self, sample_pdfs):
        # Alles mit "Arc-" rausfiltern
        kept, removed = filter_pdfs(sample_pdfs, skip_patterns=[r"Arc-"])
        assert len(removed) == 3  # Grundriss + Schnittplan + Ansichten
        assert all(not k["filename"].startswith("Arc-") for k in kept)

    def test_skip_patterns_combinable(self, sample_pdfs):
        kept, removed = filter_pdfs(
            sample_pdfs,
            skip_plans=True,
            skip_patterns=[r"(?i)vertrag"],
        )
        kept_names = [k["filename"] for k in kept]
        assert "Vertrag.pdf" not in kept_names
        assert "Arc-4-1_00 Grundriss EG.pdf" not in kept_names
        assert "Terminplan.pdf" in kept_names
        assert "Teilleistungen.pdf" in kept_names

    def test_only_patterns_keeps_matching(self, sample_pdfs):
        # Nur PDFs mit "Grundriss"
        kept, removed = filter_pdfs(sample_pdfs, only_patterns=[r"(?i)grundriss"])
        assert len(kept) == 1
        assert kept[0]["filename"] == "Arc-4-1_00 Grundriss EG.pdf"

    def test_only_then_skip(self, sample_pdfs):
        # Erst auf Plaene-Ordner einschraenken, dann Lageplan rauswerfen
        kept, removed = filter_pdfs(
            sample_pdfs,
            only_patterns=[r"Pläne"],
            skip_patterns=[r"Lageplan"],
        )
        kept_names = [k["filename"] for k in kept]
        assert "Arc-4-1_00 Grundriss EG.pdf" in kept_names
        assert "Ffl-4-0 Lageplan.pdf" not in kept_names

    def test_removed_includes_reason(self, sample_pdfs):
        kept, removed = filter_pdfs(sample_pdfs, skip_plans=True)
        for r in removed:
            assert "_filter_reason" in r
            assert r["_filter_reason"]

    def test_default_plan_patterns_not_too_aggressive(self):
        """Plaene-Pattern darf NICHT bei harmlosen Woertern matchen."""
        false_friends = [
            "HKS/Doc/Akquiblatt.pdf",
            "HKS/Doc/Vertrag_planungsleistung.pdf",  # enthaelt "planung" aber kein "grundriss"
            "HKS/Termine/Bauzeitenplan.pdf",  # enthaelt "plan"
            "HKS/Doc/Schnittstellenliste.pdf",  # enthaelt "schnittstelle" aber nicht "schnitt"
        ]
        for path in false_friends:
            matched = any(p.search(path) for p in DEFAULT_PLAN_PATTERNS)
            assert not matched, f"Default plan filter matched harmlessly: {path}"

    def test_default_plan_patterns_match_real_plans(self):
        true_plans = [
            "HKS/BOE/Pläne/LP4/Arc Grundriss EG.pdf",
            "HKS/BOE/Plans/section.pdf",
            "HKS/BOE/Plaene/Schnittplan.pdf",
            "HKS/BOE/Pläne/LP4/Lageplan.pdf",
            "HKS/BOE/Pläne/LP4/Detailplan_03.pdf",
            "HKS/Misc/Aufriss_West.pdf",
        ]
        for path in true_plans:
            matched = any(p.search(path) for p in DEFAULT_PLAN_PATTERNS)
            assert matched, f"Default plan filter missed: {path}"


class TestScanProjects:
    def test_scan_finds_pdfs(self, tmp_path):
        # Setup: ein Projektordner mit zwei PDFs
        (tmp_path / "Projekt1").mkdir()
        (tmp_path / "Projekt1" / "doc1.pdf").write_bytes(b"%PDF-1.4\n")
        (tmp_path / "Projekt1" / "doc2.pdf").write_bytes(b"%PDF-1.4\n")
        (tmp_path / "Projekt2").mkdir()
        (tmp_path / "Projekt2" / "doc3.pdf").write_bytes(b"%PDF-1.4\n")

        results = scan_projects(projects_dir=tmp_path)
        assert len(results) == 3

    def test_project_filter(self, tmp_path):
        (tmp_path / "A").mkdir()
        (tmp_path / "A" / "x.pdf").write_bytes(b"%PDF-1.4\n")
        (tmp_path / "B").mkdir()
        (tmp_path / "B" / "y.pdf").write_bytes(b"%PDF-1.4\n")

        results = scan_projects(projects_dir=tmp_path, project_filter="A")
        assert len(results) == 1
        assert results[0]["project"] == "A"

    def test_convert_office_requires_cache_dir(self, tmp_path):
        with pytest.raises(ValueError, match="convert_cache_dir"):
            scan_projects(projects_dir=tmp_path, convert_office=True)
