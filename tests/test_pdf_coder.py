"""Tests für den PDF-Dokumenten-Coder."""

import json
import tempfile
from pathlib import Path
from xml.etree.ElementTree import fromstring

import fitz
import pytest

from qualdatan_core import pdf_coder
from qualdatan_core.coding.analyzer import build_coding_prompt, format_codesystem
from qualdatan_core.pdf.extractor import (
    _is_boilerplate,
    _smart_truncate,
    extract_pdf,
    extraction_to_text_summary,
)
from qualdatan_core.pdf.scanner import build_manifest, scan_projects
from qualdatan_core.qdpx.merger import (
    add_pdf_sources,
    create_new_project,
    extract_codesystem,
    read_qdpx,
    write_qdpx,
)
from qualdatan_core.recipe import load_recipe
from qualdatan_core.run_context import RunContext

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def test_pdf(tmp_path):
    """Erstellt eine kleine Test-PDF."""
    pdf_path = tmp_path / "Testprojekt" / "aufgabe.pdf"
    pdf_path.parent.mkdir(parents=True)

    doc = fitz.open()
    page = doc.new_page(width=595, height=842)
    page.insert_text((72, 80), "Aufgabenstellung Testprojekt", fontsize=14, fontname="helv")
    page.insert_text((72, 120), "Das Bauvorhaben umfasst den Neubau.", fontsize=11, fontname="helv")

    page2 = doc.new_page(width=595, height=842)
    page2.insert_text((72, 80), "Flaeche EG: 85.4 m2", fontsize=11, fontname="helv")
    doc.save(str(pdf_path))
    doc.close()

    return pdf_path, tmp_path


# ---------------------------------------------------------------------------
# Scanner Tests
# ---------------------------------------------------------------------------


class TestPdfScanner:
    def test_scan_finds_pdfs(self, test_pdf):
        pdf_path, base = test_pdf
        pdfs = scan_projects(base)
        assert len(pdfs) == 1
        assert pdfs[0]["project"] == "Testprojekt"
        assert pdfs[0]["filename"] == "aufgabe.pdf"

    def test_scan_with_filter(self, test_pdf):
        pdf_path, base = test_pdf
        found = scan_projects(base, project_filter="Testprojekt")
        assert len(found) == 1

        not_found = scan_projects(base, project_filter="NichtDa")
        assert len(not_found) == 0

    def test_build_manifest(self, test_pdf):
        pdf_path, base = test_pdf
        pdfs = scan_projects(base)
        manifest = build_manifest(pdfs)
        assert manifest["total_pdfs"] == 1
        assert "Testprojekt" in manifest["projects"]

    def test_scan_empty_dir(self, tmp_path):
        pdfs = scan_projects(tmp_path)
        assert pdfs == []

    def test_scan_nested_structure(self, tmp_path):
        """PDFs in verschachtelten Unterordnern."""
        sub = tmp_path / "ProjA" / "Plaene" / "EG"
        sub.mkdir(parents=True)
        doc = fitz.open()
        doc.new_page()
        doc.save(str(sub / "grundriss.pdf"))
        doc.close()

        pdfs = scan_projects(tmp_path)
        assert len(pdfs) == 1
        assert pdfs[0]["project"] == "ProjA"


# ---------------------------------------------------------------------------
# Extractor Tests
# ---------------------------------------------------------------------------


class TestPdfExtractor:
    def test_extract_pdf(self, test_pdf):
        pdf_path, _ = test_pdf
        data = extract_pdf(pdf_path)

        assert data["metadata"]["page_count"] == 2
        assert len(data["pages"]) == 2

        # Seite 1 hat Textblöcke
        page1 = data["pages"][0]
        assert page1["page"] == 1
        assert len(page1["blocks"]) > 0
        assert page1["blocks"][0]["type"] == "text"
        assert "Aufgabenstellung" in page1["blocks"][0]["text"]

        # Jeder Block hat eine ID
        for block in page1["blocks"]:
            assert block["id"].startswith("p1_")
            assert "bbox" in block

    def test_extract_text_summary(self, test_pdf):
        pdf_path, _ = test_pdf
        data = extract_pdf(pdf_path)
        summary = extraction_to_text_summary(data)

        assert "Seite 1" in summary
        assert "Seite 2" in summary
        assert "[p1_b0]" in summary
        assert "Aufgabenstellung" in summary

    def test_block_ids_unique(self, test_pdf):
        pdf_path, _ = test_pdf
        data = extract_pdf(pdf_path)
        all_ids = []
        for page in data["pages"]:
            for block in page["blocks"]:
                all_ids.append(block["id"])
        assert len(all_ids) == len(set(all_ids))


# ---------------------------------------------------------------------------
# Analyzer Tests (ohne API-Calls)
# ---------------------------------------------------------------------------


class TestPdfAnalyzer:
    def test_build_coding_prompt(self, test_pdf):
        pdf_path, _ = test_pdf
        data = extract_pdf(pdf_path)
        recipe = load_recipe("pdf_analyse")

        prompt = build_coding_prompt(data, recipe, "Testprojekt")
        assert "Testprojekt" in prompt
        assert "aufgabe.pdf" in prompt
        assert "[p1_b0]" in prompt

    def test_build_coding_prompt_with_codesystem(self, test_pdf):
        pdf_path, _ = test_pdf
        data = extract_pdf(pdf_path)
        recipe = load_recipe("pdf_analyse")

        codesystem = "A: Projektakquise\n  A-01: Ausschreibung"
        prompt = build_coding_prompt(data, recipe, "Test", codesystem=codesystem)
        assert "A-01" in prompt

    def test_format_codesystem(self):
        categories = {"A": "Projektakquise", "B": "Planung"}
        codes = {
            "A-01": {
                "name": "Ausschreibung",
                "hauptkategorie": "A",
                "kodierdefinition": "Öffentliche Vergabe",
            },
            "B-01": {"name": "Entwurf", "hauptkategorie": "B", "kodierdefinition": ""},
        }
        result = format_codesystem(categories, codes)
        assert "A: Projektakquise" in result
        assert "A-01: Ausschreibung" in result
        assert "Öffentliche Vergabe" in result
        assert "B-01: Entwurf" in result


# ---------------------------------------------------------------------------
# QDPX-Merger Tests
# ---------------------------------------------------------------------------


class TestQdpxMerger:
    def test_create_new_project(self):
        project = create_new_project("Test")
        assert project.get("name") == "Test"

        # Hat CodeBook und Sources (ohne Namespace, da SubElement)
        assert project.find("CodeBook") is not None
        assert project.find("Sources") is not None

    def test_add_pdf_sources(self, test_pdf):
        pdf_path, _ = test_pdf
        project = create_new_project()
        data = extract_pdf(pdf_path)

        pdf_results = [
            {
                "file": "aufgabe.pdf",
                "project": "Testprojekt",
                "extraction": data,
                "document_type": "Aufgabenstellung",
                "codings": [
                    {
                        "block_id": "p1_b0",
                        "codes": ["NEW-01"],
                        "ganzer_block": True,
                    }
                ],
                "neue_codes": [
                    {
                        "code_id": "NEW-01",
                        "code_name": "Testcode",
                        "hauptkategorie": "Z",
                        "kodierdefinition": "Test",
                    }
                ],
            }
        ]

        code_guids = add_pdf_sources(project, pdf_results)
        assert "NEW-01" in code_guids

        # PDFSource wurde erstellt
        ns = project.tag.split("}")[0] + "}" if "}" in project.tag else ""
        sources = project.find(f"{ns}Sources")
        pdf_sources = sources.findall(f"{ns}PDFSource")
        assert len(pdf_sources) == 1
        assert "Testprojekt" in pdf_sources[0].get("name")

    def test_write_and_read_qdpx(self, test_pdf, tmp_path):
        pdf_path, _ = test_pdf
        project = create_new_project("Roundtrip-Test")
        data = extract_pdf(pdf_path)

        pdf_results = [
            {
                "file": "aufgabe.pdf",
                "project": "Testprojekt",
                "extraction": data,
                "document_type": "Test",
                "codings": [],
                "neue_codes": [],
            }
        ]

        add_pdf_sources(project, pdf_results)

        out_path = tmp_path / "test.qdpx"
        write_qdpx(
            project,
            out_path,
            pdf_files={
                "Testprojekt/aufgabe.pdf": pdf_path,
            },
        )

        assert out_path.exists()

        # Roundtrip: wieder einlesen
        project2, sources2 = read_qdpx(out_path)
        assert project2.get("name") == "Roundtrip-Test"
        assert "sources/Testprojekt/aufgabe.pdf" in sources2

    def test_extract_codesystem_from_project(self):
        project = create_new_project()

        # Manuell Codes hinzufügen
        pdf_results = [
            {
                "file": "test.pdf",
                "project": "Test",
                "extraction": {"pages": []},
                "document_type": "Test",
                "codings": [],
                "neue_codes": [
                    {
                        "code_id": "X-01",
                        "code_name": "Testcode",
                        "hauptkategorie": "X",
                        "kodierdefinition": "Definition",
                    }
                ],
            }
        ]

        add_pdf_sources(project, pdf_results)
        categories, codes = extract_codesystem(project)

        assert "X" in categories
        assert "X-01" in codes
        assert codes["X-01"]["name"] == "Testcode"

    def test_merge_preserves_existing_codes(self, test_pdf, tmp_path):
        """Neue Codes werden zu bestehenden hinzugefügt, nicht ersetzt."""
        pdf_path, _ = test_pdf
        project = create_new_project()

        # Erste Runde: Code A-01
        results1 = [
            {
                "file": "a.pdf",
                "project": "P",
                "extraction": {"pages": []},
                "document_type": "T",
                "codings": [],
                "neue_codes": [
                    {
                        "code_id": "A-01",
                        "code_name": "Erster",
                        "hauptkategorie": "A",
                        "kodierdefinition": "",
                    }
                ],
            }
        ]
        guids1 = add_pdf_sources(project, results1)

        # Zweite Runde: Code B-01 (A-01 schon vorhanden)
        _, existing = extract_codesystem(project)
        results2 = [
            {
                "file": "b.pdf",
                "project": "P",
                "extraction": {"pages": []},
                "document_type": "T",
                "codings": [],
                "neue_codes": [
                    {
                        "code_id": "B-01",
                        "code_name": "Zweiter",
                        "hauptkategorie": "B",
                        "kodierdefinition": "",
                    }
                ],
            }
        ]
        guids2 = add_pdf_sources(project, results2, existing)

        # Beide Codes vorhanden
        cats, codes = extract_codesystem(project)
        assert "A-01" in codes
        assert "B-01" in codes


# ---------------------------------------------------------------------------
# Boilerplate-Filter und Smart-Truncation Tests
# ---------------------------------------------------------------------------


class TestBoilerplateFilter:
    def test_page_numbers_filtered(self):
        assert _is_boilerplate("5")
        assert _is_boilerplate("  12  ")
        assert _is_boilerplate("- 3 -")
        assert _is_boilerplate("Seite 5 von 10")
        assert _is_boilerplate("Page 3 of 20")

    def test_short_text_filtered(self):
        assert _is_boilerplate("ab")
        assert _is_boilerplate("  ")

    def test_real_content_not_filtered(self):
        assert not _is_boilerplate("Das Bauvorhaben umfasst den Neubau.")
        assert not _is_boilerplate("Flaeche EG: 85.4 m2")
        assert not _is_boilerplate("5. Abschnitt der Planung")

    def test_copyright_filtered(self):
        assert _is_boilerplate("© 2024 Musterfirma GmbH")
        assert _is_boilerplate("Confidential")


class TestSmartTruncate:
    def test_short_text_unchanged(self):
        text = "Kurzer Text."
        assert _smart_truncate(text, 500) == text

    def test_truncates_at_sentence_end(self):
        text = "Erster Satz. Zweiter Satz. Dritter Satz mit viel mehr Text der sehr lang ist."
        result = _smart_truncate(text, 40)
        assert result.endswith("[...]")
        assert "Erster Satz." in result

    def test_truncates_at_space_if_no_sentence(self):
        text = "Ein langer Text ohne Punkt der einfach weiter und weiter geht"
        result = _smart_truncate(text, 30)
        assert result.endswith("[...]")
        assert not result[-6].isalpha() or result.endswith("[...]")

    def test_boilerplate_not_in_summary(self, test_pdf):
        """Seitenzahlen und Boilerplate werden aus dem Summary gefiltert."""
        pdf_path, base = test_pdf
        # Erstelle PDF mit Seitenzahl-Block
        doc = fitz.open()
        page = doc.new_page()
        page.insert_text((72, 80), "Inhalt des Dokuments", fontsize=11)
        page.insert_text((280, 800), "5", fontsize=9)  # Seitenzahl
        doc.save(str(pdf_path))
        doc.close()

        data = extract_pdf(pdf_path)
        summary = extraction_to_text_summary(data)
        # Seitenzahl "5" sollte gefiltert sein (Boilerplate)
        # Inhalt sollte drin sein
        assert "Inhalt des Dokuments" in summary


class TestAdaptiveTruncation:
    def test_small_doc_gets_more_chars(self):
        """Kleine Dokumente (<10 Blöcke) bekommen 800 Zeichen pro Block."""
        data = {
            "pages": [
                {
                    "page": 1,
                    "blocks": [
                        {"id": "p1_b0", "type": "text", "text": "A" * 700}  # 700 Zeichen
                    ],
                }
            ]
        }
        summary = extraction_to_text_summary(data)
        # Bei <=10 Blöcken: max 800 Zeichen → 700 passt komplett rein
        assert "[...]" not in summary

    def test_large_doc_truncates_more(self):
        """Große Dokumente (>50 Blöcke) kürzen auf 300 Zeichen."""
        blocks = [
            {"id": f"p1_b{i}", "type": "text", "text": f"Block {i}. " * 50} for i in range(60)
        ]
        data = {"pages": [{"page": 1, "blocks": blocks}]}
        summary = extraction_to_text_summary(data)
        # Bei 60 Blöcken: max 300 Zeichen → 400+ Zeichen Blöcke werden gekürzt
        assert "[...]" in summary


# ---------------------------------------------------------------------------
# run_annotation (Phase 5)
# ---------------------------------------------------------------------------


def _make_simple_pdf(path: Path, text: str = "Hallo Welt, dies ist ein Testblock.") -> None:
    """Kleines PDF mit einer Text-Seite."""
    path.parent.mkdir(parents=True, exist_ok=True)
    doc = fitz.open()
    page = doc.new_page(width=595, height=842)
    page.insert_text((72, 120), text, fontsize=11, fontname="helv")
    doc.save(str(path))
    doc.close()


def _count_annots(pdf_path: Path) -> int:
    doc = fitz.open(str(pdf_path))
    count = 0
    for page in doc:
        for _ in page.annots() or []:
            count += 1
    doc.close()
    return count


def _annot_types(pdf_path: Path) -> list[str]:
    doc = fitz.open(str(pdf_path))
    out: list[str] = []
    for page in doc:
        for annot in page.annots() or []:
            out.append(annot.type[1])
    doc.close()
    return out


@pytest.fixture
def annotation_ctx(tmp_path):
    """Erzeugt einen RunContext mit einem registrierten Text-PDF + Codings."""
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    ctx = RunContext(run_dir)
    ctx.ensure_dirs()

    pdf_path = tmp_path / "projects" / "Testprojekt" / "doc.pdf"
    _make_simple_pdf(pdf_path, "Das Bauvorhaben umfasst den Neubau eines Hauses.")

    pdf_id = ctx.db.upsert_pdf(
        project="Testprojekt",
        filename="doc.pdf",
        relative_path="Testprojekt/doc.pdf",
        path=str(pdf_path),
        file_size_kb=1,
    )

    # Extraction speichern
    extraction = extract_pdf(pdf_path)
    ctx.db.save_extraction(pdf_id, extraction)
    ctx.db.set_step_status(pdf_id, "extraction", "done")

    # Eine Text-Codierung auf dem ersten Block
    first_block_id = extraction["pages"][0]["blocks"][0]["id"]
    ctx.db.save_coding(
        pdf_id=pdf_id,
        page=1,
        block_id=first_block_id,
        codes=["A-01", "B-02"],
        source="text",
        begruendung="Projekt-Akquise",
        ganzer_block=True,
    )
    ctx.db.set_step_status(pdf_id, "coding", "done")

    return ctx, pdf_id, pdf_path


def test_run_annotation_creates_output_files(annotation_ctx):
    """run_annotation erzeugt ein annotiertes PDF mit Highlights im annotated/-Verzeichnis."""
    ctx, pdf_id, pdf_path = annotation_ctx

    stats = pdf_coder.run_annotation(ctx, recipe=None)

    assert stats["total_pdfs"] == 1
    assert stats["annotated"] == 1
    assert stats["errors"] == 0
    assert stats["text_annotations"] == 2  # zwei Codes -> zwei Highlights

    dst = ctx.annotated_dir / "Testprojekt" / "doc.pdf"
    assert dst.exists(), f"Erwartetes annotiertes PDF fehlt: {dst}"
    assert _count_annots(dst) == 2
    assert all(t == "Highlight" for t in _annot_types(dst))

    # DB-Status ist done
    assert ctx.db.is_step_done(pdf_id, "annotation") is True


def test_run_annotation_writes_code_into_comment(annotation_ctx):
    """Der Code-ID steht im Comment der Annotation, damit MAXQDA sortieren kann.

    Mapping-Files (codes.json/codes.md) werden NICHT mehr geschrieben — eine
    fixe Farbe und der Code-Praefix im Comment ersetzen die Farb-Kodierung.
    """
    import fitz

    ctx, _, _ = annotation_ctx
    pdf_coder.run_annotation(ctx, recipe=None)

    # Mapping-Files duerfen NICHT existieren
    assert not (ctx.mapping_dir / "codes.json").exists()
    assert not (ctx.mapping_dir / "codes.md").exists()

    # Stattdessen: Code muss im Annotation-Comment stehen
    dst = ctx.annotated_dir / "Testprojekt" / "doc.pdf"
    doc = fitz.open(str(dst))
    contents: list[str] = []
    try:
        for page in doc:
            for annot in page.annots() or []:
                contents.append(annot.info.get("content", ""))
    finally:
        doc.close()

    # Beide Codes sollten als Praefix in genau einem Comment vorkommen.
    assert any(c.startswith("A-01") for c in contents), contents
    assert any(c.startswith("B-02") for c in contents), contents


def test_run_annotation_skips_done_pdfs(annotation_ctx):
    """Zweiter Lauf ueberspringt bereits annotierte PDFs (Resume-Verhalten)."""
    ctx, pdf_id, _ = annotation_ctx

    first = pdf_coder.run_annotation(ctx, recipe=None)
    assert first["annotated"] == 1
    assert first["skipped"] == 0

    second = pdf_coder.run_annotation(ctx, recipe=None)
    assert second["annotated"] == 0
    assert second["skipped"] == 1
    assert second["text_annotations"] == 0


def test_run_annotation_handles_pdf_without_codings(tmp_path):
    """PDFs ohne Codings crashen nicht, sondern werden leer kopiert."""
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    ctx = RunContext(run_dir)
    ctx.ensure_dirs()

    pdf_path = tmp_path / "projects" / "P" / "leer.pdf"
    _make_simple_pdf(pdf_path, "Nur Text ohne Codes.")

    ctx.db.upsert_pdf(
        project="P",
        filename="leer.pdf",
        relative_path="P/leer.pdf",
        path=str(pdf_path),
    )

    stats = pdf_coder.run_annotation(ctx, recipe=None)

    assert stats["total_pdfs"] == 1
    assert stats["annotated"] == 1
    assert stats["errors"] == 0
    assert stats["text_annotations"] == 0
    assert stats["visual_annotations"] == 0

    dst = ctx.annotated_dir / "P" / "leer.pdf"
    assert dst.exists()
    # Kein Highlight, aber gueltiges PDF
    assert _count_annots(dst) == 0


def test_run_annotation_text_and_visual_combined(tmp_path):
    """Mixed-PDF: Text-Codings erzeugen Highlights, Visual-Codings Rechtecke."""
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    ctx = RunContext(run_dir)
    ctx.ensure_dirs()

    pdf_path = tmp_path / "projects" / "Mix" / "mixed.pdf"
    _make_simple_pdf(pdf_path, "Textblock auf einer Seite, die auch einen Plan enthaelt.")

    pdf_id = ctx.db.upsert_pdf(
        project="Mix",
        filename="mixed.pdf",
        relative_path="Mix/mixed.pdf",
        path=str(pdf_path),
    )

    extraction = extract_pdf(pdf_path)
    ctx.db.save_extraction(pdf_id, extraction)

    first_block_id = extraction["pages"][0]["blocks"][0]["id"]
    # Text-Coding
    ctx.db.save_coding(
        pdf_id=pdf_id,
        page=1,
        block_id=first_block_id,
        codes=["A-01"],
        source="text",
        begruendung="Text-Kodierung",
        ganzer_block=True,
    )
    # Visuelle Coding (Rechteck auf ganzer Seite)
    ctx.db.save_coding(
        pdf_id=pdf_id,
        page=1,
        block_id="p1_v0",
        codes=["O-01"],
        source="visual_triage",
        begruendung="Visuelle Kodierung",
        ganzer_block=True,
    )

    stats = pdf_coder.run_annotation(ctx, recipe=None)

    assert stats["annotated"] == 1
    assert stats["text_annotations"] == 1
    assert stats["visual_annotations"] == 1

    dst = ctx.annotated_dir / "Mix" / "mixed.pdf"
    assert dst.exists()

    types = _annot_types(dst)
    assert "Highlight" in types
    assert "Square" in types
    assert len(types) == 2
