"""Tests fuer die PDF-Klassifikation."""

import fitz
import pytest

from qualdatan_core.pdf_classifier import (
    _compute_page_metrics,
    _classify_page_local,
    _detect_title_block,
    _aggregate_document_type,
    classify_document,
    split_by_type,
    PageClassification,
    DocumentClassification,
)


# ---------------------------------------------------------------------------
# Helpers: Test-PDFs erzeugen
# ---------------------------------------------------------------------------

@pytest.fixture
def text_pdf(tmp_path):
    """PDF mit viel Fliesstext (simuliert Aufgabenstellung)."""
    pdf_path = tmp_path / "Testprojekt" / "aufgabenstellung.pdf"
    pdf_path.parent.mkdir(parents=True)

    doc = fitz.open()
    for page_idx in range(3):
        page = doc.new_page(width=595, height=842)  # A4
        y = 72
        for line_idx in range(30):
            page.insert_text(
                (72, y),
                f"Dies ist Zeile {line_idx + 1} auf Seite {page_idx + 1}. "
                f"Das Bauvorhaben umfasst den Neubau eines Einfamilienhauses "
                f"mit insgesamt 150 Quadratmetern Wohnflaeche.",
                fontsize=11, fontname="helv",
            )
            y += 20
    doc.save(str(pdf_path))
    doc.close()
    return pdf_path


@pytest.fixture
def plan_pdf(tmp_path):
    """PDF mit Zeichnungen und Schriftfeld (simuliert Grundriss)."""
    pdf_path = tmp_path / "Testprojekt" / "eg_grundriss.pdf"
    pdf_path.parent.mkdir(parents=True, exist_ok=True)

    doc = fitz.open()
    # A3 Querformat
    page = doc.new_page(width=1190, height=842)

    # Viele Linien zeichnen (simuliert Grundriss)
    shape = page.new_shape()
    for i in range(0, 800, 20):
        shape.draw_line((100 + i, 100), (100 + i, 700))
        shape.draw_line((100, 100 + i * 0.75), (900, 100 + i * 0.75))
    shape.finish(color=(0, 0, 0), width=0.5)
    shape.commit()

    # Schriftfeld unten rechts
    page.insert_text((800, 750), "Massstab: 1:100", fontsize=8, fontname="helv")
    page.insert_text((800, 765), "Plan-Nr.: A-01-EG-001", fontsize=8, fontname="helv")
    page.insert_text((800, 780), "Bauherr: Familie Mueller", fontsize=8, fontname="helv")
    page.insert_text((800, 795), "Datum: 15.03.2026", fontsize=8, fontname="helv")
    page.insert_text((800, 810), "Gewerk: Architektur", fontsize=8, fontname="helv")

    doc.save(str(pdf_path))
    doc.close()
    return pdf_path


@pytest.fixture
def photo_pdf(tmp_path):
    """PDF mit grossem Bild und wenig Text (simuliert Fotodoku)."""
    pdf_path = tmp_path / "Testprojekt" / "fotos.pdf"
    pdf_path.parent.mkdir(parents=True, exist_ok=True)

    doc = fitz.open()
    page = doc.new_page(width=595, height=842)

    # Grosses farbiges Rechteck als "Foto-Ersatz"
    shape = page.new_shape()
    shape.draw_rect(fitz.Rect(20, 20, 575, 780))
    shape.finish(color=(0.3, 0.5, 0.2), fill=(0.3, 0.5, 0.2))
    shape.commit()

    # Minimaler Text (Bildunterschrift)
    page.insert_text((72, 810), "Abb. 1", fontsize=9, fontname="helv")

    doc.save(str(pdf_path))
    doc.close()
    return pdf_path


@pytest.fixture
def empty_pdf(tmp_path):
    """Leere PDF-Seite."""
    pdf_path = tmp_path / "Testprojekt" / "leer.pdf"
    pdf_path.parent.mkdir(parents=True, exist_ok=True)

    doc = fitz.open()
    doc.new_page(width=595, height=842)
    doc.save(str(pdf_path))
    doc.close()
    return pdf_path


# ---------------------------------------------------------------------------
# Seitenmetriken Tests
# ---------------------------------------------------------------------------

class TestPageMetrics:
    def test_text_pdf_metrics(self, text_pdf):
        doc = fitz.open(str(text_pdf))
        metrics = _compute_page_metrics(doc[0])
        doc.close()

        assert metrics["text_char_count"] > 200
        assert metrics["text_coverage"] > 0.1
        assert metrics["page_format"] == "A4"
        assert not metrics["is_landscape"]

    def test_plan_pdf_metrics(self, plan_pdf):
        doc = fitz.open(str(plan_pdf))
        metrics = _compute_page_metrics(doc[0])
        doc.close()

        assert metrics["drawing_count"] >= 1  # Shape-Operationen
        assert metrics["is_landscape"]
        assert metrics["text_char_count"] < 300  # wenig Text
        # A3 Querformat
        assert metrics["page_format"] == "A3"

    def test_empty_pdf_metrics(self, empty_pdf):
        doc = fitz.open(str(empty_pdf))
        metrics = _compute_page_metrics(doc[0])
        doc.close()

        assert metrics["text_char_count"] == 0
        assert metrics["drawing_count"] == 0
        assert metrics["text_coverage"] == 0.0

    def test_aspect_ratio(self, plan_pdf):
        doc = fitz.open(str(plan_pdf))
        metrics = _compute_page_metrics(doc[0])
        doc.close()

        # A3 Querformat: 1190/842 ≈ 1.41
        assert metrics["aspect_ratio"] > 1.3


# ---------------------------------------------------------------------------
# Schriftfeld-Erkennung Tests
# ---------------------------------------------------------------------------

class TestTitleBlock:
    def test_detect_title_block_in_plan(self, plan_pdf):
        doc = fitz.open(str(plan_pdf))
        has_tb, metadata = _detect_title_block(doc[0])
        doc.close()

        assert has_tb is True
        assert "massstab" in metadata
        assert metadata["massstab"] == "1:100"
        assert "plan_nr" in metadata
        assert metadata["plan_nr"] == "A-01-EG-001"

    def test_no_title_block_in_text(self, text_pdf):
        doc = fitz.open(str(text_pdf))
        has_tb, metadata = _detect_title_block(doc[0])
        doc.close()

        assert has_tb is False

    def test_no_title_block_in_empty(self, empty_pdf):
        doc = fitz.open(str(empty_pdf))
        has_tb, metadata = _detect_title_block(doc[0])
        doc.close()

        assert has_tb is False

    def test_datum_extraction(self, plan_pdf):
        doc = fitz.open(str(plan_pdf))
        _, metadata = _detect_title_block(doc[0])
        doc.close()

        assert "datum" in metadata
        assert "15.03.2026" in metadata["datum"]


# ---------------------------------------------------------------------------
# Lokale Klassifikation Tests
# ---------------------------------------------------------------------------

class TestLocalClassification:
    def test_text_page(self):
        metrics = {
            "text_coverage": 0.5,
            "image_coverage": 0.0,
            "text_char_count": 2000,
            "drawing_count": 0,
            "is_landscape": False,
            "page_format": "A4",
        }
        page_type, conf, subtype = _classify_page_local(metrics, False)
        assert page_type == "text"
        assert conf >= 0.85

    def test_plan_with_title_block(self):
        metrics = {
            "text_coverage": 0.05,
            "image_coverage": 0.1,
            "text_char_count": 50,
            "drawing_count": 200,
            "is_landscape": True,
            "page_format": "A3",
        }
        page_type, conf, subtype = _classify_page_local(metrics, True)
        assert page_type == "plan"
        assert conf >= 0.85

    def test_plan_by_drawings(self):
        metrics = {
            "text_coverage": 0.02,
            "image_coverage": 0.0,
            "text_char_count": 30,
            "drawing_count": 150,
            "is_landscape": True,
            "page_format": "A1",
        }
        page_type, conf, subtype = _classify_page_local(metrics, False)
        assert page_type == "plan"
        assert conf >= 0.7

    def test_photo_page(self):
        metrics = {
            "text_coverage": 0.01,
            "image_coverage": 0.8,
            "text_char_count": 10,
            "drawing_count": 0,
            "is_landscape": False,
            "page_format": "A4",
        }
        page_type, conf, subtype = _classify_page_local(metrics, False)
        assert page_type == "photo"
        assert conf >= 0.7

    def test_mixed_page(self):
        metrics = {
            "text_coverage": 0.15,
            "image_coverage": 0.15,
            "text_char_count": 100,
            "drawing_count": 15,
            "is_landscape": False,
            "page_format": "A4",
        }
        page_type, conf, subtype = _classify_page_local(metrics, False)
        # Low confidence or mixed
        assert conf < 0.8 or page_type == "mixed"

    def test_large_format_hints_plan(self):
        metrics = {
            "text_coverage": 0.03,
            "image_coverage": 0.0,
            "text_char_count": 80,
            "drawing_count": 25,
            "is_landscape": True,
            "page_format": "A1",
        }
        page_type, conf, subtype = _classify_page_local(metrics, False)
        assert page_type == "plan"


# ---------------------------------------------------------------------------
# Dokument-Level Aggregation Tests
# ---------------------------------------------------------------------------

class TestDocumentAggregation:
    def test_all_text_pages(self):
        pages = [
            PageClassification(page=i, page_type="text", confidence=0.9)
            for i in range(1, 6)
        ]
        doc_type, conf = _aggregate_document_type(pages)
        assert doc_type == "text"
        assert conf >= 0.8

    def test_all_plan_pages(self):
        pages = [
            PageClassification(page=i, page_type="plan", confidence=0.85)
            for i in range(1, 4)
        ]
        doc_type, conf = _aggregate_document_type(pages)
        assert doc_type == "plan"

    def test_mixed_document(self):
        pages = [
            PageClassification(page=1, page_type="text", confidence=0.9),
            PageClassification(page=2, page_type="plan", confidence=0.8),
            PageClassification(page=3, page_type="photo", confidence=0.7),
        ]
        doc_type, conf = _aggregate_document_type(pages)
        assert doc_type == "mixed"

    def test_dominant_type_wins(self):
        pages = [
            PageClassification(page=1, page_type="plan", confidence=0.9),
            PageClassification(page=2, page_type="plan", confidence=0.8),
            PageClassification(page=3, page_type="plan", confidence=0.85),
            PageClassification(page=4, page_type="text", confidence=0.9),
        ]
        doc_type, conf = _aggregate_document_type(pages)
        assert doc_type == "plan"  # 75% plan

    def test_empty_pages(self):
        doc_type, conf = _aggregate_document_type([])
        assert doc_type == "text"
        assert conf == 0.0


# ---------------------------------------------------------------------------
# End-to-End Klassifikation Tests
# ---------------------------------------------------------------------------

class TestClassifyDocument:
    def test_classify_text_document(self, text_pdf):
        cls = classify_document(text_pdf, mode="local")
        assert cls.document_type == "text"
        assert cls.page_count == 3
        assert cls.confidence >= 0.7
        assert all(p.page_type == "text" for p in cls.pages)

    def test_classify_plan_document(self, plan_pdf):
        cls = classify_document(plan_pdf, mode="local")
        assert cls.document_type == "plan"
        assert cls.page_count == 1
        # Schriftfeld sollte erkannt sein
        assert cls.title_block_metadata.get("massstab") == "1:100"

    def test_to_dict_roundtrip(self, text_pdf):
        cls = classify_document(text_pdf, mode="local")
        d = cls.to_dict()
        assert d["document_type"] == cls.document_type
        assert len(d["pages"]) == cls.page_count

    def test_summary_string(self, text_pdf):
        cls = classify_document(text_pdf, mode="local")
        s = cls.summary()
        assert "text" in s.lower()

    def test_invalid_mode_raises(self, text_pdf):
        with pytest.raises(ValueError, match="Unbekannter Modus"):
            classify_document(text_pdf, mode="invalid")


# ---------------------------------------------------------------------------
# split_by_type Tests
# ---------------------------------------------------------------------------

class TestSplitByType:
    def test_split_groups_correctly(self):
        pdfs = [
            {"relative_path": "P/a.pdf"},
            {"relative_path": "P/b.pdf"},
            {"relative_path": "P/c.pdf"},
        ]
        classifications = {
            "P/a.pdf": DocumentClassification(
                file="a.pdf", document_type="text",
                confidence=0.9, page_count=1,
            ),
            "P/b.pdf": DocumentClassification(
                file="b.pdf", document_type="plan",
                confidence=0.8, page_count=1,
            ),
            "P/c.pdf": DocumentClassification(
                file="c.pdf", document_type="photo",
                confidence=0.7, page_count=1,
            ),
        }
        groups = split_by_type(pdfs, classifications)
        assert len(groups["text"]) == 1
        assert len(groups["plan"]) == 1
        assert len(groups["photo"]) == 1

    def test_unclassified_defaults_to_text(self):
        pdfs = [{"relative_path": "P/unknown.pdf"}]
        groups = split_by_type(pdfs, {})
        assert len(groups["text"]) == 1


# ---------------------------------------------------------------------------
# Caching Tests
# ---------------------------------------------------------------------------

class TestClassificationCache:
    def test_cache_write_and_read(self, text_pdf, tmp_path):
        from src.pdf_classifier import classify_project_pdfs

        pdfs = [{
            "path": str(text_pdf),
            "relative_path": "Testprojekt/aufgabenstellung.pdf",
            "project": "Testprojekt",
            "filename": "aufgabenstellung.pdf",
        }]

        cache_dir = tmp_path / ".cache"

        # Erster Durchlauf: klassifiziert
        results1 = classify_project_pdfs(pdfs, mode="local", cache_dir=cache_dir)
        assert len(results1) == 1

        # Zweiter Durchlauf: aus Cache
        results2 = classify_project_pdfs(pdfs, mode="local", cache_dir=cache_dir)
        assert len(results2) == 1

        key = list(results1.keys())[0]
        assert results1[key].document_type == results2[key].document_type
