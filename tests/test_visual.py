"""Tests fuer die visuelle Analyse-Pipeline (Phase 2 + Phase 4)."""

import json
from unittest.mock import MagicMock

import fitz
import pytest

from qualdatan_core.coding.visual import (
    TriageResult,
    DetailResult,
    ElementDetail,
    VisualAnalysisResult,
    render_page_thumbnail,
    estimate_image_tokens,
    run_localisation,
    _element_to_codes,
    _triage_to_codes,
    _triage_from_dict,
    _detail_from_dict,
    _is_valid_bbox,
)


# ---------------------------------------------------------------------------
# Fixtures: Test-PDFs
# ---------------------------------------------------------------------------

@pytest.fixture
def plan_pdf(tmp_path):
    """PDF mit Zeichnungen (simuliert Grundriss)."""
    pdf_path = tmp_path / "Testprojekt" / "eg_grundriss.pdf"
    pdf_path.parent.mkdir(parents=True, exist_ok=True)

    doc = fitz.open()
    page = doc.new_page(width=1190, height=842)  # A3 Querformat

    # Linien zeichnen
    shape = page.new_shape()
    for i in range(0, 800, 20):
        shape.draw_line((100 + i, 100), (100 + i, 700))
        shape.draw_line((100, 100 + i * 0.75), (900, 100 + i * 0.75))
    shape.finish(color=(0, 0, 0), width=0.5)
    shape.commit()

    page.insert_text((800, 750), "Massstab: 1:100", fontsize=8, fontname="helv")

    doc.save(str(pdf_path))
    doc.close()
    return pdf_path


@pytest.fixture
def multi_page_pdf(tmp_path):
    """PDF mit mehreren Seiten verschiedener Typen."""
    pdf_path = tmp_path / "Testprojekt" / "mixed.pdf"
    pdf_path.parent.mkdir(parents=True, exist_ok=True)

    doc = fitz.open()

    # Seite 1: Text
    page = doc.new_page(width=595, height=842)
    for i in range(20):
        page.insert_text((72, 72 + i * 30), f"Textzeile {i+1}", fontsize=11)

    # Seite 2: Plan (Querformat)
    page = doc.new_page(width=1190, height=842)
    shape = page.new_shape()
    for i in range(0, 600, 15):
        shape.draw_line((50 + i, 50), (50 + i, 700))
    shape.finish(color=(0, 0, 0), width=0.3)
    shape.commit()

    # Seite 3: Foto-artig (grosses Rechteck)
    page = doc.new_page(width=595, height=842)
    shape = page.new_shape()
    shape.draw_rect(fitz.Rect(20, 20, 575, 780))
    shape.finish(fill=(0.5, 0.3, 0.1))
    shape.commit()

    doc.save(str(pdf_path))
    doc.close()
    return pdf_path


# ---------------------------------------------------------------------------
# Thumbnail-Rendering Tests
# ---------------------------------------------------------------------------

class TestThumbnailRendering:
    def test_render_thumbnail_returns_base64(self, plan_pdf):
        doc = fitz.open(str(plan_pdf))
        b64 = render_page_thumbnail(doc[0], dpi=72)
        doc.close()

        assert isinstance(b64, str)
        assert len(b64) > 100
        # Sollte valides Base64 sein
        import base64
        raw = base64.b64decode(b64)
        # PNG Header
        assert raw[:4] == b"\x89PNG"

    def test_render_thumbnail_higher_dpi_is_larger(self, plan_pdf):
        doc = fitz.open(str(plan_pdf))
        b64_72 = render_page_thumbnail(doc[0], dpi=72)
        b64_150 = render_page_thumbnail(doc[0], dpi=150)
        doc.close()

        assert len(b64_150) > len(b64_72)

    def test_estimate_image_tokens(self):
        # 100KB base64 -> ~75KB raw -> ~25K tokens
        b64 = "A" * 100_000
        tokens = estimate_image_tokens(b64)
        assert tokens > 0
        assert tokens >= 200  # Minimum

    def test_estimate_tokens_minimum(self):
        tokens = estimate_image_tokens("abc")
        assert tokens == 200  # Minimum threshold


# ---------------------------------------------------------------------------
# Code-Mapping Tests
# ---------------------------------------------------------------------------

class TestCodeMapping:
    def test_element_to_codes_wall(self):
        elem = ElementDetail(
            element_type="Tragende Wand",
            ifc_class="IfcWall",
            log_achieved="LOG-03",
            log_evidence="Mehrschichtiger Aufbau",
            visible_parameters=["Wanddicke", "Material"],
        )
        codes = _element_to_codes(elem)
        assert "O-01" in codes  # Tragende Bauteile
        assert "Q-01" in codes  # LOG-Evidenz
        assert "Q-02" in codes  # LOI-Evidenz (visible_parameters)

    def test_element_to_codes_door(self):
        elem = ElementDetail(
            element_type="Tuer",
            ifc_class="IfcDoor",
            log_achieved="LOG-02",
        )
        codes = _element_to_codes(elem)
        assert "O-03" in codes  # Oeffnungen

    def test_element_to_codes_stair(self):
        elem = ElementDetail(element_type="Treppe", ifc_class="IfcStair")
        codes = _element_to_codes(elem)
        assert "O-04" in codes

    def test_element_to_codes_roof(self):
        elem = ElementDetail(element_type="Dachkonstruktion")
        codes = _element_to_codes(elem)
        assert "O-05" in codes

    def test_element_to_codes_no_log(self):
        elem = ElementDetail(element_type="Fenster")
        codes = _element_to_codes(elem)
        assert "O-03" in codes
        assert "Q-01" not in codes  # Kein LOG

    def test_element_to_codes_tga(self):
        elem = ElementDetail(element_type="Heizungsrohr", log_achieved="LOG-02")
        codes = _element_to_codes(elem)
        assert "O-07" in codes

    def test_triage_to_codes_floor_plan(self):
        triage = TriageResult(
            page=1,
            page_type="floor_plan",
            building_elements=["Waende", "Tueren", "Fenster"],
            estimated_log="LOG-03",
        )
        codes = _triage_to_codes(triage)
        assert "P-01" in codes   # Grundriss
        assert "O-01" in codes   # Waende
        assert "O-03" in codes   # Tueren/Fenster
        assert "Q-01" in codes   # LOG

    def test_triage_to_codes_section(self):
        triage = TriageResult(page=1, page_type="section")
        codes = _triage_to_codes(triage)
        assert "P-02" in codes

    def test_triage_to_codes_elevation(self):
        triage = TriageResult(page=1, page_type="elevation")
        codes = _triage_to_codes(triage)
        assert "P-03" in codes

    def test_triage_to_codes_no_elements(self):
        triage = TriageResult(page=1, page_type="text")
        codes = _triage_to_codes(triage)
        # text hat kein P-Code Mapping
        assert not any(c.startswith("P-") for c in codes)


# ---------------------------------------------------------------------------
# Datenstruktur Tests
# ---------------------------------------------------------------------------

class TestDataStructures:
    def test_triage_result_to_dict(self):
        t = TriageResult(
            page=1, page_type="floor_plan",
            building_elements=["Waende", "Tueren"],
            estimated_log="LOG-03", priority="high",
            description="EG Grundriss", confidence=0.9,
        )
        d = t.to_dict()
        assert d["page"] == 1
        assert d["page_type"] == "floor_plan"
        assert d["priority"] == "high"
        assert "Waende" in d["building_elements"]

    def test_detail_result_to_dict(self):
        d = DetailResult(
            page=1,
            building_elements=[
                ElementDetail(
                    element_type="Wand", ifc_class="IfcWall",
                    log_achieved="LOG-03",
                )
            ],
            annotations=["Bemassung"],
        )
        result = d.to_dict()
        assert result["page"] == 1
        assert len(result["building_elements"]) == 1
        assert result["building_elements"][0]["ifc_class"] == "IfcWall"

    def test_visual_analysis_result_to_dict(self):
        result = VisualAnalysisResult(
            file="test.pdf", project="P", page_count=2,
            triage=[TriageResult(page=1, page_type="floor_plan", priority="high")],
            details=[DetailResult(page=1)],
            token_usage=5000,
        )
        d = result.to_dict()
        assert d["file"] == "test.pdf"
        assert d["page_count"] == 2
        assert len(d["triage"]) == 1
        assert len(d["details"]) == 1
        assert d["token_usage"] == 5000

    def test_triage_from_dict_roundtrip(self):
        original = TriageResult(
            page=3, page_type="section",
            building_elements=["Decken", "Waende"],
            estimated_log="LOG-02", priority="medium",
            description="Laengsschnitt", confidence=0.85,
        )
        d = original.to_dict()
        restored = _triage_from_dict(d)
        assert restored.page == original.page
        assert restored.page_type == original.page_type
        assert restored.priority == original.priority
        assert restored.building_elements == original.building_elements

    def test_detail_from_dict_roundtrip(self):
        original = DetailResult(
            page=2,
            building_elements=[
                ElementDetail(
                    element_type="Fenster", ifc_class="IfcWindow",
                    log_achieved="LOG-02", log_evidence="Vereinfacht",
                    visible_parameters=["Breite"], region="top-left",
                )
            ],
            annotations=["Raumstempel"],
            cross_references=["Schnitt A-A"],
            description="OG Grundriss",
        )
        d = original.to_dict()
        restored = _detail_from_dict(d)
        assert restored.page == 2
        assert len(restored.building_elements) == 1
        assert restored.building_elements[0].ifc_class == "IfcWindow"
        assert restored.annotations == ["Raumstempel"]


# ---------------------------------------------------------------------------
# visual_codings() Tests
# ---------------------------------------------------------------------------

class TestVisualCodings:
    def test_visual_codings_from_detail(self):
        result = VisualAnalysisResult(
            file="plan.pdf", project="P", page_count=1,
            triage=[TriageResult(page=1, page_type="floor_plan", priority="high")],
            details=[DetailResult(
                page=1,
                building_elements=[
                    ElementDetail(
                        element_type="Tragende Wand", ifc_class="IfcWall",
                        log_achieved="LOG-03",
                        log_evidence="Mehrschichtiger Wandaufbau",
                        visible_parameters=["Wanddicke"],
                    ),
                    ElementDetail(
                        element_type="Tuer", ifc_class="IfcDoor",
                        log_achieved="LOG-02",
                    ),
                ],
            )],
        )
        codings = result.visual_codings()
        assert len(codings) == 2
        # Wand-Kodierung
        assert "O-01" in codings[0]["codes"]
        assert "Q-01" in codings[0]["codes"]
        assert codings[0]["block_id"].startswith("p1_v")
        assert codings[0]["source"] == "visual_detail"
        # Tuer-Kodierung
        assert "O-03" in codings[1]["codes"]

    def test_visual_codings_from_triage_only(self):
        result = VisualAnalysisResult(
            file="plan.pdf", project="P", page_count=1,
            triage=[TriageResult(
                page=1, page_type="floor_plan", priority="medium",
                building_elements=["Waende", "Fenster"],
                estimated_log="LOG-02",
                description="Grundriss EG",
            )],
            details=[],  # Kein Detail-Pass
        )
        codings = result.visual_codings()
        assert len(codings) == 1
        assert "P-01" in codings[0]["codes"]
        assert "O-01" in codings[0]["codes"]  # Waende
        assert codings[0]["source"] == "visual_triage"

    def test_visual_codings_skip_priority(self):
        result = VisualAnalysisResult(
            file="plan.pdf", project="P", page_count=1,
            triage=[TriageResult(page=1, page_type="text", priority="skip")],
        )
        codings = result.visual_codings()
        assert len(codings) == 0

    def test_visual_codings_block_ids_unique(self):
        result = VisualAnalysisResult(
            file="plan.pdf", project="P", page_count=2,
            triage=[
                TriageResult(page=1, page_type="floor_plan", priority="high",
                             building_elements=["Waende"]),
                TriageResult(page=2, page_type="section", priority="high",
                             building_elements=["Decken"]),
            ],
            details=[
                DetailResult(page=1, building_elements=[
                    ElementDetail(element_type="Wand", log_achieved="LOG-03"),
                ]),
                DetailResult(page=2, building_elements=[
                    ElementDetail(element_type="Decke", log_achieved="LOG-02"),
                ]),
            ],
        )
        codings = result.visual_codings()
        block_ids = [c["block_id"] for c in codings]
        assert len(block_ids) == len(set(block_ids))  # Alle unique


# ---------------------------------------------------------------------------
# QDPX Visual Selections Tests (Phase 4)
# ---------------------------------------------------------------------------

from qualdatan_core.qdpx.merger import (
    create_new_project, add_visual_sources, extract_codesystem,
    write_qdpx, read_qdpx,
)


class TestQdpxVisualSelections:
    def test_add_visual_sources_creates_codes(self):
        project = create_new_project()
        visual_results = [{
            "file": "eg_grundriss.pdf",
            "project": "Testprojekt",
            "page_dimensions": {1: (1190, 842)},
            "description": "Grundriss EG, 1:100",
            "visual_codings": [{
                "block_id": "p1_v0",
                "page": 1,
                "codes": ["O-01", "P-01", "Q-01"],
                "description": "Tragende Waende, LOG-03",
            }],
        }]

        code_guids = add_visual_sources(project, visual_results)

        # Alle O/P/Q Codes sollten erstellt sein
        assert "O-01" in code_guids
        assert "P-01" in code_guids
        assert "Q-01" in code_guids

    def test_add_visual_sources_creates_pdf_source(self):
        project = create_new_project()
        visual_results = [{
            "file": "schnitt.pdf",
            "project": "P",
            "page_dimensions": {1: (595, 842)},
            "visual_codings": [{
                "block_id": "p1_v0",
                "page": 1,
                "codes": ["P-02"],
                "description": "Laengsschnitt durch Gebaeude",
            }],
        }]

        add_visual_sources(project, visual_results)

        ns = ""
        sources = project.findall(f"{ns}Sources/{ns}PDFSource")
        # create_new_project hat keine Sources, nur wir fuegen hinzu
        pdf_sources = [s for s in sources if s.get("name", "").startswith("schnitt")]
        assert len(pdf_sources) == 1
        assert "internal://P/schnitt.pdf" in pdf_sources[0].get("path", "")

    def test_visual_selection_has_full_page_bbox(self):
        project = create_new_project()
        visual_results = [{
            "file": "plan.pdf",
            "project": "P",
            "page_dimensions": {1: (1190, 842)},
            "visual_codings": [{
                "block_id": "p1_v0",
                "page": 1,
                "codes": ["O-01"],
                "description": "Wand",
            }],
        }]

        add_visual_sources(project, visual_results)

        # PDFSelection finden
        ns = ""
        selections = project.findall(f".//{ns}PDFSelection")
        assert len(selections) >= 1

        sel = selections[0]
        assert sel.get("firstX") == "0"
        assert sel.get("firstY") == "0"
        assert sel.get("secondX") == "1190"
        assert sel.get("secondY") == "842"
        # REFI-QDA: 0-basierter Index (page=1 -> "0")
        assert sel.get("page") == "0"

    def test_visual_selection_has_description(self):
        project = create_new_project()
        visual_results = [{
            "file": "plan.pdf",
            "project": "P",
            "page_dimensions": {1: (595, 842)},
            "visual_codings": [{
                "block_id": "p1_v0",
                "page": 1,
                "codes": ["O-03"],
                "description": "Fenster LOG-02, vereinfachte Darstellung",
            }],
        }]

        add_visual_sources(project, visual_results)

        ns = ""
        selections = project.findall(f".//{ns}PDFSelection")
        assert len(selections) >= 1

        # Description-Element pruefen
        desc = selections[0].find(f"{ns}Description")
        assert desc is not None
        assert "Fenster" in desc.text

    def test_visual_selection_has_coding_with_code_ref(self):
        project = create_new_project()
        visual_results = [{
            "file": "plan.pdf",
            "project": "P",
            "page_dimensions": {1: (595, 842)},
            "visual_codings": [{
                "block_id": "p1_v0",
                "page": 1,
                "codes": ["O-01"],
                "description": "Wand",
            }],
        }]

        code_guids = add_visual_sources(project, visual_results)

        ns = ""
        coding = project.find(f".//{ns}Coding")
        assert coding is not None
        code_ref = coding.find(f"{ns}CodeRef")
        assert code_ref is not None
        assert code_ref.get("targetGUID") == code_guids["O-01"]

    def test_multiple_codes_per_selection(self):
        project = create_new_project()
        visual_results = [{
            "file": "plan.pdf",
            "project": "P",
            "page_dimensions": {1: (595, 842)},
            "visual_codings": [{
                "block_id": "p1_v0",
                "page": 1,
                "codes": ["O-01", "Q-01"],
                "description": "Wand mit LOG-03",
            }],
        }]

        add_visual_sources(project, visual_results)

        ns = ""
        selections = project.findall(f".//{ns}PDFSelection")
        # Jeder Code bekommt eine eigene Selection
        assert len(selections) == 2

    def test_write_and_read_qdpx_with_visual(self, tmp_path):
        project = create_new_project()
        visual_results = [{
            "file": "eg.pdf",
            "project": "P",
            "page_dimensions": {1: (1190, 842)},
            "description": "EG Grundriss",
            "visual_codings": [
                {
                    "block_id": "p1_v0",
                    "page": 1,
                    "codes": ["O-01", "P-01", "Q-01"],
                    "description": "Waende LOG-03",
                },
                {
                    "block_id": "p1_v1",
                    "page": 1,
                    "codes": ["O-03"],
                    "description": "Tueren",
                },
            ],
        }]

        add_visual_sources(project, visual_results)

        # Schreiben
        output = tmp_path / "test.qdpx"
        write_qdpx(project, output)
        assert output.exists()

        # Lesen und pruefen
        read_project, _ = read_qdpx(output)
        categories, codes = extract_codesystem(read_project)

        assert "O" in categories
        assert "P" in categories
        assert "Q" in categories
        assert "O-01" in codes
        assert "P-01" in codes
        assert "Q-01" in codes
        assert "O-03" in codes

    def test_empty_visual_codings_skipped(self):
        project = create_new_project()
        visual_results = [{
            "file": "empty.pdf",
            "project": "P",
            "page_dimensions": {},
            "visual_codings": [],
        }]

        add_visual_sources(project, visual_results)

        ns = ""
        sources_elem = project.find(f"{ns}Sources")
        pdf_sources = sources_elem.findall(f"{ns}PDFSource") if sources_elem is not None else []
        assert len(pdf_sources) == 0

    def test_existing_codes_reused(self):
        project = create_new_project()

        # Erste Runde: Codes anlegen
        result1 = [{
            "file": "a.pdf", "project": "P",
            "page_dimensions": {1: (595, 842)},
            "visual_codings": [{"block_id": "p1_v0", "page": 1, "codes": ["O-01"], "description": ""}],
        }]
        guids1 = add_visual_sources(project, result1)

        # Zweite Runde: gleiche Codes wiederverwenden
        _, existing_codes = extract_codesystem(project)
        result2 = [{
            "file": "b.pdf", "project": "P",
            "page_dimensions": {1: (595, 842)},
            "visual_codings": [{"block_id": "p1_v0", "page": 1, "codes": ["O-01"], "description": ""}],
        }]
        guids2 = add_visual_sources(project, result2, existing_codes)

        # GUID sollte identisch sein (Code wiederverwendet)
        assert guids1["O-01"] == guids2["O-01"]


# ---------------------------------------------------------------------------
# Pass 3 (Localisation) Tests
# ---------------------------------------------------------------------------

class TestElementDetailBbox:
    def test_element_detail_has_bbox_field(self):
        # Default: None
        elem = ElementDetail(element_type="Wand")
        assert elem.bbox is None

        # Setzbar
        elem.bbox = [0.1, 0.2, 0.5, 0.6]
        assert elem.bbox == [0.1, 0.2, 0.5, 0.6]

    def test_element_detail_bbox_roundtrip_via_detail_from_dict(self):
        original = DetailResult(
            page=1,
            building_elements=[
                ElementDetail(
                    element_type="Wand",
                    ifc_class="IfcWall",
                    log_achieved="LOG-03",
                    bbox=[0.1, 0.2, 0.5, 0.6],
                ),
                ElementDetail(
                    element_type="Tuer",
                    ifc_class="IfcDoor",
                    # bewusst ohne bbox
                ),
            ],
        )
        d = original.to_dict()
        assert d["building_elements"][0]["bbox"] == [0.1, 0.2, 0.5, 0.6]
        assert d["building_elements"][1]["bbox"] is None

        restored = _detail_from_dict(d)
        assert restored.building_elements[0].bbox == [0.1, 0.2, 0.5, 0.6]
        assert restored.building_elements[1].bbox is None


class TestIsValidBbox:
    def test_valid_bbox(self):
        assert _is_valid_bbox([0.1, 0.2, 0.5, 0.6]) is True

    def test_valid_bbox_full_page(self):
        assert _is_valid_bbox([0.0, 0.0, 1.0, 1.0]) is True

    def test_valid_bbox_ints(self):
        # 0/1 ints sollen akzeptiert werden
        assert _is_valid_bbox([0, 0, 1, 1]) is True

    def test_invalid_bbox_none(self):
        assert _is_valid_bbox(None) is False

    def test_invalid_bbox_wrong_length(self):
        assert _is_valid_bbox([0.1, 0.2, 0.5]) is False
        assert _is_valid_bbox([0.1, 0.2, 0.5, 0.6, 0.7]) is False

    def test_invalid_bbox_out_of_range(self):
        assert _is_valid_bbox([-0.1, 0.2, 0.5, 0.6]) is False
        assert _is_valid_bbox([0.1, 0.2, 1.5, 0.6]) is False

    def test_invalid_bbox_inverted(self):
        # x0 >= x1
        assert _is_valid_bbox([0.5, 0.2, 0.1, 0.6]) is False
        # y0 >= y1
        assert _is_valid_bbox([0.1, 0.6, 0.5, 0.2]) is False

    def test_invalid_bbox_degenerate(self):
        # Null-flaeche
        assert _is_valid_bbox([0.1, 0.2, 0.1, 0.6]) is False
        assert _is_valid_bbox([0.1, 0.2, 0.5, 0.2]) is False

    def test_invalid_bbox_not_a_list(self):
        assert _is_valid_bbox("not a list") is False
        assert _is_valid_bbox(42) is False

    def test_invalid_bbox_non_numeric(self):
        assert _is_valid_bbox([0.1, "foo", 0.5, 0.6]) is False


class TestVisualCodingsWithBbox:
    def test_visual_codings_with_bbox_no_dims(self):
        """Ohne page_dimensions bleibt die bestehende Ausgabe unveraendert."""
        result = VisualAnalysisResult(
            file="plan.pdf", project="P", page_count=1,
            triage=[TriageResult(page=1, page_type="floor_plan", priority="high")],
            details=[DetailResult(
                page=1,
                building_elements=[
                    ElementDetail(
                        element_type="Tragende Wand",
                        log_achieved="LOG-03",
                        bbox=[0.1, 0.2, 0.5, 0.6],
                    ),
                ],
            )],
        )
        codings = result.visual_codings()  # kein page_dimensions
        assert len(codings) == 1
        assert "bbox" not in codings[0]

    def test_visual_codings_with_bbox_and_dims(self):
        """Mit bbox und Dimensionen werden PDF-Punkte berechnet."""
        result = VisualAnalysisResult(
            file="plan.pdf", project="P", page_count=1,
            triage=[TriageResult(page=1, page_type="floor_plan", priority="high")],
            details=[DetailResult(
                page=1,
                building_elements=[
                    ElementDetail(
                        element_type="Tragende Wand",
                        log_achieved="LOG-03",
                        bbox=[0.1, 0.2, 0.5, 0.6],
                    ),
                ],
            )],
        )
        codings = result.visual_codings(page_dimensions={1: (1000, 800)})
        assert len(codings) == 1
        assert codings[0]["bbox"] == [100.0, 160.0, 500.0, 480.0]

    def test_visual_codings_mixed(self):
        """Einige Elemente mit bbox, einige ohne - nur erstere bekommen bbox."""
        result = VisualAnalysisResult(
            file="plan.pdf", project="P", page_count=1,
            triage=[TriageResult(page=1, page_type="floor_plan", priority="high")],
            details=[DetailResult(
                page=1,
                building_elements=[
                    ElementDetail(
                        element_type="Tragende Wand",
                        log_achieved="LOG-03",
                        bbox=[0.1, 0.2, 0.5, 0.6],
                    ),
                    ElementDetail(
                        element_type="Tuer",
                        log_achieved="LOG-02",
                        # keine bbox
                    ),
                    ElementDetail(
                        element_type="Fenster",
                        log_achieved="LOG-02",
                        bbox=[0.7, 0.1, 0.9, 0.3],
                    ),
                ],
            )],
        )
        codings = result.visual_codings(page_dimensions={1: (1000, 800)})
        assert len(codings) == 3
        assert codings[0]["bbox"] == [100.0, 160.0, 500.0, 480.0]
        assert "bbox" not in codings[1]
        assert codings[2]["bbox"] == [700.0, 80.0, 900.0, 240.0]

    def test_visual_codings_invalid_bbox_skipped(self):
        """Ungueltige bbox-Werte landen nicht in der Ausgabe."""
        result = VisualAnalysisResult(
            file="plan.pdf", project="P", page_count=1,
            triage=[TriageResult(page=1, page_type="floor_plan", priority="high")],
            details=[DetailResult(
                page=1,
                building_elements=[
                    ElementDetail(
                        element_type="Wand",
                        log_achieved="LOG-03",
                        bbox=[0.5, 0.6, 0.1, 0.2],  # invertiert
                    ),
                ],
            )],
        )
        codings = result.visual_codings(page_dimensions={1: (1000, 800)})
        assert len(codings) == 1
        assert "bbox" not in codings[0]


class TestDbVisualDetailBboxRoundtrip:
    def test_db_visual_detail_roundtrip_preserves_bbox(self, tmp_path):
        from src.db import PipelineDB

        db = PipelineDB(tmp_path / "test.db")
        pid = db.upsert_pdf("P", "a.pdf", "P/a.pdf", "/p/a.pdf")

        elem = ElementDetail(
            element_type="Wand",
            ifc_class="IfcWall",
            log_achieved="LOG-03",
            bbox=[0.1, 0.2, 0.5, 0.6],
        )
        detail = DetailResult(page=1, building_elements=[elem])

        # Ueber to_dict in die DB speichern - so wie die echte Pipeline das macht
        db.save_visual_detail(pid, 1, detail.to_dict())

        # Laden und pruefen
        results = db.get_visual_detail(pid)
        assert len(results) == 1
        loaded_elements = results[0]["building_elements"]
        assert len(loaded_elements) == 1
        assert loaded_elements[0]["bbox"] == [0.1, 0.2, 0.5, 0.6]

        # Auch der _detail_from_dict-Pfad (Cache) muss die bbox rekonstruieren
        restored = _detail_from_dict({
            "page": 1,
            "building_elements": loaded_elements,
            "annotations": results[0]["annotations"],
            "cross_references": results[0]["cross_references"],
            "description": results[0]["description"],
        })
        assert restored.building_elements[0].bbox == [0.1, 0.2, 0.5, 0.6]


class TestRunLocalisation:
    def test_run_localisation_parses_response(self, plan_pdf):
        """Pass 3 parst die Modellantwort und setzt bboxes an den richtigen Indizes."""
        # Fake client: antwortet mit einem JSON-Objekt wie im Prompt beschrieben.
        fake_response = MagicMock()
        fake_response.content = [MagicMock()]
        fake_response.content[0].text = json.dumps({
            "page": 1,
            "elements": [
                {"index": 0, "bbox": [0.1, 0.05, 0.9, 0.4]},
                # Index 1 bewusst weggelassen -> bleibt None
            ],
        })
        fake_response.usage = MagicMock(input_tokens=100, output_tokens=50)

        client = MagicMock()
        client.messages.create.return_value = fake_response

        doc = fitz.open(str(plan_pdf))
        detail = DetailResult(
            page=1,
            building_elements=[
                ElementDetail(element_type="Tragende Wand", ifc_class="IfcWall"),
                ElementDetail(element_type="Tuer", ifc_class="IfcDoor"),
            ],
        )

        results, tokens = run_localisation(
            doc, [detail], client=client, model="claude-sonnet-4-20250514",
            max_tokens_budget=100000,
        )
        doc.close()

        assert tokens == 150
        assert len(results) == 1
        assert results[0].building_elements[0].bbox == [0.1, 0.05, 0.9, 0.4]
        assert results[0].building_elements[1].bbox is None
        # API wurde genau einmal aufgerufen
        assert client.messages.create.call_count == 1

    def test_run_localisation_rejects_invalid_bbox(self, plan_pdf):
        """Ungueltige bbox-Koordinaten im Response werden ignoriert."""
        fake_response = MagicMock()
        fake_response.content = [MagicMock()]
        fake_response.content[0].text = json.dumps({
            "page": 1,
            "elements": [
                # Inverted: x0 > x1
                {"index": 0, "bbox": [0.9, 0.05, 0.1, 0.4]},
                # Out of range
                {"index": 1, "bbox": [0.1, 0.05, 1.5, 0.4]},
            ],
        })
        fake_response.usage = MagicMock(input_tokens=100, output_tokens=50)

        client = MagicMock()
        client.messages.create.return_value = fake_response

        doc = fitz.open(str(plan_pdf))
        detail = DetailResult(
            page=1,
            building_elements=[
                ElementDetail(element_type="Wand"),
                ElementDetail(element_type="Tuer"),
            ],
        )

        results, _ = run_localisation(
            doc, [detail], client=client, max_tokens_budget=100000,
        )
        doc.close()

        assert results[0].building_elements[0].bbox is None
        assert results[0].building_elements[1].bbox is None

    def test_run_localisation_skips_pages_without_elements(self, plan_pdf):
        """Seiten ohne Bauelemente verursachen keinen API-Call."""
        client = MagicMock()

        doc = fitz.open(str(plan_pdf))
        detail = DetailResult(page=1, building_elements=[])

        results, tokens = run_localisation(
            doc, [detail], client=client, max_tokens_budget=100000,
        )
        doc.close()

        assert tokens == 0
        assert client.messages.create.call_count == 0
        assert len(results) == 1

    def test_run_localisation_cache_roundtrip(self, plan_pdf, tmp_path):
        """Cache-Miss schreibt bboxes, Cache-Hit laedt sie wieder."""
        fake_response = MagicMock()
        fake_response.content = [MagicMock()]
        fake_response.content[0].text = json.dumps({
            "page": 1,
            "elements": [{"index": 0, "bbox": [0.2, 0.3, 0.6, 0.7]}],
        })
        fake_response.usage = MagicMock(input_tokens=100, output_tokens=50)

        client = MagicMock()
        client.messages.create.return_value = fake_response

        cache_dir = tmp_path / "cache"
        cache_key = "test_plan"

        doc = fitz.open(str(plan_pdf))
        detail = DetailResult(
            page=1,
            building_elements=[ElementDetail(element_type="Wand")],
        )

        # Erster Aufruf: API wird benutzt
        results1, tokens1 = run_localisation(
            doc, [detail], client=client, max_tokens_budget=100000,
            cache_dir=cache_dir, cache_key=cache_key,
        )
        assert tokens1 == 150
        assert results1[0].building_elements[0].bbox == [0.2, 0.3, 0.6, 0.7]
        assert client.messages.create.call_count == 1

        # Zweiter Aufruf: Cache-Hit, kein neuer API-Call
        detail2 = DetailResult(
            page=1,
            building_elements=[ElementDetail(element_type="Wand")],
        )
        results2, tokens2 = run_localisation(
            doc, [detail2], client=client, max_tokens_budget=100000,
            cache_dir=cache_dir, cache_key=cache_key,
        )
        doc.close()

        assert tokens2 == 0
        assert results2[0].building_elements[0].bbox == [0.2, 0.3, 0.6, 0.7]
        assert client.messages.create.call_count == 1  # unveraendert
