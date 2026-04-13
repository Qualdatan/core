"""Tests fuer ``src.pdf_annotator``.

Legt kleine synthetische PDFs mit pymupdf an und prueft, dass die
Annotationen korrekt geschrieben, gefaerbt und wiedergelesen werden.
"""

from pathlib import Path

import fitz
import pytest

from qualdatan_core.pdf_annotator import annotate_text_pdf, annotate_visual_pdf


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def color_map_stub():
    class Stub:
        def get_rgb(self, code_id):
            seed = sum(ord(c) for c in code_id)
            return ((seed % 100) / 100, (seed * 2 % 100) / 100, (seed * 3 % 100) / 100)

        def get_hex(self, code_id):
            r, g, b = self.get_rgb(code_id)
            return f"#{int(r * 255):02x}{int(g * 255):02x}{int(b * 255):02x}"

    return Stub()


def _make_text_pdf(path: Path, pages: list[list[tuple[tuple[float, float], str]]]) -> None:
    """Erzeugt ein PDF mit einer Seite pro Eintrag in ``pages``.

    Jede Seite bekommt eine Liste von ``((x, y), text)`` Tupeln eingefuegt.
    """
    doc = fitz.open()
    for page_items in pages:
        page = doc.new_page(width=595, height=842)
        for (x, y), text in page_items:
            page.insert_text((x, y), text, fontsize=11)
    path.parent.mkdir(parents=True, exist_ok=True)
    doc.save(str(path))
    doc.close()


def _extract_for(pdf_path: Path, blocks: list[dict]) -> dict:
    """Baut ein minimales Extraction-Dict passend zu den Block-Definitionen.

    ``blocks`` ist eine Liste von Dicts mit ``id``, ``page`` (1-basiert),
    ``bbox`` und ``text``.
    """
    doc = fitz.open(str(pdf_path))
    pages = []
    blocks_by_page: dict[int, list[dict]] = {}
    for b in blocks:
        blocks_by_page.setdefault(b["page"], []).append(b)

    for page_idx, page in enumerate(doc):
        page_num = page_idx + 1
        page_blocks = []
        for b in blocks_by_page.get(page_num, []):
            page_blocks.append(
                {
                    "id": b["id"],
                    "type": "text",
                    "bbox": b["bbox"],
                    "text": b["text"],
                    "font": "Helvetica",
                    "size": 11.0,
                }
            )
        pages.append(
            {
                "page": page_num,
                "width": round(page.rect.width, 1),
                "height": round(page.rect.height, 1),
                "blocks": page_blocks,
            }
        )
    file_name = pdf_path.name
    doc.close()
    return {"file": file_name, "metadata": {"page_count": len(pages)}, "pages": pages}


@pytest.fixture
def two_page_pdf(tmp_path):
    pdf = tmp_path / "input.pdf"
    _make_text_pdf(
        pdf,
        [
            [((72, 100), "Erster Block Seite eins")],
            [((72, 100), "Zweiter Block Seite zwei")],
        ],
    )
    extraction = _extract_for(
        pdf,
        [
            {
                "id": "p1_b0",
                "page": 1,
                "bbox": [72.0, 88.0, 300.0, 110.0],
                "text": "Erster Block Seite eins",
            },
            {
                "id": "p2_b0",
                "page": 2,
                "bbox": [72.0, 88.0, 300.0, 110.0],
                "text": "Zweiter Block Seite zwei",
            },
        ],
    )
    return pdf, extraction


@pytest.fixture
def refinement_pdf(tmp_path):
    pdf = tmp_path / "refine.pdf"
    _make_text_pdf(
        pdf,
        [[((72, 100), "Sondertext und mehr")]],
    )
    extraction = _extract_for(
        pdf,
        [
            {
                "id": "p1_b0",
                "page": 1,
                "bbox": [72.0, 88.0, 300.0, 110.0],
                "text": "Sondertext und mehr",
            }
        ],
    )
    return pdf, extraction


@pytest.fixture
def duplicate_phrase_pdf(tmp_path):
    """PDF mit zwei Vorkommen eines Wortes, damit Refinement in den Fallback geht."""
    pdf = tmp_path / "dup.pdf"
    _make_text_pdf(
        pdf,
        [
            [
                ((72, 100), "Wiederholt und wiederholt klingt gleich."),
                ((72, 150), "Fuellzeile"),
            ]
        ],
    )
    extraction = _extract_for(
        pdf,
        [
            {
                "id": "p1_b0",
                "page": 1,
                "bbox": [72.0, 88.0, 500.0, 160.0],
                "text": "wiederholt",
            }
        ],
    )
    return pdf, extraction


@pytest.fixture
def single_page_pdf(tmp_path):
    pdf = tmp_path / "visual.pdf"
    _make_text_pdf(pdf, [[((72, 100), "Plan-Platzhalter")]])
    return pdf


# ---------------------------------------------------------------------------
# Hilfsfunktion: Annotationen aus einem PDF einsammeln
# ---------------------------------------------------------------------------


def _annots(pdf_path: Path) -> list[dict]:
    doc = fitz.open(str(pdf_path))
    out = []
    for page_idx, page in enumerate(doc):
        for annot in page.annots() or []:
            info = annot.info
            out.append(
                {
                    "page": page_idx + 1,
                    "type": annot.type[1],
                    "title": info.get("title", ""),
                    "content": info.get("content", ""),
                    "rect": fitz.Rect(annot.rect),
                    "colors": annot.colors,
                    "opacity": annot.opacity,
                }
            )
    doc.close()
    return out


# ---------------------------------------------------------------------------
# annotate_text_pdf
# ---------------------------------------------------------------------------


def test_annotate_text_pdf_highlight_on_right_page(tmp_path, two_page_pdf, color_map_stub):
    pdf, extraction = two_page_pdf
    dst = tmp_path / "out" / "annotated.pdf"
    codings = [
        {
            "block_id": "p2_b0",
            "codes": ["A-01"],
            "ganzer_block": True,
            "begruendung": "Weil es passt",
        }
    ]

    stats = annotate_text_pdf(pdf, dst, codings, extraction, color_map_stub)

    assert dst.exists()
    assert stats["annotations_added"] == 1
    assert stats["blocks_processed"] == 1
    assert stats["pages_annotated"] == 1

    annots = _annots(dst)
    assert len(annots) == 1
    assert annots[0]["page"] == 2
    assert annots[0]["type"] == "Highlight"
    assert annots[0]["title"] == "A-01"
    # Code steht im Comment-Praefix, damit MAXQDA-Sortierung danach gruppiert.
    assert annots[0]["content"] == "A-01: Weil es passt"


def test_multiple_codes_create_multiple_annotations(tmp_path, two_page_pdf, color_map_stub):
    pdf, extraction = two_page_pdf
    dst = tmp_path / "annotated.pdf"
    codings = [
        {
            "block_id": "p1_b0",
            "codes": ["A-01", "L-02", "Q-03"],
            "ganzer_block": True,
            "begruendung": "Triangulation",
        }
    ]

    stats = annotate_text_pdf(pdf, dst, codings, extraction, color_map_stub)

    assert stats["annotations_added"] == 3
    annots = _annots(dst)
    assert len(annots) == 3
    titles = sorted(a["title"] for a in annots)
    assert titles == ["A-01", "L-02", "Q-03"]
    # Alle drei liegen auf derselben Seite, ueberlappen sich bewusst.
    assert all(a["page"] == 1 for a in annots)


def test_missing_block_is_skipped_and_logged(tmp_path, two_page_pdf, color_map_stub):
    pdf, extraction = two_page_pdf
    dst = tmp_path / "annotated.pdf"
    logs: list[tuple[str, str]] = []

    codings = [
        {
            "block_id": "p1_b0",
            "codes": ["A-01"],
            "ganzer_block": True,
            "begruendung": "ok",
        },
        {
            "block_id": "p99_b99",
            "codes": ["A-01"],
            "ganzer_block": True,
            "begruendung": "missing",
        },
    ]

    stats = annotate_text_pdf(
        pdf, dst, codings, extraction, color_map_stub, log_callback=lambda lvl, m: logs.append((lvl, m))
    )

    assert stats["annotations_added"] == 1
    assert any("p99_b99" in msg for _, msg in logs)
    assert any(lvl == "warn" for lvl, _ in logs)
    assert any("p99_b99" in e for e in stats["errors"])


def test_refinement_resolved_via_search_for(tmp_path, refinement_pdf, color_map_stub):
    pdf, extraction = refinement_pdf
    dst = tmp_path / "annotated.pdf"
    codings = [
        {
            "block_id": "p1_b0",
            "codes": ["X-01"],
            "ganzer_block": False,
            "begruendung": "Nur Sondertext",
            "refinements": {"X-01": {"char_start": 0, "char_end": 10}},
        }
    ]

    stats = annotate_text_pdf(pdf, dst, codings, extraction, color_map_stub)

    assert stats["refinements_resolved"] == 1
    assert stats["refinements_fallback"] == 0
    assert stats["annotations_added"] == 1

    annots = _annots(dst)
    assert len(annots) == 1
    # Die aufgeloeste bbox sollte kleiner sein als der volle Block.
    full_block = fitz.Rect(72.0, 88.0, 300.0, 110.0)
    assert annots[0]["rect"].width < full_block.width
    # Das Wort "Sondertext" beginnt in der Naehe von x=72 und ist relativ kurz.
    # pymupdf blaeht den Highlight-Rect leicht auf, daher grosszuegige Grenzen.
    assert annots[0]["rect"].x0 >= 60
    assert annots[0]["rect"].x1 < 200


def test_refinement_fallback_on_multiple_matches(tmp_path, duplicate_phrase_pdf, color_map_stub):
    pdf, extraction = duplicate_phrase_pdf
    dst = tmp_path / "annotated.pdf"
    codings = [
        {
            "block_id": "p1_b0",
            "codes": ["X-01"],
            "ganzer_block": False,
            "begruendung": "Mehrdeutig",
            # block.text = "wiederholt", der Begriff taucht 2x auf der Seite auf.
            "refinements": {"X-01": {"char_start": 0, "char_end": 10}},
        }
    ]

    stats = annotate_text_pdf(pdf, dst, codings, extraction, color_map_stub)

    assert stats["refinements_resolved"] == 0
    assert stats["refinements_fallback"] == 1
    assert stats["annotations_added"] == 1


# ---------------------------------------------------------------------------
# annotate_visual_pdf
# ---------------------------------------------------------------------------


def test_annotate_visual_pdf_with_bbox(tmp_path, single_page_pdf, color_map_stub):
    dst = tmp_path / "vis_out.pdf"
    visual_codings = [
        {
            "page": 1,
            "block_id": "p1_v0",
            "codes": ["O-01"],
            "description": "Tragende Wand",
            "bbox": [100, 100, 300, 300],
        }
    ]

    stats = annotate_visual_pdf(single_page_pdf, dst, visual_codings, color_map_stub)

    assert stats["annotations_added"] == 1
    assert stats["pages_annotated"] == 1

    annots = _annots(dst)
    assert len(annots) == 1
    assert annots[0]["type"] == "Square"
    assert annots[0]["title"] == "O-01"
    assert annots[0]["content"] == "O-01: Tragende Wand"
    # Opacity 0.25
    assert abs(annots[0]["opacity"] - 0.25) < 1e-6
    # Rect stimmt (pymupdf erweitert den Square-Annot-Rect um ~1pt fuer die
    # Border, daher Toleranz).
    r = annots[0]["rect"]
    assert abs(r.x0 - 100.0) <= 2.0
    assert abs(r.y0 - 100.0) <= 2.0
    assert abs(r.x1 - 300.0) <= 2.0
    assert abs(r.y1 - 300.0) <= 2.0


def test_annotate_visual_pdf_without_bbox_uses_whole_page(
    tmp_path, single_page_pdf, color_map_stub
):
    dst = tmp_path / "vis_out.pdf"
    visual_codings = [
        {
            "page": 1,
            "block_id": "p1_v0",
            "codes": ["O-01"],
            "description": "Ganze Seite",
        }
    ]

    stats = annotate_visual_pdf(single_page_pdf, dst, visual_codings, color_map_stub)

    assert stats["annotations_added"] == 1
    annots = _annots(dst)
    assert len(annots) == 1
    r = annots[0]["rect"]
    # 595 x 842 Seite; pymupdf blaeht den Square-Annot leicht auf.
    assert abs(r.x0 - 0.0) <= 2.0
    assert abs(r.y0 - 0.0) <= 2.0
    assert abs(r.x1 - 595.0) <= 2.0
    assert abs(r.y1 - 842.0) <= 2.0


def test_output_pdf_roundtrip(tmp_path, two_page_pdf, color_map_stub):
    pdf, extraction = two_page_pdf
    dst = tmp_path / "round.pdf"
    codings = [
        {
            "block_id": "p1_b0",
            "codes": ["A-01", "L-02"],
            "ganzer_block": True,
            "begruendung": "doppel",
        },
        {
            "block_id": "p2_b0",
            "codes": ["A-01"],
            "ganzer_block": True,
            "begruendung": "einzeln",
        },
    ]

    stats = annotate_text_pdf(pdf, dst, codings, extraction, color_map_stub)
    assert stats["annotations_added"] == 3

    annots = _annots(dst)
    assert len(annots) == 3
    # Seiten-Verteilung nach Roundtrip
    pages = sorted(a["page"] for a in annots)
    assert pages == [1, 1, 2]
    # Typen sind erhalten
    assert all(a["type"] == "Highlight" for a in annots)


def test_stats_structure(tmp_path, two_page_pdf, color_map_stub, single_page_pdf):
    pdf, extraction = two_page_pdf
    dst = tmp_path / "stats.pdf"
    stats = annotate_text_pdf(
        pdf,
        dst,
        [
            {
                "block_id": "p1_b0",
                "codes": ["A-01"],
                "ganzer_block": True,
                "begruendung": "",
            }
        ],
        extraction,
        color_map_stub,
    )
    expected = {
        "annotations_added",
        "blocks_processed",
        "refinements_resolved",
        "refinements_fallback",
        "pages_annotated",
        "errors",
    }
    assert expected.issubset(stats.keys())
    assert isinstance(stats["errors"], list)

    vis_stats = annotate_visual_pdf(
        single_page_pdf,
        tmp_path / "v_stats.pdf",
        [
            {
                "page": 1,
                "block_id": "p1_v0",
                "codes": ["O-01"],
                "description": "",
            }
        ],
        color_map_stub,
    )
    assert expected.issubset(vis_stats.keys())


def test_dst_parent_is_created(tmp_path, two_page_pdf, color_map_stub):
    pdf, extraction = two_page_pdf
    dst = tmp_path / "nested" / "deeper" / "out.pdf"
    assert not dst.parent.exists()

    annotate_text_pdf(
        pdf,
        dst,
        [
            {
                "block_id": "p1_b0",
                "codes": ["A-01"],
                "ganzer_block": True,
                "begruendung": "",
            }
        ],
        extraction,
        color_map_stub,
    )

    assert dst.exists()
    assert dst.parent.is_dir()


def test_log_callback_receives_warnings(tmp_path, two_page_pdf, color_map_stub):
    pdf, extraction = two_page_pdf
    dst = tmp_path / "out.pdf"
    logs: list[tuple[str, str]] = []

    annotate_text_pdf(
        pdf,
        dst,
        [
            {
                "block_id": "does_not_exist",
                "codes": ["A-01"],
                "ganzer_block": True,
                "begruendung": "",
            }
        ],
        extraction,
        color_map_stub,
        log_callback=lambda lvl, msg: logs.append((lvl, msg)),
    )

    assert any(lvl == "warn" for lvl, _ in logs)
    assert any("does_not_exist" in msg for _, msg in logs)
