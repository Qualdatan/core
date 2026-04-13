"""PDF-Annotator: schreibt Kodierungen als native PDF-Annotationen.

MAXQDA importiert PDF-Highlights direkt. Wir verwenden eine **fixe** Farbe
fuer alle Annotationen und schreiben den Code (z.B. ``B-03``) prominent in
den Annotations-Kommentar. So kann man in MAXQDA nach Kommentar sortieren,
alle Annotationen mit gleichem Code gruppieren und per Bulk-Operation den
echten Code in MAXQDA zuweisen — Code-spezifische Farben sind dafuer
ueberfluessig.

Zwei Einstiegspunkte:
  * ``annotate_text_pdf``  - eine Highlight-Annotation pro (Block, Code).
  * ``annotate_visual_pdf`` - eine Rechteck-Annotation pro (Region, Code).

Jeder Code eines Blocks erzeugt eine eigene Annotation; Ueberlappungen sind
gewuenscht, weil sie die Triangulation (mehrere Codes auf demselben Segment)
sichtbar machen.

Der ``color_map``-Parameter bleibt aus Kompatibilitaetsgruenden in der
Signatur, wird aber nicht mehr ausgewertet. Du kannst ``None`` uebergeben.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

import fitz  # pymupdf


LogCallback = Callable[[str, str], None] | None


# Eine fixe Farbe fuer alle Annotationen — der Code kommt in den Comment,
# nicht in die Farbe (siehe Modul-Docstring). Kraeftiges Gelb, sichtbar
# sowohl auf weissem Text als auch auf Plaenen.
_HIGHLIGHT_COLOR: tuple[float, float, float] = (1.0, 0.85, 0.0)
_RECT_COLOR: tuple[float, float, float] = (1.0, 0.85, 0.0)


def _format_comment(code_id: str, text: str) -> str:
    """Setzt den Code als Praefix vor den Begruendungs-/Description-Text.

    So kann der Anwender in MAXQDA nach Comment-Text sortieren und alle
    Annotationen mit gleichem Code gruppieren.
    """
    text = (text or "").strip()
    if text:
        return f"{code_id}: {text}"
    return code_id


# ---------------------------------------------------------------------------
# Hilfsfunktionen
# ---------------------------------------------------------------------------


def _log(log_callback: LogCallback, level: str, msg: str) -> None:
    """Sendet eine Log-Zeile an das optionale Callback (sonst no-op)."""
    if log_callback is not None:
        log_callback(level, msg)


def _parse_page_from_block_id(block_id: str) -> int | None:
    """Extrahiert die 1-basierte Seitenzahl aus einer Block-ID wie ``p12_b3``.

    Gibt ``None`` zurueck, wenn das Format nicht passt.
    """
    if not block_id or not block_id.startswith("p"):
        return None
    try:
        # "p12_b3" -> "12"
        rest = block_id[1:]
        page_str = rest.split("_", 1)[0]
        return int(page_str)
    except (ValueError, IndexError):
        return None


def _build_extraction_index(
    extraction: dict,
) -> dict[str, tuple[int, dict]]:
    """Baut einen Index ``block_id -> (page_number, block_dict)``."""
    index: dict[str, tuple[int, dict]] = {}
    for page in extraction.get("pages", []):
        page_num = int(page.get("page", 0))
        for block in page.get("blocks", []):
            bid = block.get("id")
            if bid:
                index[bid] = (page_num, block)
    return index


def _rect_from_bbox(bbox: list | tuple) -> fitz.Rect:
    """Erzeugt ein ``fitz.Rect`` aus einer vierstelligen bbox."""
    x0, y0, x1, y1 = bbox
    return fitz.Rect(x0, y0, x1, y1)


# ---------------------------------------------------------------------------
# Textannotationen
# ---------------------------------------------------------------------------


def annotate_text_pdf(
    src_path: Path,
    dst_path: Path,
    codings: list[dict],
    extraction: dict,
    color_map: Any = None,
    log_callback: LogCallback = None,
) -> dict:
    """Annotiert ein Text-PDF mit Highlights, eine Annotation pro (Block, Code).

    Args:
        src_path: Quell-PDF (wird nicht veraendert).
        dst_path: Zielpfad fuer die annotierte Kopie.
        codings: Liste von Kodierungen aus :mod:`src.pdf_analyzer`.
        extraction: Extraktions-Dict aus :func:`src.pdf_extractor.extract_pdf`.
        color_map: Wird aus Kompatibilitaetsgruenden akzeptiert, aber ignoriert.
            Alle Annotationen verwenden ``_HIGHLIGHT_COLOR``.
        log_callback: Optionales Callback ``(level, msg)``.

    Returns:
        Stats-Dict mit Schluesseln ``annotations_added``, ``blocks_processed``,
        ``refinements_resolved``, ``refinements_fallback``, ``pages_annotated``,
        ``errors``.
    """
    src_path = Path(src_path)
    dst_path = Path(dst_path)
    dst_path.parent.mkdir(parents=True, exist_ok=True)

    stats: dict[str, Any] = {
        "annotations_added": 0,
        "blocks_processed": 0,
        "refinements_resolved": 0,
        "refinements_fallback": 0,
        "pages_annotated": 0,
        "errors": [],
    }

    index = _build_extraction_index(extraction)
    doc = fitz.open(str(src_path))
    pages_touched: set[int] = set()

    try:
        for coding in codings:
            block_id = coding.get("block_id")
            if not block_id:
                msg = "coding without block_id skipped"
                stats["errors"].append(msg)
                _log(log_callback, "warn", msg)
                continue

            if block_id not in index:
                msg = f"block {block_id} not found in extraction"
                stats["errors"].append(msg)
                _log(log_callback, "warn", msg)
                continue

            page_num, block = index[block_id]
            if page_num < 1 or page_num > len(doc):
                msg = f"block {block_id} has invalid page {page_num}"
                stats["errors"].append(msg)
                _log(log_callback, "warn", msg)
                continue

            page = doc[page_num - 1]
            block_text = block.get("text", "") or ""
            block_bbox = block.get("bbox")
            if not block_bbox:
                msg = f"block {block_id} has no bbox"
                stats["errors"].append(msg)
                _log(log_callback, "warn", msg)
                continue

            stats["blocks_processed"] += 1
            refinements = coding.get("refinements") or {}
            begruendung = coding.get("begruendung", "") or ""

            for code_id in coding.get("codes", []):
                refinement = refinements.get(code_id)
                highlight_target: fitz.Rect | list[fitz.Rect]
                used_refinement = False

                if refinement and isinstance(refinement, dict):
                    char_start = refinement.get("char_start")
                    char_end = refinement.get("char_end")
                    if (
                        isinstance(char_start, int)
                        and isinstance(char_end, int)
                        and 0 <= char_start < char_end <= len(block_text)
                    ):
                        substring = block_text[char_start:char_end].strip()
                        matches: list[fitz.Rect] = []
                        if substring:
                            matches = page.search_for(substring)
                        if len(matches) == 1:
                            highlight_target = matches
                            used_refinement = True
                            stats["refinements_resolved"] += 1
                            _log(
                                log_callback,
                                "info",
                                f"refinement resolved for {block_id}/{code_id}",
                            )
                        else:
                            _log(
                                log_callback,
                                "info",
                                f"refinement fallback for {block_id}/{code_id} "
                                f"({len(matches)} matches)",
                            )
                            stats["refinements_fallback"] += 1
                            highlight_target = _rect_from_bbox(block_bbox)
                    else:
                        _log(
                            log_callback,
                            "info",
                            f"refinement fallback for {block_id}/{code_id} "
                            "(invalid offsets)",
                        )
                        stats["refinements_fallback"] += 1
                        highlight_target = _rect_from_bbox(block_bbox)
                else:
                    highlight_target = _rect_from_bbox(block_bbox)

                annot = page.add_highlight_annot(highlight_target)
                if annot is None:
                    msg = (
                        f"failed to create highlight for {block_id}/{code_id}"
                    )
                    stats["errors"].append(msg)
                    _log(log_callback, "warn", msg)
                    continue

                annot.set_colors(stroke=_HIGHLIGHT_COLOR)
                annot.set_info(
                    title=code_id,
                    content=_format_comment(code_id, begruendung),
                )
                annot.update()

                stats["annotations_added"] += 1
                pages_touched.add(page_num)
                _ = used_refinement  # explizit: nur fuer Stats-Log relevant

        stats["pages_annotated"] = len(pages_touched)
        doc.save(str(dst_path), garbage=4, deflate=True)
    finally:
        doc.close()

    return stats


# ---------------------------------------------------------------------------
# Visuelle Annotationen (Plaene / Fotos)
# ---------------------------------------------------------------------------


def annotate_visual_pdf(
    src_path: Path,
    dst_path: Path,
    visual_codings: list[dict],
    color_map: Any = None,
    log_callback: LogCallback = None,
) -> dict:
    """Annotiert ein Plan-/Foto-PDF mit Rechteck-Annotationen.

    Eine Annotation pro (Region, Code). Wenn ``bbox`` in der Kodierung fehlt,
    wird die gesamte Seite umrandet.

    Args:
        color_map: Wird aus Kompatibilitaetsgruenden akzeptiert, aber ignoriert.
            Alle Rechtecke verwenden ``_RECT_COLOR``.

    Returns:
        Stats-Dict mit ``annotations_added``, ``refinements_resolved``,
        ``refinements_fallback``, ``pages_annotated``, ``errors``.
        ``blocks_processed`` wird zur Konsistenz ebenfalls gesetzt.
    """
    src_path = Path(src_path)
    dst_path = Path(dst_path)
    dst_path.parent.mkdir(parents=True, exist_ok=True)

    stats: dict[str, Any] = {
        "annotations_added": 0,
        "blocks_processed": 0,
        "refinements_resolved": 0,
        "refinements_fallback": 0,
        "pages_annotated": 0,
        "errors": [],
    }

    doc = fitz.open(str(src_path))
    pages_touched: set[int] = set()

    try:
        for coding in visual_codings:
            page_num = coding.get("page")
            if not isinstance(page_num, int) or page_num < 1 or page_num > len(doc):
                msg = (
                    f"visual coding {coding.get('block_id', '?')} "
                    f"has invalid page {page_num}"
                )
                stats["errors"].append(msg)
                _log(log_callback, "warn", msg)
                continue

            page = doc[page_num - 1]
            bbox = coding.get("bbox")
            if bbox and len(bbox) == 4:
                rect = _rect_from_bbox(bbox)
            else:
                rect = fitz.Rect(0, 0, page.rect.width, page.rect.height)
                _log(
                    log_callback,
                    "info",
                    f"visual coding {coding.get('block_id', '?')} "
                    "has no bbox, using whole page",
                )

            stats["blocks_processed"] += 1
            description = coding.get("description", "") or ""

            for code_id in coding.get("codes", []):
                annot = page.add_rect_annot(rect)
                if annot is None:
                    msg = (
                        f"failed to create rect annot for "
                        f"{coding.get('block_id', '?')}/{code_id}"
                    )
                    stats["errors"].append(msg)
                    _log(log_callback, "warn", msg)
                    continue

                annot.set_colors(stroke=_RECT_COLOR, fill=_RECT_COLOR)
                annot.set_opacity(0.25)
                annot.set_info(
                    title=code_id,
                    content=_format_comment(code_id, description),
                )
                annot.update()

                stats["annotations_added"] += 1
                pages_touched.add(page_num)

        stats["pages_annotated"] = len(pages_touched)
        doc.save(str(dst_path), garbage=4, deflate=True)
    finally:
        doc.close()

    return stats
