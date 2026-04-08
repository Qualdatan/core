"""Stufe 1: Lokale PDF-Extraktion mit pymupdf.

Extrahiert Text, Tabellen und Bild-Referenzen mit exakten
PDF-Koordinaten. Kein LLM-Call nötig.
"""

import json
import re
from pathlib import Path

import fitz  # pymupdf


# Muster für Boilerplate-Blöcke die nicht ans LLM gesendet werden
_BOILERPLATE_PATTERNS = [
    re.compile(r"^\s*\d+\s*$"),                    # Nur Seitenzahl
    re.compile(r"^\s*-\s*\d+\s*-\s*$"),            # - 5 -
    re.compile(r"^\s*Seite\s+\d+\s*(von\s+\d+)?\s*$", re.IGNORECASE),  # Seite 3 von 10
    re.compile(r"^\s*Page\s+\d+\s*(of\s+\d+)?\s*$", re.IGNORECASE),    # Page 3 of 10
    re.compile(r"^\s*©.*$"),                        # Copyright
    re.compile(r"^\s*Confidential\s*$", re.IGNORECASE),
    re.compile(r"^\s*CONFIDENTIAL\s*$"),
]


def _is_boilerplate(text: str) -> bool:
    """Prüft ob ein Textblock Boilerplate ist (Seitenzahl, Copyright, etc.)."""
    stripped = text.strip()
    if len(stripped) < 3:
        return True
    return any(p.match(stripped) for p in _BOILERPLATE_PATTERNS)


def extract_pdf(pdf_path: str | Path) -> dict:
    """Extrahiert alle Inhalte aus einer PDF-Datei.

    Args:
        pdf_path: Pfad zur PDF-Datei

    Returns:
        Strukturiertes Dict mit Seiten, Blöcken und Metadaten.
        Jeder Block hat eine eindeutige ID (z.B. "p1_b0", "p1_t0").
    """
    pdf_path = Path(pdf_path)
    doc = fitz.open(str(pdf_path))

    pages = []
    for page_idx, page in enumerate(doc):
        page_num = page_idx + 1
        page_data = {
            "page": page_num,
            "width": round(page.rect.width, 1),
            "height": round(page.rect.height, 1),
            "blocks": [],
        }

        # Textblöcke extrahieren (dict-Modus für Details)
        text_dict = page.get_text("dict", flags=fitz.TEXT_PRESERVE_WHITESPACE)
        block_idx = 0

        for block in text_dict.get("blocks", []):
            if block["type"] == 0:  # Textblock
                # Text aus allen Lines/Spans zusammenbauen
                lines_text = []
                font_name = ""
                font_size = 0.0
                for line in block.get("lines", []):
                    line_text = ""
                    for span in line.get("spans", []):
                        line_text += span.get("text", "")
                        if not font_name:
                            font_name = span.get("font", "")
                            font_size = span.get("size", 0.0)
                    lines_text.append(line_text)

                text = "\n".join(lines_text).strip()
                if not text:
                    continue

                bbox = block["bbox"]
                page_data["blocks"].append({
                    "id": f"p{page_num}_b{block_idx}",
                    "type": "text",
                    "bbox": [round(v, 1) for v in bbox],
                    "text": text,
                    "font": font_name,
                    "size": round(font_size, 1),
                })
                block_idx += 1

            elif block["type"] == 1:  # Bildblock
                bbox = block["bbox"]
                w = round(bbox[2] - bbox[0])
                h = round(bbox[3] - bbox[1])
                page_data["blocks"].append({
                    "id": f"p{page_num}_i{block_idx}",
                    "type": "image",
                    "bbox": [round(v, 1) for v in bbox],
                    "description": f"Bild ({w}x{h}px)",
                })
                block_idx += 1

        # Tabellen extrahieren (pymupdf built-in)
        try:
            tables = page.find_tables()
            for t_idx, table in enumerate(tables):
                cells = table.extract()
                if not cells or len(cells) < 2:
                    continue

                headers = [str(c or "") for c in cells[0]]
                rows = [[str(c or "") for c in row] for row in cells[1:]]
                bbox = list(table.bbox)

                page_data["blocks"].append({
                    "id": f"p{page_num}_t{t_idx}",
                    "type": "table",
                    "bbox": [round(v, 1) for v in bbox],
                    "headers": headers,
                    "rows": rows,
                })
        except Exception:
            pass  # Tabellenerkennung ist optional

        pages.append(page_data)

    # Metadaten
    metadata = {
        "title": doc.metadata.get("title", "") or "",
        "author": doc.metadata.get("author", "") or "",
        "page_count": len(doc),
        "file_size_kb": pdf_path.stat().st_size // 1024,
    }

    doc.close()

    return {
        "file": str(pdf_path.name),
        "pages": pages,
        "metadata": metadata,
    }


def build_fulltext_and_positions(data: dict) -> tuple[str, dict[str, tuple[int, int]]]:
    """Baut den Volltext aus allen Textblöcken und mappt Block-IDs auf Char-Positionen.

    Nützlich für QDPX PlainTextSelection: Blöcke werden mit \\n\\n verbunden,
    und jeder Block bekommt seine Start/End-Position im resultierenden Volltext.

    Returns:
        (fulltext, {block_id: (char_start, char_end)})
    """
    parts = []
    positions = {}
    offset = 0

    for page in data["pages"]:
        for block in page["blocks"]:
            if block["type"] != "text":
                continue
            text = block["text"]
            if not text.strip():
                continue

            if offset > 0:
                parts.append("\n\n")
                offset += 2

            positions[block["id"]] = (offset, offset + len(text))
            parts.append(text)
            offset += len(text)

    fulltext = "".join(parts)
    return fulltext, positions


def save_extraction(data: dict, output_path: Path):
    """Speichert Extraktionsergebnis als JSON."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def load_extraction(path: Path) -> dict | None:
    """Lädt gecachtes Extraktionsergebnis."""
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            pass
    return None


def _smart_truncate(text: str, max_len: int) -> str:
    """Kürzt Text am letzten Satzende vor max_len, statt mitten im Wort."""
    if len(text) <= max_len:
        return text
    # Suche letztes Satzende vor max_len
    truncated = text[:max_len]
    last_sentence = max(
        truncated.rfind(". "),
        truncated.rfind(".\n"),
        truncated.rfind("! "),
        truncated.rfind("? "),
    )
    if last_sentence > max_len * 0.5:
        return text[:last_sentence + 1] + " [...]"
    # Fallback: am letzten Leerzeichen kürzen
    last_space = truncated.rfind(" ")
    if last_space > max_len * 0.5:
        return text[:last_space] + " [...]"
    return truncated + " [...]"


def extraction_to_text_summary(data: dict, max_block_chars: int = 0) -> str:
    """Wandelt Extraktionsdaten in einen Text für den LLM-Prompt.

    Format: [block_id] (typ): "text..."

    Args:
        data: Extraktionsdaten aus extract_pdf()
        max_block_chars: Max Zeichen pro Block (0 = automatisch nach Dokumentgröße)
    """
    # Dokumentgröße bestimmen für adaptive Truncation
    if max_block_chars <= 0:
        total_blocks = sum(len(p["blocks"]) for p in data["pages"])
        if total_blocks <= 10:
            max_block_chars = 800
        elif total_blocks <= 50:
            max_block_chars = 500
        else:
            max_block_chars = 300

    lines = []
    for page in data["pages"]:
        lines.append(f"--- Seite {page['page']} ---")
        for block in page["blocks"]:
            bid = block["id"]
            if block["type"] == "text":
                text = block["text"]
                # Boilerplate filtern
                if _is_boilerplate(text):
                    continue
                text = _smart_truncate(text, max_block_chars)
                lines.append(f'[{bid}] (Text): "{text}"')

            elif block["type"] == "table":
                # Kompakte Tabellendarstellung
                headers = block["headers"]
                if len(headers) > 6:
                    headers_str = " | ".join(headers[:5]) + f" | (+{len(headers)-5})"
                else:
                    headers_str = " | ".join(headers)
                row_preview = ""
                if block["rows"]:
                    first_row = block["rows"][0]
                    if len(first_row) > 6:
                        row_preview = " | ".join(first_row[:5]) + " | ..."
                    else:
                        row_preview = " | ".join(first_row)
                    if len(block["rows"]) > 1:
                        row_preview += f" (+{len(block['rows'])-1} Zeilen)"
                lines.append(f"[{bid}] (Tabelle): {headers_str}\\n{row_preview}")

            elif block["type"] == "image":
                lines.append(f"[{bid}] (Bild): {block['description']}")

    return "\n".join(lines)
