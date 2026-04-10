"""PDF-Klassifikation: Erkennt Dokumenttyp (Text, Plan, Foto, Mixed).

Drei Modi:
  local:  Reine Heuristik auf pymupdf-Metriken (kostenlos, ~85% Trefferquote)
  llm:    Thumbnails an Vision-LLM senden (~95%+, kostet Token)
  hybrid: Lokal + LLM nur fuer unsichere Seiten (beste Preis/Leistung)
"""

import base64
import io
import json
import re
from dataclasses import dataclass, field, asdict
from pathlib import Path

import fitz  # pymupdf


# ---------------------------------------------------------------------------
# Datenstrukturen
# ---------------------------------------------------------------------------

@dataclass
class PageClassification:
    """Klassifikation einer einzelnen PDF-Seite."""
    page: int
    page_type: str          # text | plan | photo | mixed
    confidence: float       # 0.0 - 1.0
    plan_subtype: str = ""  # floor_plan | section | elevation | site_plan | detail | schedule | ""
    metrics: dict = field(default_factory=dict)
    title_block: dict = field(default_factory=dict)


@dataclass
class DocumentClassification:
    """Klassifikation eines gesamten PDF-Dokuments."""
    file: str
    document_type: str      # text | plan | photo | mixed
    confidence: float
    page_count: int
    pages: list[PageClassification] = field(default_factory=list)
    title_block_metadata: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "file": self.file,
            "document_type": self.document_type,
            "confidence": self.confidence,
            "page_count": self.page_count,
            "title_block_metadata": self.title_block_metadata,
            "pages": [asdict(p) for p in self.pages],
        }

    def summary(self) -> str:
        type_counts = {}
        for p in self.pages:
            type_counts[p.page_type] = type_counts.get(p.page_type, 0) + 1
        parts = [f"{v}x {k}" for k, v in sorted(type_counts.items())]
        return f"{self.document_type} ({', '.join(parts)})"


# ---------------------------------------------------------------------------
# Schriftfeld-Erkennung (Plankopf / Title Block)
# ---------------------------------------------------------------------------

# Schluesselwoerter die auf ein Schriftfeld hindeuten
_TITLE_BLOCK_KEYWORDS = [
    "massstab", "maßstab", "masstab", "m 1:", "1:50", "1:100", "1:200",
    "plan-nr", "plan nr", "plannr", "zeichnungs-nr", "blatt-nr",
    "leistungsphase", "lph", "l.ph",
    "bauherr", "bauvorhaben", "projekt",
    "architekt", "planer", "verfasser", "bearbeiter",
    "datum", "index", "revision", "gewerk",
    "gezeichnet", "geprüft", "geprueft", "freigabe",
]

# Regex fuer strukturierte Felder im Schriftfeld
_TITLE_BLOCK_FIELDS = {
    "massstab": re.compile(r"(?:Ma[sß]{1,2}stab|M)\s*[:\s]\s*(\d+\s*:\s*\d+)", re.IGNORECASE),
    "plan_nr": re.compile(r"(?:Plan|Zeichnungs?|Blatt)[- ]?(?:Nr|Nummer)\.?\s*[:\s]\s*([\w.\-/]+)", re.IGNORECASE),
    "leistungsphase": re.compile(r"(?:LP|LPH|Leistungsphase)\s*[:\s]*(\d+)", re.IGNORECASE),
    "gewerk": re.compile(r"Gewerk\s*[:\s]\s*(.+?)(?:\n|$)", re.IGNORECASE),
    "index": re.compile(r"(?:Index|Rev(?:ision)?)\s*[:\s]\s*([A-Z]\d?)", re.IGNORECASE),
    "datum": re.compile(r"(?:Datum|Erstellt|Stand)\s*[:\s]\s*(\d{1,2}[./]\d{1,2}[./]\d{2,4})", re.IGNORECASE),
    "bauvorhaben": re.compile(r"(?:Bauvorhaben|Projekt|Vorhaben)\s*[:\s]\s*(.+?)(?:\n|$)", re.IGNORECASE),
}


def _detect_title_block(page: fitz.Page) -> tuple[bool, dict]:
    """Erkennt ob eine Seite ein Schriftfeld hat und extrahiert Metadaten.

    Schriftfelder befinden sich typischerweise im unteren rechten Bereich
    einer Planseite (untere 20%, rechte 60%).

    Returns:
        (has_title_block, {field: value})
    """
    width = page.rect.width
    height = page.rect.height

    # Suchbereich: untere 20%, rechte 60%
    search_rect = fitz.Rect(
        width * 0.4, height * 0.80,
        width, height,
    )

    # Text im Suchbereich extrahieren
    text = page.get_text("text", clip=search_rect).lower()

    if not text.strip():
        return False, {}

    # Schluesselwoerter zaehlen
    keyword_hits = sum(1 for kw in _TITLE_BLOCK_KEYWORDS if kw in text)

    if keyword_hits < 2:
        return False, {}

    # Strukturierte Felder extrahieren (auf vollem Text der Region)
    full_text = page.get_text("text", clip=search_rect)
    metadata = {}
    for field_name, pattern in _TITLE_BLOCK_FIELDS.items():
        match = pattern.search(full_text)
        if match:
            metadata[field_name] = match.group(1).strip()

    return True, metadata


# ---------------------------------------------------------------------------
# Seitenmetriken berechnen (lokal, kein LLM)
# ---------------------------------------------------------------------------

def _compute_page_metrics(page: fitz.Page) -> dict:
    """Berechnet Metriken fuer eine PDF-Seite zur Klassifikation.

    Returns:
        Dict mit text_coverage, image_coverage, text_char_count,
        drawing_count, aspect_ratio, etc.
    """
    width = page.rect.width
    height = page.rect.height
    page_area = width * height

    if page_area == 0:
        return {
            "text_coverage": 0.0,
            "image_coverage": 0.0,
            "text_char_count": 0,
            "drawing_count": 0,
            "aspect_ratio": 1.0,
            "is_landscape": False,
            "page_format": "unknown",
        }

    # Textbloecke
    text_dict = page.get_text("dict", flags=fitz.TEXT_PRESERVE_WHITESPACE)
    text_area = 0.0
    text_chars = 0

    for block in text_dict.get("blocks", []):
        if block["type"] == 0:  # Textblock
            bbox = block["bbox"]
            block_area = (bbox[2] - bbox[0]) * (bbox[3] - bbox[1])
            text_area += block_area
            for line in block.get("lines", []):
                for span in line.get("spans", []):
                    text_chars += len(span.get("text", ""))

    # Bildblöcke
    image_area = 0.0
    image_count = 0
    for block in text_dict.get("blocks", []):
        if block["type"] == 1:  # Bildblock
            bbox = block["bbox"]
            block_area = (bbox[2] - bbox[0]) * (bbox[3] - bbox[1])
            image_area += block_area
            image_count += 1

    # Vektorzeichnungen (Linien, Kurven)
    drawings = page.get_drawings()
    drawing_count = len(drawings)

    # Seitenformat erkennen
    aspect_ratio = round(width / height, 2) if height > 0 else 1.0
    is_landscape = width > height * 1.1

    # Format-Heuristik (A-Reihe)
    longer = max(width, height)
    shorter = min(width, height)
    if 820 < longer < 860 and 590 < shorter < 600:
        page_format = "A4"
    elif 1180 < longer < 1200 and 838 < shorter < 845:
        page_format = "A3"
    elif 1680 < longer < 1700 and 1180 < shorter < 1200:
        page_format = "A2"
    elif 2380 < longer < 2400 and 1680 < shorter < 1700:
        page_format = "A1"
    elif 3360 < longer < 3380 and 2380 < shorter < 2400:
        page_format = "A0"
    else:
        page_format = "custom"

    return {
        "text_coverage": round(text_area / page_area, 3),
        "image_coverage": round(image_area / page_area, 3),
        "text_char_count": text_chars,
        "image_count": image_count,
        "drawing_count": drawing_count,
        "aspect_ratio": aspect_ratio,
        "is_landscape": is_landscape,
        "page_format": page_format,
        "page_width": round(width, 1),
        "page_height": round(height, 1),
    }


# ---------------------------------------------------------------------------
# Modus 1: Lokale Klassifikation (Heuristik)
# ---------------------------------------------------------------------------

def _classify_page_local(metrics: dict, has_title_block: bool) -> tuple[str, float, str]:
    """Klassifiziert eine Seite anhand lokaler Metriken.

    Returns:
        (page_type, confidence, plan_subtype)
    """
    text_cov = metrics["text_coverage"]
    image_cov = metrics["image_coverage"]
    text_chars = metrics["text_char_count"]
    drawing_count = metrics["drawing_count"]
    is_landscape = metrics["is_landscape"]

    # Starke Indikatoren
    if has_title_block:
        # Schriftfeld → fast sicher ein Plan
        if drawing_count > 50:
            return "plan", 0.95, ""
        return "plan", 0.85, ""

    # Ueberwiegend Bild, wenig Text → Foto
    if image_cov > 0.6 and text_chars < 50 and drawing_count < 10:
        return "photo", 0.85, ""

    # Grosses Bild mit etwas Text → wahrscheinlich Foto
    if image_cov > 0.4 and text_chars < 100 and drawing_count < 20:
        return "photo", 0.7, ""

    # Viel Text → Textdokument
    if text_cov > 0.3 and text_chars > 200:
        return "text", 0.9, ""

    if text_chars > 500:
        return "text", 0.85, ""

    # Viele Zeichnungen, wenig Text → Plan
    if drawing_count > 100 and text_chars < 300:
        return "plan", 0.8, ""

    if drawing_count > 50 and text_chars < 200:
        return "plan", 0.75, ""

    # Querformat + Zeichnungen → wahrscheinlich Plan
    if is_landscape and drawing_count > 30:
        return "plan", 0.7, ""

    # Grosses Format (A2+) → wahrscheinlich Plan
    if metrics["page_format"] in ("A2", "A1", "A0"):
        if drawing_count > 20 or text_chars < 200:
            return "plan", 0.7, ""

    # Wenig Content insgesamt
    if text_chars < 50 and image_cov < 0.1 and drawing_count < 10:
        return "text", 0.5, ""  # leere Seite, niedrige Confidence

    # Kann nicht eindeutig zugeordnet werden
    return "mixed", 0.4, ""


# ---------------------------------------------------------------------------
# Modus 2: LLM-Klassifikation (Vision)
# ---------------------------------------------------------------------------

def _render_thumbnail(page: fitz.Page, dpi: int = 72) -> str:
    """Rendert eine PDF-Seite als base64-encoded PNG Thumbnail.

    Args:
        page: pymupdf Page-Objekt
        dpi: Aufloesung (72 = klein/guenstig, 150 = detail)

    Returns:
        Base64-encoded PNG string
    """
    pixmap = page.get_pixmap(dpi=dpi)
    png_bytes = pixmap.tobytes("png")
    return base64.b64encode(png_bytes).decode("ascii")


def _build_classification_prompt(page_indices: list[int]) -> str:
    """Baut den Klassifikations-Prompt fuer die Vision-API."""
    return f"""Klassifiziere jede der {len(page_indices)} PDF-Seiten.

Fuer jede Seite (in Reihenfolge der Bilder, Seiten {', '.join(str(i) for i in page_indices)}):

Antworte als JSON-Array:
[
  {{
    "page": <Seitennummer>,
    "page_type": "text|plan|photo|mixed",
    "plan_subtype": "floor_plan|section|elevation|site_plan|detail|schedule|",
    "building_elements": ["Waende", "Tueren", ...],
    "confidence": 0.0-1.0,
    "description": "Einzeilige Beschreibung"
  }}
]

Typen:
- text: Ueberwiegend Fliesstext oder Tabellen (Berichte, Berechnungen, Vertraege)
- plan: Technische Zeichnung (Grundriss, Schnitt, Ansicht, Detail, Lageplan)
- photo: Fotografie (Baustelle, Gebaeude, Innenraum)
- mixed: Kombination aus Text und Zeichnung/Bild

Plan-Subtypen:
- floor_plan: Grundriss (Draufsicht auf ein Geschoss)
- section: Schnitt (vertikaler Durchschnitt)
- elevation: Ansicht (Fassadenansicht)
- site_plan: Lageplan / Umgebungsplan
- detail: Detailzeichnung (Anschluss, Konstruktion)
- schedule: Tuer-/Fensterliste, Raumbuch o.ae.

Bei building_elements: Nenne sichtbare Bauteile (Waende, Decken, Tueren, Fenster, Treppen, Dach, TGA etc.)"""


def classify_pages_llm(doc: fitz.Document, page_indices: list[int] = None,
                       client=None, model: str = "claude-haiku-4-5-20251001",
                       batch_size: int = 15) -> dict[int, PageClassification]:
    """Klassifiziert Seiten mittels Vision-LLM.

    Args:
        doc: Geoeffnetes pymupdf-Dokument
        page_indices: Zu klassifizierende Seiten (0-basiert). None = alle.
        client: Anthropic-Client
        model: Vision-faehiges Modell
        batch_size: Seiten pro API-Call (max ~20 wg. Token-Limit)

    Returns:
        {page_number (1-basiert): PageClassification}
    """
    from anthropic import Anthropic
    from .step1_analyze import extract_json

    if client is None:
        client = Anthropic()

    if page_indices is None:
        page_indices = list(range(len(doc)))

    results = {}

    # In Batches aufteilen
    for batch_start in range(0, len(page_indices), batch_size):
        batch = page_indices[batch_start:batch_start + batch_size]
        page_numbers = [i + 1 for i in batch]  # 1-basiert

        # Content mit Thumbnails bauen
        content = []
        for idx in batch:
            page = doc[idx]
            thumbnail_b64 = _render_thumbnail(page, dpi=72)
            content.append({
                "type": "image",
                "source": {
                    "type": "base64",
                    "media_type": "image/png",
                    "data": thumbnail_b64,
                },
            })

        content.append({
            "type": "text",
            "text": _build_classification_prompt(page_numbers),
        })

        response = client.messages.create(
            model=model,
            max_tokens=256 + len(batch) * 100,
            messages=[{"role": "user", "content": content}],
        )

        response_text = response.content[0].text
        data = extract_json(response_text)

        # JSON kann Array oder {pages: [...]} sein
        pages_data = data if isinstance(data, list) else data.get("pages", [data])

        for item in pages_data:
            page_num = item.get("page", 0)
            if page_num < 1:
                continue

            results[page_num] = PageClassification(
                page=page_num,
                page_type=item.get("page_type", "mixed"),
                confidence=item.get("confidence", 0.8),
                plan_subtype=item.get("plan_subtype", ""),
                metrics={"building_elements": item.get("building_elements", []),
                          "description": item.get("description", "")},
            )

    return results


# ---------------------------------------------------------------------------
# Hauptfunktionen
# ---------------------------------------------------------------------------

def classify_document(pdf_path: str | Path,
                      mode: str = "local",
                      client=None) -> DocumentClassification:
    """Klassifiziert ein PDF-Dokument.

    Args:
        pdf_path: Pfad zur PDF
        mode: "local", "llm", oder "hybrid"
        client: Anthropic-Client (nur fuer llm/hybrid)

    Returns:
        DocumentClassification mit Seiten-Details
    """
    pdf_path = Path(pdf_path)
    doc = fitz.open(str(pdf_path))

    page_classifications = []
    best_title_block = {}

    # --- Schritt 1: Lokale Metriken + Schriftfeld fuer ALLE Seiten ---
    local_results = {}
    for page_idx in range(len(doc)):
        page = doc[page_idx]
        page_num = page_idx + 1

        metrics = _compute_page_metrics(page)
        has_tb, tb_meta = _detect_title_block(page)

        if has_tb and tb_meta and not best_title_block:
            best_title_block = tb_meta

        page_type, confidence, plan_subtype = _classify_page_local(metrics, has_tb)

        local_results[page_num] = PageClassification(
            page=page_num,
            page_type=page_type,
            confidence=confidence,
            plan_subtype=plan_subtype,
            metrics=metrics,
            title_block=tb_meta if has_tb else {},
        )

    # --- Schritt 2: Je nach Modus LLM einsetzen ---
    if mode == "local":
        page_classifications = list(local_results.values())

    elif mode == "llm":
        # Alle Seiten per LLM klassifizieren
        llm_results = classify_pages_llm(doc, client=client)
        for page_num, local_cls in local_results.items():
            if page_num in llm_results:
                llm_cls = llm_results[page_num]
                # LLM-Ergebnis uebernehmen, lokale Metriken behalten
                llm_cls.metrics.update(local_cls.metrics)
                llm_cls.title_block = local_cls.title_block
                page_classifications.append(llm_cls)
            else:
                page_classifications.append(local_cls)

    elif mode == "hybrid":
        # LLM nur fuer unsichere Seiten
        uncertain_indices = []
        for page_num, cls in local_results.items():
            if cls.confidence < 0.7 or cls.page_type == "mixed":
                uncertain_indices.append(page_num - 1)  # 0-basiert

        if uncertain_indices:
            llm_results = classify_pages_llm(
                doc, page_indices=uncertain_indices, client=client
            )
        else:
            llm_results = {}

        for page_num, local_cls in local_results.items():
            if page_num in llm_results:
                llm_cls = llm_results[page_num]
                llm_cls.metrics.update(local_cls.metrics)
                llm_cls.title_block = local_cls.title_block
                page_classifications.append(llm_cls)
            else:
                page_classifications.append(local_cls)

    else:
        raise ValueError(f"Unbekannter Modus: {mode}. Erlaubt: local, llm, hybrid")

    doc.close()

    # --- Schritt 3: Dokument-Level Klassifikation ---
    doc_type, doc_confidence = _aggregate_document_type(page_classifications)

    return DocumentClassification(
        file=pdf_path.name,
        document_type=doc_type,
        confidence=doc_confidence,
        page_count=len(page_classifications),
        pages=page_classifications,
        title_block_metadata=best_title_block,
    )


def _aggregate_document_type(pages: list[PageClassification]) -> tuple[str, float]:
    """Bestimmt den Dokumenttyp aus den Seitenklassifikationen.

    Returns:
        (document_type, confidence)
    """
    if not pages:
        return "text", 0.0

    type_counts = {}
    type_conf_sum = {}
    for p in pages:
        type_counts[p.page_type] = type_counts.get(p.page_type, 0) + 1
        type_conf_sum[p.page_type] = type_conf_sum.get(p.page_type, 0.0) + p.confidence

    total = len(pages)

    # Dominanter Typ (>60% der Seiten)
    for typ, count in sorted(type_counts.items(), key=lambda x: -x[1]):
        ratio = count / total
        if ratio > 0.6:
            avg_conf = type_conf_sum[typ] / count
            return typ, round(avg_conf, 2)

    # Kein dominanter Typ → mixed
    avg_conf = sum(type_conf_sum.values()) / total
    return "mixed", round(avg_conf, 2)


# ---------------------------------------------------------------------------
# Batch-Klassifikation (fuer ganze Projekte)
# ---------------------------------------------------------------------------

def classify_project_pdfs(pdfs: list[dict], mode: str = "local",
                          client=None,
                          cache_dir: Path = None) -> dict[str, DocumentClassification]:
    """Klassifiziert alle PDFs eines Projekts.

    Args:
        pdfs: Liste von PDF-Eintraegen aus scan_projects()
        mode: Klassifikationsmodus
        client: Anthropic-Client
        cache_dir: Optional Cache-Verzeichnis

    Returns:
        {relative_path: DocumentClassification}
    """
    results = {}
    cached_count = 0

    for pdf in pdfs:
        rel_path = pdf["relative_path"]

        # Cache pruefen
        if cache_dir:
            cache_path = cache_dir / "classification" / f"{_safe_key(rel_path)}.json"
            if cache_path.exists():
                try:
                    data = json.loads(cache_path.read_text(encoding="utf-8"))
                    cls = _classification_from_dict(data)
                    results[rel_path] = cls
                    cached_count += 1
                    continue
                except (json.JSONDecodeError, KeyError):
                    pass

        cls = classify_document(pdf["path"], mode=mode, client=client)
        results[rel_path] = cls

        # Cachen
        if cache_dir:
            cache_path = cache_dir / "classification" / f"{_safe_key(rel_path)}.json"
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            cache_path.write_text(
                json.dumps(cls.to_dict(), ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

    if cached_count:
        print(f"  Klassifikation: {cached_count} aus Cache")

    return results


def _safe_key(path: str) -> str:
    """Erzeugt einen sicheren Dateinamen aus einem Pfad."""
    return path.replace("/", "__").replace("\\", "__").replace(" ", "_")


def _classification_from_dict(data: dict) -> DocumentClassification:
    """Rekonstruiert eine DocumentClassification aus einem Dict."""
    pages = []
    for p in data.get("pages", []):
        pages.append(PageClassification(
            page=p["page"],
            page_type=p["page_type"],
            confidence=p["confidence"],
            plan_subtype=p.get("plan_subtype", ""),
            metrics=p.get("metrics", {}),
            title_block=p.get("title_block", {}),
        ))
    return DocumentClassification(
        file=data["file"],
        document_type=data["document_type"],
        confidence=data["confidence"],
        page_count=data["page_count"],
        pages=pages,
        title_block_metadata=data.get("title_block_metadata", {}),
    )


# ---------------------------------------------------------------------------
# Hilfsfunktionen fuer Pipeline-Integration
# ---------------------------------------------------------------------------

def split_by_type(pdfs: list[dict],
                  classifications: dict[str, DocumentClassification]
                  ) -> dict[str, list[dict]]:
    """Teilt PDFs nach Dokumenttyp auf.

    Returns:
        {"text": [...], "plan": [...], "photo": [...], "mixed": [...]}
    """
    groups = {"text": [], "plan": [], "photo": [], "mixed": []}
    for pdf in pdfs:
        rel = pdf["relative_path"]
        cls = classifications.get(rel)
        doc_type = cls.document_type if cls else "text"
        groups.setdefault(doc_type, []).append(pdf)
    return groups


def print_classification_summary(classifications: dict[str, DocumentClassification]):
    """Gibt eine Uebersicht der Klassifikationen aus."""
    type_counts = {"text": 0, "plan": 0, "photo": 0, "mixed": 0}
    total_pages = 0
    plan_pages = 0

    for rel_path, cls in classifications.items():
        type_counts[cls.document_type] = type_counts.get(cls.document_type, 0) + 1
        total_pages += cls.page_count
        plan_pages += sum(1 for p in cls.pages if p.page_type == "plan")

    print(f"\n  Klassifikation: {len(classifications)} PDFs, {total_pages} Seiten")
    for typ, count in type_counts.items():
        if count > 0:
            print(f"    {typ}: {count} Dokumente")
    if plan_pages > 0:
        print(f"    → {plan_pages} Planseiten (fuer Vision-Pipeline)")

    # Schriftfeld-Funde
    tb_count = sum(1 for c in classifications.values() if c.title_block_metadata)
    if tb_count > 0:
        print(f"    → {tb_count} Dokumente mit erkanntem Schriftfeld")
