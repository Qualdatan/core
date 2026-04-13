"""Vision-Pipeline: Analyse von Plan-PDFs und Fotos mittels Claude Vision API.

Drei-Pass-Strategie:
  Pass 1 (Triage):        72 DPI Thumbnails, bis 15 Seiten pro Batch.
                           Erkennt Seitentyp, Bauelemente, LOG-Schaetzung, Prioritaet.
  Pass 2 (Detail):        150 DPI, nur priority=high/medium Seiten.
                           Bauelemente mit IFC-Klasse, LOG-Evidenz, Parameter.
  Pass 3 (Localisation):  150 DPI, pro Detail-Seite ein Call.
                           Normalisierte Bounding-Boxen pro Bauelement fuer
                           echte PDF-Annotation statt "ganze Seite".

Token-Budget wird in der Recipe konfiguriert (max_visual_tokens).
"""

import base64
import json
from dataclasses import dataclass, field, asdict
from pathlib import Path

import fitz  # pymupdf


# ---------------------------------------------------------------------------
# Datenstrukturen
# ---------------------------------------------------------------------------

@dataclass
class TriageResult:
    """Ergebnis der Thumbnail-Triage (Pass 1) fuer eine Seite."""
    page: int
    page_type: str = ""           # floor_plan|section|elevation|site_plan|detail|schedule|photo|text
    building_elements: list[str] = field(default_factory=list)
    estimated_log: str = ""       # LOG-01 bis LOG-05
    lph_evidence: str = ""        # z.B. "3-5"
    priority: str = "skip"        # high|medium|low|skip
    description: str = ""
    confidence: float = 0.0

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class ElementDetail:
    """Detail-Analyse eines Bauelements (Pass 2)."""
    element_type: str = ""         # z.B. "Tragende Wand"
    ifc_class: str = ""            # z.B. "IfcWall"
    log_achieved: str = ""         # LOG-01 bis LOG-05
    log_evidence: str = ""         # Begruendung
    visible_parameters: list[str] = field(default_factory=list)
    region: str = ""               # center|top-left|...
    # Normalisierte Bounding-Box [x0, y0, x1, y1] in 0..1-Seitenkoordinaten,
    # Ursprung oben-links. None solange Pass 3 nicht lief oder das Element
    # nicht lokalisiert werden konnte.
    bbox: list[float] | None = None


@dataclass
class DetailResult:
    """Ergebnis der Detail-Analyse (Pass 2) fuer eine Seite."""
    page: int
    building_elements: list[ElementDetail] = field(default_factory=list)
    annotations: list[str] = field(default_factory=list)
    cross_references: list[str] = field(default_factory=list)
    description: str = ""

    def to_dict(self) -> dict:
        d = asdict(self)
        return d


@dataclass
class VisualAnalysisResult:
    """Gesamtergebnis der visuellen Analyse eines PDF-Dokuments."""
    file: str
    project: str
    page_count: int
    triage: list[TriageResult] = field(default_factory=list)
    details: list[DetailResult] = field(default_factory=list)
    token_usage: int = 0

    def to_dict(self) -> dict:
        return {
            "file": self.file,
            "project": self.project,
            "page_count": self.page_count,
            "triage": [t.to_dict() for t in self.triage],
            "details": [d.to_dict() for d in self.details],
            "token_usage": self.token_usage,
        }

    def visual_codings(
        self,
        page_dimensions: dict[int, tuple[float, float]] | None = None,
    ) -> list[dict]:
        """Erzeugt Kodierungen fuer die QDPX-Integration.

        Gibt eine Liste von Kodierungen zurueck, die analog zu Text-Kodierungen
        in qdpx_merger.add_visual_sources() verwendet werden koennen.

        Args:
            page_dimensions: Optional. Mapping Seitenzahl -> (Breite, Hoehe)
                in PDF-Userspace-Punkten. Wird benoetigt um normalisierte
                bbox-Koordinaten aus Pass 3 in echte Punkte umzurechnen.
                Fehlt dieser Parameter, enthalten die Kodierungen keine
                bbox-Information (Downstream faellt auf ganze Seite zurueck).
        """
        codings = []
        coding_idx = 0

        for triage in self.triage:
            if triage.priority == "skip":
                continue

            # Detail-Ergebnis fuer diese Seite suchen
            detail = None
            for d in self.details:
                if d.page == triage.page:
                    detail = d
                    break

            if detail and detail.building_elements:
                # Detaillierte Kodierungen pro Bauelement
                dims = page_dimensions.get(triage.page) if page_dimensions else None
                for elem in detail.building_elements:
                    codes = _element_to_codes(elem)
                    if codes:
                        entry = {
                            "block_id": f"p{triage.page}_v{coding_idx}",
                            "page": triage.page,
                            "codes": codes,
                            "description": (
                                f"{elem.element_type}: {elem.log_evidence}"
                                if elem.log_evidence
                                else elem.element_type
                            ),
                            "source": "visual_detail",
                        }
                        # bbox nur einfuegen wenn Element lokalisiert ist
                        # UND Seitendimensionen verfuegbar sind.
                        if dims and elem.bbox and _is_valid_bbox(elem.bbox):
                            w, h = dims
                            x0, y0, x1, y1 = elem.bbox
                            entry["bbox"] = [x0 * w, y0 * h, x1 * w, y1 * h]
                        codings.append(entry)
                        coding_idx += 1
            else:
                # Nur Triage-Daten: grobere Kodierungen
                codes = _triage_to_codes(triage)
                if codes:
                    codings.append({
                        "block_id": f"p{triage.page}_v{coding_idx}",
                        "page": triage.page,
                        "codes": codes,
                        "description": triage.description,
                        "source": "visual_triage",
                    })
                    coding_idx += 1

        return codings


# ---------------------------------------------------------------------------
# Code-Mapping: Visuelle Ergebnisse -> Codes O/P/Q
# ---------------------------------------------------------------------------

# Bauelement-Typ -> O-Code Mapping
_ELEMENT_CODE_MAP = {
    "wand": "O-01", "waende": "O-01", "stuetze": "O-01", "stuetzen": "O-01",
    "decke": "O-01", "decken": "O-01", "fundament": "O-01", "fundamente": "O-01",
    "bodenplatte": "O-01", "tragend": "O-01",
    "trennwand": "O-02", "trennwaende": "O-02", "bruestung": "O-02",
    "nichttragend": "O-02", "leichtbauwand": "O-02",
    "tuer": "O-03", "tueren": "O-03", "fenster": "O-03", "tor": "O-03",
    "tore": "O-03", "oeffnung": "O-03", "oeffnungen": "O-03",
    "treppe": "O-04", "treppen": "O-04", "rampe": "O-04", "rampen": "O-04",
    "aufzug": "O-04",
    "dach": "O-05", "dachkonstruktion": "O-05", "sparren": "O-05",
    "pfette": "O-05", "dachstuhl": "O-05",
    "fassade": "O-06", "aussenhuelle": "O-06", "wdvs": "O-06",
    "daemmung": "O-06", "verkleidung": "O-06",
    "heizung": "O-07", "lueftung": "O-07", "sanitaer": "O-07",
    "elektro": "O-07", "tga": "O-07", "haustechnik": "O-07",
    "rohr": "O-07", "leitung": "O-07",
    "gelaende": "O-08", "aussenanlage": "O-08", "aussenanlagen": "O-08",
    "pflasterung": "O-08", "bepflanzung": "O-08",
}

# Seitentyp -> P-Code Mapping
_PAGE_TYPE_CODE_MAP = {
    "floor_plan": "P-01",
    "section": "P-02",
    "elevation": "P-03",
    "detail": "P-04",
    "site_plan": "P-05",
    "schedule": "P-07",
}

# LOG-Stufe -> Q-Code
_LOG_CODE = "Q-01"
_LOI_CODE = "Q-02"


def _element_to_codes(elem: ElementDetail) -> list[str]:
    """Mappt ein Bauelement auf O/Q-Codes."""
    codes = []

    # O-Code aus Element-Typ
    element_lower = elem.element_type.lower()
    for keyword, code in _ELEMENT_CODE_MAP.items():
        if keyword in element_lower:
            if code not in codes:
                codes.append(code)
            break

    # Q-01 wenn LOG-Evidenz vorhanden
    if elem.log_achieved:
        codes.append(_LOG_CODE)

    # Q-02 wenn sichtbare Parameter (LOI)
    if elem.visible_parameters:
        codes.append(_LOI_CODE)

    return codes


def _triage_to_codes(triage: TriageResult) -> list[str]:
    """Mappt Triage-Ergebnis auf P/O-Codes."""
    codes = []

    # P-Code aus Seitentyp
    p_code = _PAGE_TYPE_CODE_MAP.get(triage.page_type)
    if p_code:
        codes.append(p_code)

    # O-Codes aus erkannten Bauelementen
    for element in triage.building_elements:
        element_lower = element.lower()
        for keyword, code in _ELEMENT_CODE_MAP.items():
            if keyword in element_lower:
                if code not in codes:
                    codes.append(code)
                break

    # Q-01 wenn LOG geschaetzt
    if triage.estimated_log:
        codes.append(_LOG_CODE)

    return codes


# ---------------------------------------------------------------------------
# Thumbnail-Rendering
# ---------------------------------------------------------------------------

def render_page_thumbnail(page: fitz.Page, dpi: int = 72) -> str:
    """Rendert eine PDF-Seite als base64-encoded PNG.

    Args:
        page: pymupdf Page
        dpi: Aufloesung (72 = Thumbnail, 150 = Detail)

    Returns:
        Base64-encoded PNG string
    """
    pixmap = page.get_pixmap(dpi=dpi)
    png_bytes = pixmap.tobytes("png")
    return base64.b64encode(png_bytes).decode("ascii")


def estimate_image_tokens(b64_data: str) -> int:
    """Schaetzt die Token-Kosten fuer ein Base64-Bild.

    Claude Vision: ~1500 Token fuer ein 72-DPI A4-Thumbnail,
    ~6000 Token fuer ein 150-DPI A4-Bild.
    """
    raw_bytes = len(b64_data) * 3 / 4  # Base64 -> Bytes
    # Grobe Schaetzung: 1 Token pro ~3 Bytes bei PNG
    return max(200, int(raw_bytes / 3))


# ---------------------------------------------------------------------------
# Pass 1: Thumbnail-Triage
# ---------------------------------------------------------------------------

TRIAGE_PROMPT = """Du analysierst Seiten aus Bauplan-PDFs oder Projektfotos.

Fuer jede Seite (Bilder in Reihenfolge, Seiten {page_list}):

Antworte als JSON-Array:
[
  {{
    "page": <Seitennummer>,
    "page_type": "floor_plan|section|elevation|site_plan|detail|schedule|photo|text",
    "building_elements": ["Waende", "Decken", "Tueren", ...],
    "estimated_log": "LOG-01|LOG-02|LOG-03|LOG-04|LOG-05|",
    "lph_evidence": "1-2|3-5|6-8|",
    "priority": "high|medium|low|skip",
    "confidence": 0.0-1.0,
    "description": "Einzeilige Beschreibung der Seite"
  }}
]

## Seitentypen:
- floor_plan: Grundriss (Draufsicht auf Geschoss mit Raumaufteilung)
- section: Schnitt (vertikaler Durchschnitt durch Gebaeude)
- elevation: Ansicht (Fassadenansicht von aussen)
- site_plan: Lageplan / Umgebungsplan
- detail: Detailzeichnung (Anschluss, Konstruktionsdetail)
- schedule: Tabelle/Liste (Tuerliste, Raumbuch, Fensterliste)
- photo: Fotografie (Baustelle, Gebaeude)
- text: Ueberwiegend Text (Bericht, Beschreibung)

## LOG-Stufen (Geometriedetailgrad):
- LOG-01: Symbolische Darstellung (z.B. Linie fuer Wand)
- LOG-02: Vereinfachte Geometrie (z.B. Wand als Rechteck ohne Aufbau)
- LOG-03: Detaillierte Geometrie (z.B. mehrschichtiger Wandaufbau sichtbar)
- LOG-04: Ausfuehrungsreife Darstellung (z.B. mit Anschluessen und Fugen)
- LOG-05: As-built / Bestandsaufnahme

## Prioritaet fuer Detail-Analyse:
- high: Technische Zeichnung mit vielen Bauelementen und erkennbarem Detailgrad
- medium: Zeichnung mit einigen erkennbaren Elementen
- low: Einfache Darstellung, wenig Detail
- skip: Textseite, Legende, Deckblatt oder nicht relevant

## Bauelemente: Nenne alle sichtbaren Bauteile:
Waende, Decken, Stuetzen, Fundamente, Tueren, Fenster, Treppen, Rampen,
Dach, Fassade, TGA (Heizung/Lueftung/Sanitaer), Aussenanlagen, etc.

Sei praezise und konsistent. Wenn du dir unsicher bist, setze confidence < 0.7."""


def run_triage(doc: fitz.Document, page_indices: list[int] = None,
               client=None, model: str = "claude-haiku-4-5-20251001",
               batch_size: int = 15,
               cache_dir: Path = None,
               cache_key: str = "") -> list[TriageResult]:
    """Pass 1: Thumbnail-Triage fuer alle Planseiten.

    Args:
        doc: Geoeffnetes pymupdf-Dokument
        page_indices: Zu analysierende Seiten (0-basiert). None = alle.
        client: Anthropic-Client
        model: Vision-faehiges Modell
        batch_size: Seiten pro API-Call
        cache_dir: Cache-Verzeichnis
        cache_key: Basis-Schluessel fuer Cache

    Returns:
        Liste von TriageResult
    """
    from anthropic import Anthropic
    from ..steps.step1_analyze import extract_json

    if client is None:
        client = Anthropic()

    if page_indices is None:
        page_indices = list(range(len(doc)))

    # Cache pruefen
    if cache_dir and cache_key:
        cache_path = cache_dir / "visual_triage" / f"{cache_key}.json"
        if cache_path.exists():
            try:
                data = json.loads(cache_path.read_text(encoding="utf-8"))
                results = [_triage_from_dict(d) for d in data]
                print(f"    CACHE HIT (Triage): {cache_key}")
                return results
            except (json.JSONDecodeError, KeyError):
                pass

    results = []
    total_tokens = 0

    for batch_start in range(0, len(page_indices), batch_size):
        batch = page_indices[batch_start:batch_start + batch_size]
        page_numbers = [i + 1 for i in batch]

        # Thumbnails rendern
        content = []
        for idx in batch:
            b64 = render_page_thumbnail(doc[idx], dpi=72)
            total_tokens += estimate_image_tokens(b64)
            content.append({
                "type": "image",
                "source": {
                    "type": "base64",
                    "media_type": "image/png",
                    "data": b64,
                },
            })

        page_list = ", ".join(str(p) for p in page_numbers)
        prompt = TRIAGE_PROMPT.replace("{page_list}", page_list)
        content.append({"type": "text", "text": prompt})

        print(f"    Triage-Batch: Seiten {page_list} ({len(batch)} Thumbnails)")

        response = client.messages.create(
            model=model,
            max_tokens=512 + len(batch) * 150,
            messages=[{"role": "user", "content": content}],
        )

        total_tokens += response.usage.input_tokens + response.usage.output_tokens

        response_text = response.content[0].text
        data = extract_json(response_text)

        # JSON kann Array oder Objekt sein
        if isinstance(data, dict):
            pages_data = data.get("pages", data.get("results", [data]))
        else:
            pages_data = data

        if not isinstance(pages_data, list):
            pages_data = [pages_data]

        for item in pages_data:
            page_num = item.get("page", 0)
            if page_num < 1:
                continue
            results.append(TriageResult(
                page=page_num,
                page_type=item.get("page_type", ""),
                building_elements=item.get("building_elements", []),
                estimated_log=item.get("estimated_log", ""),
                lph_evidence=item.get("lph_evidence", ""),
                priority=item.get("priority", "low"),
                description=item.get("description", ""),
                confidence=item.get("confidence", 0.7),
            ))

    # Cachen
    if cache_dir and cache_key:
        cache_path = cache_dir / "visual_triage" / f"{cache_key}.json"
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(
            json.dumps([t.to_dict() for t in results], ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    return results


# ---------------------------------------------------------------------------
# Pass 2: Detail-Analyse
# ---------------------------------------------------------------------------

DETAIL_PROMPT = """Du analysierst eine technische Zeichnung aus einem Bauprojekt im Detail.
Seite {page_num}: {description}

Identifiziere alle sichtbaren Bauelemente und bewerte deren Geometriedetailgrad (LOG).

Antworte als JSON:
{{
  "page": {page_num},
  "building_elements": [
    {{
      "element_type": "z.B. Tragende Wand",
      "ifc_class": "z.B. IfcWall",
      "log_achieved": "LOG-01 bis LOG-05",
      "log_evidence": "Begruendung fuer die LOG-Einstufung",
      "visible_parameters": ["Parameter1", "Parameter2"],
      "region": "center|top-left|top-right|bottom-left|bottom-right"
    }}
  ],
  "annotations": ["Raumstempel", "Bemassung", ...],
  "cross_references": ["Verweis auf andere Plaene"],
  "description": "Zusammenfassende Beschreibung"
}}

## IFC-Klassen (haeufigste):
IfcWall, IfcSlab, IfcColumn, IfcBeam, IfcDoor, IfcWindow, IfcStair,
IfcRamp, IfcRoof, IfcCurtainWall, IfcBuildingElementProxy,
IfcFlowTerminal, IfcFlowSegment, IfcSpace, IfcSite

## LOG-Stufen:
- LOG-01: Symbol / schematisch (Strich fuer Wand)
- LOG-02: Vereinfacht (Wand als Rechteck, keine Schichten)
- LOG-03: Detailliert (Wandaufbau mit Schichten sichtbar)
- LOG-04: Ausfuehrungsreif (Anschluesse, Fugen, Bewehrung)
- LOG-05: As-built (Bestandsaufnahme mit Toleranzen)

## Sichtbare Parameter (LOI-Evidenz):
Nenne alle in der Zeichnung ablesbaren Parameter wie:
Wanddicke, Raumhoehe, Flaeche, Material, U-Wert, Tuermass, Fenstermass, etc.

Sei gruendlich aber praezise. Nur Elemente nennen die tatsaechlich sichtbar sind."""


def run_detail_analysis(doc: fitz.Document, triage_results: list[TriageResult],
                        client=None, model: str = "claude-sonnet-4-20250514",
                        max_tokens_budget: int = 500000,
                        cache_dir: Path = None,
                        cache_key: str = "") -> list[DetailResult]:
    """Pass 2: Detail-Analyse fuer high/medium-priority Seiten.

    Args:
        doc: Geoeffnetes pymupdf-Dokument
        triage_results: Ergebnisse aus Pass 1
        client: Anthropic-Client
        model: Leistungsfaehiges Modell fuer Detail-Analyse
        max_tokens_budget: Maximales Token-Budget
        cache_dir: Cache-Verzeichnis
        cache_key: Basis-Schluessel fuer Cache

    Returns:
        Liste von DetailResult
    """
    from anthropic import Anthropic
    from ..steps.step1_analyze import extract_json

    if client is None:
        client = Anthropic()

    # Cache pruefen
    if cache_dir and cache_key:
        cache_path = cache_dir / "visual_detail" / f"{cache_key}.json"
        if cache_path.exists():
            try:
                data = json.loads(cache_path.read_text(encoding="utf-8"))
                results = [_detail_from_dict(d) for d in data]
                print(f"    CACHE HIT (Detail): {cache_key}")
                return results
            except (json.JSONDecodeError, KeyError):
                pass

    # Nur high/medium Seiten, high zuerst
    priority_order = {"high": 0, "medium": 1}
    candidates = [
        t for t in triage_results
        if t.priority in ("high", "medium")
    ]
    candidates.sort(key=lambda t: priority_order.get(t.priority, 2))

    results = []
    tokens_used = 0

    for triage in candidates:
        if tokens_used >= max_tokens_budget:
            remaining = len(candidates) - len(results)
            print(f"    Token-Budget erreicht. {remaining} Seiten uebersprungen.")
            break

        page_idx = triage.page - 1  # 0-basiert
        if page_idx < 0 or page_idx >= len(doc):
            continue

        # Hochaufloesungs-Rendering
        b64 = render_page_thumbnail(doc[page_idx], dpi=150)
        img_tokens = estimate_image_tokens(b64)

        prompt = DETAIL_PROMPT.replace("{page_num}", str(triage.page))
        prompt = prompt.replace("{description}", triage.description or "Technische Zeichnung")

        content = [
            {
                "type": "image",
                "source": {
                    "type": "base64",
                    "media_type": "image/png",
                    "data": b64,
                },
            },
            {"type": "text", "text": prompt},
        ]

        print(f"    Detail-Analyse: Seite {triage.page} "
              f"(priority={triage.priority}, ~{img_tokens} img-tokens)")

        response = client.messages.create(
            model=model,
            max_tokens=2048,
            messages=[{"role": "user", "content": content}],
        )

        call_tokens = response.usage.input_tokens + response.usage.output_tokens
        tokens_used += call_tokens

        response_text = response.content[0].text
        data = extract_json(response_text)

        elements = []
        for elem_data in data.get("building_elements", []):
            elements.append(ElementDetail(
                element_type=elem_data.get("element_type", ""),
                ifc_class=elem_data.get("ifc_class", ""),
                log_achieved=elem_data.get("log_achieved", ""),
                log_evidence=elem_data.get("log_evidence", ""),
                visible_parameters=elem_data.get("visible_parameters", []),
                region=elem_data.get("region", ""),
                bbox=None,  # wird erst in Pass 3 gefuellt
            ))

        results.append(DetailResult(
            page=triage.page,
            building_elements=elements,
            annotations=data.get("annotations", []),
            cross_references=data.get("cross_references", []),
            description=data.get("description", ""),
        ))

    # Cachen
    if cache_dir and cache_key:
        cache_path = cache_dir / "visual_detail" / f"{cache_key}.json"
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(
            json.dumps([d.to_dict() for d in results], ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    return results, tokens_used


# ---------------------------------------------------------------------------
# Pass 3: Localisation (Bounding Boxes)
# ---------------------------------------------------------------------------

LOCALISATION_PROMPT = """Du siehst eine einzelne Seite aus einem Bauplan-PDF.
Seite {page_num}.

Fuer diese Seite wurden bereits folgende Bauelemente erkannt (nummeriert ab 0):

{element_list}

Deine Aufgabe: Bestimme fuer moeglichst jedes Element eine normalisierte
Bounding-Box `[x0, y0, x1, y1]` im Bildkoordinatensystem der angezeigten
Seite.

## Koordinatensystem
- x0, y0 = linke obere Ecke der Box
- x1, y1 = rechte untere Ecke der Box
- Alle Werte sind **normalisiert auf 0..1** relativ zur Seitengroesse.
  (x=0 ist der linke Rand, x=1 der rechte; y=0 oben, y=1 unten.)
- Es muss immer gelten: x0 < x1 und y0 < y1.

## Wichtig
- Gib **nur** Elemente zurueck, die du tatsaechlich im Bild lokalisieren
  kannst. Lieber ein Element weglassen als wild raten.
- Die `index`-Werte muessen den oben angegebenen Nummern entsprechen.
- Antworte mit **ausschliesslich** einem JSON-Objekt, kein Fliesstext drumherum.

## Antwortformat
```json
{{
  "page": {page_num},
  "elements": [
    {{"index": 0, "bbox": [0.10, 0.05, 0.92, 0.40]}},
    {{"index": 2, "bbox": [0.44, 0.51, 0.59, 0.62]}}
  ]
}}
```

Elemente ohne sichere Lokalisierung einfach weglassen."""


def _is_valid_bbox(bbox) -> bool:
    """Prueft ob eine bbox syntaktisch und semantisch valide ist.

    Gueltig heisst: Liste/Tuple mit vier Zahlen, alle in [0, 1],
    und x0 < x1, y0 < y1. Bewusst auslagert, damit separat testbar.
    """
    if bbox is None:
        return False
    if not isinstance(bbox, (list, tuple)) or len(bbox) != 4:
        return False
    try:
        x0, y0, x1, y1 = (float(v) for v in bbox)
    except (TypeError, ValueError):
        return False
    if not all(0.0 <= v <= 1.0 for v in (x0, y0, x1, y1)):
        return False
    if not (x0 < x1 and y0 < y1):
        return False
    return True


def _format_element_list(elements: list[ElementDetail]) -> str:
    """Formatiert die bekannten Elemente fuer den Localisation-Prompt."""
    lines = []
    for i, elem in enumerate(elements):
        label = elem.element_type or "(unbekanntes Element)"
        if elem.ifc_class:
            label += f" [{elem.ifc_class}]"
        if elem.region:
            label += f" (Region: {elem.region})"
        lines.append(f"  {i}: {label}")
    return "\n".join(lines) if lines else "  (keine)"


def run_localisation(
    doc: fitz.Document,
    detail_results: list[DetailResult],
    client=None,
    model: str = "claude-sonnet-4-20250514",
    max_tokens_budget: int = 200000,
    cache_dir: Path = None,
    cache_key: str = "",
) -> tuple[list[DetailResult], int]:
    """Pass 3: Erfragt normalisierte Bounding Boxes pro Bauelement.

    Verarbeitet jede Seite, fuer die Pass 2 Bauelemente geliefert hat.
    Sendet ein 150-DPI-Rendering plus die Liste der bekannten Elemente und
    bittet das Modell, fuer jedes Element [x0, y0, x1, y1] in normalisierten
    Koordinaten (0..1) zurueckzugeben.

    Args:
        doc: Geoeffnetes pymupdf-Dokument
        detail_results: Ergebnisse aus Pass 2 (werden inplace erweitert).
        client: Anthropic-Client
        model: Vision-faehiges Modell
        max_tokens_budget: Token-Budget nur fuer Pass 3
        cache_dir: Cache-Verzeichnis
        cache_key: Basis-Schluessel fuer Cache

    Returns:
        (aktualisierte detail_results mit bbox-Feldern, total_tokens)
    """
    from anthropic import Anthropic
    from ..steps.step1_analyze import extract_json

    if client is None:
        client = Anthropic()

    # Cache pruefen
    if cache_dir and cache_key:
        cache_path = cache_dir / "visual_localisation" / f"{cache_key}.json"
        if cache_path.exists():
            try:
                data = json.loads(cache_path.read_text(encoding="utf-8"))
                results = [_detail_from_dict(d) for d in data]
                print(f"    CACHE HIT (Localisation): {cache_key}")
                return results, 0
            except (json.JSONDecodeError, KeyError):
                pass

    tokens_used = 0
    localised_count = 0

    for detail in detail_results:
        if tokens_used >= max_tokens_budget:
            remaining = sum(
                1 for d in detail_results
                if d.page >= detail.page and d.building_elements
            )
            print(f"    Token-Budget (Pass 3) erreicht. "
                  f"{remaining} Seiten ohne Lokalisierung.")
            break

        if not detail.building_elements:
            continue

        page_idx = detail.page - 1
        if page_idx < 0 or page_idx >= len(doc):
            continue

        # Hochaufloesungs-Rendering (gleich wie Pass 2)
        b64 = render_page_thumbnail(doc[page_idx], dpi=150)
        img_tokens = estimate_image_tokens(b64)

        prompt = LOCALISATION_PROMPT.format(
            page_num=detail.page,
            element_list=_format_element_list(detail.building_elements),
        )

        content = [
            {
                "type": "image",
                "source": {
                    "type": "base64",
                    "media_type": "image/png",
                    "data": b64,
                },
            },
            {"type": "text", "text": prompt},
        ]

        print(f"    Localisation: Seite {detail.page} "
              f"({len(detail.building_elements)} Elemente, "
              f"~{img_tokens} img-tokens)")

        response = client.messages.create(
            model=model,
            max_tokens=1024,
            messages=[{"role": "user", "content": content}],
        )

        call_tokens = response.usage.input_tokens + response.usage.output_tokens
        tokens_used += call_tokens

        response_text = response.content[0].text
        try:
            data = extract_json(response_text)
        except Exception as exc:
            print(f"      JSON-Parse-Fehler: {exc}")
            continue

        if not isinstance(data, dict):
            print(f"      Unerwartetes Antwortformat (kein dict): {type(data).__name__}")
            continue

        items = data.get("elements", [])
        if not isinstance(items, list):
            print(f"      'elements' ist keine Liste, ignoriere.")
            continue

        for item in items:
            if not isinstance(item, dict):
                continue
            idx = item.get("index")
            bbox = item.get("bbox")
            if not isinstance(idx, int):
                continue
            if idx < 0 or idx >= len(detail.building_elements):
                print(f"      Ungueltiger Index {idx} (max {len(detail.building_elements) - 1})")
                continue
            if not _is_valid_bbox(bbox):
                print(f"      Ungueltige bbox fuer Index {idx}: {bbox}")
                continue
            # In eine Liste von Floats konvertieren (Modell liefert manchmal ints).
            detail.building_elements[idx].bbox = [float(v) for v in bbox]
            localised_count += 1

    # Cachen: gesamte detail_results-Liste inkl. neuer bboxes
    if cache_dir and cache_key:
        cache_path = cache_dir / "visual_localisation" / f"{cache_key}.json"
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(
            json.dumps(
                [d.to_dict() for d in detail_results],
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

    return detail_results, tokens_used


# ---------------------------------------------------------------------------
# Hauptfunktion: Analyse eines PDFs
# ---------------------------------------------------------------------------

def analyze_visual_pdf(pdf_path: str | Path, project: str = "",
                       client=None,
                       triage_model: str = "claude-haiku-4-5-20251001",
                       detail_model: str = "claude-sonnet-4-20250514",
                       max_visual_tokens: int = 500000,
                       skip_detail: bool = False,
                       skip_localisation: bool = False,
                       cache_dir: Path = None) -> VisualAnalysisResult:
    """Fuehrt die vollstaendige visuelle Analyse eines PDFs durch.

    Args:
        pdf_path: Pfad zur PDF
        project: Projektname
        client: Anthropic-Client
        triage_model: Modell fuer Pass 1
        detail_model: Modell fuer Pass 2 (und Pass 3)
        max_visual_tokens: Budget fuer Detail-Analyse
        skip_detail: Nur Triage, kein Detail-Pass
        skip_localisation: Kein Pass 3 (Bounding-Boxen)
        cache_dir: Cache-Verzeichnis

    Returns:
        VisualAnalysisResult
    """
    pdf_path = Path(pdf_path)
    doc = fitz.open(str(pdf_path))

    cache_key = _safe_key(f"{project}__{pdf_path.stem}") if project else _safe_key(pdf_path.stem)

    # Pass 1: Triage
    print(f"\n  Pass 1 (Triage): {pdf_path.name}")
    triage_results = run_triage(
        doc, client=client, model=triage_model,
        cache_dir=cache_dir, cache_key=cache_key,
    )

    # Statistik
    priority_counts = {}
    for t in triage_results:
        priority_counts[t.priority] = priority_counts.get(t.priority, 0) + 1
    print(f"    Triage: {len(triage_results)} Seiten — "
          + ", ".join(f"{v}x {k}" for k, v in sorted(priority_counts.items())))

    # Pass 2: Detail (optional)
    detail_results = []
    total_tokens = 0
    if not skip_detail:
        high_medium = [t for t in triage_results if t.priority in ("high", "medium")]
        if high_medium:
            print(f"  Pass 2 (Detail): {len(high_medium)} Seiten")
            detail_results, detail_tokens = run_detail_analysis(
                doc, triage_results, client=client, model=detail_model,
                max_tokens_budget=max_visual_tokens,
                cache_dir=cache_dir, cache_key=cache_key,
            )
            total_tokens += detail_tokens
            print(f"    Detail: {len(detail_results)} Seiten analysiert, "
                  f"{detail_tokens:,} Token")

            # Pass 3: Localisation (Bounding-Boxen)
            if not skip_localisation and detail_results:
                remaining_budget = max(0, max_visual_tokens - total_tokens)
                if remaining_budget > 0:
                    print(f"  Pass 3 (Localisation): {len(detail_results)} Seiten")
                    detail_results, loc_tokens = run_localisation(
                        doc, detail_results, client=client, model=detail_model,
                        max_tokens_budget=remaining_budget,
                        cache_dir=cache_dir, cache_key=cache_key,
                    )
                    total_tokens += loc_tokens
                    localised = sum(
                        1 for d in detail_results
                        for e in d.building_elements
                        if e.bbox is not None
                    )
                    print(f"    Pass 3 (Localisation): {localised} Elemente "
                          f"lokalisiert, {loc_tokens:,} Token")
                else:
                    print(f"    Pass 3 (Localisation): uebersprungen "
                          f"(Token-Budget erschoepft)")

    doc.close()

    return VisualAnalysisResult(
        file=pdf_path.name,
        project=project,
        page_count=len(triage_results),
        triage=triage_results,
        details=detail_results,
        token_usage=total_tokens,
    )


# ---------------------------------------------------------------------------
# Batch-Analyse (fuer ganze Projekte)
# ---------------------------------------------------------------------------

def analyze_visual_pdfs(pdfs: list[dict], client=None,
                        triage_model: str = "claude-haiku-4-5-20251001",
                        detail_model: str = "claude-sonnet-4-20250514",
                        max_visual_tokens: int = 500000,
                        skip_detail: bool = False,
                        skip_localisation: bool = False,
                        cache_dir: Path = None) -> list[VisualAnalysisResult]:
    """Analysiert mehrere Plan/Foto-PDFs visuell.

    Args:
        pdfs: PDF-Eintraege aus scan_projects() (nur plan/photo)
        client: Anthropic-Client
        triage_model: Modell fuer Triage
        detail_model: Modell fuer Detail (und Pass 3 Localisation)
        max_visual_tokens: Gesamt-Token-Budget
        skip_detail: Nur Triage
        skip_localisation: Pass 3 (Bounding-Boxen) ueberspringen
        cache_dir: Cache-Verzeichnis

    Returns:
        Liste von VisualAnalysisResult
    """
    from anthropic import Anthropic

    if client is None:
        client = Anthropic()

    results = []
    tokens_remaining = max_visual_tokens

    for pdf in pdfs:
        result = analyze_visual_pdf(
            pdf["path"], project=pdf.get("project", ""),
            client=client,
            triage_model=triage_model,
            detail_model=detail_model,
            max_visual_tokens=tokens_remaining,
            skip_detail=skip_detail,
            skip_localisation=skip_localisation,
            cache_dir=cache_dir,
        )
        results.append(result)
        tokens_remaining -= result.token_usage

        if tokens_remaining <= 0:
            remaining = len(pdfs) - len(results)
            print(f"\n  Token-Budget erschoepft. {remaining} PDFs uebersprungen.")
            break

    return results


def print_visual_summary(results: list[VisualAnalysisResult]):
    """Gibt eine Zusammenfassung der visuellen Analyse aus."""
    total_pages = sum(r.page_count for r in results)
    total_detail = sum(len(r.details) for r in results)
    total_tokens = sum(r.token_usage for r in results)
    total_elements = sum(
        len(d.building_elements) for r in results for d in r.details
    )

    print(f"\n  Visuelle Analyse: {len(results)} PDFs, {total_pages} Seiten")
    print(f"    Detail-Analysen: {total_detail} Seiten")
    print(f"    Bauelemente erkannt: {total_elements}")
    print(f"    Token-Verbrauch: {total_tokens:,}")


# ---------------------------------------------------------------------------
# Hilfsfunktionen
# ---------------------------------------------------------------------------

def _safe_key(path: str) -> str:
    return path.replace("/", "__").replace("\\", "__").replace(" ", "_")


def _triage_from_dict(d: dict) -> TriageResult:
    return TriageResult(
        page=d["page"],
        page_type=d.get("page_type", ""),
        building_elements=d.get("building_elements", []),
        estimated_log=d.get("estimated_log", ""),
        lph_evidence=d.get("lph_evidence", ""),
        priority=d.get("priority", "low"),
        description=d.get("description", ""),
        confidence=d.get("confidence", 0.7),
    )


def _detail_from_dict(d: dict) -> DetailResult:
    elements = []
    for e in d.get("building_elements", []):
        bbox = e.get("bbox")
        # Nur valide bboxes uebernehmen - so ueberleben Cache-Korruptionen nicht
        # als "falsche" Koordinaten. Ungueltige werden still auf None gesetzt.
        elements.append(ElementDetail(
            element_type=e.get("element_type", ""),
            ifc_class=e.get("ifc_class", ""),
            log_achieved=e.get("log_achieved", ""),
            log_evidence=e.get("log_evidence", ""),
            visible_parameters=e.get("visible_parameters", []),
            region=e.get("region", ""),
            bbox=list(bbox) if _is_valid_bbox(bbox) else None,
        ))
    return DetailResult(
        page=d["page"],
        building_elements=elements,
        annotations=d.get("annotations", []),
        cross_references=d.get("cross_references", []),
        description=d.get("description", ""),
    )
