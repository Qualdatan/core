"""QDPX-Merger: Bestehende .qdpx lesen, erweitern und neu schreiben.

Kann:
- Bestehendes Code-System aus .qdpx extrahieren
- PDF-Quellen mit Kodierungen als PDFSource hinzufügen
- Neue Codes ins CodeBook einfügen
- Ergebnis als neue .qdpx speichern
"""

import uuid
import zipfile
from pathlib import Path
from xml.etree.ElementTree import (
    Element, SubElement, tostring, fromstring,
)
from xml.dom import minidom
from datetime import datetime


def _uuid() -> str:
    return str(uuid.uuid4())


def _pretty_xml(elem: Element) -> bytes:
    rough = tostring(elem, encoding="unicode")
    parsed = minidom.parseString(rough)
    return parsed.toprettyxml(indent="  ", encoding="utf-8")


# ---------------------------------------------------------------------------
# QDPX lesen
# ---------------------------------------------------------------------------

def read_qdpx(qdpx_path: Path) -> tuple[Element, dict[str, bytes]]:
    """Liest eine .qdpx-Datei und gibt XML-Root + eingebettete Dateien zurück.

    Returns:
        (project_element, {"Sources/file.txt": bytes, ...})
    """
    sources = {}
    project = None

    with zipfile.ZipFile(qdpx_path, "r") as zf:
        for name in zf.namelist():
            if name == "project.qde":
                xml_bytes = zf.read(name)
                project = fromstring(xml_bytes)
            else:
                sources[name] = zf.read(name)

    if project is None:
        raise ValueError(f"Keine project.qde in {qdpx_path} gefunden")

    return project, sources


def extract_codesystem(project: Element) -> tuple[dict, dict]:
    """Extrahiert Kategorien und Codes aus dem XML-CodeBook.

    Returns:
        (categories, codes)
        categories: {key: name}  z.B. {"A": "Projektakquise"}
        codes: {code_id: {"name": ..., "hauptkategorie": ..., "guid": ..., ...}}
    """
    ns = project.tag.split("}")[0] + "}" if "}" in project.tag else ""

    categories = {}
    codes = {}

    codebook = project.find(f"{ns}CodeBook")
    if codebook is None:
        return categories, codes

    codes_elem = codebook.find(f"{ns}Codes")
    if codes_elem is None:
        return categories, codes

    for cat_elem in codes_elem.findall(f"{ns}Code"):
        cat_full_name = cat_elem.get("name", "")
        cat_guid = cat_elem.get("guid", "")

        # Parse "A: Projektakquise" → key="A", name="Projektakquise"
        if ": " in cat_full_name:
            cat_key, cat_name = cat_full_name.split(": ", 1)
        else:
            cat_key = cat_full_name
            cat_name = cat_full_name

        categories[cat_key] = cat_name

        # Subcodes
        for code_elem in cat_elem.findall(f"{ns}Code"):
            code_full_name = code_elem.get("name", "")
            code_guid = code_elem.get("guid", "")

            if ": " in code_full_name:
                code_id, code_name = code_full_name.split(": ", 1)
            else:
                code_id = code_full_name
                code_name = code_full_name

            desc_elem = code_elem.find(f"{ns}Description")
            definition = desc_elem.text if desc_elem is not None and desc_elem.text else ""

            codes[code_id] = {
                "name": code_name,
                "hauptkategorie": cat_key,
                "guid": code_guid,
                "kodierdefinition": definition,
            }

    return categories, codes


# ---------------------------------------------------------------------------
# QDPX erweitern
# ---------------------------------------------------------------------------

# Farben für Kategorien
CATEGORY_COLORS = {
    "A": "#FF6B6B", "B": "#4ECDC4", "C": "#45B7D1",
    "D": "#96CEB4", "E": "#FFEAA7", "F": "#DDA0DD",
    "G": "#98D8C8", "H": "#F7DC6F", "I": "#BB8FCE",
    "J": "#85C1E9", "K": "#F0B27A", "L": "#E8DAEF",
    "M": "#A3E4D7", "N": "#F5CBA7", "O": "#AED6F1",
    "P": "#D5DBDB", "Q": "#F9E79F",
}


def _find_or_create_code(codes_elem, cat_key: str, cat_name: str,
                         code_id: str, code_name: str,
                         definition: str = "",
                         existing_codes: dict = None,
                         ns: str = "") -> str:
    """Findet oder erstellt einen Code im CodeBook. Gibt die GUID zurück."""
    # Prüfe ob Code schon existiert
    if existing_codes and code_id in existing_codes:
        return existing_codes[code_id]["guid"]

    # Finde oder erstelle Kategorie
    cat_guid = None
    cat_elem = None
    for elem in codes_elem.findall(f"{ns}Code"):
        if elem.get("name", "").startswith(f"{cat_key}: "):
            cat_guid = elem.get("guid")
            cat_elem = elem
            break

    if cat_elem is None:
        cat_guid = _uuid()
        cat_elem = SubElement(codes_elem, "Code")
        cat_elem.set("guid", cat_guid)
        cat_elem.set("name", f"{cat_key}: {cat_name}")
        cat_elem.set("isCodable", "true")
        cat_elem.set("color", CATEGORY_COLORS.get(cat_key, "#CCCCCC"))
        desc = SubElement(cat_elem, "Description")
        desc.text = f"Hauptkategorie {cat_key}: {cat_name}"

    # Code als Subcode anlegen
    code_guid = _uuid()
    code_elem = SubElement(cat_elem, "Code")
    code_elem.set("guid", code_guid)
    code_elem.set("name", f"{code_id}: {code_name}")
    code_elem.set("isCodable", "true")
    code_elem.set("color", CATEGORY_COLORS.get(cat_key, "#CCCCCC"))

    if definition:
        desc = SubElement(code_elem, "Description")
        desc.text = definition

    return code_guid


def add_pdf_sources(project: Element, pdf_results: list[dict],
                    existing_codes: dict = None,
                    recipe_categories: dict = None) -> dict[str, str]:
    """Fügt PDF-Quellen mit Kodierungen zum Projekt hinzu.

    Args:
        project: XML-Root-Element
        pdf_results: Liste von Analyse-Ergebnissen pro PDF:
            [{"file": "...", "project": "...", "extraction": {...},
              "codings": [...], "neue_codes": [...]}]
        existing_codes: Bereits vorhandene Codes {code_id: {guid, ...}}
        recipe_categories: {kat_key: kat_name} aus dem Recipe (z.B. {"A": "Projektakquise..."})
            Wird verwendet um Kategorie-Namen fuer auto-erstellte Codes zu setzen.

    Returns:
        Mapping von neuen code_ids zu GUIDs
    """
    ns = project.tag.split("}")[0] + "}" if "}" in project.tag else ""
    now = datetime.now().isoformat()

    # User-GUID finden oder erstellen
    users = project.find(f"{ns}Users")
    if users is None:
        users = SubElement(project, "Users")
    user_elems = users.findall(f"{ns}User")
    if user_elems:
        user_guid = user_elems[0].get("guid")
    else:
        user_guid = _uuid()
        user = SubElement(users, "User")
        user.set("guid", user_guid)
        user.set("name", "PDF-Analyst")

    # CodeBook finden oder erstellen
    codebook = project.find(f"{ns}CodeBook")
    if codebook is None:
        codebook = SubElement(project, "CodeBook")
    codes_elem = codebook.find(f"{ns}Codes")
    if codes_elem is None:
        codes_elem = SubElement(codebook, "Codes")

    # Code-GUIDs sammeln (bestehende + neue)
    code_guids = {}
    if existing_codes:
        for code_id, info in existing_codes.items():
            code_guids[code_id] = info["guid"]

    # 1) Explizit als "neue Codes" deklarierte erst (mit Definition + Name)
    new_categories = {}
    for pdf_result in pdf_results:
        for new_code in pdf_result.get("neue_codes", []):
            code_id = new_code["code_id"]
            if code_id in code_guids:
                continue
            cat_key = new_code.get("hauptkategorie", "Z")
            cat_name = new_code.get("code_name", code_id)
            new_categories.setdefault(cat_key, cat_name)

            guid = _find_or_create_code(
                codes_elem, cat_key, new_categories[cat_key],
                code_id, new_code["code_name"],
                new_code.get("kodierdefinition", ""),
                existing_codes, ns,
            )
            code_guids[code_id] = guid

    # 2) ALLE in codings referenzierten Codes nachziehen
    # (LLM erfindet oft Codes wie "A-01" aus Kategorie A, ohne sie als neue_codes
    # zu deklarieren — der Code muss trotzdem im Codebook stehen)
    recipe_categories = recipe_categories or {}
    for pdf_result in pdf_results:
        for coding in pdf_result.get("codings", []):
            for code_id in coding.get("codes", []):
                if code_id in code_guids:
                    continue
                # Kategorie-Praefix extrahieren: "A-01" -> "A", "B-02" -> "B"
                cat_key = code_id.split("-")[0] if "-" in code_id else "Z"
                cat_name = recipe_categories.get(cat_key, f"Kategorie {cat_key}")
                guid = _find_or_create_code(
                    codes_elem, cat_key, cat_name,
                    code_id, code_id,  # Kein expliziter Name -> code_id als Name
                    "",  # Keine Definition
                    existing_codes, ns,
                )
                code_guids[code_id] = guid

    # Sources finden oder erstellen
    sources = project.find(f"{ns}Sources")
    if sources is None:
        sources = SubElement(project, "Sources")

    # Block-Index bauen für Koordinaten-Lookup
    for pdf_result in pdf_results:
        extraction = pdf_result.get("extraction", {})
        project_name = pdf_result.get("project", "")
        filename = pdf_result.get("file", "")

        # Block-Index
        block_index = {}
        for page in extraction.get("pages", []):
            for block in page["blocks"]:
                block_index[block["id"]] = {
                    **block,
                    "page": page["page"],
                }

        # PDFSource erstellen
        internal_path = f"{project_name}/{filename}" if project_name else filename
        doc_guid = _uuid()

        source = SubElement(sources, "PDFSource")
        source.set("guid", doc_guid)
        source.set("name", f"{Path(filename).stem} - {project_name}")
        # REFI-QDA: Attribut heisst "path", nicht "pdfPath"
        source.set("path", f"internal://{internal_path}")
        source.set("creatingUser", user_guid)
        source.set("creationDateTime", now)
        source.set("modifiedDateTime", now)

        desc = SubElement(source, "Description")
        doc_type = pdf_result.get("document_type", "Projektunterlage")
        desc.text = f"{doc_type}: {filename}"

        # Kodierungen als PDFSelection
        for coding in pdf_result.get("codings", []):
            block_id = coding.get("block_id", "")
            block_info = block_index.get(block_id)
            if not block_info:
                continue

            page_num = block_info["page"]
            bbox = block_info.get("bbox", [0, 0, 0, 0])

            for code_id in coding.get("codes", []):
                code_guid = code_guids.get(code_id)
                if not code_guid:
                    continue

                sel_guid = _uuid()
                sel = SubElement(source, "PDFSelection")
                sel.set("guid", sel_guid)
                sel.set("name", f"{code_id}")
                # REFI-QDA: Seitenzahl ist 0-basiert
                sel.set("page", str(page_num - 1))
                # REFI-QDA: Attribute heissen first/second, nicht start/end
                sel.set("firstX", str(round(bbox[0], 1)))
                sel.set("firstY", str(round(bbox[1], 1)))
                sel.set("secondX", str(round(bbox[2], 1)))
                sel.set("secondY", str(round(bbox[3], 1)))

                coding_elem = SubElement(sel, "Coding")
                coding_elem.set("guid", _uuid())
                coding_elem.set("creatingUser", user_guid)
                coding_elem.set("creationDateTime", now)
                code_ref = SubElement(coding_elem, "CodeRef")
                code_ref.set("targetGUID", code_guid)

    return code_guids


def add_visual_sources(project: Element, visual_results: list[dict],
                       existing_codes: dict = None) -> dict[str, str]:
    """Fuegt visuelle Kodierungen (Plaene/Fotos) als PDFSource hinzu.

    Visuelle Kodierungen unterscheiden sich von Text-Kodierungen:
    - BBox deckt die ganze Seite ab (kein Textblock)
    - Jede PDFSelection hat ein Description-Element mit AI-Begruendung
    - Block-IDs sind synthetisch: p{n}_v{i}

    Args:
        project: XML-Root-Element
        visual_results: Liste von visuellen Analyse-Ergebnissen:
            [{"file": ..., "project": ..., "page_dimensions": {page: (w, h)},
              "visual_codings": [...]}]
        existing_codes: Bereits vorhandene Codes {code_id: {guid, ...}}

    Returns:
        Mapping von code_ids zu GUIDs
    """
    ns = project.tag.split("}")[0] + "}" if "}" in project.tag else ""
    now = datetime.now().isoformat()

    # User-GUID finden oder erstellen
    users = project.find(f"{ns}Users")
    if users is None:
        users = SubElement(project, "Users")
    user_elems = users.findall(f"{ns}User")
    if user_elems:
        user_guid = user_elems[0].get("guid")
    else:
        user_guid = _uuid()
        user = SubElement(users, "User")
        user.set("guid", user_guid)
        user.set("name", "Vision-Analyst")

    # CodeBook finden oder erstellen
    codebook = project.find(f"{ns}CodeBook")
    if codebook is None:
        codebook = SubElement(project, "CodeBook")
    codes_elem = codebook.find(f"{ns}Codes")
    if codes_elem is None:
        codes_elem = SubElement(codebook, "Codes")

    # Code-GUIDs sammeln
    code_guids = {}
    if existing_codes:
        for code_id, info in existing_codes.items():
            code_guids[code_id] = info["guid"]

    # Neue visuelle Codes anlegen (O/P/Q)
    _VISUAL_CATEGORIES = {
        "O": "Bauelemente und Konstruktion",
        "P": "Plandarstellung und Dokumentationstyp",
        "Q": "LOD/LOG-Evidenz visuell",
    }
    _VISUAL_CODES = {
        "O-01": ("Tragende Bauteile", "Waende, Stuetzen, Decken, Fundamente"),
        "O-02": ("Nichttragende Bauteile", "Trennwaende, Bruestungen"),
        "O-03": ("Oeffnungen", "Tueren, Fenster, Tore"),
        "O-04": ("Treppen und Rampen", "Vertikale Erschliessung"),
        "O-05": ("Dachkonstruktion", "Dach, Sparren, Pfetten"),
        "O-06": ("Fassade und Aussenhuelle", "Fassade, WDVS, Daemmung"),
        "O-07": ("TGA", "Heizung, Lueftung, Sanitaer, Elektro"),
        "O-08": ("Aussenanlagen und Gelaende", "Gelaende, Pflasterung"),
        "P-01": ("Grundriss", "Geschossspezifischer Grundriss"),
        "P-02": ("Schnitt", "Vertikaler Gebaeudeeschnitt"),
        "P-03": ("Ansicht", "Fassadenansicht"),
        "P-04": ("Detailzeichnung", "Konstruktionsdetail, Anschluss"),
        "P-05": ("Lageplan", "Lageplan, Umgebungsplan"),
        "P-06": ("Massstab und Bemassung", "Massstab, Bemassungselemente"),
        "P-07": ("Legende und Symbole", "Legende, Symbole, Abkuerzungen"),
        "P-08": ("Schriftfeld", "Plankopf-Metadaten"),
        "Q-01": ("LOG-Stufe", "Geometriedetailgrad aus Plandarstellung"),
        "Q-02": ("LOI-Evidenz", "Sichtbare Attribute und Beschriftungen"),
        "Q-03": ("Detaillierungsgrad-Abweichung", "Erwartet vs. tatsaechlich"),
    }

    for code_id, (code_name, definition) in _VISUAL_CODES.items():
        if code_id in code_guids:
            continue
        cat_key = code_id.split("-")[0]
        cat_name = _VISUAL_CATEGORIES.get(cat_key, cat_key)
        guid = _find_or_create_code(
            codes_elem, cat_key, cat_name,
            code_id, code_name, definition,
            existing_codes, ns,
        )
        code_guids[code_id] = guid

    # Sources
    sources = project.find(f"{ns}Sources")
    if sources is None:
        sources = SubElement(project, "Sources")

    for vis_result in visual_results:
        filename = vis_result.get("file", "")
        project_name = vis_result.get("project", "")
        page_dims = vis_result.get("page_dimensions", {})
        visual_codings = vis_result.get("visual_codings", [])
        doc_description = vis_result.get("description", "")

        if not visual_codings:
            continue

        internal_path = f"{project_name}/{filename}" if project_name else filename
        doc_guid = _uuid()

        source = SubElement(sources, "PDFSource")
        source.set("guid", doc_guid)
        source.set("name", f"{Path(filename).stem} - {project_name}")
        # REFI-QDA: Attribut heisst "path", nicht "pdfPath"
        source.set("path", f"internal://{internal_path}")
        source.set("creatingUser", user_guid)
        source.set("creationDateTime", now)
        source.set("modifiedDateTime", now)

        desc = SubElement(source, "Description")
        desc.text = doc_description or f"Visuelle Analyse: {filename}"

        for coding in visual_codings:
            page_num = coding.get("page", 1)
            coding_desc = coding.get("description", "")
            codes = coding.get("codes", [])
            block_id = coding.get("block_id", "")

            # Seitendimensionen: ganze Seite als BBox
            dims = page_dims.get(page_num, page_dims.get(str(page_num), (595, 842)))
            page_w, page_h = dims

            for code_id in codes:
                code_guid = code_guids.get(code_id)
                if not code_guid:
                    continue

                sel_guid = _uuid()
                sel = SubElement(source, "PDFSelection")
                sel.set("guid", sel_guid)
                sel.set("name", f"{code_id}: {block_id}")
                # REFI-QDA: Seitenzahl ist 0-basiert
                sel.set("page", str(page_num - 1))
                # REFI-QDA: first/second statt start/end
                sel.set("firstX", "0")
                sel.set("firstY", "0")
                sel.set("secondX", str(round(page_w, 1)))
                sel.set("secondY", str(round(page_h, 1)))

                # Description-Element mit AI-Begruendung
                if coding_desc:
                    sel_desc = SubElement(sel, "Description")
                    sel_desc.text = coding_desc

                coding_elem = SubElement(sel, "Coding")
                coding_elem.set("guid", _uuid())
                coding_elem.set("creatingUser", user_guid)
                coding_elem.set("creationDateTime", now)
                code_ref = SubElement(coding_elem, "CodeRef")
                code_ref.set("targetGUID", code_guid)

    return code_guids


# ---------------------------------------------------------------------------
# QDPX schreiben
# ---------------------------------------------------------------------------

def write_qdpx(project: Element, output_path: Path,
               existing_sources: dict[str, bytes] = None,
               pdf_files: dict[str, Path] = None):
    """Schreibt die erweiterte .qdpx-Datei.

    Args:
        project: XML-Root-Element
        output_path: Ausgabepfad
        existing_sources: Bereits vorhandene Dateien aus der Original-.qdpx
        pdf_files: Neue PDFs zum Einbetten {internal_path: local_path}
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    xml_bytes = _pretty_xml(project)

    with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("project.qde", xml_bytes)

        # Bestehende Sources übernehmen
        if existing_sources:
            for name, data in existing_sources.items():
                zf.writestr(name, data)

        # Neue PDFs einbetten (REFI-QDA: sources/ in Kleinschreibung)
        if pdf_files:
            for internal_path, local_path in pdf_files.items():
                if local_path.exists():
                    zf.write(local_path, f"sources/{internal_path}")

    print(f"  QDPX gespeichert: {output_path}")


def create_new_project(name: str = "PDF-Dokumentenanalyse") -> Element:
    """Erstellt ein leeres REFI-QDA Projekt-Element."""
    now = datetime.now().isoformat()

    project = Element("Project")
    project.set("xmlns", "urn:QDA-XML:project:1.0")
    project.set("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance")
    project.set("name", name)
    project.set("origin", "Python PDF-Coder")
    project.set("creatingUserGUID", _uuid())
    project.set("creationDateTime", now)
    project.set("modifiedDateTime", now)

    users = SubElement(project, "Users")
    user = SubElement(users, "User")
    user.set("guid", _uuid())
    user.set("name", "PDF-Analyst")

    SubElement(project, "CodeBook")
    SubElement(project, "Sources")

    return project
