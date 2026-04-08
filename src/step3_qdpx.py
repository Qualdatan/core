"""Schritt 3: REFI-QDA 1.5 Projektdatei (.qdpx) für MAXQDA 2024 generieren."""

import uuid
import zipfile
from pathlib import Path
from xml.etree.ElementTree import Element, SubElement, tostring, ElementTree
from xml.dom import minidom
from datetime import datetime

from .config import QDPX_FILE, HAUPTKATEGORIEN, TRANSCRIPTS_DIR
from .models import AnalysisResult


def _uuid() -> str:
    return str(uuid.uuid4())


def _pretty_xml(elem: Element) -> bytes:
    """Gibt hübsch formatiertes XML als bytes zurück."""
    rough = tostring(elem, encoding="unicode")
    parsed = minidom.parseString(rough)
    return parsed.toprettyxml(indent="  ", encoding="utf-8")


def build_refi_qda_xml(result: AnalysisResult) -> bytes:
    """Baut das REFI-QDA 1.5 project.qde XML."""

    now = datetime.now().isoformat()

    # Root-Element
    project = Element("Project")
    project.set("xmlns", "urn:QDA-XML:project:1.0")
    project.set("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance")
    project.set("xsi:schemaLocation",
                "urn:QDA-XML:project:1.0 http://schema.qdasoftware.org/versions/Project/v1.5/Project.xsd")
    project.set("name", "Mayring Qualitative Inhaltsanalyse")
    project.set("origin", "Python Mayring Analyzer")
    project.set("creatingUserGUID", _uuid())
    project.set("creationDateTime", now)
    project.set("modifiedDateTime", now)

    # === Users ===
    users = SubElement(project, "Users")
    user = SubElement(users, "User")
    user_guid = _uuid()
    user.set("guid", user_guid)
    user.set("name", "Analyst")

    # === CodeBook ===
    codebook = SubElement(project, "CodeBook")
    codes_elem = SubElement(codebook, "Codes")

    # Farben für Kategorien
    category_colors = {
        "A": "#FF6B6B", "B": "#4ECDC4", "C": "#45B7D1",
        "D": "#96CEB4", "E": "#FFEAA7", "F": "#DDA0DD",
        "G": "#98D8C8", "H": "#F7DC6F", "I": "#BB8FCE",
        "J": "#85C1E9", "K": "#F0B27A",
    }

    # GUIDs für Codes und Kategorien
    code_guids = {}     # code_id -> guid
    cat_guids = {}      # cat_key -> guid

    # Erst Hauptkategorien als übergeordnete Codes
    for cat_key, cat_name in HAUPTKATEGORIEN.items():
        cat_guid = _uuid()
        cat_guids[cat_key] = cat_guid
        cat_elem = SubElement(codes_elem, "Code")
        cat_elem.set("guid", cat_guid)
        cat_elem.set("name", f"{cat_key}: {cat_name}")
        cat_elem.set("isCodable", "true")
        color = category_colors.get(cat_key, "#CCCCCC")
        cat_elem.set("color", color)

        # Beschreibung
        desc = SubElement(cat_elem, "Description")
        desc.text = f"Hauptkategorie {cat_key}: {cat_name}"

    # Dann einzelne Codes als Kinder der Kategorien
    codes_by_cat = {}
    for code_id, info in result.codes.items():
        cat = info["hauptkategorie"]
        codes_by_cat.setdefault(cat, []).append((code_id, info))

    for cat_key in sorted(codes_by_cat.keys()):
        # Finde das Kategorie-Element
        cat_elem = None
        for elem in codes_elem:
            if elem.get("guid") == cat_guids.get(cat_key):
                cat_elem = elem
                break

        if cat_elem is None:
            continue

        for code_id, info in sorted(codes_by_cat[cat_key]):
            code_guid = _uuid()
            code_guids[code_id] = code_guid
            code_elem = SubElement(cat_elem, "Code")
            code_elem.set("guid", code_guid)
            code_elem.set("name", f"{code_id}: {info['name']}")
            code_elem.set("isCodable", "true")
            color = category_colors.get(cat_key, "#CCCCCC")
            code_elem.set("color", color)

            desc = SubElement(code_elem, "Description")
            desc.text = info.get("kodierdefinition", "")

    # === Sources (Dokumente) ===
    sources = SubElement(project, "Sources")
    doc_guids = {}  # filename -> guid

    for i, filename in enumerate(sorted(result.documents.keys()), 1):
        doc_guid = _uuid()
        doc_guids[filename] = doc_guid

        source = SubElement(sources, "TextSource")
        source.set("guid", doc_guid)
        source.set("name", Path(filename).stem)
        source.set("richTextPath", f"internal://{filename}")
        source.set("plainTextPath", f"internal://{Path(filename).stem}.txt")
        source.set("creatingUser", user_guid)
        source.set("creationDateTime", now)
        source.set("modifiedDateTime", now)

        desc = SubElement(source, "Description")
        desc.text = f"Transkript: {filename}"

        # PlainTextContent für die Zeichenpositionen
        plain = SubElement(source, "PlainTextContent")
        full_text = result.documents.get(filename, "")
        plain.text = full_text

        # Kodierungen als PlainTextSelection
        coding_elem = SubElement(source, "Coding")
        for seg in result.segments:
            if seg.document != filename:
                continue
            code_guid = code_guids.get(seg.code_id)
            if not code_guid:
                continue

            code_ref = SubElement(coding_elem, "CodeRef")
            code_ref.set("targetGUID", code_guid)

            sel_guid = _uuid()
            sel = SubElement(source, "PlainTextSelection")
            sel.set("guid", sel_guid)
            sel.set("name", f"{seg.code_id}: {seg.code_name}")
            sel.set("startPosition", str(seg.char_start))
            sel.set("endPosition", str(seg.char_end))

            # Verknüpfe Selection mit Code
            coding2 = SubElement(sel, "Coding")
            coding2.set("guid", _uuid())
            coding2.set("creatingUser", user_guid)
            coding2.set("creationDateTime", now)
            code_ref2 = SubElement(coding2, "CodeRef")
            code_ref2.set("targetGUID", code_guid)

    return _pretty_xml(project)


def generate_qdpx(result: AnalysisResult, output_path=None):
    """Generiert die .qdpx Datei (ZIP mit project.qde + Quelldokumenten)."""
    output_path = output_path or QDPX_FILE

    xml_bytes = build_refi_qda_xml(result)

    with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as zf:
        # project.qde (die XML-Datei)
        zf.writestr("project.qde", xml_bytes)

        # Quelldokumente als Plaintext einbetten
        for filename, text in result.documents.items():
            txt_name = Path(filename).stem + ".txt"
            zf.writestr(f"Sources/{txt_name}", text.encode("utf-8"))

        # Originale .docx auch einbetten
        for filename in result.documents:
            docx_path = TRANSCRIPTS_DIR / filename
            if docx_path.exists():
                zf.write(docx_path, f"Sources/{filename}")

    print(f"  QDPX gespeichert: {output_path}")
    print(f"  Enthält: project.qde + {len(result.documents)} Dokumente")
