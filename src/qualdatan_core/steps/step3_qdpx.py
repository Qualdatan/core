"""Schritt 3: REFI-QDA 1.5 Projektdatei (.qdpx) für MAXQDA 2024 generieren."""

import uuid
import zipfile
from datetime import datetime
from pathlib import Path
from xml.dom import minidom
from xml.etree.ElementTree import Element, SubElement, tostring

from ..coding.colors import CodeColorMap
from ..config import TRANSCRIPTS_DIR
from ..models import AnalysisResult


def _uuid() -> str:
    return str(uuid.uuid4())


def _pretty_xml(elem: Element) -> bytes:
    """Gibt hübsch formatiertes XML als bytes zurück."""
    rough = tostring(elem, encoding="unicode")
    parsed = minidom.parseString(rough)
    return parsed.toprettyxml(indent="  ", encoding="utf-8")


def build_refi_qda_xml(
    result: AnalysisResult,
    codebase_codes: dict[str, dict] | None = None,
    codebase_name: str | None = None,
) -> bytes:
    """Baut das REFI-QDA 1.5 project.qde XML."""

    now = datetime.now().isoformat()

    # Root-Element
    project = Element("Project")
    project.set("xmlns", "urn:QDA-XML:project:1.0")
    project.set("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance")
    project.set(
        "xsi:schemaLocation",
        "urn:QDA-XML:project:1.0 http://schema.qdasoftware.org/versions/Project/v1.5/Project.xsd",
    )
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

    # ---- Code-Universum bestimmen ----
    # Wenn ein Codebook mitgegeben wurde: dessen volle Code-Liste verwenden,
    # damit die QDPX immer das gesamte Kodiersystem enthaelt - auch wenn die
    # LLM keine Codings geliefert hat. Andernfalls Fallback auf das alte
    # Verhalten (nur result.codes + result.categories).
    use_full_codebook = bool(codebase_codes)

    if use_full_codebook:
        all_code_ids = list(codebase_codes.keys())
        # Plus Hauptkategorien aus result.categories, falls sie nicht im
        # Codebook als eigene Eintraege vorkommen.
        for cat_key in result.categories.keys():
            if cat_key not in codebase_codes:
                all_code_ids.append(cat_key)
        # Plus evtl. zusaetzlich von der LLM induktiv vergebene Codes.
        for code_id in result.codes.keys():
            if code_id not in codebase_codes:
                all_code_ids.append(code_id)
    else:
        all_code_ids = list(result.categories.keys()) + list(result.codes.keys())

    color_map = CodeColorMap(codes=all_code_ids)

    # GUIDs für Codes und Kategorien
    code_guids: dict[str, str] = {}  # code_id -> guid
    cat_guids: dict[str, str] = {}  # cat_key -> guid (Hauptkategorien)

    def _mk_code_elem(parent, code_id: str, name: str, description: str) -> Element:
        guid = _uuid()
        code_guids[code_id] = guid
        elem = SubElement(parent, "Code")
        elem.set("guid", guid)
        elem.set("name", f"{code_id}: {name}" if name else code_id)
        elem.set("isCodable", "true")
        elem.set("color", color_map.get_hex(code_id))
        if description:
            desc = SubElement(elem, "Description")
            desc.text = description
        return elem

    if use_full_codebook:
        # Gruppiere Codes nach category (Hauptkategorie). parse_codebase_yaml
        # liefert fuer jede Zeile bereits category / subcategory.
        cat_order: list[str] = []
        cat_info: dict[str, dict] = {}
        children_by_cat: dict[str, list[tuple[str, dict]]] = {}

        for code_id, info in codebase_codes.items():
            cat_key = info.get("category") or code_id
            if code_id == cat_key:
                # Hauptkategorie
                if cat_key not in cat_info:
                    cat_order.append(cat_key)
                cat_info[cat_key] = info
            else:
                if cat_key not in children_by_cat:
                    # Falls Kategorie als eigener Eintrag fehlt, spaeter dummy anlegen
                    if cat_key not in cat_info and cat_key not in cat_order:
                        cat_order.append(cat_key)
                children_by_cat.setdefault(cat_key, []).append((code_id, info))

        # Zusaetzliche Kategorien aus result.categories (z.B. wenn sie im
        # Codebook unter anderem Schluessel stehen)
        for cat_key, _cat_name in result.categories.items():
            if cat_key not in cat_info and cat_key not in cat_order:
                cat_order.append(cat_key)

        # XML aufbauen
        for cat_key in cat_order:
            info = cat_info.get(cat_key, {})
            cat_name = info.get("name") or result.categories.get(cat_key, cat_key)
            description = info.get("description", "") or f"Hauptkategorie {cat_key}: {cat_name}"
            cat_elem = _mk_code_elem(codes_elem, cat_key, cat_name, description)
            cat_guids[cat_key] = code_guids[cat_key]

            kids = children_by_cat.get(cat_key, [])
            # Sortierstabil
            kids.sort(key=lambda t: t[0])
            for code_id, cinfo in kids:
                _mk_code_elem(
                    cat_elem,
                    code_id,
                    cinfo.get("name", code_id),
                    cinfo.get("description", ""),
                )

        # Induktive Codes der LLM, die nicht im Codebook stehen: unter ihre
        # Hauptkategorie (oder als Top-Level) einhaengen.
        for code_id, rinfo in result.codes.items():
            if code_id in code_guids:
                continue
            cat_key = rinfo.get("hauptkategorie") or ""
            parent = codes_elem
            if cat_key and cat_key in cat_guids:
                for elem in codes_elem.iter("Code"):
                    if elem.get("guid") == cat_guids[cat_key]:
                        parent = elem
                        break
            _mk_code_elem(
                parent,
                code_id,
                rinfo.get("name", code_id),
                rinfo.get("kodierdefinition", ""),
            )
    else:
        # --- Legacy-Verhalten: nur result.categories + result.codes ---
        for cat_key, cat_name in result.categories.items():
            cat_elem = _mk_code_elem(
                codes_elem,
                cat_key,
                cat_name,
                f"Hauptkategorie {cat_key}: {cat_name}",
            )
            cat_guids[cat_key] = code_guids[cat_key]

        codes_by_cat: dict[str, list[tuple[str, dict]]] = {}
        for code_id, info in result.codes.items():
            cat = info.get("hauptkategorie", "")
            codes_by_cat.setdefault(cat, []).append((code_id, info))

        for cat_key in sorted(codes_by_cat.keys()):
            parent = codes_elem
            if cat_key in cat_guids:
                for elem in codes_elem.iter("Code"):
                    if elem.get("guid") == cat_guids[cat_key]:
                        parent = elem
                        break
            for code_id, info in sorted(codes_by_cat[cat_key]):
                _mk_code_elem(
                    parent,
                    code_id,
                    info.get("name", code_id),
                    info.get("kodierdefinition", ""),
                )

    # === Sources (Dokumente) ===
    sources = SubElement(project, "Sources")
    doc_guids = {}  # filename -> guid

    for filename in sorted(result.documents.keys()):
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


def generate_qdpx(
    result: AnalysisResult,
    output_path=None,
    codebase_codes: dict[str, dict] | None = None,
    codebase_name: str | None = None,
):
    """Generiert die .qdpx Datei (ZIP mit project.qde + Quelldokumenten).

    Args:
        result: Analyseergebnis.
        output_path: Zielpfad der .qdpx-Datei.
        codebase_codes: Optional das geparste Codebook (siehe
            :func:`src.recipe.parse_codebase_yaml`). Wenn gesetzt, enthaelt
            das erzeugte Codesystem immer das VOLLE Codebook -- unabhaengig
            davon, wie viele Codes tatsaechlich von der LLM vergeben wurden.
        codebase_name: Optional der Name der Codebase (nur fuer Logging).
    """
    if output_path is None:
        raise ValueError("output_path muss angegeben werden")

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    if codebase_codes and codebase_name:
        print(f"  QDPX: nutze Codebook '{codebase_name}' mit {len(codebase_codes)} Codes")
    elif codebase_codes:
        print(f"  QDPX: nutze vollstaendiges Codebook ({len(codebase_codes)} Codes)")

    xml_bytes = build_refi_qda_xml(
        result,
        codebase_codes=codebase_codes,
        codebase_name=codebase_name,
    )

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
