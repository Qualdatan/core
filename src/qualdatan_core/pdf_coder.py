#!/usr/bin/env python3
"""
PDF-Dokumenten-Coder: Analysiert PDFs aus Projekt-Ordnern und
erzeugt eine kodierte .qdpx-Datei fuer MAXQDA.

3-Stufen-Architektur:
  Stufe 1: Lokale PDF-Extraktion (pymupdf) -> JSON
  Stufe 2: Code-Zuweisung (Sonnet) -> Block-ID -> Codes
  Stufe 3: Position-Refinement (Haiku) -> char_start/end

Alle Zwischen-Ergebnisse in pipeline.db (SQLite).
Prompts und Responses als Textdateien (Debugging).

Entry point is main.py; this module is imported as ``src.pdf_coder``.
"""

import concurrent.futures
import json
import sys
import warnings
from pathlib import Path

warnings.filterwarnings("ignore", message=".*pymupdf_layout.*")

from anthropic import Anthropic
from .config import PROJECTS_DIR, OUTPUT_ROOT
from .recipe import load_recipe
from .run_context import RunContext, create_run, find_interrupted_runs, resume_run
from .pdf.scanner import scan_projects, build_manifest, save_manifest, print_manifest_summary
from .pdf.extractor import extract_pdf, extraction_to_text_summary
from .coding.analyzer import (
    analyze_pdf_codes, refine_positions, format_codesystem,
)
from .qdpx.merger import (
    read_qdpx, extract_codesystem, add_pdf_sources, add_visual_sources,
    write_qdpx, create_new_project,
)
from .coding.classifier import (
    classify_project_pdfs, split_by_type,
    print_classification_summary,
)
from .coding.visual import (
    analyze_visual_pdfs, print_visual_summary,
)
from .pdf.annotator import annotate_text_pdf, annotate_visual_pdf
from ._console import (
    console, print_step, print_success, print_warning, print_error,
    print_header, print_summary,
)


def _short(rel_path: str) -> str:
    """Just the filename, for cleaner logs."""
    return rel_path.rsplit("/", 1)[-1]


def _cache_key(project: str, filename: str) -> str:
    """Erzeugt einen Cache-Schluessel aus Projekt + Dateiname."""
    safe = f"{project}__{Path(filename).stem}"
    return safe.replace("/", "_").replace("\\", "_").replace(" ", "_")


def _mirror_pdfs(pdfs: list[dict], ctx: RunContext) -> int:
    """Spiegelt PDF-Materials in die App-DB (no-op ohne Attach).

    Nutzt pro PDF :meth:`RunContext.register_material` mit
    ``material_kind="pdf_text"``, ``path`` = absoluter Pfad,
    ``relative_path`` = projektrelativer Pfad und ``source_label`` =
    Projekt-/Ordnername. Ohne App-DB-Anbindung no-op.

    Args:
        pdfs: Liste der PDF-Eintraege (wie von ``scan_projects`` geliefert).
        ctx: Aktueller RunContext.

    Returns:
        Anzahl erfolgreich registrierter Materials.
    """
    count = 0
    for pdf in pdfs:
        mid = ctx.register_material(
            "pdf_text",
            pdf.get("path", ""),
            relative_path=pdf.get("relative_path", ""),
            source_label=pdf.get("project", ""),
        )
        if mid is not None:
            count += 1
    return count


def _register_pdfs(pdfs: list[dict], ctx: RunContext) -> dict[str, int]:
    """Registriert alle PDFs in der Datenbank.

    Returns:
        {relative_path: pdf_id}
    """
    pdf_ids = {}
    for pdf in pdfs:
        file_size = 0
        try:
            file_size = Path(pdf["path"]).stat().st_size // 1024
        except OSError:
            pass
        pdf_id = ctx.db.upsert_pdf(
            project=pdf["project"],
            filename=pdf["filename"],
            relative_path=pdf["relative_path"],
            path=pdf["path"],
            file_size_kb=file_size,
        )
        pdf_ids[pdf["relative_path"]] = pdf_id
    # D.3: Materials in App-DB spiegeln (no-op wenn nicht attached)
    _mirror_pdfs(pdfs, ctx)
    return pdf_ids


# ---------------------------------------------------------------------------
# Stufe 1: Lokale Extraktion
# ---------------------------------------------------------------------------

def _extract_single_pdf(
    pdf: dict, ctx: RunContext, pdf_id: int,
) -> tuple[int, str, dict | None, bool]:
    """Extrahiert eine einzelne PDF. Thread-safe.

    Returns:
        (pdf_id, relative_path, extraction_data | None, was_cached)
    """
    if ctx.db.has_extraction(pdf_id):
        data = ctx.db.load_extraction(pdf_id)
        ctx.db.set_step_status(pdf_id, "extraction", "done")
        return pdf_id, pdf["relative_path"], data, True

    try:
        data = extract_pdf(pdf["path"])
        ctx.db.save_extraction(pdf_id, data)
        ctx.db.set_step_status(pdf_id, "extraction", "done")
        return pdf_id, pdf["relative_path"], data, False
    except Exception as e:
        ctx.db.set_step_status(pdf_id, "extraction", "error", str(e))
        print_error(f"{_short(pdf['relative_path'])}: {e}")
        return pdf_id, pdf["relative_path"], None, False


def run_extraction(pdfs: list[dict], ctx: RunContext, pdf_ids: dict[str, int],
                   max_workers: int = 8) -> dict[int, dict]:
    """Stufe 1: Extrahiert alle PDFs lokal mit pymupdf (parallelisiert).

    Returns:
        {pdf_id: extraction_data}
    """
    print_step("Stufe 1: Lokale PDF-Extraktion", "pymupdf", phase="scan")

    n_total = len(pdfs)
    n_parallel = min(max_workers, n_total)
    console.print(f"  [dim]Starte {n_parallel} parallele Extraktionen fuer {n_total} PDFs...[/dim]")

    results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=n_parallel) as pool:
        futures = {}
        for pdf in pdfs:
            pid = pdf_ids[pdf["relative_path"]]
            ctx.db.set_step_status(pid, "extraction", "running")
            future = pool.submit(_extract_single_pdf, pdf, ctx, pid)
            futures[future] = pdf

        completed = 0
        cached_count = 0
        for future in concurrent.futures.as_completed(futures):
            completed += 1
            pdf_id, rel_path, data, was_cached = future.result()
            if data is not None:
                results[pdf_id] = data
                total_blocks = sum(len(p["blocks"]) for p in data["pages"])
                if was_cached:
                    cached_count += 1
                console.print(f"  [dim]\\[{completed}/{n_total}][/dim] {_short(rel_path)} "
                      f"[dim]({data['metadata']['page_count']} Seiten, "
                      f"{total_blocks} Bloecke)[/dim]")

    if cached_count:
        console.print(f"  [dim]({cached_count} aus DB-Cache)[/dim]")
    print_success(f"Stufe 1: {len(results)}/{len(pdfs)} PDFs extrahiert")
    return results


# ---------------------------------------------------------------------------
# Stufe 2+3: LLM-Analyse
# ---------------------------------------------------------------------------

def _analyze_single_pdf(client: Anthropic, pdf: dict, extraction: dict,
                        recipe, codesystem: str, ctx: RunContext,
                        pdf_id: int) -> dict | None:
    """Analysiert eine einzelne PDF (Stufe 2+3). Thread-safe."""
    cache_key = _cache_key(pdf["project"], pdf["filename"])

    coding_result = analyze_pdf_codes(
        client, extraction, recipe, pdf["project"],
        codesystem=codesystem,
        cache_dir=ctx.cache_dir,
        cache_key=cache_key,
    )

    codings = coding_result.get("codings", [])
    needs_refinement = any(
        not c.get("ganzer_block", True) for c in codings
    )
    if needs_refinement:
        codings = refine_positions(client, codings, extraction)

    # In DB speichern
    for coding in codings:
        block_id = coding.get("block_id", "")
        # Seitennummer aus Block-ID extrahieren (p1_b0 -> 1)
        page = 1
        if block_id.startswith("p"):
            try:
                page = int(block_id.split("_")[0][1:])
            except (ValueError, IndexError):
                pass
        ctx.db.save_coding(
            pdf_id=pdf_id,
            page=page,
            block_id=block_id,
            codes=coding.get("codes", []),
            source="text",
            description=coding.get("begruendung", ""),
            ganzer_block=coding.get("ganzer_block", True),
            begruendung=coding.get("begruendung", ""),
        )

    neue_codes = coding_result.get("neue_codes", [])
    # Strict-Strategie: neue_codes verwerfen (LLM ignoriert Anweisung manchmal)
    if getattr(recipe, "coding_strategy", "hybrid") == "strict" and neue_codes:
        print_warning(
            f"strict-Strategie, LLM lieferte {len(neue_codes)} "
            f"neue Code(s) in {pdf['filename']} — werden verworfen."
        )
        neue_codes = []
        coding_result["neue_codes"] = []
    if neue_codes:
        ctx.db.save_neue_codes(neue_codes, pdf_id)

    ctx.db.set_step_status(pdf_id, "coding", "done")

    return {
        "file": pdf["filename"],
        "project": pdf["project"],
        "relative_path": pdf["relative_path"],
        "path": pdf["path"],
        "extraction": extraction,
        "document_type": coding_result.get("document_type", ""),
        "codings": codings,
        "neue_codes": neue_codes,
    }


def run_coding(pdfs: list[dict], extractions: dict[int, dict],
               recipe, codesystem: str, ctx: RunContext,
               pdf_ids: dict[str, int],
               max_workers: int = 2) -> list[dict]:
    """Stufe 2+3: Code-Zuweisung (Sonnet) + Refinement (Haiku). Parallelisiert.

    max_workers=2 bewusst niedrig: Sonnet output ist gross (~16K tokens),
    bei OPM-Limit von 8K fuehrt zu hoeher Konkurrenz zu Rate-Limit-Errors.

    Returns:
        Liste von PDF-Ergebnissen mit codings + neue_codes
    """
    print_step("Stufe 2: Code-Zuweisung", "LLM", phase="ai")

    # Hoehere Retry-Toleranz fuer Rate-Limit-Errors (default ist 2)
    client = Anthropic(max_retries=6)

    # Vorbereiten: nur PDFs mit Extraktion
    tasks = []
    for pdf in pdfs:
        pid = pdf_ids[pdf["relative_path"]]
        extraction = extractions.get(pid)
        if extraction is None:
            console.print(f"  [dim]SKIP[/dim] {_short(pdf['relative_path'])} (keine Extraktion)")
            continue
        # Cache pruefen: schon kodiert?
        if ctx.db.is_step_done(pid, "coding"):
            console.print(f"  [dim cyan]CACHE[/dim cyan] {_short(pdf['relative_path'])}")
            continue
        tasks.append((pdf, extraction, pid))

    if not tasks:
        console.print("  [dim]Keine neuen PDFs zu kodieren.[/dim]")
        return []

    n_total = len(tasks)
    n_parallel = min(max_workers, n_total)
    console.print(f"  [dim]Starte {n_parallel} parallele API-Calls fuer {n_total} PDFs...[/dim]")

    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {}
        for pdf, extraction, pid in tasks:
            ctx.db.set_step_status(pid, "coding", "running")
            future = pool.submit(
                _analyze_single_pdf, client, pdf, extraction,
                recipe, codesystem, ctx, pid,
            )
            futures[future] = pdf["relative_path"]

        completed = 0
        # rel_path -> pid mapping fuer Error-Status-Update
        rel_to_pid = {pdf["relative_path"]: pid for pdf, _, pid in tasks}

        for future in concurrent.futures.as_completed(futures):
            rel_path = futures[future]
            completed += 1
            try:
                result = future.result()
                if result:
                    results.append(result)
                    n_codes = sum(len(c.get("codes", [])) for c in result["codings"])
                    n_new = len(result.get("neue_codes", []))
                    console.print(f"  [dim]\\[{completed}/{n_total}][/dim] {_short(rel_path)} "
                          f"[dim]({n_codes} Kodierungen, {n_new} neue Codes)[/dim]")
            except Exception as e:
                # Fehler in DB persistieren, damit Resume es nochmal versucht
                pid = rel_to_pid.get(rel_path)
                if pid:
                    ctx.db.set_step_status(pid, "coding", "error", str(e)[:500])
                print_error(f"[{completed}/{n_total}] {_short(rel_path)}: {e}")

    print_success(f"Stufe 2+3: {len(results)} PDFs analysiert")
    return results


# ---------------------------------------------------------------------------
# Stufe Visual: Vision-Pipeline fuer Plaene und Fotos
# ---------------------------------------------------------------------------

def run_visual(pdfs: list[dict], ctx: RunContext,
               pdf_ids: dict[str, int],
               skip_detail: bool = False,
               max_visual_tokens: int = 500000) -> list[dict]:
    """Vision-Pipeline: Triage + Detail-Analyse fuer Plan/Foto-PDFs.

    Returns:
        Liste von visual_result dicts fuer QDPX-Integration
    """
    print_step("Visuelle Analyse", "Vision-Pipeline", phase="ai")

    if not pdfs:
        console.print("  [dim]Keine visuellen PDFs.[/dim]")
        return []

    import fitz

    # Hoehere Retry-Toleranz fuer Rate-Limit-Errors
    client = Anthropic(max_retries=6)

    visual_results = analyze_visual_pdfs(
        pdfs, client=client,
        max_visual_tokens=max_visual_tokens,
        skip_detail=skip_detail,
        cache_dir=ctx.cache_dir,
    )

    print_visual_summary(visual_results)

    # Ergebnisse in DB + QDPX-Format aufbereiten
    qdpx_visual = []
    for vis in visual_results:
        pdf_path = None
        pid = None
        for pdf in pdfs:
            if pdf["filename"] == vis.file:
                pdf_path = pdf["path"]
                pid = pdf_ids.get(pdf["relative_path"])
                break

        page_dims = {}
        if pdf_path:
            doc = fitz.open(str(pdf_path))
            for i in range(len(doc)):
                page = doc[i]
                page_dims[i + 1] = (round(page.rect.width, 1), round(page.rect.height, 1))
            doc.close()

        # In DB speichern
        if pid:
            for t in vis.triage:
                ctx.db.save_visual_triage(pid, t.page, t.to_dict())
            for d in vis.details:
                ctx.db.save_visual_detail(pid, d.page, d.to_dict())
            ctx.db.set_step_status(pid, "visual_triage", "done")
            if vis.details:
                ctx.db.set_step_status(pid, "visual_detail", "done")

        codings = vis.visual_codings()

        # Visuelle Kodierungen auch in codings-Tabelle speichern
        if pid:
            for coding in codings:
                ctx.db.save_coding(
                    pdf_id=pid,
                    page=coding["page"],
                    block_id=coding["block_id"],
                    codes=coding["codes"],
                    source=coding.get("source", "visual_triage"),
                    description=coding.get("description", ""),
                )

        qdpx_visual.append({
            "file": vis.file,
            "project": vis.project,
            "path": pdf_path,
            "page_dimensions": page_dims,
            "description": f"Visuelle Analyse: {vis.file} ({vis.page_count} Seiten)",
            "visual_codings": codings,
        })

    # JSON-Export (Backup / Debug)
    visual_path = ctx.run_dir / "visual_analysis_results.json"
    visual_slim = [
        {k: v for k, v in r.items() if k != "path"}
        for r in qdpx_visual
    ]
    visual_path.write_text(
        json.dumps(visual_slim, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print_success(f"Visuelle Ergebnisse gespeichert: {visual_path}")

    return qdpx_visual


# ---------------------------------------------------------------------------
# Annotation (Phase 5)
# ---------------------------------------------------------------------------


def _build_text_codings_from_db(ctx: RunContext, pdf_id: int) -> list[dict]:
    """Aggregiert Text-Codings aus der DB pro Block-ID fuer den Annotator."""
    rows = ctx.db.get_codings_for_pdf(pdf_id)
    text_rows = [r for r in rows if r.get("source") == "text"]

    by_block: dict[str, dict] = {}
    for r in text_rows:
        bid = r.get("block_id") or ""
        if not bid:
            continue
        if bid not in by_block:
            by_block[bid] = {
                "block_id": bid,
                "codes": [],
                "ganzer_block": bool(r.get("ganzer_block", 1)),
                "begruendung": r.get("begruendung", "") or "",
            }
        for c in r.get("codes", []):
            if c and c not in by_block[bid]["codes"]:
                by_block[bid]["codes"].append(c)
    return list(by_block.values())


def _build_visual_codings_from_db(ctx: RunContext, pdf_id: int) -> list[dict]:
    """Aggregiert visuelle Codings aus der DB pro Block-ID fuer den Annotator."""
    rows = ctx.db.get_codings_for_pdf(pdf_id)
    visual_rows = [
        r for r in rows if str(r.get("source", "")).startswith("visual")
    ]

    by_block: dict[str, dict] = {}
    for r in visual_rows:
        bid = r.get("block_id") or ""
        if not bid:
            continue
        if bid not in by_block:
            by_block[bid] = {
                "block_id": bid,
                "page": int(r.get("page", 1) or 1),
                "codes": [],
                "description": (r.get("begruendung") or r.get("description") or ""),
                "source": r.get("source", "visual_triage"),
            }
        for c in r.get("codes", []):
            if c and c not in by_block[bid]["codes"]:
                by_block[bid]["codes"].append(c)
    return list(by_block.values())


def _pdf_has_visual_pages(ctx: RunContext, pdf_id: int) -> bool:
    """Prueft ob eine PDF mindestens eine Plan/Foto-Seite hat."""
    for cls in ctx.db.get_classifications(pdf_id):
        if cls.get("page_type") in ("plan", "photo"):
            return True
    return False


def run_annotation(ctx: RunContext, recipe=None) -> dict:
    """Annotiere alle PDFs der Run-DB mit Text- und Visual-Highlights.

    Fuer jede PDF werden Text- und Visual-Codings aus der DB geladen und
    via ``annotate_text_pdf`` / ``annotate_visual_pdf`` als gelbe
    Annotationen ins PDF geschrieben. Der Code (z.B. ``B-03``) steht im
    Comment der Annotation, sodass der Anwender in MAXQDA nach Comment
    sortieren und Bulk-Code-Zuweisungen machen kann.

    Text-PDFs bekommen Highlights; Plan/Foto-PDFs bekommen Rechtecke;
    Mixed-PDFs bekommen beides hintereinander (erst Text, dann Visual,
    jeweils auf denselben Output).

    Resume: PDFs mit ``pipeline_status.step='annotation', status='done'``
    werden uebersprungen.

    Args:
        ctx: RunContext mit Verbindungen zur pipeline.db.
        recipe: Wird aus Konsistenzgruenden akzeptiert, aber nicht mehr
            ausgewertet (Annotator-Farbe ist fix).

    Returns:
        Dict mit den Keys ``total_pdfs``, ``annotated``, ``skipped``,
        ``errors``, ``text_annotations``, ``visual_annotations``.
    """
    print_step("Annotation", "annotierte PDFs erzeugen", phase="annotate")

    stats = {
        "total_pdfs": 0,
        "annotated": 0,
        "skipped": 0,
        "errors": 0,
        "text_annotations": 0,
        "visual_annotations": 0,
    }

    pdfs = ctx.db.get_all_pdfs()
    stats["total_pdfs"] = len(pdfs)
    if not pdfs:
        console.print("  [dim]Keine PDFs in der DB.[/dim]")
        return stats

    annotated_root = ctx.annotated_dir
    annotated_root.mkdir(parents=True, exist_ok=True)

    # --- PDFs annotieren ---
    for pdf in pdfs:
        pdf_id = pdf["id"]
        src = Path(pdf["path"])
        rel_path = pdf.get("relative_path") or pdf.get("filename", "")
        project = pdf.get("project") or ""

        # Resume: bereits fertige PDFs ueberspringen
        if ctx.db.is_step_done(pdf_id, "annotation"):
            console.print(f"  [dim cyan]CACHE[/dim cyan] {_short(rel_path)}")
            stats["skipped"] += 1
            continue

        if not src.exists():
            msg = f"Quell-PDF nicht gefunden: {src}"
            console.print(f"  [dim]SKIP[/dim] {_short(rel_path)} ({msg})")
            ctx.db.set_step_status(pdf_id, "annotation", "error", msg)
            stats["errors"] += 1
            continue

        # Codings aus DB bauen
        text_codings = _build_text_codings_from_db(ctx, pdf_id)
        visual_codings = _build_visual_codings_from_db(ctx, pdf_id)

        # Klassifikationen (falls vorhanden) — fuer Mixed-Detection
        has_visual_pages = _pdf_has_visual_pages(ctx, pdf_id)

        # Entscheidung: welche Annotator-Calls laufen?
        # - Text-Annotator laeuft wenn es Text-Codings gibt.
        # - Visual-Annotator laeuft wenn es Visual-Codings oder eine
        #   als plan/photo klassifizierte Seite gibt.
        do_text = bool(text_codings)
        do_visual = bool(visual_codings) or has_visual_pages and not do_text

        if not text_codings and not visual_codings:
            # Leer-Annotation: source 1:1 nach dst kopieren, damit
            # Resume + Output-Layout konsistent sind.
            dst = ctx.annotated_path_for(project, rel_path)
            try:
                dst.write_bytes(src.read_bytes())
                ctx.db.set_step_status(pdf_id, "annotation", "done")
                stats["annotated"] += 1
                console.print(f"  [dim yellow]EMPTY[/dim yellow] {_short(rel_path)} (keine Codings)")
            except Exception as e:
                ctx.db.set_step_status(pdf_id, "annotation", "error", str(e)[:500])
                stats["errors"] += 1
                print_error(f"{_short(rel_path)} (copy): {e}")
            continue

        dst = ctx.annotated_path_for(project, rel_path)

        ctx.db.set_step_status(pdf_id, "annotation", "running")

        try:
            if do_text:
                extraction = ctx.db.load_extraction(pdf_id)
                if not extraction:
                    msg = "Text-Codings vorhanden aber keine Extraction"
                    raise RuntimeError(msg)
                t_stats = annotate_text_pdf(
                    src, dst, text_codings, extraction,
                )
                stats["text_annotations"] += t_stats.get("annotations_added", 0)

            if visual_codings:
                if do_text:
                    # Mixed-PDF: erst Text auf dst, dann Visual vom dst ueber
                    # einen temporaeren Zwischen-Pfad auf dst (pymupdf erlaubt
                    # kein save-to-same-path).
                    tmp = dst.with_suffix(dst.suffix + ".tmp")
                    dst.replace(tmp)
                    try:
                        v_stats = annotate_visual_pdf(
                            tmp, dst, visual_codings,
                        )
                    finally:
                        if tmp.exists():
                            tmp.unlink()
                else:
                    v_stats = annotate_visual_pdf(
                        src, dst, visual_codings,
                    )
                stats["visual_annotations"] += v_stats.get("annotations_added", 0)

            ctx.db.set_step_status(pdf_id, "annotation", "done")
            stats["annotated"] += 1

            t_count = sum(len(c.get("codes", [])) for c in text_codings)
            v_count = sum(len(c.get("codes", [])) for c in visual_codings)
            tag = "MIXED" if (do_text and visual_codings) else (
                "TEXT" if do_text else "VIS"
            )
            console.print(f"  [dim]{tag}[/dim] {_short(rel_path)} "
                  f"[dim](+{t_count} text, +{v_count} visual)[/dim]")

        except Exception as e:
            ctx.db.set_step_status(pdf_id, "annotation", "error", str(e)[:500])
            stats["errors"] += 1
            print_error(f"{_short(rel_path)}: {e}")

    print_success(f"Annotation: {stats['annotated']}/{stats['total_pdfs']} PDFs "
          f"({stats['skipped']} skipped, {stats['errors']} errors)")
    console.print(f"  [dim]Text-Highlights:    {stats['text_annotations']}[/dim]")
    console.print(f"  [dim]Visual-Rectangles:  {stats['visual_annotations']}[/dim]")
    console.print(f"  [dim]Output: {annotated_root}[/dim]")

    return stats


# ---------------------------------------------------------------------------
# Export
# ---------------------------------------------------------------------------

def run_export(pdf_results: list[dict], ctx: RunContext,
               qdpx_path: Path = None,
               visual_results: list[dict] = None,
               recipe=None):
    """Erzeugt die .qdpx-Datei (neu oder erweitert).

    Args:
        pdf_results: Ergebnisse aus Stufe 2+3
        ctx: RunContext fuer Ausgabepfade
        qdpx_path: Optionale bestehende .qdpx zum Erweitern
        visual_results: Visuelle Analyse-Ergebnisse (Phase 2)
        recipe: Recipe-Objekt (fuer Kategorie-Namen beim Auto-Anlegen von Codes)
    """
    print_step("Export: QDPX erzeugen", phase="output")

    existing_sources = {}
    existing_codes = {}

    if qdpx_path and qdpx_path.exists():
        console.print(f"  [dim]Lade bestehende QDPX: {qdpx_path}[/dim]")
        project, existing_sources = read_qdpx(qdpx_path)
        _, existing_codes = extract_codesystem(project)
        console.print(f"  [dim]-> {len(existing_codes)} bestehende Codes[/dim]")
    else:
        project = create_new_project()

    # Recipe-Kategorien fuer Auto-Code-Erstellung
    recipe_categories = recipe.categories if recipe else None

    # PDF-Sources hinzufuegen (Text-Kodierungen)
    code_guids = {}
    if pdf_results:
        code_guids = add_pdf_sources(
            project, pdf_results, existing_codes,
            recipe_categories=recipe_categories,
        )
        console.print(f"  [dim]-> {len(code_guids)} Codes (Text)[/dim]")

    # Visuelle Sources hinzufuegen (Plan/Foto-Kodierungen)
    if visual_results:
        _, updated_codes = extract_codesystem(project)
        visual_guids = add_visual_sources(project, visual_results, updated_codes)
        code_guids.update(visual_guids)
        n_visual = sum(len(r.get("visual_codings", [])) for r in visual_results)
        console.print(f"  [dim]-> {len(visual_guids)} Codes (Visuell), {n_visual} Kodierungen[/dim]")

    console.print(f"  [dim]-> {len(code_guids)} Codes gesamt[/dim]")

    # PDF-Dateien zum Einbetten sammeln
    pdf_files = {}
    for result in pdf_results:
        proj = result["project"]
        fname = result["file"]
        internal_path = f"{proj}/{fname}" if proj else fname
        pdf_files[internal_path] = Path(result["path"])

    if visual_results:
        for result in visual_results:
            path = result.get("path")
            if path:
                proj = result.get("project", "")
                fname = result["file"]
                internal_path = f"{proj}/{fname}" if proj else fname
                pdf_files[internal_path] = Path(path)

    # Schreiben
    output_path = ctx.run_dir / "qda" / "project.qdpx"
    write_qdpx(project, output_path, existing_sources, pdf_files)

    # DB-Status
    coding_summary = ctx.db.get_coding_summary()
    if coding_summary:
        console.print(f"  [dim]DB: {len(coding_summary)} verschiedene Codes verwendet[/dim]")

    print_success(f"QDPX: {len(pdf_files)} PDFs exportiert")


# ---------------------------------------------------------------------------
# Ergebnisse speichern/laden (JSON-Backup neben DB)
# ---------------------------------------------------------------------------

def save_results(pdf_results: list[dict], path: Path):
    """Speichert Analyse-Ergebnisse als JSON (Backup neben DB)."""
    slim = []
    for r in pdf_results:
        slim.append({
            "file": r["file"],
            "project": r["project"],
            "relative_path": r["relative_path"],
            "document_type": r.get("document_type", ""),
            "codings": r.get("codings", []),
            "neue_codes": r.get("neue_codes", []),
        })
    path.write_text(
        json.dumps(slim, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print_success(f"Ergebnisse gespeichert: {path}")


def load_results(path: Path) -> list[dict]:
    """Laedt gespeicherte Ergebnisse."""
    return json.loads(path.read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

def run_classification(pdfs: list[dict], ctx: RunContext,
                       pdf_ids: dict[str, int],
                       classify_mode: str = "local") -> dict:
    """Klassifiziert alle PDFs nach Dokumenttyp.

    Args:
        pdfs: PDF-Liste aus scan_projects()
        ctx: RunContext fuer Cache
        pdf_ids: {relative_path: pdf_id}
        classify_mode: "local", "llm", oder "hybrid"

    Returns:
        {relative_path: DocumentClassification}
    """
    print_step("Klassifikation", classify_mode, phase="scan")
    classifications = classify_project_pdfs(
        pdfs, mode=classify_mode, cache_dir=ctx.cache_dir,
    )
    print_classification_summary(classifications)

    # Klassifikation in DB speichern
    for rel_path, cls in classifications.items():
        pid = pdf_ids.get(rel_path)
        if pid is None:
            continue

        ctx.db.update_pdf_classification(pid, cls.document_type, cls.confidence)

        for page_cls in cls.pages:
            ctx.db.save_page_metrics(pid, page_cls.page, page_cls.metrics)
            ctx.db.save_classification(
                pid, page_cls.page,
                page_type=page_cls.page_type,
                confidence=page_cls.confidence,
                plan_subtype=page_cls.plan_subtype,
                has_title_block=bool(page_cls.title_block),
                title_block=page_cls.title_block,
            )

        ctx.db.set_step_status(pid, "classification", "done")

    return classifications


def run_pipeline(ctx: RunContext, recipe_id: str = "pdf_analyse",
                 project_filter: str = None,
                 qdpx_path: Path = None,
                 step: str = None,
                 mode: str = None,
                 classify_mode: str = "local",
                 skip_plans: bool = False,
                 skip_patterns: list[str] | None = None,
                 convert_office: bool = True):
    """Hauptpipeline.

    Args:
        ctx: RunContext
        recipe_id: Analyse-Recipe
        project_filter: Nur dieses Projekt
        qdpx_path: Bestehende .qdpx zum Erweitern
        step: Nur diesen Schritt ausfuehren
        mode: "text" (nur Text-PDFs), "visual" (nur Plaene/Fotos),
              "classify" (nur Klassifikation), None (alles)
        classify_mode: "local", "llm", "hybrid"
        skip_plans: Plaene anhand Default-Patterns rausfiltern
        skip_patterns: Zusaetzliche Regex-Patterns zum Filtern
        convert_office: docx/xlsx automatisch zu PDF konvertieren
    """
    from .pdf_scanner import filter_pdfs

    recipe = load_recipe(recipe_id)

    # Codesystem aus bestehender .qdpx laden
    codesystem = ""
    if qdpx_path and qdpx_path.exists():
        project_xml, _ = read_qdpx(qdpx_path)
        categories, codes = extract_codesystem(project_xml)
        codesystem = format_codesystem(categories, codes)
        console.print(f"  [dim]Codesystem geladen: {len(codes)} Codes aus {qdpx_path.name}[/dim]")

    # Scannen (mit optionaler Office-Konvertierung)
    convert_cache = ctx.run_dir / "converted" if convert_office else None
    pdfs = scan_projects(
        project_filter=project_filter,
        convert_office=convert_office,
        convert_cache_dir=convert_cache,
    )
    if not pdfs:
        print_error("Keine PDFs gefunden in input/projects/")
        sys.exit(1)

    # Filtern: Plaene und/oder beliebige Patterns rauswerfen
    if skip_plans or skip_patterns:
        before = len(pdfs)
        pdfs, removed = filter_pdfs(pdfs, skip_plans=skip_plans,
                                    skip_patterns=skip_patterns)
        if removed:
            console.print(f"  [dim]Filter: {len(removed)}/{before} PDFs entfernt[/dim]")
            for r in removed[:10]:
                console.print(f"    [dim]- {_short(r['relative_path'])}  ({r.get('_filter_reason','')})[/dim]")
            if len(removed) > 10:
                console.print(f"    [dim]... und {len(removed)-10} weitere[/dim]")

    if not pdfs:
        print_error("Nach Filtern keine PDFs uebrig.")
        sys.exit(1)

    manifest = build_manifest(pdfs)
    save_manifest(manifest, ctx.cache_dir / "manifest.json")
    print_manifest_summary(manifest)

    # PDFs in DB registrieren
    pdf_ids = _register_pdfs(pdfs, ctx)

    print_header("PDF-Dokumenten-Coder", recipe.name)
    summary_rows = [
        ("Run", ctx.run_dir.name),
        ("DB", f"{ctx.db.db_path.name} ({len(pdf_ids)} PDFs)"),
    ]
    if mode:
        summary_rows.append(("Modus", f"{mode} | Klassifikation: {classify_mode}"))
    print_summary(summary_rows)

    # --- Klassifikation (immer, ausser step=code/annotate/export) ---
    classifications = None
    if step not in ("code", "annotate", "export"):
        classifications = run_classification(pdfs, ctx, pdf_ids, classify_mode)

        if mode == "classify" or step == "classify":
            ctx.mark_step_done(0)
            print_success("Klassifikation abgeschlossen (kein Coding).")
            return

    # --- PDFs nach Typ filtern ---
    visual_pdfs = []
    text_pdfs = pdfs  # Default: alle als Text

    if classifications:
        groups = split_by_type(pdfs, classifications)
        if mode == "text":
            text_pdfs = groups.get("text", []) + groups.get("mixed", [])
            visual_pdfs = []
            console.print(f"  [dim]Text-Modus: {len(text_pdfs)} Text/Mixed-PDFs[/dim]")
        elif mode == "visual":
            text_pdfs = []
            visual_pdfs = groups.get("plan", []) + groups.get("photo", [])
            console.print(f"  [dim]Visual-Modus: {len(visual_pdfs)} Plan/Foto-PDFs[/dim]")
            if not visual_pdfs:
                console.print("  [dim]Keine visuellen PDFs gefunden.[/dim]")
                return
        else:
            text_pdfs = groups.get("text", []) + groups.get("mixed", [])
            visual_pdfs = groups.get("plan", []) + groups.get("photo", [])
            console.print(f"  [dim]Alle PDFs: {len(text_pdfs)} Text/Mixed, {len(visual_pdfs)} Plan/Foto[/dim]")

    results_path = ctx.run_dir / "pdf_analysis_results.json"
    pdf_results = []
    visual_qdpx_results = []

    # --- Text-Pipeline ---
    if text_pdfs and step in (None, "extract", "code"):
        if step == "extract" or step is None:
            extractions = run_extraction(text_pdfs, ctx, pdf_ids)
            if step == "extract":
                ctx.mark_step_done(1)
                return

        if step == "code" or step is None:
            if step == "code":
                # Extraktionen aus DB laden
                extractions = {}
                for pdf in text_pdfs:
                    pid = pdf_ids[pdf["relative_path"]]
                    data = ctx.db.load_extraction(pid)
                    if data:
                        extractions[pid] = data

            pdf_results = run_coding(
                text_pdfs, extractions, recipe, codesystem, ctx, pdf_ids,
            )
            save_results(pdf_results, results_path)

            if step == "code":
                ctx.mark_step_done(2)
                return

    # --- Vision-Pipeline ---
    if visual_pdfs and step in (None, "visual"):
        visual_qdpx_results = run_visual(visual_pdfs, ctx, pdf_ids)

    # --- Annotation (Phase 5) ---
    if step == "annotate" or step is None:
        run_annotation(ctx, recipe=recipe)
        if step == "annotate":
            ctx.mark_step_done(4)
            return

    # --- Export ---
    if step == "export" or step is None:
        if step == "export":
            if results_path.exists():
                pdf_results = load_results(results_path)
                # Extraktionen aus DB nachladen
                for result in pdf_results:
                    pid = pdf_ids.get(result.get("relative_path"))
                    if pid:
                        data = ctx.db.load_extraction(pid)
                        if data:
                            result["extraction"] = data

            # Visuelle Ergebnisse nachladen
            visual_path = ctx.run_dir / "visual_analysis_results.json"
            if visual_path.exists() and not visual_qdpx_results:
                visual_qdpx_results = json.loads(
                    visual_path.read_text(encoding="utf-8")
                )

        run_export(
            pdf_results, ctx, qdpx_path,
            visual_results=visual_qdpx_results if visual_qdpx_results else None,
            recipe=recipe,
        )

    # DB-Zusammenfassung
    step_summary = ctx.db.get_step_summary()
    if step_summary:
        console.print("\n  [dim]DB Pipeline-Status:[/dim]")
        for s, counts in sorted(step_summary.items()):
            parts = [f"{v} {k}" for k, v in counts.items()]
            console.print(f"    [dim]{s}: {', '.join(parts)}[/dim]")

    ctx.mark_completed()
    print_header("Fertig!", "PDF-Dokumenten-Coder")
    print_summary([
        ("Ergebnisse", str(ctx.run_dir)),
        ("Datenbank", str(ctx.db.db_path)),
    ])


