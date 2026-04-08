"""Schritt 1: Transkripte lesen und per Claude API analysieren.

Nutzt pymupdf für die lokale Extraktion (Block-IDs + Koordinaten),
sendet nur die Block-Zusammenfassung an das LLM (Token-Ersparnis),
und mappt die Ergebnisse zurück auf exakte Zeichenpositionen.
"""

import json
import re
import concurrent.futures
from pathlib import Path
from anthropic import Anthropic

from .config import TRANSCRIPTS_DIR
from .models import AnalysisResult, CodedSegment
from .recipe import Recipe
from .run_context import RunContext
from .pdf_extractor import (
    extract_pdf, extraction_to_text_summary, build_fulltext_and_positions,
)


def read_transcripts(folder: Path = TRANSCRIPTS_DIR) -> dict[str, str]:
    """Liest alle .docx-Transkripte via pymupdf und gibt {Dateiname: Volltext} zurück."""
    docs = {}
    for f in sorted(folder.glob("*.docx")):
        data = extract_pdf(f)
        fulltext, _ = build_fulltext_and_positions(data)
        docs[f.name] = fulltext
        print(f"  Gelesen: {f.name} ({len(fulltext)} Zeichen)")
    return docs


def extract_transcripts(folder: Path = TRANSCRIPTS_DIR) -> dict[str, dict]:
    """Extrahiert alle .docx-Transkripte und gibt strukturierte Daten zurück.

    Returns:
        {filename: {"extraction": data, "fulltext": str,
                     "positions": {block_id: (start, end)},
                     "block_index": {block_id: block_data}}}
    """
    results = {}
    for f in sorted(folder.glob("*.docx")):
        data = extract_pdf(f)
        fulltext, positions = build_fulltext_and_positions(data)

        # Block-Index für schnellen Lookup
        block_index = {}
        for page in data["pages"]:
            for block in page["blocks"]:
                block_index[block["id"]] = block

        results[f.name] = {
            "extraction": data,
            "fulltext": fulltext,
            "positions": positions,
            "block_index": block_index,
        }

        total_blocks = sum(len(p["blocks"]) for p in data["pages"])
        print(f"  Extrahiert: {f.name} ({total_blocks} Blöcke, "
              f"{len(fulltext)} Zeichen)")
    return results


# ---------------------------------------------------------------------------
# JSON-Parsing
# ---------------------------------------------------------------------------

def extract_json(response_text: str) -> dict:
    """Extrahiert JSON aus der API-Antwort, auch bei abgeschnittenem Output."""
    text = response_text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)

    start = text.find("{")
    if start < 0:
        raise ValueError("Kein JSON in der Antwort gefunden")
    text = text[start:]

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        repaired = text
        if repaired.count('"') % 2 == 1:
            repaired += '"'
        open_brackets = repaired.count("[") - repaired.count("]")
        open_braces = repaired.count("{") - repaired.count("}")
        repaired = re.sub(r",\s*$", "", repaired)
        repaired += "]" * open_brackets
        repaired += "}" * open_braces
        try:
            return json.loads(repaired)
        except json.JSONDecodeError:
            last_good = repaired.rfind("},")
            if last_good > 0:
                truncated = repaired[:last_good + 1]
                open_brackets = truncated.count("[") - truncated.count("]")
                open_braces = truncated.count("{") - truncated.count("}")
                truncated += "]" * open_brackets
                truncated += "}" * open_braces
                return json.loads(truncated)
            raise


# ---------------------------------------------------------------------------
# Positionsvalidierung (Legacy, fuer altes Format mit char_start/char_end)
# ---------------------------------------------------------------------------

def validate_positions(segments: list[dict], full_text: str) -> list[dict]:
    """Korrigiert Zeichenpositionen durch Textsuche im Originaldokument.

    Legacy-Funktion, wird bei Block-ID-Ansatz nicht mehr benötigt.
    """
    for seg in segments:
        excerpt = seg["text"]
        pos = full_text.find(excerpt)
        if pos >= 0:
            seg["char_start"] = pos
            seg["char_end"] = pos + len(excerpt)
        else:
            short = excerpt[:60]
            pos = full_text.find(short)
            if pos >= 0:
                seg["char_start"] = pos
                seg["char_end"] = pos + len(excerpt)
            else:
                words = excerpt.split()[:5]
                if words:
                    pattern = r"\b" + r"\s+".join(re.escape(w) for w in words) + r"\b"
                    match = re.search(pattern, full_text)
                    if match:
                        seg["char_start"] = match.start()
                        seg["char_end"] = match.start() + len(excerpt)
    return segments


# ---------------------------------------------------------------------------
# Block-ID → Zeichenposition Mapping
# ---------------------------------------------------------------------------

def resolve_block_codings(codings: list[dict],
                          positions: dict[str, tuple[int, int]],
                          block_index: dict[str, dict]) -> list[dict]:
    """Wandelt Block-basierte Codings in Segmente mit Zeichenpositionen um.

    Args:
        codings: LLM-Ergebnis mit block_id, code_id etc.
        positions: {block_id: (char_start, char_end)} im Volltext
        block_index: {block_id: block_data} für Textinhalte

    Returns:
        Liste von Segmenten im alten Format (code_id, text, char_start, char_end, ...)
    """
    segments = []
    for coding in codings:
        block_id = coding.get("block_id", "")
        if block_id not in positions:
            continue

        char_start, char_end = positions[block_id]
        block = block_index.get(block_id, {})
        text = block.get("text", "")

        segments.append({
            "code_id": coding["code_id"],
            "code_name": coding.get("code_name", ""),
            "hauptkategorie": coding.get("hauptkategorie", ""),
            "text": text,
            "char_start": char_start,
            "char_end": char_end,
            "kodierdefinition": coding.get("kodierdefinition", ""),
            "ankerbeispiel": coding.get("ankerbeispiel", ""),
            "abgrenzungsregel": coding.get("abgrenzungsregel", ""),
        })

    return segments


# ---------------------------------------------------------------------------
# Analyse
# ---------------------------------------------------------------------------

def analyze_transcript(client: Anthropic, recipe: Recipe, content: str,
                       filename: str, codebase: str = "",
                       ctx: RunContext | None = None) -> dict:
    """Analysiert ein einzelnes Transkript via Claude API (mit Cache).

    Args:
        client: Anthropic-Client
        recipe: Recipe mit Prompt-Template
        content: Block-basierte Textzusammenfassung (von extraction_to_text_summary)
        filename: Dateiname des Transkripts
        codebase: Optionale Codebasis
        ctx: RunContext für Caching
    """
    # Cache pruefen (parsed JSON aus vorherigem Run)
    if ctx:
        cached = ctx.get_cached_parsed(filename)
        if cached is not None:
            print(f"  CACHE HIT: {filename}")
            return cached

    prompt = recipe.build_prompt("", filename, codebase, content=content)

    # Prompt cachen
    if ctx:
        ctx.cache_prompt(filename, prompt)

    # Dynamische max_tokens: kürzere Transkripte brauchen weniger Output
    # ~1 Coding pro 500 Zeichen Input, ~150 Token pro Coding
    dynamic_max = min(recipe.max_tokens, max(4096, len(content) // 3 + 2000))
    print(f"  Sende an Claude API ({len(content)} Zeichen, "
          f"max_tokens={dynamic_max}, Methode: {recipe.id})...")

    response = client.messages.create(
        model=recipe.model,
        max_tokens=dynamic_max,
        messages=[{"role": "user", "content": prompt}],
    )

    if response.stop_reason == "max_tokens":
        print(f"  WARNUNG: Antwort abgeschnitten (max_tokens={recipe.max_tokens}). Versuche Reparatur...")

    response_text = response.content[0].text

    # Rohe Antwort cachen
    if ctx:
        ctx.cache_response(filename, response_text)

    data = extract_json(response_text)

    # Parsed JSON cachen
    if ctx:
        ctx.cache_parsed(filename, data)

    return data


def _process_single_result(filename: str, data: dict,
                           tdata: dict) -> tuple[list[CodedSegment], list, dict]:
    """Verarbeitet das LLM-Ergebnis eines Transkripts.

    Returns:
        (segments, kernergebnisse, code_registry_entries)
    """
    codings = data.get("codings", [])

    # Legacy-Fallback: altes Format mit "segments" statt "codings"
    if not codings and "segments" in data:
        raw_segments = validate_positions(data["segments"], tdata["fulltext"])
    else:
        raw_segments = resolve_block_codings(
            codings, tdata["positions"], tdata["block_index"]
        )

    segments = []
    code_entries = {}
    for seg in raw_segments:
        code_id = seg["code_id"]
        segments.append(CodedSegment(
            code_id=code_id,
            code_name=seg["code_name"],
            hauptkategorie=seg["hauptkategorie"],
            text=seg["text"],
            char_start=seg["char_start"],
            char_end=seg["char_end"],
            document=filename,
            kodierdefinition=seg.get("kodierdefinition", ""),
            ankerbeispiel=seg.get("ankerbeispiel", ""),
            abgrenzungsregel=seg.get("abgrenzungsregel", ""),
        ))

        if code_id not in code_entries:
            code_entries[code_id] = {
                "name": seg["code_name"],
                "hauptkategorie": seg["hauptkategorie"],
                "kodierdefinition": seg.get("kodierdefinition", ""),
                "ankerbeispiel": seg.get("ankerbeispiel", ""),
                "abgrenzungsregel": seg.get("abgrenzungsregel", ""),
                "count": 0,
            }
        code_entries[code_id]["count"] += 1

    return segments, data.get("kernergebnisse", []), code_entries


def run_analysis(recipe: Recipe, ctx: RunContext,
                 transcripts_dir: Path = TRANSCRIPTS_DIR,
                 codebase: str = "",
                 max_workers: int = 4) -> AnalysisResult:
    """Fuehrt die komplette Analyse aller Transkripte durch (parallel).

    Args:
        max_workers: Max parallele API-Calls (default: 4)
    """
    client = Anthropic()
    result = AnalysisResult()
    result.recipe_id = recipe.id
    result.categories = recipe.categories

    print("=== Schritt 1: Transkripte extrahieren (pymupdf) ===")
    transcript_data = extract_transcripts(transcripts_dir)

    if not transcript_data:
        raise FileNotFoundError(f"Keine .docx-Dateien in {transcripts_dir} gefunden.")

    # Volltexte für QDPX-Export speichern
    result.documents = {
        fname: tdata["fulltext"] for fname, tdata in transcript_data.items()
    }

    # Nur noch nicht analysierte Transkripte verarbeiten
    pending = ctx.get_pending_transcripts()
    already_done = [f for f in transcript_data if f not in pending]
    if already_done:
        print(f"\n  {len(already_done)} Transkript(e) bereits analysiert (aus Cache)")

    # --- Parallele API-Calls ---
    n_total = len(transcript_data)
    if n_total > 1:
        print(f"\n  Starte {min(max_workers, n_total)} parallele API-Calls "
              f"für {n_total} Transkripte...")

    # Für jedes Transkript: Content vorbereiten und API-Call abschicken
    futures = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
        for filename, tdata in transcript_data.items():
            content = extraction_to_text_summary(tdata["extraction"])
            future = pool.submit(
                analyze_transcript, client, recipe, content,
                filename, codebase, ctx,
            )
            futures[future] = filename

        # Ergebnisse einsammeln (in Reihenfolge der Fertigstellung)
        completed = 0
        api_results = {}  # filename → data
        for future in concurrent.futures.as_completed(futures):
            filename = futures[future]
            completed += 1
            try:
                data = future.result()
                api_results[filename] = data
                n_codings = len(data.get("codings", data.get("segments", [])))
                print(f"  [{completed}/{n_total}] Fertig: {filename} "
                      f"({n_codings} Kodierungen)")
            except Exception as e:
                print(f"  [{completed}/{n_total}] FEHLER: {filename}: {e}")

    # --- Ergebnisse in stabiler Reihenfolge verarbeiten ---
    all_kernergebnisse = []
    code_registry = {}

    for filename in transcript_data:  # Originalreihenfolge
        if filename not in api_results:
            continue

        tdata = transcript_data[filename]
        data = api_results[filename]

        segments, kernergebnisse, code_entries = _process_single_result(
            filename, data, tdata
        )

        result.segments.extend(segments)
        all_kernergebnisse.extend(kernergebnisse)

        for code_id, entry in code_entries.items():
            if code_id not in code_registry:
                code_registry[code_id] = entry
            else:
                code_registry[code_id]["count"] += entry["count"]

        ctx.mark_transcript_done(filename)

    result.codes = code_registry
    result.kernergebnisse = all_kernergebnisse

    result.save(ctx.analysis_json)
    ctx.mark_step_done(1)
    print(f"\n=== Analyse gespeichert: {ctx.analysis_json} ===")
    print(f"    {len(result.segments)} kodierte Segmente")
    print(f"    {len(result.codes)} verschiedene Codes")
    print(f"    {len(result.kernergebnisse)} Kernergebnisse")

    return result
