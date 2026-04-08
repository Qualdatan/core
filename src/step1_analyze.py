"""Schritt 1: Transkripte lesen und per Claude API analysieren."""

import json
import re
from pathlib import Path
from docx import Document
from anthropic import Anthropic

from .config import TRANSCRIPTS_DIR
from .models import AnalysisResult, CodedSegment
from .recipe import Recipe
from .run_context import RunContext


def read_transcripts(folder: Path = TRANSCRIPTS_DIR) -> dict[str, str]:
    """Liest alle .docx-Transkripte und gibt {Dateiname: Volltext} zurück."""
    docs = {}
    for f in sorted(folder.glob("*.docx")):
        doc = Document(f)
        text = "\n".join(p.text for p in doc.paragraphs)
        docs[f.name] = text
        print(f"  Gelesen: {f.name} ({len(text)} Zeichen)")
    return docs


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
# Positionsvalidierung
# ---------------------------------------------------------------------------

def validate_positions(segments: list[dict], full_text: str) -> list[dict]:
    """Korrigiert Zeichenpositionen durch Textsuche im Originaldokument."""
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
# Analyse
# ---------------------------------------------------------------------------

def analyze_transcript(client: Anthropic, recipe: Recipe, text: str,
                       filename: str, codebase: str = "",
                       ctx: RunContext | None = None) -> dict:
    """Analysiert ein einzelnes Transkript via Claude API (mit Cache)."""
    # Cache pruefen (parsed JSON aus vorherigem Run)
    if ctx:
        cached = ctx.get_cached_parsed(filename)
        if cached is not None:
            print(f"  CACHE HIT: {filename}")
            return cached

    prompt = recipe.build_prompt(text, filename, codebase)

    # Prompt cachen
    if ctx:
        ctx.cache_prompt(filename, prompt)

    print(f"  Sende an Claude API ({len(text)} Zeichen, Methode: {recipe.id})...")

    response = client.messages.create(
        model=recipe.model,
        max_tokens=recipe.max_tokens,
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


def run_analysis(recipe: Recipe, ctx: RunContext,
                 transcripts_dir: Path = TRANSCRIPTS_DIR,
                 codebase: str = "") -> AnalysisResult:
    """Fuehrt die komplette Analyse aller Transkripte durch."""
    client = Anthropic()
    result = AnalysisResult()
    result.recipe_id = recipe.id
    result.categories = recipe.categories

    print("=== Schritt 1: Transkripte lesen ===")
    result.documents = read_transcripts(transcripts_dir)

    if not result.documents:
        raise FileNotFoundError(f"Keine .docx-Dateien in {transcripts_dir} gefunden.")

    # Nur noch nicht analysierte Transkripte verarbeiten
    pending = ctx.get_pending_transcripts()
    already_done = [f for f in result.documents if f not in pending]
    if already_done:
        print(f"\n  {len(already_done)} Transkript(e) bereits analysiert (aus Cache)")

    all_kernergebnisse = []
    code_registry = {}

    for filename, text in result.documents.items():
        print(f"\n=== Analysiere: {filename} ===")
        data = analyze_transcript(client, recipe, text, filename, codebase, ctx)

        segments = validate_positions(data.get("segments", []), text)

        for seg in segments:
            code_id = seg["code_id"]
            segment = CodedSegment(
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
            )
            result.segments.append(segment)

            if code_id not in code_registry:
                code_registry[code_id] = {
                    "name": seg["code_name"],
                    "hauptkategorie": seg["hauptkategorie"],
                    "kodierdefinition": seg.get("kodierdefinition", ""),
                    "ankerbeispiel": seg.get("ankerbeispiel", ""),
                    "abgrenzungsregel": seg.get("abgrenzungsregel", ""),
                    "count": 0,
                }
            code_registry[code_id]["count"] += 1

        for ke in data.get("kernergebnisse", []):
            all_kernergebnisse.append(ke)

        # Transkript als fertig markieren
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
