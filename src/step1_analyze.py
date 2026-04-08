"""Schritt 1: Transkripte lesen und per Claude API nach Mayring analysieren."""

import json
import os
import re
from pathlib import Path
from docx import Document
from anthropic import Anthropic

from .config import (
    TRANSCRIPTS_DIR, ANALYSIS_JSON, HAUPTKATEGORIEN,
    CLAUDE_MODEL, MAX_TOKENS,
)
from .models import AnalysisResult, CodedSegment


def read_transcripts(folder: Path = TRANSCRIPTS_DIR) -> dict[str, str]:
    """Liest alle .docx-Transkripte und gibt {Dateiname: Volltext} zurück."""
    docs = {}
    for f in sorted(folder.glob("*.docx")):
        doc = Document(f)
        text = "\n".join(p.text for p in doc.paragraphs)
        docs[f.name] = text
        print(f"  Gelesen: {f.name} ({len(text)} Zeichen)")
    return docs


def build_analysis_prompt(text: str, filename: str) -> str:
    """Baut den Analyse-Prompt für ein einzelnes Transkript."""
    kategorien_text = "\n".join(
        f"  {k}: {v}" for k, v in HAUPTKATEGORIEN.items()
    )
    return f"""Du bist ein Experte für qualitative Inhaltsanalyse nach Mayring (deduktiv-induktiv).

Analysiere das folgende Transkript eines Experteninterviews aus dem Bereich Bauwesen/Architektur.

## Hauptkategorien (deduktiv vorgegeben):
{kategorien_text}

## Aufgabe:
1. Identifiziere alle relevanten Textstellen und weise ihnen Codes zu.
2. Jeder Code gehört zu genau einer Hauptkategorie (A-K).
3. Vergib Code-IDs im Format "X-NN" (z.B. "A-01", "B-03").
4. Für jede kodierte Stelle: gib den EXAKTEN Text an sowie die Zeichenposition (Start/Ende) im Gesamttext.
5. Erstelle eine Kodierdefinition, ein Ankerbeispiel und eine Abgrenzungsregel für jeden Code.
6. Formuliere 3-5 Kernergebnisse als wissenschaftliche Befunde.

## Transkript ({filename}):
---
{text}
---

## Antwortformat (strikt JSON, keine Erklärungen davor/danach):
{{
  "segments": [
    {{
      "code_id": "A-01",
      "code_name": "Kurzer Code-Name",
      "hauptkategorie": "A",
      "text": "Exakter Text aus dem Transkript",
      "char_start": 0,
      "char_end": 100,
      "kodierdefinition": "Definition des Codes",
      "ankerbeispiel": "Typisches Beispiel",
      "abgrenzungsregel": "Abgrenzung zum Nachbarcode"
    }}
  ],
  "kernergebnisse": [
    {{
      "nr": 1,
      "befund": "Kurzer Befund",
      "erlaeuterung": "Wissenschaftliche Erläuterung"
    }}
  ]
}}

WICHTIG:
- char_start und char_end müssen die tatsächlichen Zeichenpositionen im Gesamttext sein.
- Gib NUR valides JSON zurück, keine Markdown-Codeblöcke.
- Sei gründlich: kodiere ALLE relevanten Stellen, nicht nur die offensichtlichsten.
- Nutze pro Hauptkategorie mehrere spezifische Codes (mindestens 2-3 verschiedene wo möglich).
"""


def extract_json(response_text: str) -> dict:
    """Extrahiert JSON aus der API-Antwort, auch wenn Markdown-Blöcke drumherum sind."""
    # Versuche erst direkt
    text = response_text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        # Suche nach dem ersten { ... letzten }
        start = text.find("{")
        end = text.rfind("}") + 1
        if start >= 0 and end > start:
            return json.loads(text[start:end])
        raise


def validate_positions(segments: list[dict], full_text: str, filename: str) -> list[dict]:
    """Korrigiert Zeichenpositionen durch Textsuche im Originaldokument."""
    for seg in segments:
        excerpt = seg["text"]
        # Versuche exakten Match
        pos = full_text.find(excerpt)
        if pos >= 0:
            seg["char_start"] = pos
            seg["char_end"] = pos + len(excerpt)
        else:
            # Versuche mit kürzerem Snippet (erste 60 Zeichen)
            short = excerpt[:60]
            pos = full_text.find(short)
            if pos >= 0:
                seg["char_start"] = pos
                seg["char_end"] = pos + len(excerpt)
            else:
                # Fuzzy: suche nach Wörtern
                words = excerpt.split()[:5]
                pattern = r"\b" + r"\s+".join(re.escape(w) for w in words) + r"\b"
                match = re.search(pattern, full_text)
                if match:
                    seg["char_start"] = match.start()
                    seg["char_end"] = match.start() + len(excerpt)
                # Sonst: behalte die KI-Positionen als Fallback
    return segments


def analyze_transcript(client: Anthropic, text: str, filename: str) -> dict:
    """Analysiert ein einzelnes Transkript via Claude API."""
    prompt = build_analysis_prompt(text, filename)
    print(f"  Sende an Claude API ({len(text)} Zeichen)...")

    response = client.messages.create(
        model=CLAUDE_MODEL,
        max_tokens=MAX_TOKENS,
        messages=[{"role": "user", "content": prompt}],
    )

    response_text = response.content[0].text
    return extract_json(response_text)


def run_analysis(transcripts_dir: Path = TRANSCRIPTS_DIR) -> AnalysisResult:
    """Führt die komplette Analyse aller Transkripte durch."""
    client = Anthropic()  # nutzt ANTHROPIC_API_KEY aus Umgebung
    result = AnalysisResult()

    print("=== Schritt 1: Transkripte lesen ===")
    result.documents = read_transcripts(transcripts_dir)

    if not result.documents:
        raise FileNotFoundError(f"Keine .docx-Dateien in {transcripts_dir} gefunden.")

    all_kernergebnisse = []
    code_registry = {}  # code_id -> code-info

    for filename, text in result.documents.items():
        print(f"\n=== Analysiere: {filename} ===")
        data = analyze_transcript(client, text, filename)

        # Positionen validieren/korrigieren
        segments = validate_positions(data.get("segments", []), text, filename)

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

            # Code-Registry aktualisieren
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

        # Kernergebnisse sammeln
        for ke in data.get("kernergebnisse", []):
            all_kernergebnisse.append(ke)

    result.codes = code_registry
    result.kernergebnisse = all_kernergebnisse

    # Speichern
    result.save(ANALYSIS_JSON)
    print(f"\n=== Analyse gespeichert: {ANALYSIS_JSON} ===")
    print(f"    {len(result.segments)} kodierte Segmente")
    print(f"    {len(result.codes)} verschiedene Codes")
    print(f"    {len(result.kernergebnisse)} Kernergebnisse")

    return result
