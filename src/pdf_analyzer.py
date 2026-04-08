"""Stufe 2+3: LLM-basierte Code-Zuweisung für PDF-Extrakte.

Stufe 2 (Sonnet): Bekommt Block-IDs + Text, weist Codes zu.
Stufe 3 (Haiku):  Verfeinert Positionen innerhalb von Blöcken.
                  Nur wenn ganzer_block=false. Parallelisierbar.
"""

import json
import concurrent.futures
from pathlib import Path

from anthropic import Anthropic

from .step1_analyze import extract_json
from .pdf_extractor import extraction_to_text_summary
from .recipe import Recipe


# ---------------------------------------------------------------------------
# Stufe 2: Code-Zuweisung (Sonnet)
# ---------------------------------------------------------------------------

def build_coding_prompt(extraction_data: dict, recipe: Recipe,
                        project_name: str, codesystem: str = "") -> str:
    """Baut den Prompt für Stufe 2: Code-Zuweisung.

    Args:
        extraction_data: JSON aus pdf_extractor
        recipe: Recipe mit prompt_template
        project_name: Name des Projekts
        codesystem: Bestehendes Codesystem als Text
    """
    text_summary = extraction_to_text_summary(extraction_data)
    filename = extraction_data["file"]

    # Recipe-Template nutzen falls vorhanden, sonst Default
    if hasattr(recipe, "prompt_template") and recipe.prompt_template:
        prompt = recipe.prompt_template
        categories_text = "\n".join(
            f"  {k}: {v}" for k, v in recipe.categories.items()
        )
        prompt = prompt.replace("{categories}", categories_text)
        prompt = prompt.replace("{project_name}", project_name)
        prompt = prompt.replace("{filename}", filename)
        prompt = prompt.replace("{content}", text_summary)

        if codesystem:
            prompt = prompt.replace("{codebase_section}",
                                   f"## Bestehendes Codesystem:\n{codesystem}")
        else:
            prompt = prompt.replace("{codebase_section}", "")
        return prompt

    # Fallback: Default-Prompt
    parts = [
        f"Du analysierst extrahierte Inhalte aus einer Projektunterlage.",
        f"Projekt: {project_name}",
        f"Datei: {filename}",
        "",
    ]

    if codesystem:
        parts.append("## Bestehendes Codesystem (deduktiv anwenden):")
        parts.append(codesystem)
        parts.append("")

    if recipe.categories:
        parts.append("## Hauptkategorien:")
        for k, v in recipe.categories.items():
            parts.append(f"  {k}: {v}")
        parts.append("")

    parts.append("## Extrahierte Inhalte:")
    parts.append(text_summary)
    parts.append("")
    parts.append("""## Aufgabe:
1. Weise jedem relevanten Block Codes aus dem Codesystem zu.
2. Wenn ein Block keinen existierenden Code passt, schlage einen neuen vor.
3. Setze ganzer_block=true wenn der gesamte Block kodiert werden soll.
4. Setze ganzer_block=false wenn nur ein Teil des Blocks relevant ist.

Antworte ausschliesslich als JSON:
{
  "document_type": "Typ des Dokuments (z.B. Flächenberechnung, Plan, Aufgabenstellung)",
  "codings": [
    {
      "block_id": "p1_b0",
      "codes": ["A-01"],
      "ganzer_block": true,
      "begruendung": "Kurze Begründung"
    }
  ],
  "neue_codes": [
    {
      "code_id": "L-01",
      "code_name": "Name",
      "hauptkategorie": "L",
      "kodierdefinition": "Definition des neuen Codes"
    }
  ]
}""")

    return "\n".join(parts)


def analyze_pdf_codes(client: Anthropic, extraction_data: dict,
                      recipe: Recipe, project_name: str,
                      codesystem: str = "",
                      cache_dir: Path = None,
                      cache_key: str = "") -> dict:
    """Stufe 2: Sendet extrahierten Text an Sonnet für Code-Zuweisung.

    Args:
        client: Anthropic-Client
        extraction_data: JSON aus pdf_extractor
        recipe: Recipe mit Modell-Config
        project_name: Projektname
        codesystem: Bestehendes Codesystem
        cache_dir: Verzeichnis für Cache-Dateien
        cache_key: Schlüssel für Cache-Datei

    Returns:
        Dict mit codings und neue_codes
    """
    # Cache prüfen
    if cache_dir and cache_key:
        parsed_path = cache_dir / "parsed" / f"{cache_key}.json"
        if parsed_path.exists():
            try:
                cached = json.loads(parsed_path.read_text(encoding="utf-8"))
                print(f"    CACHE HIT: {cache_key}")
                return cached
            except (json.JSONDecodeError, OSError):
                pass

    prompt = build_coding_prompt(extraction_data, recipe,
                                 project_name, codesystem)

    # Prompt cachen
    if cache_dir and cache_key:
        prompt_path = cache_dir / "prompts" / f"{cache_key}.txt"
        prompt_path.parent.mkdir(parents=True, exist_ok=True)
        prompt_path.write_text(prompt, encoding="utf-8")

    # Dynamische max_tokens: kürzere Dokumente brauchen weniger Output
    n_blocks = sum(len(p["blocks"]) for p in extraction_data["pages"])
    dynamic_max = min(recipe.max_tokens, max(4096, n_blocks * 200 + 2000))
    print(f"    Sende an {recipe.model} ({len(prompt)} Zeichen, "
          f"max_tokens={dynamic_max})...")

    response = client.messages.create(
        model=recipe.model,
        max_tokens=dynamic_max,
        messages=[{"role": "user", "content": prompt}],
    )

    if response.stop_reason == "max_tokens":
        print(f"    WARNUNG: Antwort abgeschnitten")

    response_text = response.content[0].text

    # Response cachen
    if cache_dir and cache_key:
        resp_path = cache_dir / "responses" / f"{cache_key}.txt"
        resp_path.parent.mkdir(parents=True, exist_ok=True)
        resp_path.write_text(response_text, encoding="utf-8")

    data = extract_json(response_text)

    # Parsed cachen
    if cache_dir and cache_key:
        parsed_path = cache_dir / "parsed" / f"{cache_key}.json"
        parsed_path.parent.mkdir(parents=True, exist_ok=True)
        parsed_path.write_text(
            json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
        )

    return data


# ---------------------------------------------------------------------------
# Stufe 3: Position-Refinement (Haiku, parallelisiert)
# ---------------------------------------------------------------------------

REFINEMENT_MODEL = "claude-haiku-4-5-20251001"


REFINEMENT_BATCH_SIZE = 20


def _refine_batch(client: Anthropic, items: list[dict]) -> dict[str, dict]:
    """Verfeinert bis zu REFINEMENT_BATCH_SIZE Block+Code-Paare in einem Call.

    Args:
        items: Liste von {key, block_id, code_id, text}

    Returns:
        {key: {char_start, char_end}}
    """
    parts = []
    for i, item in enumerate(items, 1):
        parts.append(
            f'{i}. ID="{item["key"]}" | Code="{item["code_id"]}"\n'
            f'   Text: """{item["text"]}"""'
        )

    prompt = f"""Für jeden der folgenden Textblöcke: Bestimme char_start und char_end
des Abschnitts, der zum genannten Code gehört. Nicht der gesamte Block ist relevant.

{chr(10).join(parts)}

Antworte als JSON-Objekt mit den IDs als Schlüssel:
{{{", ".join(f'"{item["key"]}": {{"char_start": N, "char_end": M}}' for item in items[:2])}{"..." if len(items) > 2 else ""}}}"""

    response = client.messages.create(
        model=REFINEMENT_MODEL,
        max_tokens=256 + len(items) * 48,
        messages=[{"role": "user", "content": prompt}],
    )

    data = extract_json(response.content[0].text)

    # Falls nur ein Item → Ergebnis direkt als {char_start, char_end}
    if len(items) == 1 and "char_start" in data:
        return {items[0]["key"]: data}

    return data


def refine_positions(client: Anthropic, codings: list[dict],
                     extraction_data: dict,
                     max_workers: int = 5) -> list[dict]:
    """Stufe 3: Verfeinert Positionen für Blöcke mit ganzer_block=false.

    Batcht mehrere Refinements pro API-Call (bis zu REFINEMENT_BATCH_SIZE)
    und parallelisiert die Batches mit ThreadPoolExecutor.

    Args:
        client: Anthropic-Client
        codings: Liste von Code-Zuweisungen aus Stufe 2
        extraction_data: Originale Extraktionsdaten
        max_workers: Max parallele Haiku-Calls

    Returns:
        Aktualisierte codings mit char_start/char_end
    """
    # Block-ID → Block-Daten Index bauen
    block_index = {}
    for page in extraction_data["pages"]:
        for block in page["blocks"]:
            block_index[block["id"]] = block

    # Nur Blöcke die Refinement brauchen
    to_refine = []
    for coding in codings:
        if coding.get("ganzer_block", True):
            continue
        block = block_index.get(coding["block_id"])
        if not block or block["type"] != "text":
            continue
        for code_id in coding.get("codes", []):
            key = f"{coding['block_id']}_{code_id}"
            to_refine.append({
                "key": key,
                "block_id": coding["block_id"],
                "code_id": code_id,
                "text": block["text"],
            })

    if not to_refine:
        return codings

    # In Batches aufteilen
    batches = [
        to_refine[i:i + REFINEMENT_BATCH_SIZE]
        for i in range(0, len(to_refine), REFINEMENT_BATCH_SIZE)
    ]

    print(f"    Stufe 3: {len(to_refine)} Positionen in {len(batches)} "
          f"Batches verfeinern (Haiku)...")

    # Batches parallel ausführen
    results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(_refine_batch, client, batch): i
            for i, batch in enumerate(batches)
        }

        for future in concurrent.futures.as_completed(futures):
            batch_idx = futures[future]
            try:
                batch_results = future.result()
                results.update(batch_results)
            except Exception as e:
                print(f"    Refinement-Batch {batch_idx} fehlgeschlagen: {e}")

    # Ergebnisse in codings einpflegen
    for coding in codings:
        if coding.get("ganzer_block", True):
            continue
        for code_id in coding.get("codes", []):
            key = f"{coding['block_id']}_{code_id}"
            if key in results:
                coding.setdefault("refinements", {})[code_id] = results[key]

    return codings


# ---------------------------------------------------------------------------
# Codesystem aus Analyse-Ergebnis formatieren
# ---------------------------------------------------------------------------

def format_codesystem(categories: dict, codes: dict) -> str:
    """Formatiert bestehendes Codesystem als Text für den Prompt.

    Args:
        categories: {key: name} aus AnalysisResult
        codes: {code_id: {name, hauptkategorie, ...}} aus AnalysisResult
    """
    lines = []
    codes_by_cat = {}
    for code_id, info in codes.items():
        cat = info.get("hauptkategorie", "?")
        codes_by_cat.setdefault(cat, []).append((code_id, info))

    for cat_key in sorted(categories.keys()):
        cat_name = categories[cat_key]
        lines.append(f"{cat_key}: {cat_name}")
        for code_id, info in sorted(codes_by_cat.get(cat_key, [])):
            name = info.get("name", "")
            definition = info.get("kodierdefinition", "")
            line = f"  {code_id}: {name}"
            if definition:
                line += f" — {definition}"
            lines.append(line)

    return "\n".join(lines)
