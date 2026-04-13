"""Recipe-Loader: Liest Analyse-Methoden aus YAML-Dateien.

Methoden liegen unter METHODS_DIR (Default: methods/) und sind in
Subdirectories nach Anwendungsfall organisiert, z.B.:

    methods/
      interviewanalysis/
        mayring.yaml
        prisma.yaml
      documentanalysis/
        pdf_analyse.yaml
        visual_analyse.yaml

Der Loader sucht rekursiv. Die Subdir-Struktur ist organisatorisch und
wird in `Recipe.category` exponiert (= Name des direkten Eltern-Ordners).
"""

from pathlib import Path
from dataclasses import dataclass
import yaml

from .config import METHODS_DIR, CODEBASES_DIR, ENV_CLAUDE_MODEL, ENV_CLAUDE_MAX_TOKENS


# Erlaubte Coding-Strategien fuer Recipe + CLI
CODING_STRATEGIES = ("strict", "hybrid", "inductive")


class _SafeFormatDict(dict):
    """Dict, das bei fehlenden Keys den Platzhalter leer laesst statt zu crashen."""
    def __missing__(self, key):
        return ""


def _strategy_instruction(strategy: str, has_codebase: bool) -> str:
    """Liefert den Instruktions-Block fuer eine Coding-Strategie.

    Der zurueckgegebene Text wird in den ``codebase_section`` des Prompts
    injiziert und steuert, ob die LLM neue Codes induktiv erfinden darf
    oder nicht.

    - ``strict``: **ausschliesslich** Codes aus der Codebase.
    - ``hybrid``: primaer Codes aus der Codebase, neue als Fallback erlaubt.
    - ``inductive``: keine Vorgabe, alles induktiv.

    Wenn keine Codebase vorhanden ist, macht ``strict`` streng genommen
    keinen Sinn — wir geben trotzdem eine Warnung aus, damit der LLM
    wenigstens versucht, mit den Hauptkategorien allein zu arbeiten.
    """
    if strategy == "strict":
        if has_codebase:
            return (
                "## Coding-Strategie: STRICT\n"
                "Du DARFST AUSSCHLIESSLICH Codes aus der oben angegebenen "
                "Codebase verwenden. Erfinde KEINE neuen Codes. Wenn kein "
                "Code passt, lasse das Segment unkodiert. Das Feld "
                "'neue_codes' MUSS im JSON-Output leer bleiben (`[]`)."
            )
        return (
            "## Coding-Strategie: STRICT (ohne Codebase)\n"
            "Es wurde keine Codebase mitgegeben — arbeite streng mit den "
            "oben definierten Hauptkategorien. Erfinde KEINE neuen Codes. "
            "Das Feld 'neue_codes' MUSS im JSON-Output leer bleiben (`[]`)."
        )
    if strategy == "hybrid":
        if has_codebase:
            return (
                "## Coding-Strategie: HYBRID\n"
                "Verwende primaer Codes aus der oben angegebenen Codebase. "
                "Schlage neue Codes nur dann vor, wenn wirklich kein "
                "bestehender passt."
            )
        return (
            "## Coding-Strategie: HYBRID (ohne Codebase)\n"
            "Es wurde keine Codebase mitgegeben — entwickle Codes primaer "
            "induktiv und halte dich an die oben definierten Hauptkategorien."
        )
    if strategy == "inductive":
        return (
            "## Coding-Strategie: INDUCTIVE\n"
            "Es gibt keine Vorgaben. Entwickle Codes induktiv aus dem "
            "Material. Nutze die Hauptkategorien als grobe Orientierung, "
            "aber sei offen fuer neue Codes."
        )
    # Unbekannte Strategie: keine Extra-Instruktion (Fallback auf alt)
    return ""


@dataclass
class Recipe:
    """Eine Analyse-Methode (z.B. Mayring, PRISMA).

    Felder:
        coding_strategy: 'strict' | 'hybrid' | 'inductive' — steuert ob die
            LLM neue Codes erfinden darf. Wird per CLI ueberschrieben.
        category: 'interviewanalysis' | 'documentanalysis' | ... — abgeleitet
            aus dem Subdirectory unter METHODS_DIR. Nur informativ.
    """
    id: str
    name: str
    description: str
    model: str
    max_tokens: int
    categories: dict[str, str]
    prompt_template: str
    codebase_prompt: str = ""
    coding_strategy: str = "hybrid"
    category: str = ""

    def build_prompt(self, text: str, filename: str, codebase: str = "",
                     content: str = "", **extra) -> str:
        """Baut den Analyse-Prompt für ein Transkript oder Dokument.

        Args:
            text: Volltext (Legacy, für {text} im Template)
            filename: Dateiname
            codebase: Optionale Codebasis
            content: Block-basierter Inhalt (für {content} im Template)
            **extra: Zusätzliche Platzhalter (z.B. goal, company, project)
        """
        lines = []
        for k, v in self.categories.items():
            if isinstance(v, dict):
                name = v.get("name", k)
                desc = v.get("description", "")
                lines.append(f"  {k}: {name} — {desc}" if desc else f"  {k}: {name}")
            else:
                lines.append(f"  {k}: {v}")
        categories_text = "\n".join(lines)
        category_keys = ", ".join(self.categories.keys())

        has_codebase = bool(codebase)
        codebase_section_parts: list[str] = []
        if has_codebase and self.codebase_prompt:
            codebase_section_parts.append(
                self.codebase_prompt.format(codebase=codebase)
            )

        strategy_block = _strategy_instruction(
            self.coding_strategy, has_codebase
        )
        if strategy_block:
            codebase_section_parts.append(strategy_block)

        codebase_section = "\n\n".join(codebase_section_parts).strip()

        fmt = dict(
            categories=categories_text,
            category_keys=category_keys,
            codebase_section=codebase_section,
            filename=filename,
            text=text,
            content=content or text,
            **extra,
        )
        return self.prompt_template.format_map(
            _SafeFormatDict(fmt)
        )


def _iter_recipe_files() -> list[Path]:
    """Sucht alle *.yaml-Dateien rekursiv unter METHODS_DIR."""
    if not METHODS_DIR.exists():
        return []
    return sorted(METHODS_DIR.rglob("*.yaml"))


def _category_for(path: Path) -> str:
    """Gibt den Subdir-Namen unter METHODS_DIR zurueck (oder '' wenn flach)."""
    try:
        rel = path.relative_to(METHODS_DIR)
    except ValueError:
        return ""
    parts = rel.parts
    return parts[0] if len(parts) > 1 else ""


def list_recipes() -> list[dict]:
    """Listet alle verfügbaren Recipes mit id, name und category."""
    recipes = []
    for f in _iter_recipe_files():
        with open(f, encoding="utf-8") as fh:
            data = yaml.safe_load(fh)
        recipes.append({
            "id": data["id"],
            "name": data["name"],
            "description": data.get("description", ""),
            "category": _category_for(f),
            "path": f,
        })
    return recipes


def load_recipe(recipe_id: str) -> Recipe:
    """Lädt ein Recipe anhand seiner ID."""
    for f in _iter_recipe_files():
        with open(f, encoding="utf-8") as fh:
            data = yaml.safe_load(fh)
        if data["id"] == recipe_id:
            # .env ueberschreibt Recipe-Defaults
            model = ENV_CLAUDE_MODEL or data.get("model", "claude-sonnet-4-20250514")
            max_tokens = int(ENV_CLAUDE_MAX_TOKENS) if ENV_CLAUDE_MAX_TOKENS else data.get("max_tokens", 16384)
            coding_strategy = data.get("coding_strategy", "hybrid")
            if coding_strategy not in CODING_STRATEGIES:
                raise ValueError(
                    f"Recipe '{recipe_id}': coding_strategy='{coding_strategy}' "
                    f"ungueltig. Erlaubt: {CODING_STRATEGIES}"
                )
            return Recipe(
                id=data["id"],
                name=data["name"],
                description=data.get("description", ""),
                model=model,
                max_tokens=max_tokens,
                categories=data.get("categories", {}),
                prompt_template=data.get("prompt_template", ""),
                codebase_prompt=data.get("codebase_prompt", ""),
                coding_strategy=coding_strategy,
                category=_category_for(f),
            )
    available = [r["id"] for r in list_recipes()]
    raise FileNotFoundError(
        f"Recipe '{recipe_id}' nicht gefunden. Verfügbar: {available}"
    )


def load_codebase(name: str) -> str:
    """Lädt eine Codebasis aus input/codebases/{name}.txt oder .yaml."""
    for ext in [".txt", ".yaml", ".yml", ".csv"]:
        path = CODEBASES_DIR / f"{name}{ext}"
        if path.exists():
            return path.read_text(encoding="utf-8")
    # Versuche exakten Dateinamen
    path = CODEBASES_DIR / name
    if path.exists():
        return path.read_text(encoding="utf-8")
    available = [f.name for f in CODEBASES_DIR.iterdir() if f.is_file()]
    raise FileNotFoundError(
        f"Codebasis '{name}' nicht gefunden in {CODEBASES_DIR}. Vorhanden: {available}"
    )


def parse_codebase_yaml(name: str) -> dict[str, dict]:
    """Laedt eine Codebase aus input/codebases/ und liefert eine flache Dict-Struktur.

    Das Ergebnis ist ein Mapping ``code_id -> {name, description, category,
    subcategory, ankerbeispiel, abgrenzungsregel}``. Sowohl Hauptkategorien,
    Zwischen-Codes als auch Subcodes sind enthalten.

    Unterstuetzt wird die im Projekt uebliche YAML-Struktur::

        kategorien:
          - id: PROC
            name: Prozesse
            definition: ...
            codes:
              - id: PROC-ACQ
                name: ...
                definition / description / kodierdefinition: ...
                subcodes:
                  - id: PROC-ACQ-01
                    name: ...
                    ankerbeispiel: ...

    Alternativ wird auch die flache Form ``codes: {A-01: {name, ...}}`` oder
    ``categories: {...}`` unterstuetzt. Bei nicht-YAML-Dateien oder fehlenden
    Daten wird ein leeres Dict zurueckgegeben.
    """
    flat: dict[str, dict] = {}

    # Versuche zuerst, die YAML-Datei direkt zu lesen (robust ggue. .yml/.yaml)
    data = None
    for ext in [".yml", ".yaml"]:
        path = CODEBASES_DIR / f"{name}{ext}"
        if path.exists():
            try:
                with path.open("r", encoding="utf-8") as fh:
                    data = yaml.safe_load(fh)
            except yaml.YAMLError:
                return {}
            break
    if data is None:
        # Fallback: load_codebase kann noch anderen Content liefern (txt/csv).
        try:
            text = load_codebase(name)
        except FileNotFoundError:
            return {}
        try:
            data = yaml.safe_load(text)
        except yaml.YAMLError:
            return {}

    if not isinstance(data, dict):
        return {}

    def _desc(d: dict) -> str:
        for k in ("kodierdefinition", "definition", "description", "beschreibung"):
            v = d.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()
        return ""

    def _add(code_id, info):
        if not isinstance(code_id, str) or not code_id.strip():
            return
        flat[code_id.strip()] = info

    # --- Variante 1: kategorien: [ {id, codes: [ {id, subcodes: [...]}]} ] ---
    kats = data.get("kategorien")
    if isinstance(kats, list):
        for kat in kats:
            if not isinstance(kat, dict):
                continue
            kat_id = kat.get("id")
            kat_name = kat.get("name", kat_id or "")
            _add(kat_id, {
                "name": kat_name,
                "description": _desc(kat),
                "category": kat_id,
                "subcategory": "",
                "ankerbeispiel": kat.get("ankerbeispiel", ""),
                "abgrenzungsregel": kat.get("abgrenzungsregel", ""),
            })
            for code in kat.get("codes", []) or []:
                if not isinstance(code, dict):
                    continue
                code_id = code.get("id")
                _add(code_id, {
                    "name": code.get("name", code_id or ""),
                    "description": _desc(code),
                    "category": kat_id,
                    "subcategory": code_id,
                    "ankerbeispiel": code.get("ankerbeispiel", ""),
                    "abgrenzungsregel": code.get("abgrenzungsregel", ""),
                })
                for sub in code.get("subcodes", []) or []:
                    if not isinstance(sub, dict):
                        continue
                    sub_id = sub.get("id")
                    _add(sub_id, {
                        "name": sub.get("name", sub_id or ""),
                        "description": _desc(sub),
                        "category": kat_id,
                        "subcategory": code_id,
                        "ankerbeispiel": sub.get("ankerbeispiel", ""),
                        "abgrenzungsregel": sub.get("abgrenzungsregel", ""),
                    })
        if flat:
            return flat

    # --- Variante 2: flache codes: {id: {name, ...}} ---
    codes = data.get("codes")
    if isinstance(codes, dict):
        for code_id, info in codes.items():
            if not isinstance(info, dict):
                continue
            _add(code_id, {
                "name": info.get("name", code_id),
                "description": _desc(info),
                "category": info.get("hauptkategorie") or info.get("category", ""),
                "subcategory": info.get("subcategory", ""),
                "ankerbeispiel": info.get("ankerbeispiel", ""),
                "abgrenzungsregel": info.get("abgrenzungsregel", ""),
            })
    elif isinstance(codes, list):
        for info in codes:
            if not isinstance(info, dict):
                continue
            _add(info.get("id"), {
                "name": info.get("name", info.get("id", "")),
                "description": _desc(info),
                "category": info.get("hauptkategorie") or info.get("category", ""),
                "subcategory": info.get("subcategory", ""),
                "ankerbeispiel": info.get("ankerbeispiel", ""),
                "abgrenzungsregel": info.get("abgrenzungsregel", ""),
            })

    # Hauptkategorien aus `categories:` ergaenzen (nicht ueberschreiben)
    cats = data.get("categories")
    if isinstance(cats, dict):
        for cat_id, cat_info in cats.items():
            if cat_id in flat:
                continue
            if isinstance(cat_info, dict):
                _add(cat_id, {
                    "name": cat_info.get("name", cat_id),
                    "description": _desc(cat_info),
                    "category": cat_id,
                    "subcategory": "",
                    "ankerbeispiel": cat_info.get("ankerbeispiel", ""),
                    "abgrenzungsregel": cat_info.get("abgrenzungsregel", ""),
                })
            elif isinstance(cat_info, str):
                _add(cat_id, {
                    "name": cat_info,
                    "description": "",
                    "category": cat_id,
                    "subcategory": "",
                    "ankerbeispiel": "",
                    "abgrenzungsregel": "",
                })

    return flat


def list_codebases() -> list[str]:
    """Listet alle verfügbaren Codebasen."""
    if not CODEBASES_DIR.exists():
        return []
    return [f.stem for f in sorted(CODEBASES_DIR.iterdir()) if f.is_file()]
