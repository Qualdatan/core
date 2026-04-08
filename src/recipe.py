"""Recipe-Loader: Liest Analyse-Methoden aus YAML-Dateien."""

from pathlib import Path
from dataclasses import dataclass, field
import yaml

from .config import RECIPES_DIR, CODEBASES_DIR, ENV_CLAUDE_MODEL, ENV_CLAUDE_MAX_TOKENS


@dataclass
class Recipe:
    """Eine Analyse-Methode (z.B. Mayring, PRISMA)."""
    id: str
    name: str
    description: str
    model: str
    max_tokens: int
    categories: dict[str, str]
    prompt_template: str
    codebase_prompt: str = ""

    def build_prompt(self, text: str, filename: str, codebase: str = "",
                     content: str = "") -> str:
        """Baut den Analyse-Prompt für ein Transkript.

        Args:
            text: Volltext (Legacy, für {text} im Template)
            filename: Dateiname
            codebase: Optionale Codebasis
            content: Block-basierter Inhalt (für {content} im Template)
        """
        categories_text = "\n".join(
            f"  {k}: {v}" for k, v in self.categories.items()
        )
        category_keys = ", ".join(self.categories.keys())

        codebase_section = ""
        if codebase and self.codebase_prompt:
            codebase_section = self.codebase_prompt.format(codebase=codebase)

        return self.prompt_template.format(
            categories=categories_text,
            category_keys=category_keys,
            codebase_section=codebase_section,
            filename=filename,
            text=text,
            content=content or text,
        )


def list_recipes() -> list[dict]:
    """Listet alle verfügbaren Recipes mit id und name."""
    recipes = []
    for f in sorted(RECIPES_DIR.glob("*.yaml")):
        with open(f, encoding="utf-8") as fh:
            data = yaml.safe_load(fh)
        recipes.append({
            "id": data["id"],
            "name": data["name"],
            "description": data.get("description", ""),
            "path": f,
        })
    return recipes


def load_recipe(recipe_id: str) -> Recipe:
    """Lädt ein Recipe anhand seiner ID."""
    for f in RECIPES_DIR.glob("*.yaml"):
        with open(f, encoding="utf-8") as fh:
            data = yaml.safe_load(fh)
        if data["id"] == recipe_id:
            # .env ueberschreibt Recipe-Defaults
            model = ENV_CLAUDE_MODEL or data.get("model", "claude-sonnet-4-20250514")
            max_tokens = int(ENV_CLAUDE_MAX_TOKENS) if ENV_CLAUDE_MAX_TOKENS else data.get("max_tokens", 16384)
            return Recipe(
                id=data["id"],
                name=data["name"],
                description=data.get("description", ""),
                model=model,
                max_tokens=max_tokens,
                categories=data["categories"],
                prompt_template=data["prompt_template"],
                codebase_prompt=data.get("codebase_prompt", ""),
            )
    available = [r["id"] for r in list_recipes()]
    raise FileNotFoundError(
        f"Recipe '{recipe_id}' nicht gefunden. Verfügbar: {available}"
    )


def load_codebase(name: str) -> str:
    """Lädt eine Codebasis aus input/codebases/{name}.txt oder .yaml."""
    for ext in [".txt", ".yaml", ".csv"]:
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


def list_codebases() -> list[str]:
    """Listet alle verfügbaren Codebasen."""
    if not CODEBASES_DIR.exists():
        return []
    return [f.stem for f in sorted(CODEBASES_DIR.iterdir()) if f.is_file()]
