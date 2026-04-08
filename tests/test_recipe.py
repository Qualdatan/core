"""Tests für das Recipe-System."""

import pytest
from src.recipe import list_recipes, load_recipe


def test_list_recipes_returns_entries():
    recipes = list_recipes()
    assert len(recipes) >= 2
    ids = [r["id"] for r in recipes]
    assert "mayring" in ids
    assert "prisma" in ids


def test_load_mayring_recipe():
    recipe = load_recipe("mayring")
    assert recipe.id == "mayring"
    assert recipe.name
    assert len(recipe.categories) == 11
    assert "A" in recipe.categories
    assert "K" in recipe.categories
    assert recipe.prompt_template
    assert recipe.max_tokens > 0


def test_load_prisma_recipe():
    recipe = load_recipe("prisma")
    assert recipe.id == "prisma"
    assert len(recipe.categories) >= 5
    assert recipe.prompt_template


def test_load_unknown_recipe_raises():
    with pytest.raises(FileNotFoundError, match="nicht gefunden"):
        load_recipe("nonexistent_method")


def test_recipe_build_prompt():
    recipe = load_recipe("mayring")
    prompt = recipe.build_prompt("Hier ist ein Testtext.", "test.docx")
    assert "Testtext" in prompt
    assert "test.docx" in prompt
    assert "A:" in prompt  # Kategorie A sollte drin sein


def test_recipe_build_prompt_with_codebase():
    recipe = load_recipe("mayring")
    prompt = recipe.build_prompt(
        "Testtext", "test.docx", codebase="C-01: BIM-Nutzung\nC-02: CAD-Tools"
    )
    assert "BIM-Nutzung" in prompt
    assert "vorrangig verwendet" in prompt.lower() or "Codebasis" in prompt
