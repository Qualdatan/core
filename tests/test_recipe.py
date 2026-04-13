"""Tests für das Recipe-System."""

import dataclasses

import pytest
from qualdatan_core.recipe import (
    list_recipes, load_recipe, _strategy_instruction, CODING_STRATEGIES,
)


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


# ---------------------------------------------------------------------------
# Coding-Strategie: strict / hybrid / inductive (Phase 4)
# ---------------------------------------------------------------------------


class TestStrategyInstruction:
    """_strategy_instruction liefert pro Strategy einen klaren Block."""

    def test_strict_with_codebase(self):
        text = _strategy_instruction("strict", has_codebase=True)
        assert "STRICT" in text
        assert "AUSSCHLIESSLICH" in text
        assert "neue_codes" in text.lower() or "neue code" in text.lower()

    def test_strict_without_codebase(self):
        text = _strategy_instruction("strict", has_codebase=False)
        assert "STRICT" in text
        assert "KEINE" in text

    def test_hybrid_with_codebase(self):
        text = _strategy_instruction("hybrid", has_codebase=True)
        assert "HYBRID" in text
        assert "primaer" in text.lower()

    def test_hybrid_without_codebase(self):
        text = _strategy_instruction("hybrid", has_codebase=False)
        assert "HYBRID" in text

    def test_inductive(self):
        text = _strategy_instruction("inductive", has_codebase=False)
        assert "INDUCTIVE" in text
        assert "induktiv" in text.lower()

    def test_unknown_strategy_returns_empty(self):
        assert _strategy_instruction("nonsense", has_codebase=True) == ""


class TestRecipeBuildPromptStrategy:
    """build_prompt injiziert den Strategy-Block je nach coding_strategy."""

    def test_default_hybrid_without_codebase_has_strategy_block(self):
        recipe = load_recipe("mayring")
        assert recipe.coding_strategy == "hybrid"
        prompt = recipe.build_prompt("Text", "test.docx")
        assert "HYBRID" in prompt

    def test_strict_with_codebase_forbids_new_codes(self):
        recipe = load_recipe("mayring")
        strict = dataclasses.replace(recipe, coding_strategy="strict")
        prompt = strict.build_prompt(
            "Text", "t.docx", codebase="A-01: Testcode"
        )
        assert "STRICT" in prompt
        assert "AUSSCHLIESSLICH" in prompt
        # Das Original-Codebase-Template-Snippet bleibt erhalten
        assert "A-01" in prompt

    def test_strict_without_codebase_still_has_block(self):
        recipe = load_recipe("mayring")
        strict = dataclasses.replace(recipe, coding_strategy="strict")
        prompt = strict.build_prompt("Text", "t.docx")
        assert "STRICT" in prompt
        assert "KEINE" in prompt

    def test_inductive_emits_inductive_hint(self):
        recipe = load_recipe("mayring")
        inductive = dataclasses.replace(recipe, coding_strategy="inductive")
        prompt = inductive.build_prompt("Text", "t.docx")
        assert "INDUCTIVE" in prompt

    def test_all_strategies_are_valid_choices(self):
        for strat in CODING_STRATEGIES:
            assert _strategy_instruction(strat, has_codebase=False)
