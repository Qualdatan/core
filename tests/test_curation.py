"""Tests fuer die Codebook-Curation (Phase 4).

Die Tests vermeiden echte LLM-Calls: ``analyze_transcript`` wird per
monkeypatch durch eine Stub-Funktion ersetzt, die ein deterministisches
Ergebnis liefert. Wir pruefen:

- bootstrap_codebook ohne Seed -> rein induktives Codebook
- bootstrap_codebook mit Seed  -> Merge mit provided-Codes
- strict-Strategy filtert neue_codes (auf der step1-Pipeline-Ebene)
- draft_codebook.yml ist valide YAML im erwarteten Schema
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from qualdatan_core.curation import bootstrap as codebook_curation
from qualdatan_core.curation.bootstrap import bootstrap_codebook, CurationStats
from qualdatan_core.models import AnalysisResult, CodedSegment
from qualdatan_core.recipe import Recipe
from qualdatan_core.run_context import RunContext


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_recipe(coding_strategy: str = "hybrid",
                 categories: dict | None = None) -> Recipe:
    """Minimales Recipe fuer Tests — kein echter Template-Aufbau."""
    return Recipe(
        id="test_recipe",
        name="Test",
        description="",
        model="claude-sonnet-4-20250514",
        max_tokens=4096,
        categories=categories or {
            "A": "Projektakquise",
            "B": "Planungsprozess",
            "C": "Digitale Werkzeuge",
        },
        prompt_template="{categories} {codebase_section} {filename} {content}",
        codebase_prompt="Codebase: {codebase}",
        coding_strategy=coding_strategy,
        category="interviewanalysis",
    )


@pytest.fixture
def ctx(tmp_path) -> RunContext:
    run_dir = tmp_path / "run_curate"
    run_dir.mkdir()
    c = RunContext(run_dir)
    c.ensure_dirs()
    return c


def _make_analysis_result(codes_meta: dict,
                          segments: list[dict]) -> AnalysisResult:
    """Baut ein AnalysisResult aus einem kompakten Dict."""
    result = AnalysisResult()
    result.categories = {"A": "Akquise", "B": "Planung", "C": "Digitales"}
    result.codes = codes_meta
    result.segments = [
        CodedSegment(
            code_id=s["code_id"],
            code_name=s.get("code_name", ""),
            hauptkategorie=s.get("hauptkategorie", ""),
            text=s.get("text", ""),
            char_start=0,
            char_end=len(s.get("text", "")),
            document=s.get("document", "interview1.docx"),
            kodierdefinition=s.get("kodierdefinition", ""),
            ankerbeispiel=s.get("ankerbeispiel", ""),
            abgrenzungsregel=s.get("abgrenzungsregel", ""),
        )
        for s in segments
    ]
    return result


# ---------------------------------------------------------------------------
# bootstrap_codebook — ohne Seed (rein induktiv)
# ---------------------------------------------------------------------------


class TestBootstrapWithoutSeed:
    def test_induktiv_schreibt_yaml(self, ctx):
        recipe = _make_recipe()
        codes_meta = {
            "A-01": {
                "name": "Ausschreibung", "hauptkategorie": "A",
                "kodierdefinition": "Oeffentliche Vergabe",
                "ankerbeispiel": "VgV-Verfahren",
                "abgrenzungsregel": "", "count": 0,
            },
            "B-01": {
                "name": "Entwurfsplanung", "hauptkategorie": "B",
                "kodierdefinition": "Definition",
                "ankerbeispiel": "", "abgrenzungsregel": "",
                "count": 0,
            },
        }
        segments = [
            {"code_id": "A-01", "code_name": "Ausschreibung",
             "hauptkategorie": "A", "text": "VgV-Verfahren X"},
            {"code_id": "A-01", "code_name": "Ausschreibung",
             "hauptkategorie": "A", "text": "Kurzes Beispiel"},
            {"code_id": "B-01", "code_name": "Entwurfsplanung",
             "hauptkategorie": "B", "text": "Langes Beispiel text text"},
        ]
        result = _make_analysis_result(codes_meta, segments)

        path, stats = bootstrap_codebook(
            ctx=ctx, recipe=recipe,
            sample_files=[Path("interview1.docx")],
            codebase_seed=None, analysis_result=result,
        )

        assert path.exists()
        assert path.name == "draft_codebook.yml"
        assert isinstance(stats, CurationStats)
        assert stats.total_codes == 2
        assert stats.inductive_codes == 2
        assert stats.provided_codes == 0
        assert stats.unused_provided == 0

        # Inhalt parsen
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
        assert "kategorien" in data
        code_ids = {
            c["id"]
            for kat in data["kategorien"]
            for c in kat.get("codes", [])
        }
        assert {"A-01", "B-01"} == code_ids

        # Metadaten pruefen
        for kat in data["kategorien"]:
            for code in kat["codes"]:
                meta = code.get("_meta", {})
                assert meta.get("source") == "inductive"
                assert meta.get("frequency") >= 1
                assert "first_seen" in meta

    def test_single_use_warnung(self, ctx):
        recipe = _make_recipe()
        result = _make_analysis_result(
            codes_meta={
                "A-01": {"name": "Ausschreibung", "hauptkategorie": "A",
                         "kodierdefinition": "d", "ankerbeispiel": "",
                         "abgrenzungsregel": "", "count": 0},
            },
            segments=[
                {"code_id": "A-01", "code_name": "Ausschreibung",
                 "hauptkategorie": "A", "text": "einmalig"},
            ],
        )
        _, stats = bootstrap_codebook(
            ctx=ctx, recipe=recipe,
            sample_files=[Path("x.docx")],
            analysis_result=result,
        )
        assert stats.single_use == 1

    def test_shortest_example_wins(self, ctx):
        recipe = _make_recipe()
        result = _make_analysis_result(
            codes_meta={
                "A-01": {"name": "Ausschreibung", "hauptkategorie": "A",
                         "kodierdefinition": "", "ankerbeispiel": "",
                         "abgrenzungsregel": "", "count": 0},
            },
            segments=[
                {"code_id": "A-01", "code_name": "A",
                 "hauptkategorie": "A", "text": "ein ziemlich langer Text"},
                {"code_id": "A-01", "code_name": "A",
                 "hauptkategorie": "A", "text": "kurz"},
            ],
        )
        path, _ = bootstrap_codebook(
            ctx=ctx, recipe=recipe,
            sample_files=[Path("x.docx")],
            analysis_result=result,
        )
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
        code = data["kategorien"][0]["codes"][0]
        assert code["ankerbeispiel"] == "kurz"


# ---------------------------------------------------------------------------
# bootstrap_codebook — mit Seed (Merge)
# ---------------------------------------------------------------------------


class TestBootstrapWithSeed:
    def _write_seed(self, tmp_path: Path,
                    monkeypatch: pytest.MonkeyPatch) -> str:
        """Schreibt eine Seed-YAML und lenkt CODEBASES_DIR auf tmp_path um."""
        seed = {
            "kategorien": [
                {
                    "id": "A",
                    "name": "Projektakquise",
                    "codes": [
                        {
                            "id": "A-01",
                            "name": "Oeffentliche Ausschreibung",
                            "definition": "VgV/VOB",
                            "ankerbeispiel": "VgV-Verfahren",
                        },
                        {
                            "id": "A-99",
                            "name": "Ungenutzter Seed",
                            "definition": "nichts",
                            "ankerbeispiel": "",
                        },
                    ],
                },
                {
                    "id": "D",
                    "name": "Kommunikation",
                    "codes": [
                        {
                            "id": "D-01",
                            "name": "Meetings",
                            "definition": "",
                            "ankerbeispiel": "",
                        },
                    ],
                },
            ],
        }
        seed_path = tmp_path / "test_seed.yml"
        seed_path.write_text(
            yaml.safe_dump(seed, allow_unicode=True),
            encoding="utf-8",
        )
        monkeypatch.setattr(codebook_curation, "CODEBASES_DIR", tmp_path)
        return "test_seed"

    def test_merge_seed_und_induktiv(self, ctx, tmp_path, monkeypatch):
        seed_name = self._write_seed(tmp_path, monkeypatch)
        recipe = _make_recipe()

        result = _make_analysis_result(
            codes_meta={
                "A-01": {"name": "Ausschreibung", "hauptkategorie": "A",
                         "kodierdefinition": "", "ankerbeispiel": "",
                         "abgrenzungsregel": "", "count": 0},
                "C-07": {"name": "Neuer Code", "hauptkategorie": "C",
                         "kodierdefinition": "neu", "ankerbeispiel": "",
                         "abgrenzungsregel": "", "count": 0},
            },
            segments=[
                {"code_id": "A-01", "code_name": "Ausschreibung",
                 "hauptkategorie": "A", "text": "Beispiel A"},
                {"code_id": "C-07", "code_name": "Neuer Code",
                 "hauptkategorie": "C", "text": "Beispiel C"},
            ],
        )
        path, stats = bootstrap_codebook(
            ctx=ctx, recipe=recipe,
            sample_files=[Path("a.docx")],
            codebase_seed=seed_name,
            analysis_result=result,
        )

        data = yaml.safe_load(path.read_text(encoding="utf-8"))
        code_by_id = {
            c["id"]: c
            for kat in data["kategorien"]
            for c in kat.get("codes", [])
        }

        # Seed-Codes sind da
        assert "A-01" in code_by_id
        assert "A-99" in code_by_id
        assert "D-01" in code_by_id
        # Induktiver Code ist auch da
        assert "C-07" in code_by_id

        # Quellen-Meta stimmt
        assert code_by_id["A-01"]["_meta"]["source"] == "provided"
        assert code_by_id["A-99"]["_meta"]["source"] == "provided"
        assert code_by_id["C-07"]["_meta"]["source"] == "inductive"

        # A-01 wurde einmal kodiert, A-99 gar nicht
        assert code_by_id["A-01"]["_meta"]["frequency"] == 1
        assert code_by_id["A-99"]["_meta"]["frequency"] == 0

        # Stats
        assert stats.provided_codes == 3  # A-01, A-99, D-01
        assert stats.inductive_codes == 1  # C-07
        assert stats.unused_provided == 2  # A-99, D-01

        # Kategorie-Name aus dem Seed wurde uebernommen
        kat_names = {kat["id"]: kat["name"] for kat in data["kategorien"]}
        assert kat_names.get("A") == "Projektakquise"
        assert kat_names.get("D") == "Kommunikation"

    def test_seed_not_found_raises(self, ctx, tmp_path, monkeypatch):
        monkeypatch.setattr(codebook_curation, "CODEBASES_DIR", tmp_path)
        recipe = _make_recipe()
        result = _make_analysis_result({}, [])
        with pytest.raises(FileNotFoundError):
            bootstrap_codebook(
                ctx=ctx, recipe=recipe,
                sample_files=[Path("x.docx")],
                codebase_seed="doesnt_exist",
                analysis_result=result,
            )


# ---------------------------------------------------------------------------
# Schema + YAML-Validitaet
# ---------------------------------------------------------------------------


class TestDraftYamlSchema:
    def test_empty_result_produces_valid_yaml_with_catchall(self, ctx):
        recipe = _make_recipe()
        result = _make_analysis_result({}, [])
        path, stats = bootstrap_codebook(
            ctx=ctx, recipe=recipe,
            sample_files=[Path("nope.docx")],
            analysis_result=result,
        )
        assert path.exists()
        assert stats.total_codes == 0

        data = yaml.safe_load(path.read_text(encoding="utf-8"))
        assert isinstance(data, dict)
        assert "kategorien" in data
        assert isinstance(data["kategorien"], list)

    def test_yaml_header_comment_present(self, ctx):
        recipe = _make_recipe()
        result = _make_analysis_result({}, [])
        path, _ = bootstrap_codebook(
            ctx=ctx, recipe=recipe,
            sample_files=[Path("x.docx")],
            analysis_result=result,
        )
        first_line = path.read_text(encoding="utf-8").splitlines()[0]
        assert first_line.startswith("#")


# ---------------------------------------------------------------------------
# strict-Strategy: neue_codes werden gefiltert
# ---------------------------------------------------------------------------


class TestStrictStrategyFilter:
    def test_enforce_strict_drops_neue_codes(self):
        from src.step1_analyze import enforce_strict_strategy

        recipe = _make_recipe(coding_strategy="strict")
        data = {
            "codings": [
                {"block_id": "p1_b0", "code_id": "A-01", "code_name": "X",
                 "hauptkategorie": "A"},
            ],
            "neue_codes": [
                {"code_id": "Z-99", "code_name": "Neuer Spam",
                 "hauptkategorie": "Z", "kodierdefinition": "..."},
            ],
        }
        out = enforce_strict_strategy(data, recipe, filename="x.docx")
        assert out["neue_codes"] == []
        # codings bleiben unveraendert
        assert out["codings"][0]["code_id"] == "A-01"

    def test_hybrid_keeps_neue_codes(self):
        from src.step1_analyze import enforce_strict_strategy

        recipe = _make_recipe(coding_strategy="hybrid")
        data = {
            "codings": [],
            "neue_codes": [
                {"code_id": "Z-99", "code_name": "X",
                 "hauptkategorie": "Z", "kodierdefinition": ""},
            ],
        }
        out = enforce_strict_strategy(data, recipe, filename="x.docx")
        assert len(out["neue_codes"]) == 1

    def test_enforce_strict_handles_missing_key(self):
        from src.step1_analyze import enforce_strict_strategy

        recipe = _make_recipe(coding_strategy="strict")
        data = {"codings": []}
        out = enforce_strict_strategy(data, recipe, filename="x.docx")
        assert out is data
        assert out.get("neue_codes") in (None, [])

    def test_enforce_non_dict_noop(self):
        from src.step1_analyze import enforce_strict_strategy

        recipe = _make_recipe(coding_strategy="strict")
        assert enforce_strict_strategy([], recipe) == []
        assert enforce_strict_strategy(None, recipe) is None


# ---------------------------------------------------------------------------
# DB-Ingest (Dokumenten-Flow)
# ---------------------------------------------------------------------------


class TestDbIngest:
    def test_reads_codings_and_neue_codes_from_db(self, ctx):
        # PDF + Coding registrieren
        pdf_id = ctx.db.upsert_pdf(
            project="sample",
            filename="doc.pdf",
            relative_path="sample/doc.pdf",
            path="/tmp/doc.pdf",
        )
        ctx.db.save_coding(
            pdf_id=pdf_id, page=1, block_id="p1_b0",
            codes=["A-01"], source="text",
            begruendung="weil es wichtig ist",
        )
        ctx.db.save_coding(
            pdf_id=pdf_id, page=1, block_id="p1_b1",
            codes=["A-01", "B-02"], source="text",
            begruendung="zweite Begruendung",
        )
        ctx.db.save_neue_codes([
            {"code_id": "C-99", "code_name": "Neuer Code",
             "hauptkategorie": "C", "kodierdefinition": "dummy"},
        ], pdf_id)

        recipe = _make_recipe()
        path, stats = bootstrap_codebook(
            ctx=ctx, recipe=recipe,
            sample_files=[Path("doc.pdf")],
            analysis_result=None,  # DB-only
        )
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
        code_ids = {
            c["id"]
            for kat in data["kategorien"]
            for c in kat.get("codes", [])
        }
        # A-01 erscheint zweimal (in zwei codings), B-02 einmal, C-99 aus neue_codes
        assert {"A-01", "B-02", "C-99"}.issubset(code_ids)

        # Frequenzen
        by_id = {
            c["id"]: c
            for kat in data["kategorien"]
            for c in kat.get("codes", [])
        }
        assert by_id["A-01"]["_meta"]["frequency"] == 2
        assert by_id["B-02"]["_meta"]["frequency"] == 1
