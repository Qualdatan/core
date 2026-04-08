"""Tests fuer step1_analyze (JSON-Parsing, Position-Validierung)."""

import json
import pytest
from src.step1_analyze import extract_json, validate_positions
from src.run_context import RunContext


class TestExtractJson:
    def test_clean_json(self):
        text = '{"segments": [], "kernergebnisse": []}'
        result = extract_json(text)
        assert result["segments"] == []

    def test_json_in_markdown_block(self):
        text = '```json\n{"segments": [{"code_id": "A-01"}]}\n```'
        result = extract_json(text)
        assert result["segments"][0]["code_id"] == "A-01"

    def test_json_with_preamble(self):
        text = 'Hier ist die Analyse:\n\n{"segments": [], "kernergebnisse": []}'
        result = extract_json(text)
        assert "segments" in result

    def test_truncated_json_repaired(self):
        text = '{"segments": [{"code_id": "A-01", "text": "test"}, {"code_id": "B-01", "text": "halb'
        result = extract_json(text)
        assert len(result["segments"]) >= 1
        assert result["segments"][0]["code_id"] == "A-01"

    def test_no_json_raises(self):
        with pytest.raises(ValueError, match="Kein JSON"):
            extract_json("Keine JSON-Daten hier.")


class TestValidatePositions:
    def test_exact_match(self):
        full_text = "Dies ist ein Testtext mit Inhalt."
        segments = [{"text": "Testtext mit Inhalt", "char_start": 0, "char_end": 10}]
        result = validate_positions(segments, full_text)
        assert result[0]["char_start"] == 13
        assert result[0]["char_end"] == 13 + len("Testtext mit Inhalt")

    def test_partial_match_via_prefix(self):
        full_text = "Einleitung. Hier beginnt der relevante Abschnitt mit viel Text und Kontext. Ende."
        segments = [{
            "text": "Hier beginnt der relevante Abschnitt mit viel Text und Kontext. Ende. Plus extra.",
            "char_start": 0, "char_end": 50,
        }]
        result = validate_positions(segments, full_text)
        assert result[0]["char_start"] == 12

    def test_no_match_keeps_original(self):
        full_text = "Komplett anderer Text."
        segments = [{"text": "Nicht vorhanden xyz abc", "char_start": 42, "char_end": 99}]
        result = validate_positions(segments, full_text)
        assert result[0]["char_start"] == 42


class TestRunContextCache:
    def test_cache_roundtrip(self, tmp_path):
        ctx = RunContext(tmp_path)
        ctx.ensure_dirs()
        data = {"segments": [{"code_id": "A-01"}], "kernergebnisse": []}

        ctx.cache_parsed("test.docx", data)
        loaded = ctx.get_cached_parsed("test.docx")

        assert loaded is not None
        assert loaded["segments"][0]["code_id"] == "A-01"

    def test_cache_miss(self, tmp_path):
        ctx = RunContext(tmp_path)
        ctx.ensure_dirs()
        assert ctx.get_cached_parsed("nonexistent.docx") is None

    def test_prompt_and_response_cached(self, tmp_path):
        ctx = RunContext(tmp_path)
        ctx.ensure_dirs()

        ctx.cache_prompt("test.docx", "Der Prompt")
        ctx.cache_response("test.docx", "Die Antwort")

        assert (ctx.prompts_dir / "test.docx.txt").exists()
        assert (ctx.responses_dir / "test.docx.txt").exists()

    def test_state_tracking(self, tmp_path):
        ctx = RunContext(tmp_path)
        ctx.ensure_dirs()
        ctx.init_state("mayring", None, ["a.docx", "b.docx"])

        assert ctx.get_pending_transcripts() == ["a.docx", "b.docx"]

        ctx.mark_transcript_done("a.docx")
        assert ctx.get_pending_transcripts() == ["b.docx"]

        ctx.mark_step_done(1)
        assert ctx.is_step_done(1)
        assert not ctx.is_step_done(2)

    def test_interrupted_detection(self, tmp_path, monkeypatch):
        from src import run_context
        monkeypatch.setattr(run_context, "OUTPUT_ROOT", tmp_path)

        # Erstelle einen "unterbrochenen" run
        run_dir = tmp_path / "2026-01-01_12-00-00"
        ctx = RunContext(run_dir)
        ctx.ensure_dirs()
        ctx.init_state("mayring", None, ["test.docx"])

        from src.run_context import find_interrupted_runs
        interrupted = find_interrupted_runs()
        assert len(interrupted) == 1
        assert interrupted[0].run_dir == run_dir

        # Nach complete: kein Interrupted mehr
        ctx.mark_completed()
        interrupted = find_interrupted_runs()
        assert len(interrupted) == 0
