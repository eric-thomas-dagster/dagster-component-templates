"""sensitivity op engine='presidio': Microsoft Presidio + spaCy NLP-based entity detection
(PERSON, LOCATION, ...) -- things the regex-only engine='regex' (the default) can't reliably
catch at all. Same construction/overlap-resolution as this repo's own log_redaction module
(duplicated, not imported -- no shared files across components). presidio-analyzer/spacy aren't
installed in this dev environment (matching log_redaction's own test suite, which has the same
gap) -- the ImportError path is exercised for real; the analyze()/overlap-resolution logic is
exercised with a fake AnalyzerEngine standing in for the real NLP model.
"""
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


class _FakeCtx:
    class log:
        @staticmethod
        def info(msg): ...
        warning = info


def _frame(mod, texts):
    import hashlib
    import pandas as pd
    return pd.DataFrame({"chunk_text": texts, "content_hash": [hashlib.sha256(t.encode()).hexdigest() for t in texts]})


def test_unknown_engine_raises(mod):
    df = _frame(mod, ["hello"])
    with pytest.raises(ValueError, match="engine must be"):
        mod._do_sensitivity(df, {"engine": "not_real"}, _FakeCtx())


def test_presidio_without_dependency_raises_clear_error(mod):
    try:
        import presidio_analyzer  # noqa: F401
        pytest.skip("presidio_analyzer is installed in this environment; nothing to assert here")
    except ImportError:
        pass
    df = _frame(mod, ["My name is Alice Smith."])
    with pytest.raises(ImportError, match="engine='presidio'"):
        mod._do_sensitivity(df, {"engine": "presidio"}, _FakeCtx())


def test_presidio_tags_and_redacts_with_mocked_analyzer(mod, monkeypatch):
    class _FakeResult:
        def __init__(self, entity_type, start, end, score):
            self.entity_type, self.start, self.end, self.score = entity_type, start, end, score

    class _FakeAnalyzer:
        def analyze(self, *, text, language, entities, score_threshold):
            if "Alice Smith" not in text:
                return []
            start = text.index("Alice Smith")
            return [_FakeResult("PERSON", start, start + len("Alice Smith"), 0.85)]

    monkeypatch.setattr(mod, "_get_presidio_analyzer", lambda: _FakeAnalyzer())

    df = _frame(mod, ["My name is Alice Smith.", "Nothing sensitive here."])
    tagged = mod._do_sensitivity(df, {"engine": "presidio"}, _FakeCtx())
    assert list(tagged["sensitivity"]) == ["pii", "none"]
    assert tagged.iloc[0]["pii_types"] == "PERSON"

    redacted = mod._do_sensitivity(df, {"engine": "presidio", "action": "redact"}, _FakeCtx())
    assert redacted.iloc[0]["chunk_text"] == "My name is [PERSON]."
    # redact rehashes content_hash (same convention as the regex engine's own redact path)
    assert redacted.iloc[0]["content_hash"] != df.iloc[0]["content_hash"]


def test_presidio_default_entities_include_person(mod, monkeypatch):
    captured = {}

    class _FakeAnalyzer:
        def analyze(self, *, text, language, entities, score_threshold):
            captured["entities"] = entities
            captured["score_threshold"] = score_threshold
            return []

    monkeypatch.setattr(mod, "_get_presidio_analyzer", lambda: _FakeAnalyzer())
    df = _frame(mod, ["text"])
    mod._do_sensitivity(df, {"engine": "presidio"}, _FakeCtx())
    assert "PERSON" in captured["entities"]
    assert captured["score_threshold"] == 0.3


def test_presidio_custom_entities_and_threshold(mod, monkeypatch):
    captured = {}

    class _FakeAnalyzer:
        def analyze(self, *, text, language, entities, score_threshold):
            captured["entities"] = entities
            captured["score_threshold"] = score_threshold
            return []

    monkeypatch.setattr(mod, "_get_presidio_analyzer", lambda: _FakeAnalyzer())
    df = _frame(mod, ["text"])
    mod._do_sensitivity(df, {"engine": "presidio", "detectors": ["LOCATION"], "presidio_score_threshold": 0.7}, _FakeCtx())
    assert captured["entities"] == ["LOCATION"]
    assert captured["score_threshold"] == 0.7
