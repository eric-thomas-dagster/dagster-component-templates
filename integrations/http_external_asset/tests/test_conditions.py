"""Unit tests for the HttpExternalAssetComponent condition language.

Covers extractors (jsonpath / regex / header / literal), operators
(equals / in / matches / exists / truthy / gt / lt), boolean composition
(any_of / all_of / not), and source selection (status / trigger / logs / headers).

Run from this directory:

    pytest -q
"""
from __future__ import annotations

import pytest

# When running outside of an installed Dagster project, import the component file
# directly so we don't depend on an installed dagster_component_templates package.
import importlib.util
import pathlib

_HERE = pathlib.Path(__file__).resolve().parent.parent
_COMPONENT_PY = _HERE / "component.py"
_spec = importlib.util.spec_from_file_location("http_external_asset_component", _COMPONENT_PY)
assert _spec is not None and _spec.loader is not None
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

_ConditionExtractor = _mod._ConditionExtractor
_SourceData = _mod._SourceData
evaluate_condition = _mod.evaluate_condition
extract_value = _mod.extract_value


# ── Extractor tests ────────────────────────────────────────────────────────

def test_jsonpath_scalar():
    cond = _ConditionExtractor(jsonpath="$.status")
    sources = _SourceData(status_response={"status": "succeeded", "id": 7})
    assert extract_value(cond, sources) == "succeeded"


def test_jsonpath_missing_returns_none():
    cond = _ConditionExtractor(jsonpath="$.nope")
    sources = _SourceData(status_response={"status": "succeeded"})
    assert extract_value(cond, sources) is None


def test_regex_default_full_match():
    cond = _ConditionExtractor(source="logs", regex=r"ERROR: \w+")
    sources = _SourceData(logs="DEBUG: ok\nERROR: bang")
    assert extract_value(cond, sources) == "ERROR: bang"


def test_regex_with_capture_group():
    cond = _ConditionExtractor(source="logs", regex=r"run-id: (\S+)", group=1)
    sources = _SourceData(logs="run-id: r-abc-123 done")
    assert extract_value(cond, sources) == "r-abc-123"


def test_header_lookup_case_insensitive():
    cond = _ConditionExtractor(source="headers", header="X-Next-Cursor")
    sources = _SourceData(status_headers={"x-next-cursor": "tok-42", "Content-Type": "x"})
    assert extract_value(cond, sources) == "tok-42"


def test_literal_returns_constant():
    cond = _ConditionExtractor(literal=42)
    sources = _SourceData()
    assert extract_value(cond, sources) == 42


# ── Operator tests ─────────────────────────────────────────────────────────

def test_equals_true_and_false():
    sources = _SourceData(status_response={"status": "succeeded"})
    assert evaluate_condition(_ConditionExtractor(jsonpath="$.status", equals="succeeded"), sources)
    assert not evaluate_condition(_ConditionExtractor(jsonpath="$.status", equals="failed"), sources)


def test_in_operator():
    cond = _ConditionExtractor(jsonpath="$.status", **{"in": ["succeeded", "failed", "errored"]})
    sources = _SourceData(status_response={"status": "errored"})
    assert evaluate_condition(cond, sources)


def test_matches_regex():
    cond = _ConditionExtractor(jsonpath="$.id", matches=r"^run-\d+$")
    assert evaluate_condition(cond, _SourceData(status_response={"id": "run-7"}))
    assert not evaluate_condition(cond, _SourceData(status_response={"id": "abc"}))


def test_exists_true_and_false():
    sources = _SourceData(status_response={"completed_at": "2026-01-01"})
    assert evaluate_condition(_ConditionExtractor(jsonpath="$.completed_at", exists=True), sources)
    assert not evaluate_condition(_ConditionExtractor(jsonpath="$.missing", exists=True), sources)


def test_gt_lt_with_numeric_coercion():
    sources = _SourceData(status_response={"rows": "1234"})
    assert evaluate_condition(_ConditionExtractor(jsonpath="$.rows", gt=1000), sources)
    assert not evaluate_condition(_ConditionExtractor(jsonpath="$.rows", lt=1000), sources)


def test_truthy_operator():
    sources = _SourceData(status_response={"empty_list": [], "filled": [1]})
    assert not evaluate_condition(_ConditionExtractor(jsonpath="$.empty_list", truthy=True), sources)
    assert evaluate_condition(_ConditionExtractor(jsonpath="$.filled", truthy=True), sources)


# ── Boolean composition ────────────────────────────────────────────────────

def test_any_of_short_circuits():
    sources = _SourceData(status_response={"status": "errored"})
    cond = _ConditionExtractor(any_of=[
        _ConditionExtractor(jsonpath="$.status", equals="succeeded"),
        _ConditionExtractor(jsonpath="$.status", equals="errored"),
    ])
    assert evaluate_condition(cond, sources)


def test_all_of():
    sources = _SourceData(status_response={"status": "succeeded", "rows": 100})
    cond = _ConditionExtractor(all_of=[
        _ConditionExtractor(jsonpath="$.status", equals="succeeded"),
        _ConditionExtractor(jsonpath="$.rows", gt=0),
    ])
    assert evaluate_condition(cond, sources)


def test_not_inverts():
    sources = _SourceData(status_response={"status": "running"})
    cond = _ConditionExtractor(**{"not": _ConditionExtractor(jsonpath="$.status", equals="running")})
    assert not evaluate_condition(cond, sources)


# ── Source selection ──────────────────────────────────────────────────────

def test_source_selection_pulls_from_named_source():
    sources = _SourceData(
        status_response={"x": "from_status"},
        trigger_response={"x": "from_trigger"},
        logs="ERROR x",
    )
    assert extract_value(_ConditionExtractor(source="status_response", jsonpath="$.x"), sources) == "from_status"
    assert extract_value(_ConditionExtractor(source="trigger_response", jsonpath="$.x"), sources) == "from_trigger"
    assert extract_value(_ConditionExtractor(source="logs", regex=r"ERROR \w"), sources) == "ERROR x"


# ── Validation ─────────────────────────────────────────────────────────────

def test_combining_leaf_and_boolean_rejected():
    with pytest.raises(Exception):
        _ConditionExtractor(jsonpath="$.x", any_of=[_ConditionExtractor(jsonpath="$.y")])


def test_no_extractor_rejected():
    with pytest.raises(Exception):
        _ConditionExtractor(equals="x")


def test_two_leaf_extractors_rejected():
    with pytest.raises(Exception):
        _ConditionExtractor(jsonpath="$.x", regex=r"foo")
