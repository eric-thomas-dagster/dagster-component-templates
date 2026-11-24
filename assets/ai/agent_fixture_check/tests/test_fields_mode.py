"""Real, committed tests for AgentFixtureCheckComponent's `match_mode: "fields"` -- field-level
fuzzy grading of structured extraction output (e.g. a document-extraction agent's outcome), added
alongside (not replacing) the original free-text whole-word-substring matcher. Exercises the real
Dagster asset_check binding end to end plus the pure agent_fixture_grade_fields function directly
for the edge cases.
"""
import json

import dagster as dg
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


_FIELD_FIXTURES = {
    "fixtures": [
        {"id": "vendor_name", "field": "vendor_name", "expected": "Acme Corporation"},
        {"id": "invoice_total", "field": "invoice_total", "expected": 1042.50},
        {"id": "invoice_date", "field": "invoice_date", "expected": "2026-01-15"},
    ]
}


def _write_fixtures(tmp_path, fixtures=None):
    p = tmp_path / "fixtures.json"
    p.write_text(json.dumps(fixtures if fixtures is not None else _FIELD_FIXTURES))
    return p


def _component(mod, fixtures_path, **overrides):
    kwargs = dict(asset_key="my_outcome", fixtures_file=str(fixtures_path), match_mode="fields")
    kwargs.update(overrides)
    return mod.AgentFixtureCheckComponent(**kwargs)


def _materialize_with_outcome(component, outcome: dict):
    @dg.asset(key=dg.AssetKey.from_user_string(component.asset_key))
    def _outcome():
        return outcome

    check_def = next(iter(component.build_defs(context=None).asset_checks))
    return dg.materialize([_outcome, check_def], raise_on_error=True)


# ── end-to-end via the real asset_check binding ──────────────────────────────────────────────────


def test_string_field_within_fuzzy_tolerance_matches(mod, tmp_path):
    """A typo'd/minor-variation string ('Acme Corp' vs 'Acme Corporation') matches at the default
    fuzzy_field_threshold (0.7), but a genuinely different value does not."""
    fixtures_path = _write_fixtures(tmp_path, {
        "fixtures": [{"id": "vendor_name", "field": "vendor_name", "expected": "Acme Corporation"}]
    })
    component = _component(mod, fixtures_path, min_recall=0.5)
    assert component.fuzzy_field_threshold == 0.7

    close = _materialize_with_outcome(component, {"fields": {"vendor_name": "Acme Corp"}})
    [eval_close] = close.get_asset_check_evaluations()
    assert eval_close.metadata["matched_fixture"].value == ["vendor_name"]
    assert eval_close.passed is True

    far = _materialize_with_outcome(component, {"fields": {"vendor_name": "Totally Different Inc"}})
    [eval_far] = far.get_asset_check_evaluations()
    assert eval_far.metadata["matched_fixture"].value == []
    assert eval_far.passed is False


def test_numeric_field_within_tolerance_matches(mod, tmp_path):
    """1042.50 vs an expected 1042.51 matches within a small relative tolerance; a clearly
    different number does not."""
    fixtures_path = _write_fixtures(tmp_path, {
        "fixtures": [{"id": "invoice_total", "field": "invoice_total", "expected": 1042.50}]
    })
    component = _component(mod, fixtures_path, min_recall=0.5, numeric_tolerance=0.001)

    close = _materialize_with_outcome(component, {"fields": {"invoice_total": 1042.51}})
    [eval_close] = close.get_asset_check_evaluations()
    assert eval_close.metadata["matched_fixture"].value == ["invoice_total"]

    far = _materialize_with_outcome(component, {"fields": {"invoice_total": 950.00}})
    [eval_far] = far.get_asset_check_evaluations()
    assert eval_far.metadata["matched_fixture"].value == []


def test_per_field_recall_and_precision_over_a_small_fixture_set(mod, tmp_path):
    """3 fixtures, 2 extracted fields match (vendor_name, invoice_total), 1 missed
    (invoice_date is wrong) -- recall and precision both reflect that."""
    fixtures_path = _write_fixtures(tmp_path)
    component = _component(mod, fixtures_path, min_recall=0.5)
    outcome = {
        "fields": {
            "vendor_name": "Acme Corp",       # fuzzy-matches "Acme Corporation"
            "invoice_total": 1042.50,          # exact match
            "invoice_date": "unknown",          # wrong -- doesn't match "2026-01-15"
        }
    }
    result = _materialize_with_outcome(component, outcome)
    [evaluation] = result.get_asset_check_evaluations()
    assert set(evaluation.metadata["matched_fixture"].value) == {"vendor_name", "invoice_total"}
    assert evaluation.metadata["missed_fixture"].value == ["invoice_date"]
    assert evaluation.metadata["recall"].value == round(2 / 3, 2)
    assert evaluation.metadata["precision"].value == round(2 / 3, 2)
    assert evaluation.passed is True


def test_custom_fields_key(mod, tmp_path):
    fixtures_path = _write_fixtures(tmp_path, {
        "fixtures": [{"id": "invoice_total", "field": "invoice_total", "expected": 1042.50}]
    })
    component = _component(mod, fixtures_path, min_recall=0.5, fields_key="extracted")
    result = _materialize_with_outcome(component, {"extracted": {"invoice_total": 1042.50}})
    [evaluation] = result.get_asset_check_evaluations()
    assert evaluation.metadata["matched_fixture"].value == ["invoice_total"]


def test_text_mode_is_unaffected_by_default(mod, tmp_path):
    """match_mode defaults to 'text' -- an AgentFixtureCheckComponent built without match_mode at
    all still runs the original free-text matcher."""
    fixtures_path = tmp_path / "fixtures.json"
    fixtures_path.write_text(json.dumps({"fixtures": [{"id": "flaky_retry", "any": [["flaky", "retry"]]}]}))
    component = mod.AgentFixtureCheckComponent(asset_key="my_outcome", fixtures_file=str(fixtures_path), min_recall=0.5)
    assert component.match_mode == "text"

    @dg.asset(key=dg.AssetKey.from_user_string(component.asset_key))
    def _outcome():
        return {"filed": [{"title": "flaky retry bug", "body": ""}], "text": ""}

    check_def = next(iter(component.build_defs(context=None).asset_checks))
    result = dg.materialize([_outcome, check_def], raise_on_error=True)
    [evaluation] = result.get_asset_check_evaluations()
    assert evaluation.metadata["matched_fixture"].value == ["flaky_retry"]


# ── pure grading function (edge cases without a full materialize) ───────────────────────────────


def test_grade_fields_string_similarity(mod):
    fixtures = [{"id": "vendor_name", "field": "vendor_name", "expected": "Acme Corporation"}]
    close = mod.agent_fixture_grade_fields(
        {"fields": {"vendor_name": "Acme Corp"}}, fixtures,
        fields_key="fields", fuzzy_field_threshold=0.6, numeric_tolerance=0.01,
    )
    assert close["matched"] == ["vendor_name"]

    far = mod.agent_fixture_grade_fields(
        {"fields": {"vendor_name": "Globex LLC"}}, fixtures,
        fields_key="fields", fuzzy_field_threshold=0.6, numeric_tolerance=0.01,
    )
    assert far["matched"] == []


def test_grade_fields_numeric_tolerance(mod):
    fixtures = [{"id": "invoice_total", "field": "invoice_total", "expected": 1042.50}]
    close = mod.agent_fixture_grade_fields(
        {"fields": {"invoice_total": 1042.51}}, fixtures,
        fields_key="fields", fuzzy_field_threshold=0.85, numeric_tolerance=0.001,
    )
    assert close["matched"] == ["invoice_total"]
    assert close["recall"] == 1.0

    far = mod.agent_fixture_grade_fields(
        {"fields": {"invoice_total": 2000.00}}, fixtures,
        fields_key="fields", fuzzy_field_threshold=0.85, numeric_tolerance=0.001,
    )
    assert far["matched"] == []
    assert far["recall"] == 0.0


def test_grade_fields_numeric_string_is_coerced(mod):
    """A numeric field extracted as a string (e.g. '1,042.50' from an LLM) still compares
    numerically, not as text."""
    fixtures = [{"id": "invoice_total", "field": "invoice_total", "expected": 1042.50}]
    result = mod.agent_fixture_grade_fields(
        {"fields": {"invoice_total": "1,042.50"}}, fixtures,
        fields_key="fields", fuzzy_field_threshold=0.85, numeric_tolerance=0.001,
    )
    assert result["matched"] == ["invoice_total"]


def test_grade_fields_missing_field_is_unmatched(mod):
    fixtures = [{"id": "invoice_total", "field": "invoice_total", "expected": 1042.50}]
    result = mod.agent_fixture_grade_fields(
        {"fields": {}}, fixtures, fields_key="fields", fuzzy_field_threshold=0.85, numeric_tolerance=0.01,
    )
    assert result["matched"] == []
    assert result["missed"] == ["invoice_total"]
    assert result["precision"] is None  # nothing extracted at all to compute precision over


def test_grade_fields_recall_is_none_with_zero_fixtures(mod):
    result = mod.agent_fixture_grade_fields(
        {"fields": {"a": "b"}}, [], fields_key="fields", fuzzy_field_threshold=0.85, numeric_tolerance=0.01,
    )
    assert result["recall"] is None
