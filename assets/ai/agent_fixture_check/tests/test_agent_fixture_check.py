"""Real, committed tests for AgentFixtureCheckComponent -- generalized from an internal
engineering-agents reference's defs/grades.py (not hardcoded to one asset). Exercises the real
Dagster asset_check binding end to end (a dummy outcome-shaped asset materialized alongside the
check, not just the pure grading function in isolation) plus the pure agent_fixture_grade function
directly for the edge cases.
"""
import json

import dagster as dg
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


_FIXTURES = {
    "fixtures": [
        {"id": "flaky_retry", "any": [["flaky", "retry"]]},
        {"id": "missing_field", "any": [["missing", "field"], ["required", "column"]]},
        {"id": "deprecated_api", "any": [["deprecated"]]},
    ]
}


def _write_fixtures(tmp_path, fixtures=None):
    p = tmp_path / "fixtures.json"
    p.write_text(json.dumps(fixtures if fixtures is not None else _FIXTURES))
    return p


def _component(mod, fixtures_path, **overrides):
    kwargs = dict(asset_key="my_outcome", fixtures_file=str(fixtures_path))
    kwargs.update(overrides)
    return mod.AgentFixtureCheckComponent(**kwargs)


def _materialize_with_outcome(component, outcome: dict):
    @dg.asset(key=dg.AssetKey.from_user_string(component.asset_key))
    def _outcome():
        return outcome

    check_def = next(iter(component.build_defs(context=None).asset_checks))
    return dg.materialize([_outcome, check_def], raise_on_error=True)


def test_passes_when_recall_meets_the_threshold(mod, tmp_path):
    fixtures_path = _write_fixtures(tmp_path)
    component = _component(mod, fixtures_path, min_recall=0.3)
    outcome = {"filed": [{"title": "flaky retry bug in orders", "body": ""}], "text": ""}
    result = _materialize_with_outcome(component, outcome)
    assert result.success
    [evaluation] = result.get_asset_check_evaluations()
    assert evaluation.passed is True
    assert evaluation.metadata["recall"].value == round(1 / 3, 2)
    assert evaluation.metadata["matched_fixture"].value == ["flaky_retry"]
    assert evaluation.metadata["missed_fixture"].value == ["missing_field", "deprecated_api"]


def test_fails_when_recall_is_below_the_threshold(mod, tmp_path):
    fixtures_path = _write_fixtures(tmp_path)
    component = _component(mod, fixtures_path, min_recall=0.9)
    outcome = {"filed": [{"title": "flaky retry bug", "body": ""}], "text": ""}
    result = _materialize_with_outcome(component, outcome)
    [evaluation] = result.get_asset_check_evaluations()
    assert evaluation.passed is False


def test_would_file_findings_count_as_evidence_too(mod, tmp_path):
    """practice mode's findings land under 'would_file', not 'filed' -- both are pooled by default."""
    fixtures_path = _write_fixtures(tmp_path)
    component = _component(mod, fixtures_path, min_recall=0.3)
    outcome = {"filed": [], "would_file": [{"title": "deprecated API used here", "body": ""}], "text": ""}
    result = _materialize_with_outcome(component, outcome)
    [evaluation] = result.get_asset_check_evaluations()
    assert evaluation.metadata["matched_fixture"].value == ["deprecated_api"]


def test_falls_back_to_text_field_when_no_structured_findings_exist(mod, tmp_path):
    fixtures_path = _write_fixtures(tmp_path)
    component = _component(mod, fixtures_path, min_recall=0.3)
    outcome = {"filed": [], "would_file": [], "text": "Nothing filed, but I noticed a deprecated API."}
    result = _materialize_with_outcome(component, outcome)
    [evaluation] = result.get_asset_check_evaluations()
    assert evaluation.metadata["matched_fixture"].value == ["deprecated_api"]


def test_custom_findings_keys_and_fallback_text_key(mod, tmp_path):
    fixtures_path = _write_fixtures(tmp_path)
    component = _component(
        mod, fixtures_path, min_recall=0.3,
        findings_keys=["custom_findings"], fallback_text_key="summary",
    )
    outcome = {"custom_findings": [{"title": "flaky retry", "body": ""}], "summary": ""}
    result = _materialize_with_outcome(component, outcome)
    [evaluation] = result.get_asset_check_evaluations()
    assert evaluation.metadata["matched_fixture"].value == ["flaky_retry"]


def test_error_severity_is_respected(mod, tmp_path):
    fixtures_path = _write_fixtures(tmp_path)
    component = _component(mod, fixtures_path, min_recall=0.9, severity="ERROR")
    outcome = {"filed": [], "would_file": [], "text": ""}
    result = _materialize_with_outcome(component, outcome)
    [evaluation] = result.get_asset_check_evaluations()
    assert evaluation.passed is False
    assert evaluation.severity == dg.AssetCheckSeverity.ERROR


def test_fixtures_file_relative_to_component_path(mod, tmp_path):
    """Relative fixtures_file paths resolve against context.path.parent (the component's own
    defs.yaml directory), not the process cwd."""
    subdir = tmp_path / "my_component_dir"
    subdir.mkdir()
    fixtures_path = _write_fixtures(subdir)
    component = _component(mod, "fixtures.json", min_recall=0.3)

    class _FakeContext:
        path = subdir / "defs.yaml"

    @dg.asset(key=dg.AssetKey.from_user_string(component.asset_key))
    def _outcome():
        return {"filed": [{"title": "flaky retry bug", "body": ""}], "text": ""}

    check_def = next(iter(component.build_defs(context=_FakeContext()).asset_checks))
    result = dg.materialize([_outcome, check_def], raise_on_error=True)
    assert result.success


# ── pure grading function (edge cases without a full materialize) ───────────────────────────────

def test_grade_recall_and_precision_are_none_with_zero_findings_and_zero_fixtures(mod):
    assert mod.agent_fixture_grade({"filed": [], "text": ""}, [], findings_keys=["filed"], fallback_text_key="text")["recall"] is None


def test_grade_precision_counts_only_pool_entries_matching_some_fixture(mod):
    outcome = {"filed": [{"title": "flaky retry", "body": ""}, {"title": "unrelated noise", "body": ""}]}
    result = mod.agent_fixture_grade(outcome, _FIXTURES["fixtures"], findings_keys=["filed"], fallback_text_key="text")
    assert result["findings_count"] == 2
    assert result["findings_matching_a_fixture"] == 1
    assert result["precision"] == 0.5
