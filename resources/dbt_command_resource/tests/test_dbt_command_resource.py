"""Real, committed tests for DbtCommandResource -- real `dbt` CLI invocations against a tiny
fixture project (dbt-core + dbt-duckdb, both free/local, matching this repo's "mock only the
paid/external call" rule). No mocking of dbt or subprocess.
"""
import dagster as dg
import pytest


def test_dbt_run_succeeds_and_parses_run_results(mod, dbt_project):
    resource = mod.DbtCommandResource(project_dir=dbt_project["project_dir"], profiles_dir=dbt_project["profiles_dir"])
    result = resource.run(command="run", select="good_model")
    assert result["exit_code"] == 0
    assert result["dry_run"] is False
    assert result["dbt_node_count"] == 1
    assert result["dbt_status_counts"] == {"success": 1}
    assert result["dbt_elapsed_seconds"] > 0
    assert "good_model" in result["command"]


def test_dbt_build_runs_model_and_schema_tests(mod, dbt_project):
    resource = mod.DbtCommandResource(project_dir=dbt_project["project_dir"], profiles_dir=dbt_project["profiles_dir"])
    result = resource.run(command="build", select="good_model")
    assert result["dbt_status_counts"] == {"success": 1, "pass": 2}
    assert result["dbt_node_count"] == 3


def test_failing_model_raises_dagster_failure(mod, dbt_project):
    resource = mod.DbtCommandResource(project_dir=dbt_project["project_dir"], profiles_dir=dbt_project["profiles_dir"])
    with pytest.raises(dg.Failure):
        resource.run(command="run", select="failing_model")


def test_full_refresh_flag_is_passed(mod, dbt_project):
    resource = mod.DbtCommandResource(project_dir=dbt_project["project_dir"], profiles_dir=dbt_project["profiles_dir"])
    result = resource.run(command="run", select="good_model", full_refresh=True)
    assert "--full-refresh" in result["command"]


def test_dry_run_does_not_invoke_dbt(mod, dbt_project):
    resource = mod.DbtCommandResource(project_dir=dbt_project["project_dir"], profiles_dir=dbt_project["profiles_dir"])
    result = resource.run(command="run", select="good_model", dry_run=True)
    assert result["dry_run"] is True
    assert result["exit_code"] is None
    assert "good_model" in result["command"]


def test_parse_run_results_can_be_disabled(mod, dbt_project):
    resource = mod.DbtCommandResource(project_dir=dbt_project["project_dir"], profiles_dir=dbt_project["profiles_dir"])
    result = resource.run(command="run", select="good_model", parse_run_results=False)
    assert "dbt_node_count" not in result
    assert "dbt_status_counts" not in result


def test_extra_args_appended(mod, dbt_project):
    resource = mod.DbtCommandResource(project_dir=dbt_project["project_dir"], profiles_dir=dbt_project["profiles_dir"])
    result = resource.run(command="run", select="good_model", extra_args=["--quiet"])
    assert result["command"].strip().endswith("--quiet")


def test_vars_passed_as_json(mod, dbt_project):
    resource = mod.DbtCommandResource(project_dir=dbt_project["project_dir"], profiles_dir=dbt_project["profiles_dir"])
    result = resource.run(command="run", select="good_model", vars={"foo": "bar"})
    assert '{"foo": "bar"}' in result["command"]


def test_missing_run_results_is_handled_gracefully(mod, tmp_path):
    """project_dir with no target/run_results.json at all (e.g. a dry_run-only project, or one
    that was never actually run) -- parse_run_results must not raise."""
    empty_project = tmp_path / "empty"
    empty_project.mkdir()
    summary = mod._parse_run_results(str(empty_project), None)
    assert summary is None


def test_component_registers_resource_under_key(mod, dbt_project):
    comp = mod.DbtCommandResourceComponent(project_dir=dbt_project["project_dir"], profiles_dir=dbt_project["profiles_dir"])
    defs = comp.build_defs(None)
    assert "dbt_command_resource" in defs.resources
    assert isinstance(defs.resources["dbt_command_resource"], mod.DbtCommandResource)
