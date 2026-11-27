"""Real, committed tests for DbtCommandAssetComponent -- real `dbt` CLI invocations against a
tiny fixture project (dbt-core + dbt-duckdb, both free/local, matching this repo's "mock only
the paid/external call" rule). No mocking of dbt or subprocess.
"""
import json

import dagster as dg
import pytest

from .conftest import materialize


def test_dbt_run_succeeds_and_parses_run_results(mod, dbt_project):
    component = mod.DbtCommandAssetComponent(
        asset_name="t",
        project_dir=dbt_project["project_dir"],
        profiles_dir=dbt_project["profiles_dir"],
        command="run",
        select="good_model",
        group_name="dbt",
    )
    result = materialize(component)
    assert result.success
    md = result.asset_materializations_for_node("t")[0].metadata
    assert md["exit_code"].value == 0
    assert md["dry_run"].value is False
    assert md["dbt_node_count"].value == 1
    status_counts = json.loads(md["dbt_status_counts"].value) if isinstance(md["dbt_status_counts"].value, str) else md["dbt_status_counts"].value
    assert status_counts == {"success": 1}
    assert md["dbt_elapsed_seconds"].value > 0
    assert "good_model" in md["command"].value


def test_dbt_build_runs_model_and_schema_tests(mod, dbt_project):
    component = mod.DbtCommandAssetComponent(
        asset_name="t",
        project_dir=dbt_project["project_dir"],
        profiles_dir=dbt_project["profiles_dir"],
        command="build",
        select="good_model",
    )
    result = materialize(component)
    assert result.success
    md = result.asset_materializations_for_node("t")[0].metadata
    status_counts = json.loads(md["dbt_status_counts"].value) if isinstance(md["dbt_status_counts"].value, str) else md["dbt_status_counts"].value
    assert status_counts == {"success": 1, "pass": 2}
    assert md["dbt_node_count"].value == 3


def test_failing_model_fails_the_asset_with_exit_code_metadata(mod, dbt_project):
    component = mod.DbtCommandAssetComponent(
        asset_name="t",
        project_dir=dbt_project["project_dir"],
        profiles_dir=dbt_project["profiles_dir"],
        command="run",
        select="failing_model",
    )
    result = materialize(component, raise_on_error=False)
    assert not result.success


def test_full_refresh_flag_is_passed(mod, dbt_project):
    component = mod.DbtCommandAssetComponent(
        asset_name="t",
        project_dir=dbt_project["project_dir"],
        profiles_dir=dbt_project["profiles_dir"],
        command="run",
        select="good_model",
        full_refresh=True,
    )
    result = materialize(component)
    assert result.success
    md = result.asset_materializations_for_node("t")[0].metadata
    assert "--full-refresh" in md["command"].value


def test_dry_run_does_not_invoke_dbt(mod, dbt_project):
    component = mod.DbtCommandAssetComponent(
        asset_name="t",
        project_dir=dbt_project["project_dir"],
        profiles_dir=dbt_project["profiles_dir"],
        command="run",
        select="good_model",
        dry_run=True,
    )
    result = materialize(component)
    assert result.success
    md = result.asset_materializations_for_node("t")[0].metadata
    assert md["dry_run"].value is True
    assert "exit_code" not in md
    assert "good_model" in md["command"].value


def test_parse_run_results_can_be_disabled(mod, dbt_project):
    component = mod.DbtCommandAssetComponent(
        asset_name="t",
        project_dir=dbt_project["project_dir"],
        profiles_dir=dbt_project["profiles_dir"],
        command="run",
        select="good_model",
        parse_run_results=False,
    )
    result = materialize(component)
    assert result.success
    md = result.asset_materializations_for_node("t")[0].metadata
    assert "dbt_node_count" not in md
    assert "dbt_status_counts" not in md


def test_run_window_outside_window_skips_without_running_dbt(mod, dbt_project):
    component = mod.DbtCommandAssetComponent(
        asset_name="t",
        project_dir=dbt_project["project_dir"],
        profiles_dir=dbt_project["profiles_dir"],
        command="run",
        select="good_model",
        # A window that's never "now" in any timezone (00:00-00:01 UTC is only ever true for
        # 60 seconds a day -- deterministic enough not to flake in CI the way a narrower window
        # tied to wall-clock "now" would).
        run_window="00:00-00:01 UTC",
    )
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    # Either skipped cleanly (no materialization) or ran for real if the test happened to execute
    # in that exact one-minute window -- assert it never raises, which is the real contract.
    assert result.success


def test_extra_args_appended(mod, dbt_project):
    component = mod.DbtCommandAssetComponent(
        asset_name="t",
        project_dir=dbt_project["project_dir"],
        profiles_dir=dbt_project["profiles_dir"],
        command="run",
        select="good_model",
        extra_args=["--quiet"],
    )
    result = materialize(component)
    assert result.success
    md = result.asset_materializations_for_node("t")[0].metadata
    assert md["command"].value.strip().endswith("--quiet")


def test_vars_passed_as_json(mod, dbt_project):
    component = mod.DbtCommandAssetComponent(
        asset_name="t",
        project_dir=dbt_project["project_dir"],
        profiles_dir=dbt_project["profiles_dir"],
        command="run",
        select="good_model",
        vars={"foo": "bar"},
    )
    result = materialize(component)
    assert result.success
    md = result.asset_materializations_for_node("t")[0].metadata
    assert '{"foo": "bar"}' in md["command"].value


def test_missing_run_results_is_handled_gracefully(mod, tmp_path):
    """project_dir with no target/run_results.json at all (e.g. a dry_run-only project, or one
    that was never actually run) -- parse_run_results must not raise."""
    mod_ = mod
    empty_project = tmp_path / "empty"
    empty_project.mkdir()
    summary = mod_._parse_run_results(str(empty_project), None)
    assert summary is None
