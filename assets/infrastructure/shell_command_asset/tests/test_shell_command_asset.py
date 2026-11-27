"""Real, committed tests for ShellCommandAssetComponent -- real dg.materialize() throughout,
real subprocess calls (no mocking: this component's only "external" surface is the shell
itself, which is free/local, matching this repo's "mock only the paid/external call" rule).
"""
import time

import dagster as dg
import pytest

from .conftest import materialize


def test_no_pool_by_default(mod):
    component = mod.ShellCommandAssetComponent(asset_name="t", command="echo hello")
    asset_def = component.build_defs(context=None).assets[0]
    assert asset_def.op.pool is None


def test_pool_wires_through(mod):
    component = mod.ShellCommandAssetComponent(asset_name="t", command="echo hello", pool="rpa_bots")
    asset_def = component.build_defs(context=None).assets[0]
    assert asset_def.op.pool == "rpa_bots"


def test_return_last_stdout_line_off_by_default(mod):
    component = mod.ShellCommandAssetComponent(asset_name="t", command="printf 'a\\nb\\n'")
    result = materialize(component)
    assert result.success
    md = result.asset_materializations_for_node("t")[0].metadata
    assert "b" in md["stdout_preview"].value


def test_return_last_stdout_line_returns_the_last_nonempty_line_as_value(mod):
    component = mod.ShellCommandAssetComponent(
        asset_name="t", command="printf 'first\\nsecond\\n\\n'", return_last_stdout_line=True,
    )
    result = materialize(component)
    assert result.success
    assert result.output_for_node("t") == "second"


def test_parse_json_output_returns_parsed_value_when_valid_json(mod):
    component = mod.ShellCommandAssetComponent(
        asset_name="t", command='echo \'{"a": 1}\'', return_last_stdout_line=True, parse_json_output=True,
    )
    result = materialize(component)
    assert result.success
    assert result.output_for_node("t") == {"a": 1}


def test_parse_json_output_falls_back_to_raw_string_when_not_json(mod):
    component = mod.ShellCommandAssetComponent(
        asset_name="t", command="echo not-json", return_last_stdout_line=True, parse_json_output=True,
    )
    result = materialize(component)
    assert result.success
    assert result.output_for_node("t") == "not-json"


def test_return_last_stdout_line_with_skip_on_exit_code(mod):
    component = mod.ShellCommandAssetComponent(
        asset_name="t", command="echo value-before-skip; exit 99",
        skip_on_exit_code=99, return_last_stdout_line=True,
    )
    result = materialize(component)
    assert result.success
    assert result.output_for_node("t") == "value-before-skip"


def test_basic_command_succeeds_with_exit_code_and_stdout_metadata(mod):
    component = mod.ShellCommandAssetComponent(asset_name="t", command="echo hello")
    result = materialize(component)
    assert result.success
    md = result.asset_materializations_for_node("t")[0].metadata
    assert md["exit_code"].value == 0
    assert "hello" in md["stdout_preview"].value


def test_nonzero_exit_fails_the_run(mod):
    component = mod.ShellCommandAssetComponent(asset_name="t", command="exit 7")
    result = materialize(component, raise_on_error=False)
    assert not result.success


def test_skip_on_exit_code_is_a_successful_materialization_not_a_failure(mod):
    component = mod.ShellCommandAssetComponent(asset_name="t", command="exit 99", skip_on_exit_code=99)
    result = materialize(component)
    assert result.success
    md = result.asset_materializations_for_node("t")[0].metadata
    assert md["skipped"].value is True
    assert md["exit_code"].value == 99


def test_skip_on_exit_code_unset_treats_the_same_exit_code_as_a_real_failure(mod):
    component = mod.ShellCommandAssetComponent(asset_name="t", command="exit 99")
    result = materialize(component, raise_on_error=False)
    assert not result.success


def test_success_exit_codes_counts_as_a_plain_success_not_skipped(mod):
    component = mod.ShellCommandAssetComponent(asset_name="t", command="exit 5", success_exit_codes=[5, 6])
    result = materialize(component)
    assert result.success
    md = result.asset_materializations_for_node("t")[0].metadata
    assert md["exit_code"].value == 5
    assert "skipped" not in md  # distinct from skip_on_exit_code -- no skipped marker


def test_success_exit_codes_unset_treats_the_code_as_a_real_failure(mod):
    component = mod.ShellCommandAssetComponent(asset_name="t", command="exit 5")
    result = materialize(component, raise_on_error=False)
    assert not result.success


def test_success_exit_codes_does_not_affect_other_nonzero_codes(mod):
    component = mod.ShellCommandAssetComponent(asset_name="t", command="exit 7", success_exit_codes=[5, 6])
    result = materialize(component, raise_on_error=False)
    assert not result.success


def test_dry_run_skips_the_actual_command(mod, tmp_path):
    marker = tmp_path / "marker"
    component = mod.ShellCommandAssetComponent(asset_name="t", command=f"touch {marker}", dry_run=True)
    result = materialize(component)
    assert result.success
    assert not marker.exists()
    md = result.asset_materializations_for_node("t")[0].metadata
    assert md["dry_run"].value is True
    assert "exit_code" not in md


def test_dry_run_false_by_default_and_marks_a_real_run(mod):
    component = mod.ShellCommandAssetComponent(asset_name="t", command="echo hello")
    result = materialize(component)
    assert result.success
    md = result.asset_materializations_for_node("t")[0].metadata
    assert md["dry_run"].value is False


def test_timeout_seconds_kills_a_long_running_command(mod):
    component = mod.ShellCommandAssetComponent(asset_name="t", command="sleep 30", timeout_seconds=1)
    start = time.monotonic()
    result = materialize(component, raise_on_error=False)
    assert not result.success
    assert time.monotonic() - start < 20  # killed well before the sleep would finish


def test_bash_specific_syntax_works_not_just_posix_sh(mod):
    """[[ ]] is a bashism -- dash (/bin/sh on many Linux distros) rejects it. Confirms
    executable="/bin/bash" is actually wired in, not just shell=True's OS-default shell."""
    component = mod.ShellCommandAssetComponent(asset_name="t", command='[[ "abc" == a* ]] && echo matched')
    result = materialize(component)
    assert result.success
    md = result.asset_materializations_for_node("t")[0].metadata
    assert "matched" in md["stdout_preview"].value


def test_env_vars_are_merged_with_the_parent_environment(mod, monkeypatch):
    monkeypatch.setenv("SHELL_ASSET_TEST_PARENT_VAR", "from-parent")
    component = mod.ShellCommandAssetComponent(
        asset_name="t", command="echo $SHELL_ASSET_TEST_PARENT_VAR $SHELL_ASSET_TEST_OWN_VAR",
        env_vars={"SHELL_ASSET_TEST_OWN_VAR": "from-component"},
    )
    result = materialize(component)
    md = result.asset_materializations_for_node("t")[0].metadata
    assert "from-parent" in md["stdout_preview"].value
    assert "from-component" in md["stdout_preview"].value


def test_partition_key_is_substituted_into_command_and_cwd(mod, tmp_path):
    (tmp_path / "2024-01-05").mkdir()
    component = mod.ShellCommandAssetComponent(
        asset_name="t", command="echo partition={partition_key}", cwd=str(tmp_path) + "/{partition_key}",
        partition_type="daily", partition_start="2024-01-01",
    )
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize(
        [asset_def], resources={"io_manager": dg.InMemoryIOManager()}, partition_key="2024-01-05",
    )
    assert result.success
    md = result.asset_materializations_for_node("t")[0].metadata
    assert "partition=2024-01-05" in md["stdout_preview"].value


def test_command_asset_key_uses_the_upstream_value_as_the_command(mod):
    @dg.asset(key=dg.AssetKey.from_user_string("cmd_source"))
    def cmd_source():
        return "echo from-upstream"

    component = mod.ShellCommandAssetComponent(asset_name="t", command_asset_key="cmd_source")
    result = materialize(component, assets_extra=[cmd_source])
    assert result.success
    md = result.asset_materializations_for_node("t")[0].metadata
    assert "from-upstream" in md["stdout_preview"].value


def test_command_asset_key_falls_back_to_static_command_when_upstream_value_is_falsy(mod):
    @dg.asset(key=dg.AssetKey.from_user_string("cmd_source"))
    def cmd_source():
        return ""

    component = mod.ShellCommandAssetComponent(asset_name="t", command_asset_key="cmd_source", command="echo fallback")
    result = materialize(component, assets_extra=[cmd_source])
    assert result.success
    md = result.asset_materializations_for_node("t")[0].metadata
    assert "fallback" in md["stdout_preview"].value


def test_requires_command_or_command_asset_key(mod):
    with pytest.raises(Exception, match="command"):
        mod.ShellCommandAssetComponent(asset_name="t")


def test_retry_policy_wires_through(mod):
    component = mod.ShellCommandAssetComponent(
        asset_name="t", command="echo hi",
        retry_policy_max_retries=3, retry_policy_delay_seconds=2, retry_policy_backoff="linear",
    )
    asset_def = component.build_defs(context=None).assets[0]
    rp = asset_def.op.retry_policy
    assert rp is not None and rp.max_retries == 3 and rp.delay == 2


def test_static_partition_type_wires_through(mod):
    component = mod.ShellCommandAssetComponent(
        asset_name="t", command="echo {partition_key}", partition_type="static", partition_values="a,b,c",
    )
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize(
        [asset_def], resources={"io_manager": dg.InMemoryIOManager()}, partition_key="b",
    )
    assert result.success


# --- run_window ------------------------------------------------------------------------------

def test_run_window_outside_the_window_skips_cleanly(tmp_path, mod):
    marker = tmp_path / "never.txt"
    component = mod.ShellCommandAssetComponent(
        asset_name="t", command=f"touch {marker}", run_window="00:00-00:01 UTC",
    )
    result = materialize(component)
    assert result.success  # skip, not fail
    assert not marker.exists()
    assert result.asset_materializations_for_node("t") == []


def test_run_window_covering_all_day_always_runs(tmp_path, mod):
    marker = tmp_path / "ran.txt"
    component = mod.ShellCommandAssetComponent(
        asset_name="t", command=f"touch {marker}", run_window="00:00-23:59 UTC",
    )
    result = materialize(component)
    assert result.success
    assert marker.exists()


def test_run_window_unset_always_runs(tmp_path, mod):
    marker = tmp_path / "ran.txt"
    component = mod.ShellCommandAssetComponent(asset_name="t", command=f"touch {marker}")
    result = materialize(component)
    assert result.success
    assert marker.exists()


def test_run_window_malformed_spec_fails_open_and_still_runs(tmp_path, mod):
    marker = tmp_path / "ran.txt"
    component = mod.ShellCommandAssetComponent(
        asset_name="t", command=f"touch {marker}", run_window="not a valid window spec",
    )
    result = materialize(component)
    assert result.success
    assert marker.exists()


def test_no_schedule_by_default(mod):
    component = mod.ShellCommandAssetComponent(asset_name="t", command="echo hello")
    defs = component.build_defs(context=None)
    assert defs.jobs is None or len(defs.jobs) == 0
    assert defs.schedules is None or len(defs.schedules) == 0


def test_cron_schedule_wires_a_job_and_schedule_targeting_this_asset(mod):
    component = mod.ShellCommandAssetComponent(
        asset_name="t", command="echo hello", cron_schedule="0 */6 * * *",
    )
    defs = component.build_defs(context=None)
    assert len(defs.jobs) == 1
    assert defs.jobs[0].name == "t_job"
    assert len(defs.schedules) == 1
    schedule = defs.schedules[0]
    assert schedule.cron_schedule == "0 */6 * * *"
    assert schedule.name == "t_schedule"
    assert schedule.default_status == dg.DefaultScheduleStatus.STOPPED


def test_cron_schedule_respects_custom_job_name_and_running_default_status(mod):
    component = mod.ShellCommandAssetComponent(
        asset_name="t", command="echo hello", cron_schedule="0 0 * * *",
        schedule_job_name="my_job", schedule_default_status="RUNNING",
    )
    defs = component.build_defs(context=None)
    assert defs.jobs[0].name == "my_job"
    assert defs.schedules[0].name == "my_job_schedule"
    assert defs.schedules[0].default_status == dg.DefaultScheduleStatus.RUNNING


def test_cron_schedule_sanitizes_slash_delimited_asset_name(mod):
    """Regression: a hierarchical asset_name like "sh/a" is a valid AssetKey but not a valid
    Dagster job/schedule name on its own -- used to raise DagsterInvalidDefinitionError."""
    component = mod.ShellCommandAssetComponent(
        asset_name="sh/a", command="echo hello", cron_schedule="0 * * * *",
    )
    defs = component.build_defs(context=None)
    assert defs.jobs[0].name == "sh__a_job"
    assert defs.schedules[0].name == "sh__a_schedule"


def test_cron_schedule_explicit_job_name_still_used_with_slash_asset_name(mod):
    component = mod.ShellCommandAssetComponent(
        asset_name="sh/a", command="echo hello", cron_schedule="0 * * * *",
        schedule_job_name="my_job",
    )
    defs = component.build_defs(context=None)
    assert defs.jobs[0].name == "my_job"
    assert defs.schedules[0].name == "my_job_schedule"
