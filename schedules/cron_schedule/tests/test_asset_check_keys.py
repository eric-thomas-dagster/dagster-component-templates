"""Real test for a gap a user found live: CronScheduleComponent could only
select assets (`asset_keys`), never asset checks -- there was no way to put
a check on its own schedule (e.g. "re-validate this data quality check every
night without re-materializing the asset").

Confirmed directly against real Dagster first (not assumed): `AssetSelection
.checks(AssetCheckKey(...))` really does run just the check, with ZERO
re-materialization of the parent asset -- `result.get_asset_materialization_
events()` comes back empty for a check-only run. Fixed by adding
`asset_check_keys` (parsed from '<asset_key>:<check_name>' strings) and
union-ing it with the asset selection via `AssetSelection.assets(...) |
AssetSelection.checks(...)`, so a schedule can run assets, checks, or both.
"""
import importlib.util
import pathlib

import dagster as dg
import pytest

_HERE = pathlib.Path(__file__).resolve().parent.parent
_COMPONENT_PY = _HERE / "component.py"
_spec = importlib.util.spec_from_file_location("cron_schedule_component", _COMPONENT_PY)
assert _spec is not None and _spec.loader is not None
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

CronScheduleComponent = _mod.CronScheduleComponent


@pytest.fixture
def defs_with_asset_and_check():
    @dg.asset(name="orders")
    def orders():
        return 1

    @dg.asset_check(asset=orders, name="row_count_check")
    def row_count_check():
        return dg.AssetCheckResult(passed=True)

    return orders, row_count_check


def test_check_only_schedule_runs_the_check_with_zero_materializations(defs_with_asset_and_check):
    orders, row_count_check = defs_with_asset_and_check
    c = CronScheduleComponent(
        schedule_name="nightly_check",
        cron_expression="0 0 * * *",
        asset_check_keys=["orders:row_count_check"],
    )
    result = c.build_defs(None)
    job = result.jobs[0]

    defs = dg.Definitions(assets=[orders], asset_checks=[row_count_check], jobs=[job])
    run_result = defs.resolve_job_def(job.name).execute_in_process()

    assert run_result.success
    assert [e.check_name for e in run_result.get_asset_check_evaluations()] == ["row_count_check"]
    assert list(run_result.get_asset_materialization_events()) == []


def test_asset_and_check_together_both_run(defs_with_asset_and_check):
    orders, row_count_check = defs_with_asset_and_check
    c = CronScheduleComponent(
        schedule_name="nightly_both",
        cron_expression="0 0 * * *",
        asset_keys=["orders"],
        asset_check_keys=["orders:row_count_check"],
    )
    result = c.build_defs(None)
    job = result.jobs[0]

    defs = dg.Definitions(assets=[orders], asset_checks=[row_count_check], jobs=[job])
    run_result = defs.resolve_job_def(job.name).execute_in_process()

    assert run_result.success
    assert [e.check_name for e in run_result.get_asset_check_evaluations()] == ["row_count_check"]
    assert len(list(run_result.get_asset_materialization_events())) == 1


def test_neither_asset_keys_nor_check_keys_raises_a_clear_error():
    c = CronScheduleComponent(schedule_name="empty", cron_expression="0 0 * * *")
    with pytest.raises(ValueError, match="Set at least one of asset_keys or asset_check_keys"):
        c.build_defs(None)


def test_malformed_check_key_raises_a_clear_error():
    c = CronScheduleComponent(
        schedule_name="bad",
        cron_expression="0 0 * * *",
        asset_check_keys=["not_a_valid_check_key"],
    )
    with pytest.raises(ValueError, match="must be '<asset_key>:<check_name>'"):
        c.build_defs(None)


def test_asset_keys_only_still_works_unchanged():
    @dg.asset(name="metrics")
    def metrics():
        return 1

    c = CronScheduleComponent(
        schedule_name="nightly_metrics",
        cron_expression="0 0 * * *",
        asset_keys=["metrics"],
    )
    result = c.build_defs(None)
    job = result.jobs[0]
    defs = dg.Definitions(assets=[metrics], jobs=[job])
    run_result = defs.resolve_job_def(job.name).execute_in_process()
    assert run_result.success
    assert len(list(run_result.get_asset_materialization_events())) == 1
