"""Same real gap as cron_schedule/interval_schedule (see
cron_schedule/tests/test_asset_check_keys.py for the full writeup):
AssetJobComponent could only select assets, never asset checks. Fixed
identically -- `asset_check_keys` parsed from '<asset_key>:<check_name>'
strings, union-ed with the asset selection via `AssetSelection.assets(...)
| AssetSelection.checks(...)`.
"""
import importlib.util
import pathlib

import dagster as dg
import pytest

_HERE = pathlib.Path(__file__).resolve().parent.parent
_COMPONENT_PY = _HERE / "component.py"
_spec = importlib.util.spec_from_file_location("asset_job_component", _COMPONENT_PY)
assert _spec is not None and _spec.loader is not None
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

AssetJobComponent = _mod.AssetJobComponent


@pytest.fixture
def defs_with_asset_and_check():
    @dg.asset(name="orders")
    def orders():
        return 1

    @dg.asset_check(asset=orders, name="row_count_check")
    def row_count_check():
        return dg.AssetCheckResult(passed=True)

    return orders, row_count_check


def test_check_only_job_runs_the_check_with_zero_materializations(defs_with_asset_and_check):
    orders, row_count_check = defs_with_asset_and_check
    c = AssetJobComponent(job_name="check_job", asset_check_keys=["orders:row_count_check"])
    result = c.build_defs(None)
    job = result.jobs[0]

    defs = dg.Definitions(assets=[orders], asset_checks=[row_count_check], jobs=[job])
    run_result = defs.resolve_job_def(job.name).execute_in_process()

    assert run_result.success
    assert [e.check_name for e in run_result.get_asset_check_evaluations()] == ["row_count_check"]
    assert list(run_result.get_asset_materialization_events()) == []


def test_asset_and_check_together_both_run(defs_with_asset_and_check):
    orders, row_count_check = defs_with_asset_and_check
    c = AssetJobComponent(
        job_name="both_job",
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
    c = AssetJobComponent(job_name="empty")
    with pytest.raises(ValueError, match="Set at least one of asset_keys or asset_check_keys"):
        c.build_defs(None)


def test_malformed_check_key_raises_a_clear_error():
    c = AssetJobComponent(job_name="bad", asset_check_keys=["not_a_valid_check_key"])
    with pytest.raises(ValueError, match="must be '<asset_key>:<check_name>'"):
        c.build_defs(None)


def test_asset_keys_only_still_works_unchanged():
    @dg.asset(name="metrics")
    def metrics():
        return 1

    c = AssetJobComponent(job_name="metrics_job", asset_keys=["metrics"])
    result = c.build_defs(None)
    job = result.jobs[0]
    defs = dg.Definitions(assets=[metrics], jobs=[job])
    run_result = defs.resolve_job_def(job.name).execute_in_process()
    assert run_result.success
    assert len(list(run_result.get_asset_materialization_events())) == 1
