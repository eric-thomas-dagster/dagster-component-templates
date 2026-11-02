"""Real, committed tests for StuckRunTerminatorJobComponent.

Same bug class as jobs/run_history_to_warehouse_job: `get_runs()` returns
`DagsterRun` objects, which have no `start_time`/`create_timestamp`
attributes at all (confirmed via `DagsterRun._fields` against installed
dagster==1.13.24) -- only `RunRecord` (from `get_run_records()`) has them.
The shipped code did `r.start_time or (r.create_timestamp.timestamp() ...)`
directly on `DagsterRun` instances, so `r.start_time` raised `AttributeError`
immediately (evaluated before the `or`/`hasattr` fallback could help) on
every tick that found >=1 in-flight run matching the status filter.
"""
import dagster as dg
import pytest

from .conftest import load_component_module


@pytest.fixture
def mod():
    return load_component_module()


def test_dry_run_detects_in_flight_run_without_crashing(mod):
    """Regression test for the direct r.start_time AttributeError. A
    currently-executing run is genuinely "in flight", so sweeping the
    instance mid-run exercises the real crash path this fix addresses."""
    with dg.instance_for_test() as instance:

        @dg.job
        def probe_job():
            @dg.op
            def o():
                return 1
            o()

        component = mod.StuckRunTerminatorJobComponent(
            job_name="stuck_probe", schedule=None,
            stuck_after_hours=0.0000001, scan_limit=100, dry_run=True,
        )
        defs = component.build_defs(context=None)
        job = defs.get_job_def("stuck_probe")
        result = job.execute_in_process(instance=instance)
        assert result.success
        out = result.output_for_node("stuck_probe_op")
        assert out["checked"] >= 1
        assert out["stuck_found"] >= 1
        assert out["terminated"] == 0  # dry_run=True
        assert out["terminated_run_ids"] == []


def test_no_in_flight_runs_returns_zero_without_crashing(mod):
    with dg.instance_for_test() as instance:

        @dg.job
        def ok_job():
            @dg.op
            def o():
                return 1
            o()

        ok_job.execute_in_process(instance=instance)

        component = mod.StuckRunTerminatorJobComponent(
            job_name="stuck_probe2", schedule=None,
            stuck_after_hours=9999, scan_limit=100, dry_run=True,
        )
        defs = component.build_defs(context=None)
        job = defs.get_job_def("stuck_probe2")
        result = job.execute_in_process(instance=instance)
        assert result.success
        out = result.output_for_node("stuck_probe2_op")
        # The only in-flight run when this executes is itself, and
        # stuck_after_hours=9999 means it won't be flagged as stuck.
        assert out["terminated"] == 0
