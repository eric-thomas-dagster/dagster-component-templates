"""Real, committed tests for DagsterRunPruneJobComponent.

Same bug class as jobs/run_history_to_warehouse_job: `get_runs()` returns
`DagsterRun` objects, which have no `create_timestamp` attribute at all
(confirmed via `DagsterRun._fields` against installed dagster==1.13.24) --
only `RunRecord` (from `get_run_records()`) has it. The shipped code did
`group.sort(key=lambda r: r.create_timestamp, ...)` directly on `DagsterRun`
instances, which raised `AttributeError` on every tick that found >=1
candidate run to prune. Also fixed: `RunsFilter(updated_before=...,
updated_after=...)` were passed raw epoch floats where the installed
dagster requires real `datetime.datetime` objects.
"""
import dagster as dg
import pytest

from .conftest import load_component_module


@pytest.fixture
def mod():
    return load_component_module()


@pytest.fixture
def instance_with_runs():
    with dg.instance_for_test() as instance:

        @dg.job
        def ok_job():
            @dg.op
            def o():
                return 1
            o()

        ok_job.execute_in_process(instance=instance)
        ok_job.execute_in_process(instance=instance)
        yield instance


def test_dry_run_does_not_crash_with_no_old_candidates(mod, instance_with_runs):
    """Regression test: this crashed with AttributeError on create_timestamp
    for ANY candidate run before the RunRecord fix -- here there are none
    (runs are fresh), but build_defs/the op must still execute cleanly."""
    component = mod.DagsterRunPruneJobComponent(
        job_name="prune_probe", schedule=None, max_age_days=9999,
        keep_last_n_per_job=1, max_deletes_per_run=100,
        only_terminal_states=True, dry_run=True,
    )
    defs = component.build_defs(context=None)
    job = defs.get_job_def("prune_probe")
    result = job.execute_in_process(instance=instance_with_runs)
    assert result.success
    out = result.output_for_node("prune_probe_op")
    assert out == {"deleted": 0, "kept_by_recency": 0, "checked": 0}


def test_real_prune_sorts_and_deletes_real_candidates(mod, instance_with_runs):
    """Regression test for the actual crash path: >=1 real candidate run,
    requiring the create_timestamp sort to succeed and delete_run to receive
    a real run_id (not crash pulling it off the wrong object type)."""
    real_time = mod.time.time
    mod.time.time = lambda: real_time() + 2 * 86400.0
    try:
        component = mod.DagsterRunPruneJobComponent(
            job_name="prune_probe2", schedule=None, max_age_days=1,
            keep_last_n_per_job=0, max_deletes_per_run=100,
            only_terminal_states=True, dry_run=False,
        )
        defs = component.build_defs(context=None)
        job = defs.get_job_def("prune_probe2")
        result = job.execute_in_process(instance=instance_with_runs)
        assert result.success
        out = result.output_for_node("prune_probe2_op")
        assert out["deleted"] >= 2
        assert out["dry_run"] is False
    finally:
        mod.time.time = real_time


def test_unknown_run_after_delete_confirms_real_deletion(mod, instance_with_runs):
    before = len(instance_with_runs.get_run_records())
    assert before >= 2

    real_time = mod.time.time
    mod.time.time = lambda: real_time() + 2 * 86400.0
    try:
        component = mod.DagsterRunPruneJobComponent(
            job_name="prune_probe3", schedule=None, max_age_days=1,
            keep_last_n_per_job=0, max_deletes_per_run=100,
            only_terminal_states=True, dry_run=False,
        )
        defs = component.build_defs(context=None)
        job = defs.get_job_def("prune_probe3")
        job.execute_in_process(instance=instance_with_runs)
    finally:
        mod.time.time = real_time

    remaining_ids = {r.dagster_run.run_id for r in instance_with_runs.get_run_records()}
    # The job's own just-completed run is never a candidate for its own sweep
    # (it wasn't in storage yet when get_run_records() was called), but the
    # pre-existing ok_job runs must be gone.
    assert len(remaining_ids) < before
