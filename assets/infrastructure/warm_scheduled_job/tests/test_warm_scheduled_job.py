"""Committed regression tests for WarmScheduledJobComponent.

Uses real `croniter` 6-field (second-precision) cron strings (e.g.
'*/1 * * * * *' = every second) so these tests genuinely exercise the
precise-timing loop live, in real wall-clock time, without needing to wait
a full minute per tick the way standard 5-field cron would require.
"""
import importlib
import time

import dagster as dg
import pytest

from .conftest import load_component_module, requires_croniter

# Imported via the exact same absolute dotted path the component's own
# `_resolve()` uses below (not `from . import _fixtures`) -- otherwise
# pytest's test-collection import and the component's importlib.import_module
# call can land on two SEPARATE module objects (different sys.modules keys),
# each with its own independent copy of the CALLS dict, silently breaking
# every assertion that depends on shared mutable state.
_fixtures = importlib.import_module("assets.infrastructure.warm_scheduled_job.tests._fixtures")

pytestmark = requires_croniter

_WARMUP = "assets.infrastructure.warm_scheduled_job.tests._fixtures:warmup"
_TICK = "assets.infrastructure.warm_scheduled_job.tests._fixtures:run_tick"
_TICK_RAISES = "assets.infrastructure.warm_scheduled_job.tests._fixtures:run_tick_raises"
_TICK_RAISES_ONCE = "assets.infrastructure.warm_scheduled_job.tests._fixtures:run_tick_raises_once_then_ok"
_TICK_JOB_A = "assets.infrastructure.warm_scheduled_job.tests._fixtures:run_tick_job_a"
_TICK_JOB_B = "assets.infrastructure.warm_scheduled_job.tests._fixtures:run_tick_job_b"
_WARMUP_DEMO_RESOURCE = "assets.infrastructure.warm_scheduled_job.tests._fixtures:warmup_with_demo_resource"
_WARMJOB_TICK = "assets.infrastructure.warm_scheduled_job.tests._fixtures:warmjob_test_job"
_WARMJOB_FAILING_TICK = "assets.infrastructure.warm_scheduled_job.tests._fixtures:warmjob_failing_test_job"


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture(autouse=True)
def _reset_fixture_state():
    _fixtures.reset()
    yield
    _fixtures.reset()


def _materialize(component):
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    return dg.materialize([asset_def])


def _metadata_for(result, asset_name: str, node_name: str = None) -> dict:
    """MaterializeResult metadata is only retrievable this way (not as a
    plain return value) -- AssetSpec-based multi_asset outputs default to
    Dagster's `Nothing` type, so a returned/yielded Python value isn't an
    option here. Matches the convention already used by this session's other
    multi_asset-based components' tests (e.g. hotjar_ingestion).

    `asset_materializations_for_node` filters by the underlying OP's node
    name, NOT the asset key -- confirmed directly against dagster's source.
    In single-job (legacy) mode the op's name equals asset_name (unchanged
    from the original component), so node_name defaults to asset_name; in
    multi-job mode, several jobs share ONE op/node, so the caller must pass
    that shared node_name and this filters the node's full event list down
    to the one asset key being asked about.
    """
    node_name = node_name or asset_name
    events = result.asset_materializations_for_node(node_name)
    matching = [e for e in events if e.asset_key == dg.AssetKey.from_user_string(asset_name)]
    # [-1], not [0]: this component emits many per-tick AssetMaterialization
    # events via context.log_event() (one per tick) PLUS one final summary
    # MaterializeResult per asset at the very end -- the summary (with
    # stop_reason/total_ticks/etc.) is always the LAST materialization event
    # for a given asset key, not the first.
    raw = matching[-1].metadata
    out = {}
    for k, v in raw.items():
        out[k] = v.value if hasattr(v, "value") else v
    return out


def test_warmup_runs_once_and_ticks_reuse_same_warm_state(mod):
    component = mod.WarmScheduledJobComponent(
        asset_name="warm_job_out",
        schedule="*/1 * * * * *",
        second_precision=True,
        warmup_fn=_WARMUP,
        tick_fn=_TICK,
        max_ticks=3,
        max_seconds=30,
    )
    result = _materialize(component)
    assert result.success
    assert _fixtures.CALLS["warmup_count"] == 1, "warmup must run exactly once per bounded run, not once per tick"
    assert _fixtures.CALLS["tick_count"] == 3
    # Every tick must have seen the SAME warm_state object returned by the
    # single warmup call -- this is the whole point (warm reuse across ticks).
    seen_states = _fixtures.CALLS["tick_states"]
    assert len(seen_states) == 3
    assert all(s == seen_states[0] for s in seen_states)
    assert seen_states[0] == {"warmed_at_call": 1}

    out = _metadata_for(result, "warm_job_out")
    assert out["total_ticks"] == 3
    assert out["stop_reason"] == "max_ticks"


def test_drift_is_small(mod):
    """The whole value proposition is precise timing -- assert the measured
    drift (actual fire time vs scheduled instant) is genuinely small, not
    just that the component runs."""
    component = mod.WarmScheduledJobComponent(
        asset_name="warm_job_out",
        schedule="*/1 * * * * *",
        second_precision=True,
        tick_fn=_TICK,
        max_ticks=2,
        max_seconds=30,
    )
    result = _materialize(component)
    assert result.success
    out = _metadata_for(result, "warm_job_out")
    assert out["max_drift_seconds"] < 0.5, (
        f"expected sub-500ms drift against the scheduled instant, got {out['max_drift_seconds']}"
    )


def test_tick_error_handling_continue_survives_a_failed_tick(mod):
    component = mod.WarmScheduledJobComponent(
        asset_name="warm_job_out",
        schedule="*/1 * * * * *",
        second_precision=True,
        tick_fn=_TICK_RAISES_ONCE,
        tick_error_handling="continue",
        max_ticks=2,
        max_seconds=30,
    )
    result = _materialize(component)
    assert result.success
    out = _metadata_for(result, "warm_job_out")
    assert out["total_ticks"] == 2
    assert out["failed_ticks"] == 1


def test_tick_error_handling_raise_fails_the_run(mod):
    component = mod.WarmScheduledJobComponent(
        asset_name="warm_job_out",
        schedule="*/1 * * * * *",
        second_precision=True,
        tick_fn=_TICK_RAISES,
        tick_error_handling="raise",
        max_seconds=30,
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], raise_on_error=False)
    assert not result.success


def test_max_seconds_stops_the_loop(mod):
    component = mod.WarmScheduledJobComponent(
        asset_name="warm_job_out",
        schedule="*/1 * * * * *",
        second_precision=True,
        tick_fn=_TICK,
        max_seconds=2,
    )
    start = time.time()
    result = _materialize(component)
    elapsed = time.time() - start
    assert result.success
    out = _metadata_for(result, "warm_job_out")
    assert out["stop_reason"] == "max_seconds"
    assert elapsed < 5, "should stop at max_seconds=2, not run away"


def test_invalid_schedule_raises(mod):
    with pytest.raises(ValueError, match="expected exactly 5"):
        mod.WarmScheduledJobComponent(
            asset_name="x", schedule="not a cron string", tick_fn=_TICK,
        ).build_defs(context=None)


def test_six_field_schedule_without_second_precision_raises(mod):
    """A 6-field (second-precision) cron string given without
    second_precision=True must be rejected, not silently misparsed --
    regression test for croniter.is_valid() being too lenient on its own
    (confirmed live: it accepts 6-field, and even 7-field, strings
    regardless of second_at_beginning)."""
    with pytest.raises(ValueError, match="expected exactly 5"):
        mod.WarmScheduledJobComponent(
            asset_name="x", schedule="*/1 * * * * *", second_precision=False, tick_fn=_TICK,
        ).build_defs(context=None)


def test_invalid_callable_path_raises(mod):
    # warmup_fn/tick_fn are resolved at materialize time (inside the asset
    # body), not at build_defs time -- same convention as
    # dynamic_fanout_asset's _resolve(), so instantiating/validating this
    # component doesn't require the user's own project modules to be
    # importable. So this must materialize to actually exercise the check.
    component = mod.WarmScheduledJobComponent(
        asset_name="x", schedule="*/15 * * * *", tick_fn="not_a_valid_path_no_colon",
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], raise_on_error=False)
    assert not result.success


def test_invalid_timezone_raises(mod):
    with pytest.raises(ValueError, match="invalid timezone"):
        mod.WarmScheduledJobComponent(
            asset_name="x", schedule="*/15 * * * *", tick_fn=_TICK, timezone="Not/A_Real_Zone",
        ).build_defs(context=None)


def test_invalid_tick_error_handling_raises(mod):
    with pytest.raises(ValueError, match="tick_error_handling"):
        mod.WarmScheduledJobComponent(
            asset_name="x", schedule="*/15 * * * *", tick_fn=_TICK, tick_error_handling="explode",
        ).build_defs(context=None)


def test_mode_mutual_exclusivity_both_set_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.WarmScheduledJobComponent(
            asset_name="x", schedule="*/15 * * * *", tick_fn=_TICK,
            jobs=[{"asset_name": "y", "schedule": "*/15 * * * *", "tick_fn": _TICK}],
        ).build_defs(context=None)


def test_mode_mutual_exclusivity_neither_set_raises(mod):
    with pytest.raises(ValueError, match="must set either"):
        mod.WarmScheduledJobComponent().build_defs(context=None)


def test_duplicate_asset_name_in_jobs_raises(mod):
    with pytest.raises(ValueError, match="duplicate asset_name"):
        mod.WarmScheduledJobComponent(
            jobs=[
                {"asset_name": "dup", "schedule": "*/15 * * * *", "tick_fn": _TICK},
                {"asset_name": "dup", "schedule": "*/20 * * * *", "tick_fn": _TICK},
            ],
        ).build_defs(context=None)


def test_multi_job_shares_one_warmup_and_both_jobs_tick_independently(mod):
    """Two independently-scheduled jobs on ONE instance: warmup_fn must run
    exactly once (shared), and each job's tick_fn must fire on its own
    cadence with its own tick count -- the whole point of the multi-job
    extension (share one warm process/warmup across unrelated automations)."""
    component = mod.WarmScheduledJobComponent(
        warmup_fn=_WARMUP,
        max_seconds=30,
        jobs=[
            {"asset_name": "job_a", "schedule": "*/1 * * * * *", "second_precision": True,
             "tick_fn": _TICK_JOB_A, "max_ticks": 2},
            {"asset_name": "job_b", "schedule": "*/1 * * * * *", "second_precision": True,
             "tick_fn": _TICK_JOB_B, "max_ticks": 2},
        ],
    )
    defs = component.build_defs(context=None)
    asset_defs = list(defs.assets)
    # Real warm multi_asset is first -- second_precision means no
    # informational-schedule assets are added, so this must be the only asset.
    assert len(asset_defs) == 1
    result = dg.materialize(asset_defs)
    assert result.success

    assert _fixtures.CALLS["warmup_count"] == 1, "warmup must be shared across both jobs, not re-run per job"
    assert _fixtures.CALLS["job_a_ticks"] == 2
    assert _fixtures.CALLS["job_b_ticks"] == 2

    out_a = _metadata_for(result, "job_a", node_name="warm_scheduled_jobs_multi")
    out_b = _metadata_for(result, "job_b", node_name="warm_scheduled_jobs_multi")
    assert out_a["total_ticks"] == 2
    assert out_b["total_ticks"] == 2
    assert out_a["stop_reason"] == "max_ticks"
    assert out_b["stop_reason"] == "max_ticks"


def test_per_job_max_ticks_drops_out_while_other_job_keeps_running(mod):
    """job_a has a tight max_ticks cap; job_b has none (bounded only by
    max_seconds). job_a must stop firing once it hits its cap while job_b
    keeps ticking on its own cadence -- confirms per-job drop-out, not a
    global one-job-done-means-all-done behavior."""
    component = mod.WarmScheduledJobComponent(
        max_seconds=3,
        jobs=[
            {"asset_name": "job_a", "schedule": "*/1 * * * * *", "second_precision": True,
             "tick_fn": _TICK_JOB_A, "max_ticks": 1},
            {"asset_name": "job_b", "schedule": "*/1 * * * * *", "second_precision": True,
             "tick_fn": _TICK_JOB_B},
        ],
    )
    defs = component.build_defs(context=None)
    result = dg.materialize(list(defs.assets))
    assert result.success
    assert _fixtures.CALLS["job_a_ticks"] == 1
    assert _fixtures.CALLS["job_b_ticks"] >= 2, "job_b should keep ticking after job_a drops out"

    out_a = _metadata_for(result, "job_a", node_name="warm_scheduled_jobs_multi")
    out_b = _metadata_for(result, "job_b", node_name="warm_scheduled_jobs_multi")
    assert out_a["total_ticks"] == 1
    assert out_b["total_ticks"] == _fixtures.CALLS["job_b_ticks"]
    # Both report the same GLOBAL stop_reason (why the shared process exited)
    assert out_a["stop_reason"] == out_b["stop_reason"] == "max_seconds"


def test_informational_schedule_created_for_standard_cron_default_stopped(mod):
    component = mod.WarmScheduledJobComponent(
        asset_name="warm_job_out", schedule="*/15 * * * *", tick_fn=_TICK,
    )
    defs = component.build_defs(context=None)
    schedules = list(defs.schedules)
    assert len(schedules) == 1
    sched = schedules[0]
    assert sched.name == "warm_job_out_informational_schedule"
    assert sched.cron_schedule == "*/15 * * * *"
    assert sched.default_status == dg.DefaultScheduleStatus.STOPPED

    # Must target a SEPARATE, trivial no-op asset -- never the real warm
    # asset -- so flipping it on or manually launching it can't trigger a
    # second real automation run.
    all_asset_defs = list(defs.assets)
    assert len(all_asset_defs) == 2  # real warm multi_asset + the info marker asset


def test_no_informational_schedule_for_second_precision_job(mod):
    component = mod.WarmScheduledJobComponent(
        asset_name="warm_job_out", schedule="*/1 * * * * *", second_precision=True, tick_fn=_TICK,
    )
    defs = component.build_defs(context=None)
    assert list(defs.schedules) == []
    assert len(list(defs.assets)) == 1  # no info marker asset either


def test_expose_informational_schedules_false_suppresses_entirely(mod):
    component = mod.WarmScheduledJobComponent(
        asset_name="warm_job_out", schedule="*/15 * * * *", tick_fn=_TICK,
        expose_informational_schedules=False,
    )
    defs = component.build_defs(context=None)
    assert list(defs.schedules) == []
    assert len(list(defs.assets)) == 1


def test_informational_schedule_marker_asset_is_a_real_harmless_noop(mod):
    """Materializing the informational marker asset directly (simulating
    someone flipping the decorative schedule RUNNING, or manually launching
    it) must NOT invoke the real tick_fn or touch any real automation
    state -- the whole safety property this feature depends on."""
    component = mod.WarmScheduledJobComponent(
        asset_name="warm_job_out", schedule="*/15 * * * *", tick_fn=_TICK,
    )
    defs = component.build_defs(context=None)
    info_asset = next(a for a in defs.assets if "schedule_info" in str(a.key))
    result = dg.materialize([info_asset])
    assert result.success
    assert _fixtures.CALLS["tick_count"] == 0, "the informational marker must never invoke the real tick_fn"


def test_warmjob_rejects_a_non_job_def(mod):
    with pytest.raises(TypeError, match="expected a JobDefinition"):
        mod.warmjob(lambda: None)


def test_warmjob_exposes_the_underlying_job_def(mod):
    # Duck-typed, not isinstance(dispatcher, mod.WarmJobDispatcher): _fixtures.py
    # imports `warmjob` via a normal cached import, while `mod` here is a
    # SEPARATE spec-loaded copy of component.py (see load_component_module()) --
    # two distinct class objects for the same source, real only in test
    # isolation, not in a real single-import installation. Checking `.job`'s
    # shape is what actually matters.
    dispatcher = _fixtures.warmjob_test_job
    assert type(dispatcher).__name__ == "WarmJobDispatcher"
    assert isinstance(dispatcher.job, dg.JobDefinition)
    assert dispatcher.job.name == "warmjob_test_job"


def test_warmjob_dispatched_job_executes_real_op_and_bridges_warm_state_as_resources(mod):
    """The whole point of @warmjob: warmup_fn's shared warm_state (a dict)
    bridges directly into the dispatched job's normal resource system --
    the wrapped job's op just does context.resources.demo_resource, with no
    idea it's being dispatched from inside a warm process."""
    component = mod.WarmScheduledJobComponent(
        asset_name="warm_job_out",
        schedule="*/1 * * * * *",
        second_precision=True,
        warmup_fn=_WARMUP_DEMO_RESOURCE,
        tick_fn=_WARMJOB_TICK,
        max_ticks=1,
        max_seconds=15,
    )
    result = _materialize(component)
    assert result.success
    assert _fixtures.CALLS["warmjob_op_calls"] == 1, "the real op must actually execute, not be skipped/mocked"
    assert _fixtures.CALLS["warmjob_seen_resource"] == "resource_value_from_warmup"


def test_warmjob_dispatched_run_is_really_persisted_in_the_instance(mod):
    """Confirms the central claim of @warmjob: passing instance=context.instance
    (not the ephemeral ExecuteInProcessResult default) makes the dispatched
    execution a REAL, separately-persisted run in the SAME instance -- not
    an invisible in-memory-only execution. Uses an explicit, real
    DagsterInstance.ephemeral() (genuinely instance-backed, just not
    file-persisted to disk) so we can inspect get_runs() after the fact."""
    instance = dg.DagsterInstance.ephemeral()
    component = mod.WarmScheduledJobComponent(
        asset_name="warm_job_out",
        schedule="*/1 * * * * *",
        second_precision=True,
        warmup_fn=_WARMUP_DEMO_RESOURCE,
        tick_fn=_WARMJOB_TICK,
        max_ticks=2,
        max_seconds=15,
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], instance=instance)
    assert result.success

    all_runs = instance.get_runs()
    # The outer materialize's own run, PLUS one real persisted run per
    # dispatched tick (max_ticks=2 here) -- proves each dispatch is a
    # genuinely separate, real run, not an invisible in-memory execution.
    assert len(all_runs) == 3, f"expected 1 outer run + 2 dispatched runs, got {len(all_runs)}"
    dispatched_run_ids = {r.run_id for r in all_runs if r.job_name == "warmjob_test_job"}
    assert len(dispatched_run_ids) == 2
    for run_id in dispatched_run_ids:
        assert instance.get_run_by_id(run_id).is_success


def test_warmjob_dispatch_failure_flows_into_existing_tick_error_handling_continue(mod):
    component = mod.WarmScheduledJobComponent(
        asset_name="warm_job_out",
        schedule="*/1 * * * * *",
        second_precision=True,
        tick_fn=_WARMJOB_FAILING_TICK,
        tick_error_handling="continue",
        max_ticks=2,
        max_seconds=15,
    )
    result = _materialize(component)
    assert result.success
    out = _metadata_for(result, "warm_job_out")
    assert out["total_ticks"] == 2
    assert out["failed_ticks"] == 2


def test_warmjob_dispatch_failure_with_raise_fails_the_whole_run(mod):
    component = mod.WarmScheduledJobComponent(
        asset_name="warm_job_out",
        schedule="*/1 * * * * *",
        second_precision=True,
        tick_fn=_WARMJOB_FAILING_TICK,
        tick_error_handling="raise",
        max_seconds=15,
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], raise_on_error=False)
    assert not result.success


def test_no_isolation_tag_is_set(mod):
    """Regression guard for the component's core design invariant: it must
    NEVER set dagster/isolation=disabled, since the whole value proposition
    depends on genuine isolated execution."""
    component = mod.WarmScheduledJobComponent(
        asset_name="warm_job_out", schedule="*/15 * * * *", tick_fn=_TICK,
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    tags = asset_def.spec.tags if hasattr(asset_def, "spec") else {}
    assert "dagster/isolation" not in dict(tags)
