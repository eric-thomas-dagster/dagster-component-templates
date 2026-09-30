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

    out = result.output_for_node("warm_job_out")
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
    out = result.output_for_node("warm_job_out")
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
    out = result.output_for_node("warm_job_out")
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
    out = result.output_for_node("warm_job_out")
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
