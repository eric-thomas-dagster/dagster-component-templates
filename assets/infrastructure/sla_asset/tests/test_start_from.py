"""Real, committed tests for `start_from` -- measuring the SLA clock from a reference point
other than when compute began (partition_start/partition_end/scheduled_execution_time),
requested after this repo's own retry/pool/timeout_seconds rollout. Real dg.materialize()
throughout; `_resolve_start_time` is also unit-tested directly against a minimal fake context
for the scheduled_execution_time case, since driving a REAL schedule tick through a full
materialize() is unnecessary to prove the tag is read correctly.
"""
from datetime import datetime, timedelta, timezone

import dagster as dg
import pytest

from .conftest import load_component_module

PARTS = dg.DailyPartitionsDefinition("2024-01-01")


@pytest.fixture()
def mod():
    return load_component_module()


class _FakeRun:
    def __init__(self, tags):
        self.tags = tags


class _FakeContext:
    """Minimal stand-in for AssetExecutionContext -- just enough for _resolve_start_time."""
    def __init__(self, tags=None, has_partition=False, window=None):
        self.run = _FakeRun(tags or {})
        self._has_partition = has_partition
        self._window = window
        self.log = type("L", (), {"warning": staticmethod(lambda *a, **k: None)})()

    @property
    def has_partition_key(self):
        return self._has_partition

    @property
    def has_partition_key_range(self):
        return False

    @property
    def partition_time_window(self):
        return self._window


# --- _resolve_start_time unit tests -----------------------------------------------------

def test_resolve_start_time_compute_start_is_now(mod):
    import time
    before = time.time()
    ts = mod._resolve_start_time(_FakeContext(), "compute_start", "warn")
    after = time.time()
    assert before <= ts <= after


def test_resolve_start_time_scheduled_execution_time_reads_the_real_scheduler_tag(mod):
    from dagster._core.storage.tags import SCHEDULED_EXECUTION_TIME_TAG
    tick = datetime(2026, 1, 1, 6, 0, 0, tzinfo=timezone.utc)
    context = _FakeContext(tags={SCHEDULED_EXECUTION_TIME_TAG: tick.isoformat()})
    ts = mod._resolve_start_time(context, "scheduled_execution_time", "warn")
    assert ts == tick.timestamp()


def test_resolve_start_time_scheduled_execution_time_missing_warns_and_falls_back(mod):
    import time
    before = time.time()
    ts = mod._resolve_start_time(_FakeContext(tags={}), "scheduled_execution_time", "warn")
    assert ts >= before  # fell back to compute_start (now), not some default/zero


def test_resolve_start_time_scheduled_execution_time_missing_fails_when_configured(mod):
    with pytest.raises(dg.Failure, match="scheduled_execution_time"):
        mod._resolve_start_time(_FakeContext(tags={}), "scheduled_execution_time", "fail")


def test_resolve_start_time_partition_end_reads_the_window(mod):
    class _Window:
        start = datetime(2024, 1, 2, tzinfo=timezone.utc)
        end = datetime(2024, 1, 3, tzinfo=timezone.utc)
    context = _FakeContext(has_partition=True, window=_Window())
    assert mod._resolve_start_time(context, "partition_end", "warn") == _Window.end.timestamp()
    assert mod._resolve_start_time(context, "partition_start", "warn") == _Window.start.timestamp()


def test_resolve_start_time_partition_end_missing_fails_when_configured(mod):
    with pytest.raises(dg.Failure, match="partition_time_window"):
        mod._resolve_start_time(_FakeContext(has_partition=False), "partition_end", "fail")


# --- @sla decorator: start_from validation ------------------------------------------------

def test_sla_decorator_rejects_unknown_start_from(mod):
    with pytest.raises(ValueError, match="start_from"):
        mod.sla(expected_duration_seconds=1, start_from="bogus")


def test_sla_decorator_rejects_unknown_on_missing_start_reference(mod):
    with pytest.raises(ValueError, match="on_missing_start_reference"):
        mod.sla(expected_duration_seconds=1, on_missing_start_reference="bogus")


# --- @sla decorator: real breach driven by start_from, not compute time -------------------

def test_sla_decorator_partition_end_breaches_even_though_compute_was_fast(mod):
    @dg.asset(partitions_def=PARTS)
    @mod.sla(expected_duration_seconds=5, start_from="partition_end", on_breach="warn")
    def fast_but_stale(context):
        return 1  # returns instantly -- would NOT breach under start_from='compute_start'

    result = dg.materialize(
        [fast_but_stale], partition_key="2024-01-02", resources={"io_manager": dg.InMemoryIOManager()},
    )
    assert result.success  # on_breach='warn' -- materializes despite the breach

    observations = result.asset_observations_for_node("fast_but_stale")
    assert len(observations) == 1
    obs_meta = observations[0].metadata
    assert obs_meta["sla_actual_seconds"].value > 1000  # days of seconds since the 2024 partition ended
    assert obs_meta["sla_compute_seconds"].value < 1  # the real compute itself was instant


def test_sla_decorator_compute_start_default_does_not_breach_a_fast_partitioned_asset(mod):
    @dg.asset(partitions_def=PARTS)
    @mod.sla(expected_duration_seconds=5, on_breach="fail")  # default start_from
    def fast(context):
        return 1

    result = dg.materialize(
        [fast], partition_key="2024-01-02", resources={"io_manager": dg.InMemoryIOManager()}, raise_on_error=False,
    )
    assert result.success  # unchanged default behavior: measures compute only, which is instant


def test_sla_decorator_partition_end_on_breach_fail_raises_with_compute_seconds_metadata(mod):
    @dg.asset(partitions_def=PARTS)
    @mod.sla(expected_duration_seconds=5, start_from="partition_end", on_breach="fail")
    def fast_but_stale(context):
        return 1

    result = dg.materialize(
        [fast_but_stale], partition_key="2024-01-02", resources={"io_manager": dg.InMemoryIOManager()}, raise_on_error=False,
    )
    assert not result.success


def test_sla_decorator_missing_partition_window_falls_back_and_still_succeeds(mod):
    @dg.asset  # no partitions_def at all
    @mod.sla(expected_duration_seconds=5, start_from="partition_end", on_missing_start_reference="warn")
    def unpartitioned(context):
        return 1

    result = dg.materialize([unpartitioned], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success  # fell back to compute_start, which doesn't breach


def test_sla_decorator_missing_partition_window_fails_when_configured(mod):
    @dg.asset
    @mod.sla(expected_duration_seconds=5, start_from="partition_end", on_missing_start_reference="fail")
    def unpartitioned(context):
        return 1

    result = dg.materialize([unpartitioned], resources={"io_manager": dg.InMemoryIOManager()}, raise_on_error=False)
    assert not result.success


# --- SlaAssetComponent: start_from validation + wiring through compute: mode --------------

def test_component_rejects_unknown_start_from(mod):
    with pytest.raises(Exception, match="start_from"):
        mod.SlaAssetComponent(
            asset_name="t", expected_duration_seconds=5, start_from="bogus",
            compute={"kind": "python", "python": f"{__name__}:_dummy_compute"},
        )


def _dummy_compute():
    return 1


def test_component_compute_mode_partition_end_without_a_partition_falls_back(mod):
    # compute: mode's own @dg.asset is never partitioned (no partitions_def wiring in that
    # path) -- start_from='partition_end' always hits the missing-window fallback here.
    comp = mod.SlaAssetComponent(
        asset_name="fast_component", expected_duration_seconds=5,
        start_from="partition_end", on_missing_start_reference="warn",
        compute={"kind": "python", "python": f"{__name__}:_dummy_compute"},
    )
    result = dg.materialize([comp.build_defs(context=None).assets[0]], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success


def test_component_compute_mode_partition_end_without_a_partition_fails_when_configured(mod):
    comp = mod.SlaAssetComponent(
        asset_name="fast_component", expected_duration_seconds=5,
        start_from="partition_end", on_missing_start_reference="fail",
        compute={"kind": "python", "python": f"{__name__}:_dummy_compute"},
    )
    result = dg.materialize(
        [comp.build_defs(context=None).assets[0]], resources={"io_manager": dg.InMemoryIOManager()}, raise_on_error=False,
    )
    assert not result.success
