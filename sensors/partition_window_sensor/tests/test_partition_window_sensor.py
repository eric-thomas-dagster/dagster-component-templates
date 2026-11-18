"""Real, committed tests for PartitionWindowSensorComponent -- real dg.DagsterInstance
throughout (instance.report_runless_asset_event to simulate upstream materializations,
instance.get_materialized_partitions read back by the sensor itself), no mocking.
"""
from datetime import datetime, timedelta

import pytest

from .conftest import materialize_partitions, sensor_of, tick

FIXTURES_IGNORE = None  # no external callables needed -- this sensor has no callable_path


def _comp(mod, **kw):
    kw.setdefault("sensor_name", "s")
    kw.setdefault("upstream_asset_key", "up/thing")
    kw.setdefault("job_name", "j")
    return mod.PartitionWindowSensorComponent(**kw)


# --- validation --------------------------------------------------------------------

def test_segment_requires_segment_keys(mod):
    with pytest.raises(ValueError, match="segment_keys"):
        _comp(mod, mode="rollup", window_type="segment")


def test_minimum_count_wait_policy_requires_minimum_count(mod):
    with pytest.raises(ValueError, match="minimum_count"):
        _comp(mod, mode="rollup", window_type="segment", segment_keys=["us"], wait_policy="minimum_count")


def test_fan_out_time_based_requires_max_downstream_keys(mod):
    with pytest.raises(ValueError, match="max_downstream_keys"):
        _comp(mod, mode="fan_out", window_type="day")


# --- rollup + segment (segment_region_stats_rollup) ---------------------------------

def test_rollup_segment_wait_for_all_does_not_fire_until_every_key_present(mod, instance):
    component = _comp(mod, mode="rollup", window_type="segment", segment_keys=["us", "eu", "apac"])
    sensor = sensor_of(component)

    materialize_partitions(instance, "up/thing", ["us", "eu"])
    result = tick(sensor, instance)
    assert not result.run_requests

    materialize_partitions(instance, "up/thing", ["apac"])
    result2 = tick(sensor, instance, cursor=result.cursor)
    assert len(result2.run_requests) == 1


def test_rollup_segment_does_not_refire_once_fired(mod, instance):
    component = _comp(mod, mode="rollup", window_type="segment", segment_keys=["us", "eu", "apac"])
    sensor = sensor_of(component)
    materialize_partitions(instance, "up/thing", ["us", "eu", "apac"])

    result = tick(sensor, instance)
    assert len(result.run_requests) == 1

    result2 = tick(sensor, instance, cursor=result.cursor)
    assert not result2.run_requests


# --- rollup + segment, minimum_count (segment_region_stats_early_rollup) ------------

def test_rollup_segment_minimum_count_fires_early(mod, instance):
    component = _comp(
        mod, mode="rollup", window_type="segment", segment_keys=["us", "eu", "apac"],
        wait_policy="minimum_count", minimum_count=2,
    )
    sensor = sensor_of(component)

    materialize_partitions(instance, "up/thing", ["us"])
    result = tick(sensor, instance)
    assert not result.run_requests

    materialize_partitions(instance, "up/thing", ["eu"])
    result2 = tick(sensor, instance, cursor=result.cursor)
    assert len(result2.run_requests) == 1  # fires with only 2 of 3 -- apac never arrived


# --- rollup + day window (daily_team_a_rollup) --------------------------------------

def test_rollup_day_window_waits_for_all_24_hours(mod, instance):
    component = _comp(mod, mode="rollup", window_type="day")
    sensor = sensor_of(component)
    day = "2026-03-02"

    materialize_partitions(instance, "up/thing", [f"{day}-{h:02d}:00" for h in range(12)])
    result = tick(sensor, instance)
    assert not result.run_requests

    materialize_partitions(instance, "up/thing", [f"{day}-{h:02d}:00" for h in range(12, 24)])
    result2 = tick(sensor, instance, cursor=result.cursor)
    assert len(result2.run_requests) == 1
    assert result2.run_requests[0].partition_key == day


def test_rollup_day_window_only_fires_the_complete_day(mod, instance):
    component = _comp(mod, mode="rollup", window_type="day")
    sensor = sensor_of(component)

    complete_day = "2026-03-02"
    incomplete_day = "2026-03-03"
    materialize_partitions(instance, "up/thing", [f"{complete_day}-{h:02d}:00" for h in range(24)])
    materialize_partitions(instance, "up/thing", [f"{incomplete_day}-{h:02d}:00" for h in range(5)])

    result = tick(sensor, instance)
    assert [rr.partition_key for rr in result.run_requests] == [complete_day]


# --- rollup + week window (chained rollup, Monday-start) -----------------------------

def test_rollup_week_window_anchors_to_monday_and_waits_for_7_days(mod, instance):
    component = _comp(
        mod, mode="rollup", window_type="week",
        upstream_key_format="%Y-%m-%d", downstream_key_format="%Y-%m-%d",
    )
    sensor = sensor_of(component)

    monday = datetime(2026, 3, 2)  # a real Monday
    assert monday.weekday() == 0
    days = [(monday + timedelta(days=d)).strftime("%Y-%m-%d") for d in range(7)]

    materialize_partitions(instance, "up/thing", days[:6])
    result = tick(sensor, instance)
    assert not result.run_requests

    materialize_partitions(instance, "up/thing", [days[6]])
    result2 = tick(sensor, instance, cursor=result.cursor)
    assert len(result2.run_requests) == 1
    assert result2.run_requests[0].partition_key == monday.strftime("%Y-%m-%d")


# --- rollup + month window (monthly_team_a_rollup) -----------------------------------

def test_rollup_month_window_waits_for_every_day_in_the_month(mod, instance):
    component = _comp(
        mod, mode="rollup", window_type="month",
        upstream_key_format="%Y-%m-%d", downstream_key_format="%Y-%m-%d",
    )
    sensor = sensor_of(component)

    # April 2026 has 30 days.
    all_days = [f"2026-04-{d:02d}" for d in range(1, 31)]
    materialize_partitions(instance, "up/thing", all_days[:-1])
    result = tick(sensor, instance)
    assert not result.run_requests

    materialize_partitions(instance, "up/thing", [all_days[-1]])
    result2 = tick(sensor, instance, cursor=result.cursor)
    assert len(result2.run_requests) == 1
    assert result2.run_requests[0].partition_key == "2026-04-01"


# --- fan_out + segment (scatter_live_region_to_segments) -----------------------------

def test_fan_out_segment_fires_one_run_per_new_matching_key(mod, instance):
    component = _comp(mod, mode="fan_out", window_type="segment", segment_keys=["us", "eu", "apac"])
    sensor = sensor_of(component)

    materialize_partitions(instance, "up/thing", ["us"])
    result = tick(sensor, instance)
    assert [rr.partition_key for rr in result.run_requests] == ["us"]

    materialize_partitions(instance, "up/thing", ["eu"])
    result2 = tick(sensor, instance, cursor=result.cursor)
    assert [rr.partition_key for rr in result2.run_requests] == ["eu"]  # "us" not refired


def test_fan_out_segment_ignores_keys_outside_the_declared_set(mod, instance):
    component = _comp(mod, mode="fan_out", window_type="segment", segment_keys=["us", "eu"])
    sensor = sensor_of(component)
    materialize_partitions(instance, "up/thing", ["us", "not_a_segment"])
    result = tick(sensor, instance)
    assert [rr.partition_key for rr in result.run_requests] == ["us"]


# --- fan_out + week window (daily_inference / trailing_week_inference) ---------------

def test_fan_out_week_forward_fans_out_from_the_anchor(mod, instance):
    component = _comp(
        mod, mode="fan_out", window_type="week", max_downstream_keys=7, direction="forward",
        upstream_key_format="%Y-%m-%d", downstream_key_format="%Y-%m-%d",
    )
    sensor = sensor_of(component)
    monday = datetime(2026, 3, 2)
    materialize_partitions(instance, "up/thing", [monday.strftime("%Y-%m-%d")])

    result = tick(sensor, instance)
    got = sorted(rr.partition_key for rr in result.run_requests)
    expected = sorted((monday + timedelta(days=d)).strftime("%Y-%m-%d") for d in range(7))
    assert got == expected


def test_fan_out_week_backward_fans_out_ending_at_the_anchor(mod, instance):
    component = _comp(
        mod, mode="fan_out", window_type="week", max_downstream_keys=7, direction="backward",
        upstream_key_format="%Y-%m-%d", downstream_key_format="%Y-%m-%d",
    )
    sensor = sensor_of(component)
    anchor = datetime(2026, 3, 8)
    materialize_partitions(instance, "up/thing", [anchor.strftime("%Y-%m-%d")])

    result = tick(sensor, instance)
    got = sorted(rr.partition_key for rr in result.run_requests)
    expected = sorted((anchor - timedelta(days=d)).strftime("%Y-%m-%d") for d in range(7))
    assert got == expected


def test_fan_out_does_not_refire_already_seen_upstream_keys(mod, instance):
    component = _comp(
        mod, mode="fan_out", window_type="week", max_downstream_keys=7,
        upstream_key_format="%Y-%m-%d", downstream_key_format="%Y-%m-%d",
    )
    sensor = sensor_of(component)
    anchor = datetime(2026, 3, 2)
    materialize_partitions(instance, "up/thing", [anchor.strftime("%Y-%m-%d")])

    result = tick(sensor, instance)
    assert len(result.run_requests) == 7

    result2 = tick(sensor, instance, cursor=result.cursor)
    assert not result2.run_requests
