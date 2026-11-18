"""Committed tests for PythonSensorComponent -- both modes.

Callables are real, importable module-level functions in fixture_callables.py (the component
runs for real; no mocking of its own logic, only of the state the test fixtures mutate).
"""
import importlib
import json
from datetime import datetime, timedelta, timezone

import dagster as dg
import pytest

from .conftest import load_component_module

FIXTURES = "sensors.python_sensor.tests.fixture_callables"


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture(autouse=True)
def _reset_fixture_state():
    fixtures = importlib.import_module(FIXTURES)
    fixtures.reset()
    yield
    fixtures.reset()


@pytest.fixture()
def instance():
    with dg.DagsterInstance.ephemeral() as inst:
        yield inst


def _comp(mod, **kw):
    kw.setdefault("sensor_name", "s")
    kw.setdefault("job_name", "j")
    return mod.PythonSensorComponent(**kw)


def _sensor_of(comp):
    return list(comp.build_defs(None).sensors)[0]


def _tick(sensor, instance, cursor=None):
    return sensor(dg.build_sensor_context(instance=instance, cursor=cursor))


# ---- validation ----

def test_rejects_unknown_mode(mod):
    with pytest.raises(ValueError, match="mode"):
        _comp(mod, callable_path=f"{FIXTURES}:always_true", mode="bogus")


def test_rejects_unknown_fire_mode(mod):
    with pytest.raises(ValueError, match="fire_mode"):
        _comp(mod, callable_path=f"{FIXTURES}:always_true", fire_mode="bogus")


def test_discover_partitions_requires_dynamic_partitions_name(mod):
    with pytest.raises(ValueError, match="dynamic_partitions_name"):
        _comp(mod, callable_path=f"{FIXTURES}:list_of_item_keys", mode="discover_partitions")


# ---- trigger mode: every_true_tick (default) ----

def test_trigger_false_skips(mod, instance):
    sensor = _sensor_of(_comp(mod, callable_path=f"{FIXTURES}:always_false"))
    result = _tick(sensor, instance)
    assert isinstance(result, dg.SkipReason)


def test_trigger_true_fires(mod, instance):
    sensor = _sensor_of(_comp(mod, callable_path=f"{FIXTURES}:always_true"))
    result = _tick(sensor, instance)
    assert len(result.run_requests) == 1


def test_trigger_every_true_tick_fires_every_time(mod, instance):
    sensor = _sensor_of(_comp(mod, callable_path=f"{FIXTURES}:always_true"))
    r1 = _tick(sensor, instance)
    r2 = _tick(sensor, instance)
    assert len(r1.run_requests) == 1
    assert len(r2.run_requests) == 1


def test_trigger_tuple_payload_sets_run_config(mod, instance):
    sensor = _sensor_of(_comp(mod, callable_path=f"{FIXTURES}:true_with_payload"))
    result = _tick(sensor, instance)
    assert result.run_requests[0].run_config["ops"]["config"]["config"]["amount"] == 42


def test_trigger_dict_payload_sets_run_config(mod, instance):
    sensor = _sensor_of(_comp(mod, callable_path=f"{FIXTURES}:dict_payload"))
    result = _tick(sensor, instance)
    assert result.run_requests[0].run_config["ops"]["config"]["config"]["amount"] == 7


def test_callable_exception_returns_skip_not_crash(mod, instance):
    sensor = _sensor_of(_comp(mod, callable_path=f"{FIXTURES}:raises"))
    result = _tick(sensor, instance)
    assert isinstance(result, dg.SkipReason)


def test_static_args_and_kwargs_passed_through(mod, instance):
    sensor = _sensor_of(_comp(
        mod, callable_path=f"{FIXTURES}:with_static_args", args=["a_val", "b_val"], kwargs={"flag": True},
    ))
    _tick(sensor, instance)
    fixtures = importlib.import_module(FIXTURES)
    assert fixtures.STATE["last_args"] == ("a_val", "b_val", True)


# ---- trigger mode: fire_mode='once' ----

def test_fire_mode_once_fires_exactly_once(mod, instance):
    sensor = _sensor_of(_comp(mod, callable_path=f"{FIXTURES}:always_true", fire_mode="once"))
    r1 = _tick(sensor, instance)
    assert len(r1.run_requests) == 1
    assert r1.cursor == "fired"

    r2 = _tick(sensor, instance, cursor="fired")
    assert isinstance(r2, dg.SkipReason)


def test_fire_mode_once_waits_for_truthy(mod, instance):
    sensor = _sensor_of(_comp(mod, callable_path=f"{FIXTURES}:second_call_true", fire_mode="once"))
    r1 = _tick(sensor, instance)  # calls=1 -> False
    assert isinstance(r1, dg.SkipReason)
    r2 = _tick(sensor, instance)  # calls=2 -> True
    assert len(r2.run_requests) == 1


# ---- trigger mode: fire_mode='edge_rising' ----

def test_edge_rising_fires_only_on_transition(mod, instance):
    sensor = _sensor_of(_comp(mod, callable_path=f"{FIXTURES}:always_true", fire_mode="edge_rising"))
    r1 = _tick(sensor, instance)
    assert len(r1.run_requests) == 1
    assert r1.cursor == "true"

    r2 = _tick(sensor, instance, cursor="true")
    assert isinstance(r2, dg.SkipReason)


def test_edge_rising_rearms_after_falling_edge(mod, instance):
    sensor = _sensor_of(_comp(mod, callable_path=f"{FIXTURES}:always_false", fire_mode="edge_rising"))
    r1 = _tick(sensor, instance, cursor="true")  # was true, now false -> reset
    assert r1.cursor == "false"


# ---- discover_partitions mode ----

def _set_items(items):
    fixtures = importlib.import_module(FIXTURES)
    fixtures.STATE["items"] = items


def test_discover_partitions_fires_once_per_new_item(mod, instance, tmp_path):
    _set_items(["a", "b"])
    state_path = str(tmp_path / "state.json")
    sensor = _sensor_of(_comp(
        mod, callable_path=f"{FIXTURES}:list_of_item_keys", mode="discover_partitions",
        dynamic_partitions_name="items", dynamic_partition_state_path=state_path,
    ))
    result = _tick(sensor, instance)
    assert {rr.partition_key for rr in result.run_requests} == {"a", "b"}
    add_reqs = [r for r in result.dynamic_partitions_requests if type(r).__name__ == "AddDynamicPartitionsRequest"]
    assert add_reqs[0].partition_keys == ["a", "b"] or set(add_reqs[0].partition_keys) == {"a", "b"}


def test_discover_partitions_dedupes_already_known_items(mod, instance, tmp_path):
    _set_items(["a", "b"])
    state_path = str(tmp_path / "state.json")
    sensor = _sensor_of(_comp(
        mod, callable_path=f"{FIXTURES}:list_of_item_keys", mode="discover_partitions",
        dynamic_partitions_name="items", dynamic_partition_state_path=state_path,
    ))
    _tick(sensor, instance)

    _set_items(["a", "b", "c"])
    result = _tick(sensor, instance)
    assert {rr.partition_key for rr in result.run_requests} == {"c"}


def test_discover_partitions_tuple_payload_sets_run_config(mod, instance, tmp_path):
    _set_items([("x", {"foo": "bar"})])
    state_path = str(tmp_path / "state.json")
    sensor = _sensor_of(_comp(
        mod, callable_path=f"{FIXTURES}:list_of_item_tuples", mode="discover_partitions",
        dynamic_partitions_name="items", dynamic_partition_state_path=state_path,
    ))
    result = _tick(sensor, instance)
    rr = result.run_requests[0]
    assert rr.partition_key == "x"
    assert rr.run_config["ops"]["config"]["config"]["foo"] == "bar"


def test_discover_partitions_no_items_skips(mod, instance, tmp_path):
    _set_items([])
    state_path = str(tmp_path / "state.json")
    sensor = _sensor_of(_comp(
        mod, callable_path=f"{FIXTURES}:list_of_item_keys", mode="discover_partitions",
        dynamic_partitions_name="items", dynamic_partition_state_path=state_path,
    ))
    result = _tick(sensor, instance)
    assert isinstance(result, dg.SkipReason)


def test_discover_partitions_prunes_stale_entries(mod, instance, tmp_path):
    state_path = str(tmp_path / "state.json")
    old_seen = (datetime.now(timezone.utc) - timedelta(days=10)).isoformat()
    with open(state_path, "w") as f:
        json.dump({"stale_key": old_seen}, f)

    _set_items([])
    sensor = _sensor_of(_comp(
        mod, callable_path=f"{FIXTURES}:list_of_item_keys", mode="discover_partitions",
        dynamic_partitions_name="items", dynamic_partition_state_path=state_path,
        dynamic_partition_keep_days=7,
    ))
    result = _tick(sensor, instance)
    delete_reqs = [r for r in result.dynamic_partitions_requests if type(r).__name__ == "DeleteDynamicPartitionsRequest"]
    assert delete_reqs[0].partition_keys == ["stale_key"]


def test_discover_partitions_without_keep_days_still_persists_dedup_state(mod, instance, tmp_path):
    state_path = str(tmp_path / "state.json")
    _set_items(["a"])
    sensor = _sensor_of(_comp(
        mod, callable_path=f"{FIXTURES}:list_of_item_keys", mode="discover_partitions",
        dynamic_partitions_name="items", dynamic_partition_state_path=state_path,
    ))
    _tick(sensor, instance)
    assert "a" in json.loads(open(state_path).read())
