"""SQLMonitorSensorComponent: partition_mode='dynamic_partition'/'both' -- the Item B
(dynamic task mapping) gap for a SQL-query discovery source, mirroring s3_monitor's own
dynamic-partition mechanism (AddDynamicPartitionsRequest + RunRequest(partition_key=...)) and
its pruning behavior exactly.

Only the external DB boundary is faked, via the resource-backed seam (`resource_key`) already
used for sql_monitor's demo-mode path -- avoids mocking a real SQLAlchemy engine for determinism.
"""
import json
from datetime import datetime, timedelta, timezone

import dagster as dg
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def instance():
    with dg.DagsterInstance.ephemeral() as inst:
        yield inst


class FakeSqlResource:
    """Fake matching sql_monitor's resource-contract: .poll(table_name, cursor) -> list of
    {id, ts, source, payload}."""

    def __init__(self, events_by_cursor=None, events=None):
        # Either a fixed list every tick, or a dict keyed by the cursor value passed in.
        self._events = events
        self._events_by_cursor = events_by_cursor or {}

    def poll(self, table_name, cursor):
        if self._events is not None:
            return self._events
        return self._events_by_cursor.get(cursor, [])


def _comp(mod, resource, **kw):
    kw.setdefault("sensor_name", "watch")
    kw.setdefault("connection_string_env_var", "DATABASE_URL")
    kw.setdefault("table_name", "orders")
    kw.setdefault("watermark_column", "updated_at")
    kw.setdefault("job_name", "process_job")
    kw.setdefault("resource_key", "db")
    component = mod.SQLMonitorSensorComponent(**kw)
    sensor = list(component.build_defs(None).sensors)[0]
    return sensor


def _tick(sensor, instance, resource, cursor=None):
    return sensor(dg.build_sensor_context(instance=instance, resources={"db": resource}, cursor=cursor))


def _add_requests(result):
    return [r for r in (result.dynamic_partitions_requests or []) if type(r).__name__ == "AddDynamicPartitionsRequest"]


def _delete_requests(result):
    return [r for r in (result.dynamic_partitions_requests or []) if type(r).__name__ == "DeleteDynamicPartitionsRequest"]


# ---- validation ----

def test_rejects_unknown_partition_mode(mod):
    comp = mod.SQLMonitorSensorComponent(
        sensor_name="s", connection_string_env_var="X", table_name="t", watermark_column="w",
        job_name="j", resource_key="db", partition_mode="bogus",
    )
    with pytest.raises(ValueError, match="partition_mode"):
        comp.build_defs(None)


def test_dynamic_partition_requires_dynamic_partitions_name(mod):
    comp = mod.SQLMonitorSensorComponent(
        sensor_name="s", connection_string_env_var="X", table_name="t", watermark_column="w",
        job_name="j", resource_key="db", partition_mode="dynamic_partition",
    )
    with pytest.raises(ValueError, match="dynamic_partitions_name"):
        comp.build_defs(None)


# ---- dynamic_partition mode ----

def test_dynamic_partition_mode_registers_partition_and_run_request(mod, instance):
    resource = FakeSqlResource(events=[{"id": "42", "ts": "2026-01-01", "payload": {"amount": "9.99"}}])
    sensor = _comp(mod, resource, partition_mode="dynamic_partition", dynamic_partitions_name="order_ids")
    result = _tick(sensor, instance, resource)

    assert len(result.run_requests) == 1
    rr = result.run_requests[0]
    assert rr.partition_key == "42"
    assert not rr.run_config  # dynamic_partition-only: no run_config embedded

    add_reqs = _add_requests(result)
    assert len(add_reqs) == 1
    assert add_reqs[0].partition_keys == ["42"]
    assert add_reqs[0].partitions_def_name == "order_ids"


def test_both_mode_includes_run_config_and_partition_key(mod, instance):
    resource = FakeSqlResource(events=[{"id": "7", "ts": "2026-01-01", "payload": {"x": "1"}}])
    sensor = _comp(mod, resource, partition_mode="both", dynamic_partitions_name="order_ids")
    result = _tick(sensor, instance, resource)

    rr = result.run_requests[0]
    assert rr.partition_key == "7"
    assert rr.run_config["ops"]["config"]["config"]["row_id"] == "7"


def test_run_config_mode_unaffected(mod, instance):
    resource = FakeSqlResource(events=[{"id": "7", "ts": "2026-01-01", "payload": {"x": "1"}}])
    sensor = _comp(mod, resource)  # default partition_mode='run_config'
    result = _tick(sensor, instance, resource)

    rr = result.run_requests[0]
    assert rr.partition_key is None
    assert rr.run_config["ops"]["config"]["config"]["row_id"] == "7"
    assert result.dynamic_partitions_requests in (None, [])


def test_custom_partition_key_template(mod, instance):
    resource = FakeSqlResource(events=[{"id": "7", "ts": "2026-06-01", "payload": {}}])
    sensor = _comp(
        mod, resource, partition_mode="dynamic_partition", dynamic_partitions_name="order_ids",
        partition_key_template="orders/{row_id}/{watermark}",
    )
    result = _tick(sensor, instance, resource)
    assert result.run_requests[0].partition_key == "orders/7/2026-06-01"


# ---- pruning (mirrors s3_monitor's own tests) ----

def test_keep_days_unset_never_touches_the_state_file(mod, tmp_path, instance):
    state_path = str(tmp_path / "state.json")
    resource = FakeSqlResource(events=[])
    sensor = _comp(
        mod, resource, partition_mode="dynamic_partition", dynamic_partitions_name="order_ids",
        dynamic_partition_state_path=state_path,
    )
    _tick(sensor, instance, resource)
    import os
    assert not os.path.exists(state_path)


def test_partitions_older_than_keep_days_are_pruned(mod, tmp_path, instance):
    state_path = str(tmp_path / "state.json")
    old_seen = (datetime.now(timezone.utc) - timedelta(days=10)).isoformat()
    with open(state_path, "w") as f:
        json.dump({"stale_key": old_seen}, f)

    resource = FakeSqlResource(events=[])
    sensor = _comp(
        mod, resource, partition_mode="dynamic_partition", dynamic_partitions_name="order_ids",
        dynamic_partition_state_path=state_path, dynamic_partition_keep_days=7,
    )
    result = _tick(sensor, instance, resource)

    delete_reqs = _delete_requests(result)
    assert len(delete_reqs) == 1
    assert delete_reqs[0].partition_keys == ["stale_key"]


def test_partitions_within_keep_days_are_not_pruned(mod, tmp_path, instance):
    state_path = str(tmp_path / "state.json")
    recent_seen = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
    with open(state_path, "w") as f:
        json.dump({"fresh_key": recent_seen}, f)

    resource = FakeSqlResource(events=[])
    sensor = _comp(
        mod, resource, partition_mode="dynamic_partition", dynamic_partitions_name="order_ids",
        dynamic_partition_state_path=state_path, dynamic_partition_keep_days=7,
    )
    result = _tick(sensor, instance, resource)
    assert _delete_requests(result) == []


def test_pruning_runs_even_on_a_quiet_tick(mod, tmp_path, instance):
    state_path = str(tmp_path / "state.json")
    old_seen = (datetime.now(timezone.utc) - timedelta(days=10)).isoformat()
    with open(state_path, "w") as f:
        json.dump({"stale_key": old_seen}, f)

    resource = FakeSqlResource(events=[])
    sensor = _comp(
        mod, resource, partition_mode="dynamic_partition", dynamic_partitions_name="order_ids",
        dynamic_partition_state_path=state_path, dynamic_partition_keep_days=7,
    )
    result = _tick(sensor, instance, resource)
    assert not result.run_requests
    assert len(_delete_requests(result)) == 1
