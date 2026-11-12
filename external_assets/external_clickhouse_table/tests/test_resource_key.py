"""resource_key tests for ExternalClickHouseTableComponent's create_observation_sensor=True path.

resource_key already existed on this component for a custom `.observe(source) -> dict` resource
contract -- that path is untouched and still tested here. What's new: a resource exposing only
`.get_client()` (the shape the shared ClickHouseResourceComponent, resources/clickhouse_resource,
actually provides) is now ALSO accepted on resource_key, via the same system-table queries the
native env-var path runs. The unset-resource_key path (native clickhouse-connect, env vars) is
exercised unchanged.
"""
import dagster as dg
import pytest

from .conftest import (
    FakeClickHouseClient,
    FakeGetClientResource,
    FakeObserveResource,
    install_fake_clickhouse_connect,
    load_component_module,
)

mod = load_component_module()


def _comp(**kw):
    args = dict(
        asset_key="clickhouse/analytics/events", database="analytics", table="events",
        host_env_var="CH_HOST", create_observation_sensor=True,
    )
    args.update(kw)
    return mod.ExternalClickHouseTableComponent(**args)


def _run(comp, resources):
    defs = comp.build_defs(None)
    sensor_def = next(iter(defs.sensors))
    ctx = dg.build_sensor_context(resources=resources)
    return sensor_def(ctx)


def test_resource_key_default_is_none():
    assert _comp().resource_key is None


def test_no_sensor_when_create_observation_sensor_false():
    comp = _comp(create_observation_sensor=False, resource_key="ch_shared")
    defs = comp.build_defs(None)
    assert not defs.sensors
    assert len(list(defs.assets)) == 1


def test_required_resource_keys_includes_key_when_set():
    comp = _comp(resource_key="ch_shared")
    sensor = next(iter(comp.build_defs(None).sensors))
    assert sensor.required_resource_keys == {"ch_shared"}


def test_required_resource_keys_empty_when_unset():
    sensor = next(iter(_comp().build_defs(None).sensors))
    assert sensor.required_resource_keys == set()


def test_observe_contract_resource_still_works_unchanged():
    """Pre-existing contract: resource.observe('db.table') -> {'data_version': ..., **metadata}."""
    resource = FakeObserveResource({"data_version": "42-x", "row_count": dg.MetadataValue.int(42)})
    comp = _comp(resource_key="ch_shared")
    result = _run(comp, {"ch_shared": resource})
    assert resource.calls == ["analytics.events"]
    assert len(result.asset_events) == 1
    event = result.asset_events[0]
    assert event.tags["dagster/data_version"] == "42-x"
    assert event.metadata["row_count"].value == 42


def test_get_client_only_resource_uses_shared_client_for_system_table_queries():
    """New: a resource with only get_client() (the shared ClickHouseResourceComponent's shape) is
    observed via the same system-table queries the native path runs -- resource.get_client() is
    actually called, not resource.observe()."""
    client = FakeClickHouseClient(row_count=7, size_bytes=1024, last_modified="2024-01-02 03:04:05", parts_count=2, engine="MergeTree")
    resource = FakeGetClientResource(client)
    comp = _comp(resource_key="ch_shared")
    result = _run(comp, {"ch_shared": resource})
    assert len(client.commands) == 5  # all 5 system-table queries ran against the shared client
    assert len(result.asset_events) == 1
    event = result.asset_events[0]
    assert event.metadata["row_count"].value == 7
    assert event.metadata["engine"].value == "MergeTree"
    assert event.tags["dagster/data_version"] == "7-2024-01-02 03:04:05"


def test_unset_resource_key_still_uses_native_env_var_path(monkeypatch):
    monkeypatch.setenv("CH_HOST", "inline-host")
    client = FakeClickHouseClient(row_count=3, last_modified="2024-02-01", engine="MergeTree")
    install_fake_clickhouse_connect(monkeypatch, client)
    comp = _comp()
    assert comp.resource_key is None
    result = _run(comp, {})
    assert client.kwargs["host"] == "inline-host"
    assert len(result.asset_events) == 1
    assert result.asset_events[0].metadata["row_count"].value == 3


def test_resource_not_registered_fails_loudly():
    # required_resource_keys={"missing"} on the sensor itself means Dagster's own resource
    # validation raises before the component's own body-level None-check ever runs.
    comp = _comp(resource_key="missing")
    with pytest.raises(dg.DagsterInvalidDefinitionError, match="missing"):
        _run(comp, {})
