"""Committed tests for RedisStreamObservationSensorComponent's resource_key.

Only the external service is faked; the sensor runs for real under direct invocation
(dg.build_sensor_context()).
"""
import dagster as dg
import pytest

from .conftest import FakeObserveResource, FakeRedis, install_fake_redis, load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def fake(monkeypatch):
    return install_fake_redis(monkeypatch)


def _component(mod, **kw):
    return mod.RedisStreamObservationSensorComponent(
        sensor_name="redis_stream_obs", asset_key="external/redis_stream", stream_name="orders:events", **kw)


def test_resource_key_default_none_and_inline_path_unchanged(mod, fake):
    comp = _component(mod, host="inline.example")
    assert comp.resource_key is None
    sensor_def = comp.build_defs(None).sensors[0]
    assert sensor_def.required_resource_keys == set()
    result = sensor_def(dg.build_sensor_context())
    (client,) = fake.constructed
    assert client.cfg["host"] == "inline.example"
    (event,) = result.asset_events
    assert isinstance(event, dg.AssetMaterialization)
    assert event.metadata["stream_length"].value == 42


def test_resource_path_calls_observe_not_inline_config(mod, fake):
    comp = _component(mod, resource_key="redis_shared", host="inline.example")
    sensor_def = comp.build_defs(None).sensors[0]
    assert sensor_def.required_resource_keys == {"redis_shared"}
    shared = FakeObserveResource({"stream_length": 7, "data_version": "v1"})
    result = sensor_def(dg.build_sensor_context(resources={"redis_shared": shared}))
    assert shared.calls == ["orders:events"]
    assert fake.constructed == []  # redis.Redis(...) (inline path) never called
    (event,) = result.asset_events
    assert event.metadata["stream_length"].value == 7
