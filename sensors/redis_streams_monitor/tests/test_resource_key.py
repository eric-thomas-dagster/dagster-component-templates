"""Committed tests for RedisStreamsMonitorSensorComponent's resource_key wiring.

The field and required_resource_keys plumbing already existed, but the sensor body
unconditionally built its own redis.Redis(...) client -- resource_key was set and wired
into required_resource_keys, but never actually consulted. These tests confirm the fix:
when resource_key is set, the registered resource's get_client() is used instead of the
inline host/port/password_env_var fields; when unset, the inline path is unchanged.
"""
import dagster as dg
import pytest

from .conftest import FakeRedis, FakeRedisResource, install_fake_redis, load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def fake(monkeypatch):
    return install_fake_redis(monkeypatch)


def _component(mod, **kw):
    return mod.RedisStreamsMonitorSensorComponent(
        sensor_name="redis_stream_sensor", stream_name="orders:events", job_name="process_stream_entries_job", **kw)


def test_resource_key_default_none(mod):
    comp = _component(mod)
    assert comp.resource_key is None
    sensor_def = comp.build_defs(None).sensors[0]
    assert sensor_def.required_resource_keys == set()


def test_inline_path_unchanged_when_resource_key_unset(mod, fake):
    sensor_def = _component(mod, host="inline.example").build_defs(None).sensors[0]
    result = sensor_def(dg.build_sensor_context())
    (client,) = fake.constructed
    assert client.cfg["host"] == "inline.example"
    (rr,) = result.run_requests
    assert rr.run_config["ops"]["config"]["entry_id"] == "1-0"


def test_resource_path_uses_get_client_not_inline_config(mod, fake):
    comp = _component(mod, resource_key="redis_shared", host="inline.example")
    sensor_def = comp.build_defs(None).sensors[0]
    assert sensor_def.required_resource_keys == {"redis_shared"}
    shared = FakeRedis()
    fake.constructed.clear()  # drop the construction FakeRedis() itself just did
    result = sensor_def(dg.build_sensor_context(resources={"redis_shared": FakeRedisResource(shared)}))
    assert fake.constructed == []  # redis.Redis(...) (inline path) never called
    (rr,) = result.run_requests
    assert rr.run_config["ops"]["config"]["entry_id"] == "1-0"
