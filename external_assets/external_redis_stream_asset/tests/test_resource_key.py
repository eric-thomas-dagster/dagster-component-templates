"""Committed tests for ExternalRedisStreamAsset's resource_key (observation sensor path).

resource_key only applies when create_observation_sensor=True (it configures the sensor
that polls Redis Stream, not the AssetSpec itself). These tests confirm required_resource_keys
wiring and that the registered resource's observe() is actually called, mocking only the
resource object.
"""
import dagster as dg
import pytest

from .conftest import FakeObserveResource, load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def _component(mod, **kw):
    return mod.ExternalRedisStreamAsset(asset_key="external/redis_stream", stream_name="orders:events", **kw)


def test_resource_key_default_none_no_required_resource_keys(mod):
    comp = _component(mod, create_observation_sensor=True)
    assert comp.resource_key is None
    sensor_def = comp.build_defs(None).sensors[0]
    assert sensor_def.required_resource_keys == set()


def test_resource_path_calls_observe_and_emits_materialization(mod):
    comp = _component(mod, create_observation_sensor=True, resource_key="redis_shared")
    sensor_def = comp.build_defs(None).sensors[0]
    assert sensor_def.required_resource_keys == {"redis_shared"}

    fake = FakeObserveResource({"stream_length": 12, "data_version": "abc123"})
    result = sensor_def(dg.build_sensor_context(resources={"redis_shared": fake}))

    assert fake.calls == ["orders:events"]
    (event,) = result.asset_events
    assert isinstance(event, dg.AssetMaterialization)
    assert event.metadata["stream_length"].value == 12


def test_emit_materialization_false_uses_observation_event(mod):
    comp = _component(
        mod, create_observation_sensor=True, resource_key="redis_shared", emit_materialization=False)
    sensor_def = comp.build_defs(None).sensors[0]
    fake = FakeObserveResource({"stream_length": 5})
    result = sensor_def(dg.build_sensor_context(resources={"redis_shared": fake}))
    (event,) = result.asset_events
    assert isinstance(event, dg.AssetObservation)


def test_no_observation_sensor_by_default(mod):
    defs = _component(mod).build_defs(None)
    assert defs.sensors is None or len(defs.sensors) == 0
    assert len(defs.assets) == 1
