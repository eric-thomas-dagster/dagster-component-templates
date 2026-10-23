"""Real, committed tests for a gap an external demo-builder hit: ExternalKafkaAsset
only declared one topic per component instance, so a project with many Kafka
topics needed one YAML block (and, with create_observation_sensor=true, one
polling sensor and one Kafka connection) per topic.

Adds `topics:` (a list of `{key, topic, deps, owners, metadata, group_name,
description}` entries) as an alternative to the single `asset_key`/`topic`
fields -- several topics from one component instance, sharing
bootstrap_servers, partitioning config, and ONE sensor that polls every
topic's offsets over a single Kafka connection per tick instead of N separate
connections.

The resource-injection seam (`resource_key` -> `client.observe(topic) ->
dict`) already existed before this change; these tests confirm it still
works, extended to loop over every topic in `topics:` instead of just one.
"""
import importlib.util
import pathlib

import dagster as dg
import pytest

_HERE = pathlib.Path(__file__).resolve().parent.parent
_COMPONENT_PY = _HERE / "component.py"
_spec = importlib.util.spec_from_file_location("eka_multi", _COMPONENT_PY)
assert _spec is not None and _spec.loader is not None
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

ExternalKafkaAsset = _mod.ExternalKafkaAsset


def _specs_by_key(defs: dg.Definitions):
    return {s.key: s for s in defs.assets}


# --- shape validation -------------------------------------------------------

def test_single_topic_mode_still_works_unchanged():
    comp = ExternalKafkaAsset(
        asset_key="streaming/orders", bootstrap_servers="localhost:9092", topic="orders",
    )
    defs = comp.build_defs(None)
    specs = _specs_by_key(defs)
    assert len(specs) == 1
    assert dg.AssetKey(["streaming", "orders"]) in specs


def test_topics_list_declares_several_assets_from_one_instance():
    comp = ExternalKafkaAsset(
        bootstrap_servers="localhost:9092",
        topics=[
            {"key": "streaming/orders", "topic": "orders"},
            {"key": "streaming/payments", "topic": "payments", "owners": ["team:payments"]},
            {"key": "streaming/shipments", "topic": "shipments", "deps": ["streaming/orders"]},
        ],
    )
    defs = comp.build_defs(None)
    specs = _specs_by_key(defs)
    assert len(specs) == 3
    assert specs[dg.AssetKey(["streaming", "orders"])].metadata["topic"] == "orders"
    assert specs[dg.AssetKey(["streaming", "payments"])].owners == ["team:payments"]
    assert specs[dg.AssetKey(["streaming", "shipments"])].deps[0].asset_key == dg.AssetKey(["streaming", "orders"])


def test_both_single_and_multi_shapes_set_is_rejected():
    comp = ExternalKafkaAsset(
        bootstrap_servers="localhost:9092",
        asset_key="streaming/orders", topic="orders",
        topics=[{"key": "streaming/payments", "topic": "payments"}],
    )
    with pytest.raises(ValueError, match="not both"):
        comp.build_defs(None)


def test_neither_shape_set_is_rejected():
    comp = ExternalKafkaAsset(bootstrap_servers="localhost:9092")
    with pytest.raises(ValueError, match="Set either"):
        comp.build_defs(None)


def test_topics_entry_missing_key_is_rejected():
    comp = ExternalKafkaAsset(
        bootstrap_servers="localhost:9092",
        topics=[{"topic": "orders"}],
    )
    with pytest.raises(ValueError, match="missing required field 'key'"):
        comp.build_defs(None)


def test_topics_entry_missing_topic_is_rejected():
    comp = ExternalKafkaAsset(
        bootstrap_servers="localhost:9092",
        topics=[{"key": "streaming/orders"}],
    )
    with pytest.raises(ValueError, match="missing required field 'topic'"):
        comp.build_defs(None)


def test_duplicate_keys_in_topics_rejected():
    comp = ExternalKafkaAsset(
        bootstrap_servers="localhost:9092",
        topics=[
            {"key": "streaming/orders", "topic": "orders"},
            {"key": "streaming/orders", "topic": "orders_v2"},
        ],
    )
    with pytest.raises(ValueError, match="duplicate key"):
        comp.build_defs(None)


def test_multi_topic_sensor_requires_explicit_sensor_name():
    comp = ExternalKafkaAsset(
        bootstrap_servers="localhost:9092",
        topics=[{"key": "streaming/orders", "topic": "orders"}],
        create_observation_sensor=True,
    )
    with pytest.raises(ValueError, match="sensor_name is required"):
        comp.build_defs(None)


def test_single_topic_sensor_still_derives_default_name():
    comp = ExternalKafkaAsset(
        asset_key="streaming/orders", bootstrap_servers="localhost:9092", topic="orders",
        create_observation_sensor=True,
    )
    defs = comp.build_defs(None)
    assert defs.get_sensor_def("orders__observation_sensor") is not None


# --- shared sensor, resource-backed observe() path --------------------------

class _FakeKafkaClient:
    """Stands in for a `resource_key`-exposed client with `.observe(topic) -> dict`."""

    def __init__(self, offsets_by_topic):
        self.offsets_by_topic = offsets_by_topic
        self.calls = []

    def observe(self, topic):
        self.calls.append(topic)
        return {"data_version": str(self.offsets_by_topic[topic]), "lag": 0}


def test_shared_sensor_observes_every_topic_over_one_resource_client():
    client = _FakeKafkaClient({"orders": 100, "payments": 50, "shipments": 10})
    comp = ExternalKafkaAsset(
        bootstrap_servers="localhost:9092",
        topics=[
            {"key": "streaming/orders", "topic": "orders"},
            {"key": "streaming/payments", "topic": "payments"},
            {"key": "streaming/shipments", "topic": "shipments"},
        ],
        create_observation_sensor=True,
        sensor_name="kafka_multi_sensor",
        resource_key="kafka_client",
    )
    defs = comp.build_defs(None)
    sensor_def = defs.get_sensor_def("kafka_multi_sensor")

    context = dg.build_sensor_context(resources={"kafka_client": client})
    result = sensor_def(context)

    assert client.calls == ["orders", "payments", "shipments"]
    assert len(result.asset_events) == 3
    keys = {e.asset_key for e in result.asset_events}
    assert keys == {
        dg.AssetKey(["streaming", "orders"]),
        dg.AssetKey(["streaming", "payments"]),
        dg.AssetKey(["streaming", "shipments"]),
    }


def test_shared_sensor_asset_selection_covers_every_topic():
    comp = ExternalKafkaAsset(
        bootstrap_servers="localhost:9092",
        topics=[
            {"key": "streaming/orders", "topic": "orders"},
            {"key": "streaming/payments", "topic": "payments"},
        ],
        create_observation_sensor=True,
        sensor_name="kafka_multi_sensor",
        resource_key="kafka_client",
    )
    defs = comp.build_defs(None)
    sensor_def = defs.get_sensor_def("kafka_multi_sensor")
    selected = set(sensor_def.asset_selection.selected_keys)
    assert selected == {dg.AssetKey(["streaming", "orders"]), dg.AssetKey(["streaming", "payments"])}


def test_shared_sensor_tolerates_one_topic_failing_and_still_reports_the_rest():
    class _FlakyClient:
        def observe(self, topic):
            if topic == "payments":
                raise RuntimeError("boom")
            return {"data_version": "1"}

    comp = ExternalKafkaAsset(
        bootstrap_servers="localhost:9092",
        topics=[
            {"key": "streaming/orders", "topic": "orders"},
            {"key": "streaming/payments", "topic": "payments"},
        ],
        create_observation_sensor=True,
        sensor_name="kafka_multi_sensor",
        resource_key="kafka_client",
    )
    defs = comp.build_defs(None)
    sensor_def = defs.get_sensor_def("kafka_multi_sensor")
    context = dg.build_sensor_context(resources={"kafka_client": _FlakyClient()})
    result = sensor_def(context)

    assert len(result.asset_events) == 1
    assert result.asset_events[0].asset_key == dg.AssetKey(["streaming", "orders"])
