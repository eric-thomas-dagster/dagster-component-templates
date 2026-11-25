"""Committed tests for DataframeToKafkaComponent.

Only the Kafka client boundary is faked (same pattern as
resources/kafka_resource/tests/test_observe.py: a fake `kafka` module
installed into sys.modules) -- everything the component itself does
(row -> message serialization, key encoding, batching/flush cadence,
resource_key vs inline connection resolution, validation) runs for real.
"""
import json
import sys
import types

import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module

mod = load_component_module()
DataframeToKafkaComponent = mod.DataframeToKafkaComponent


class _FakeKafkaProducer:
    instances = []

    def __init__(self, **kwargs):
        self.config = kwargs
        self.sent = []
        self.flush_calls = 0
        self.closed = False
        _FakeKafkaProducer.instances.append(self)

    def send(self, topic, value=None, key=None):
        self.sent.append((topic, value, key))

    def flush(self):
        self.flush_calls += 1

    def close(self):
        self.closed = True


def _install_fake_kafka(monkeypatch):
    _FakeKafkaProducer.instances = []
    fake = types.ModuleType("kafka")
    fake.KafkaProducer = _FakeKafkaProducer
    monkeypatch.setitem(sys.modules, "kafka", fake)


def _upstream(rows):
    @dg.asset(name="orders_raw")
    def _up() -> pd.DataFrame:
        return pd.DataFrame(rows)
    return _up


def _sink(**kw):
    kw.setdefault("asset_name", "orders_to_kafka")
    kw.setdefault("upstream_asset_key", "orders_raw")
    kw.setdefault("topic", "orders")
    return DataframeToKafkaComponent(**kw).build_defs(None).assets[0]


def _materialize(sink, rows):
    up = _upstream(rows)
    return dg.materialize([up, sink])


def test_requires_a_connection_method():
    with pytest.raises(ValueError, match="bootstrap_servers.*bootstrap_servers_env_var.*resource_key|resource_key"):
        _sink()


def test_inline_bootstrap_servers_sends_one_message_per_row(monkeypatch):
    _install_fake_kafka(monkeypatch)
    sink = _sink(bootstrap_servers="broker1:9092,broker2:9092")
    rows = [{"id": 1, "v": "a"}, {"id": 2, "v": "b"}]
    r = _materialize(sink, rows)
    assert r.success

    producer = _FakeKafkaProducer.instances[-1]
    assert producer.config["bootstrap_servers"] == "broker1:9092,broker2:9092"
    assert producer.config["security_protocol"] == "PLAINTEXT"
    assert len(producer.sent) == 2
    topic, value, key = producer.sent[0]
    assert topic == "orders"
    assert json.loads(value.decode("utf-8")) == {"id": 1, "v": "a"}
    assert key is None  # no key_column set
    assert producer.closed is True

    md = r.asset_materializations_for_node("orders_to_kafka")[0].metadata
    assert md["messages_sent"].value == 2
    assert md["topic"].value == "orders"


def test_key_column_encodes_the_row_value_as_the_message_key(monkeypatch):
    _install_fake_kafka(monkeypatch)
    sink = _sink(bootstrap_servers="broker1:9092", key_column="id")
    r = _materialize(sink, [{"id": 42, "v": "a"}])
    assert r.success
    producer = _FakeKafkaProducer.instances[-1]
    _, _, key = producer.sent[0]
    assert key == b"42"


def test_bootstrap_servers_env_var_path(monkeypatch):
    _install_fake_kafka(monkeypatch)
    monkeypatch.setenv("KAFKA_BOOTSTRAP", "envhost:9092")
    sink = _sink(bootstrap_servers_env_var="KAFKA_BOOTSTRAP")
    r = _materialize(sink, [{"id": 1}])
    assert r.success
    assert _FakeKafkaProducer.instances[-1].config["bootstrap_servers"] == "envhost:9092"


def test_sasl_fields_pass_through(monkeypatch):
    _install_fake_kafka(monkeypatch)
    monkeypatch.setenv("SASL_USER", "alice")
    monkeypatch.setenv("SASL_PASS", "secret")
    sink = _sink(
        bootstrap_servers="broker1:9092",
        security_protocol="SASL_SSL",
        sasl_mechanism="PLAIN",
        sasl_username_env_var="SASL_USER",
        sasl_password_env_var="SASL_PASS",
    )
    r = _materialize(sink, [{"id": 1}])
    assert r.success
    cfg = _FakeKafkaProducer.instances[-1].config
    assert cfg["security_protocol"] == "SASL_SSL"
    assert cfg["sasl_mechanism"] == "PLAIN"
    assert cfg["sasl_plain_username"] == "alice"
    assert cfg["sasl_plain_password"] == "secret"


def test_batch_size_controls_flush_cadence(monkeypatch):
    _install_fake_kafka(monkeypatch)
    sink = _sink(bootstrap_servers="broker1:9092", batch_size=2)
    rows = [{"id": i} for i in range(5)]
    r = _materialize(sink, rows)
    assert r.success
    producer = _FakeKafkaProducer.instances[-1]
    # flush() at rows 2 and 4 (mid-loop, every batch_size) + one final flush after the loop = 3
    assert producer.flush_calls == 3
    assert len(producer.sent) == 5


def test_resource_key_path_uses_the_resources_get_producer():
    """resource_key path never imports the real `kafka` module at all -- a
    resource-backed producer (e.g. KafkaResource.get_producer(), or a fake
    double in a test) is used as-is."""

    class _FakeProducer:
        def __init__(self):
            self.sent = []
            self.flushed = False

        def send(self, topic, value=None, key=None):
            self.sent.append((topic, value, key))

        def flush(self):
            self.flushed = True

    class _FakeKafkaResource:
        def __init__(self):
            self.producer = _FakeProducer()

        def get_producer(self):
            return self.producer

    resource = _FakeKafkaResource()
    sink = _sink(resource_key="kafka_resource")
    up = _upstream([{"id": 1, "v": "a"}])
    r = dg.materialize([up, sink], resources={"kafka_resource": resource})
    assert r.success
    assert len(resource.producer.sent) == 1
    assert resource.producer.flushed is True


def test_resource_key_missing_from_definitions_raises():
    sink = _sink(resource_key="kafka_resource")
    up = _upstream([{"id": 1}])
    with pytest.raises(dg.DagsterInvalidDefinitionError):
        dg.materialize([up, sink])
