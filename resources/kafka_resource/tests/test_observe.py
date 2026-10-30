"""Real, committed test for a gap found in review feedback: KafkaResource had
no `.observe(topic)` method, so it couldn't be used as `external_kafka_asset`'s
`resource_key` client (documented contract: `.observe(topic) -> {data_version,
**metadata}`). Mirrors that component's own native kafka-python fallback
exactly: per-partition end offsets JSON-encoded as a repartition-safe
data_version, not a single summed counter.
"""
import sys
import types

from .conftest import load_component_module

mod = load_component_module()
KafkaResource = mod.KafkaResource


class _FakeTopicPartition:
    def __init__(self, topic, partition):
        self.topic = topic
        self.partition = partition


class _FakeKafkaConsumer:
    instances = []

    def __init__(self, *topics, **kwargs):
        self.topics = topics
        self.config = kwargs
        self.closed = False
        _FakeKafkaConsumer.instances.append(self)

    def partitions_for_topic(self, topic):
        return {0, 1}

    def end_offsets(self, tps):
        return {tp: 100 + tp.partition for tp in tps}

    def close(self):
        self.closed = True


def _install_fake_kafka(monkeypatch):
    _FakeKafkaConsumer.instances = []
    fake = types.ModuleType("kafka")
    fake.KafkaConsumer = _FakeKafkaConsumer
    fake.TopicPartition = _FakeTopicPartition
    monkeypatch.setitem(sys.modules, "kafka", fake)


def test_observe_returns_data_version_and_metadata(monkeypatch):
    _install_fake_kafka(monkeypatch)
    resource = KafkaResource(bootstrap_servers="broker1:9092,broker2:9092")
    result = resource.observe("orders")

    assert result["partition_count"] == 2
    assert result["total_end_offset"] == 100 + 101  # partitions 0 and 1
    assert result["topic"] == "orders"
    assert result["data_version"] == '{"0": 100, "1": 101}'


def test_observe_data_version_changes_when_offsets_move(monkeypatch):
    """Repartition-safety: a per-partition JSON encoding, not a bare sum, so a
    rebalance that moves volume between partitions (same total) still shows up
    as a changed data_version."""
    _install_fake_kafka(monkeypatch)
    resource = KafkaResource(bootstrap_servers="broker1:9092")

    class _ShiftedConsumer(_FakeKafkaConsumer):
        def end_offsets(self, tps):
            return {tp: 150 if tp.partition == 0 else 51 for tp in tps}

    monkeypatch.setattr(sys.modules["kafka"], "KafkaConsumer", _ShiftedConsumer)
    result = resource.observe("orders")
    assert result["total_end_offset"] == 201  # same total as the 100/101 case would be (100+101=201)
    assert result["data_version"] == '{"0": 150, "1": 51}'  # but the per-partition shape is visibly different


def test_observe_closes_the_consumer(monkeypatch):
    _install_fake_kafka(monkeypatch)
    resource = KafkaResource(bootstrap_servers="broker1:9092")
    resource.observe("orders")
    assert _FakeKafkaConsumer.instances[-1].closed is True


def test_observe_passes_no_topics_to_the_consumer_constructor(monkeypatch):
    """get_consumer() with no topic args -- observe() doesn't subscribe, it
    only introspects offsets via partitions_for_topic/end_offsets."""
    _install_fake_kafka(monkeypatch)
    resource = KafkaResource(bootstrap_servers="broker1:9092")
    resource.observe("orders")
    assert _FakeKafkaConsumer.instances[-1].topics == ()
