"""Real, committed tests for PubsubResource / PubsubResourceComponent.

Mocks only the external call: fake PublisherClient/SubscriberClient whose
create_topic/get_topic/delete_topic and create_subscription/get_subscription/
delete_subscription responses and errors match the real google-cloud-pubsub
client shapes (AlreadyExists/NotFound from google.api_core.exceptions).
Everything else (idempotency logic, resource registration) is real.
"""
from datetime import timedelta

import dagster as dg
import pytest
from google.api_core import exceptions as gax_exceptions
from google.cloud import pubsub_v1

from .conftest import load_component_module

mod = load_component_module()


class FakeTopic:
    def __init__(self, name, labels=None):
        self.name = name
        self.labels = dict(labels or {})


class FakePublisherClient:
    def __init__(self, existing_topics=None):
        self.topics = dict(existing_topics or {})
        self.calls = []

    def topic_path(self, project, topic):
        return f"projects/{project}/topics/{topic}"

    def create_topic(self, request):
        self.calls.append(("create_topic", request))
        name = request["name"]
        if name in self.topics:
            raise gax_exceptions.AlreadyExists(f"Topic {name} already exists")
        topic = FakeTopic(name, request.get("labels"))
        self.topics[name] = topic
        return topic

    def get_topic(self, request):
        self.calls.append(("get_topic", request))
        name = request["topic"]
        if name not in self.topics:
            raise gax_exceptions.NotFound(f"Topic {name} not found")
        return self.topics[name]

    def delete_topic(self, request):
        self.calls.append(("delete_topic", request))
        name = request["topic"]
        if name not in self.topics:
            raise gax_exceptions.NotFound(f"Topic {name} not found")
        del self.topics[name]


class FakeFuture:
    def __init__(self, message_id=None, exc=None):
        self._message_id = message_id
        self._exc = exc

    def result(self):
        if self._exc is not None:
            raise self._exc
        return self._message_id


class FakeFuturePublisherClient(FakePublisherClient):
    """FakePublisherClient plus a fake .publish() for the publish() method tests."""

    def __init__(self, *a, message_id="msg-1", publish_exc=None, **kw):
        super().__init__(*a, **kw)
        self._message_id = message_id
        self._publish_exc = publish_exc

    def publish(self, topic, data, **attrs):
        self.calls.append(("publish", topic, data, attrs))
        if self._publish_exc is not None:
            return FakeFuture(exc=self._publish_exc)
        return FakeFuture(message_id=self._message_id)


class FakeSubscription:
    def __init__(self, name, topic):
        self.name = name
        self.topic = topic


class FakeSubscriberClient:
    def __init__(self, existing_subscriptions=None):
        self.subscriptions = dict(existing_subscriptions or {})
        self.calls = []

    def topic_path(self, project, topic):
        return f"projects/{project}/topics/{topic}"

    def subscription_path(self, project, subscription):
        return f"projects/{project}/subscriptions/{subscription}"

    def create_subscription(self, request):
        self.calls.append(("create_subscription", request))
        name = request["name"]
        if name in self.subscriptions:
            raise gax_exceptions.AlreadyExists(f"Subscription {name} already exists")
        sub = FakeSubscription(name, request["topic"])
        self.subscriptions[name] = sub
        return sub

    def get_subscription(self, request):
        self.calls.append(("get_subscription", request))
        name = request["subscription"]
        if name not in self.subscriptions:
            raise gax_exceptions.NotFound(f"Subscription {name} not found")
        return self.subscriptions[name]

    def delete_subscription(self, request):
        self.calls.append(("delete_subscription", request))
        name = request["subscription"]
        if name not in self.subscriptions:
            raise gax_exceptions.NotFound(f"Subscription {name} not found")
        del self.subscriptions[name]


def _resource(publisher=None, subscriber=None, **kw):
    r = mod.PubsubResource(**kw)
    if publisher is not None:
        r._client_cache["publisher"] = publisher
    if subscriber is not None:
        r._client_cache["subscriber"] = subscriber
    return r


# --- Topic lifecycle ---

def test_ensure_topic_creates_when_absent():
    client = FakePublisherClient()
    r = _resource(publisher=client)
    result = r.ensure_topic("proj", "orders", labels={"team": "data"})
    assert result["action"] == "created"
    assert result["topic_path"] == "projects/proj/topics/orders"
    assert any(c[0] == "create_topic" for c in client.calls)


def test_ensure_topic_already_exists_is_noop():
    topic_path = "projects/proj/topics/orders"
    client = FakePublisherClient(existing_topics={topic_path: FakeTopic(topic_path, {"env": "prod"})})
    r = _resource(publisher=client)
    result = r.ensure_topic("proj", "orders")
    assert result["action"] == "none"
    assert result["labels"] == {"env": "prod"}
    assert len([c for c in client.calls if c[0] == "create_topic"]) == 1
    assert len([c for c in client.calls if c[0] == "get_topic"]) == 1


def test_delete_topic_when_present():
    topic_path = "projects/proj/topics/orders"
    client = FakePublisherClient(existing_topics={topic_path: FakeTopic(topic_path)})
    r = _resource(publisher=client)
    result = r.delete_topic("proj", "orders")
    assert result["action"] == "deleted"
    assert topic_path not in client.topics


def test_delete_topic_already_absent_is_noop():
    client = FakePublisherClient()
    r = _resource(publisher=client)
    result = r.delete_topic("proj", "orders")
    assert result["action"] == "none"


def test_message_retention_duration_passed_as_timedelta():
    client = FakePublisherClient()
    r = _resource(publisher=client)
    r.ensure_topic("proj", "orders", message_retention_duration_seconds=3600)
    create_request = next(c[1] for c in client.calls if c[0] == "create_topic")
    assert create_request["message_retention_duration"] == timedelta(seconds=3600)


# --- Subscription lifecycle ---

def test_ensure_subscription_creates_pull_when_absent():
    client = FakeSubscriberClient()
    r = _resource(subscriber=client)
    result = r.ensure_subscription("proj", "orders", "orders-worker")
    assert result["action"] == "created"
    assert result["type"] == "pull"
    assert result["subscription_path"] == "projects/proj/subscriptions/orders-worker"
    create_request = next(c[1] for c in client.calls if c[0] == "create_subscription")
    assert "push_config" not in create_request


def test_ensure_subscription_push_when_endpoint_set():
    client = FakeSubscriberClient()
    r = _resource(subscriber=client)
    result = r.ensure_subscription("proj", "orders", "orders-worker", push_endpoint="https://example.com/hook")
    assert result["type"] == "push"
    create_request = next(c[1] for c in client.calls if c[0] == "create_subscription")
    assert create_request["push_config"] == {"push_endpoint": "https://example.com/hook"}


def test_ensure_subscription_already_exists_is_noop():
    sub_path = "projects/proj/subscriptions/orders-worker"
    client = FakeSubscriberClient(existing_subscriptions={sub_path: FakeSubscription(sub_path, "projects/proj/topics/orders")})
    r = _resource(subscriber=client)
    result = r.ensure_subscription("proj", "orders", "orders-worker")
    assert result["action"] == "none"
    assert len([c for c in client.calls if c[0] == "create_subscription"]) == 1
    assert len([c for c in client.calls if c[0] == "get_subscription"]) == 1


def test_delete_subscription_when_present():
    sub_path = "projects/proj/subscriptions/orders-worker"
    client = FakeSubscriberClient(existing_subscriptions={sub_path: FakeSubscription(sub_path, "projects/proj/topics/orders")})
    r = _resource(subscriber=client)
    result = r.delete_subscription("proj", "orders-worker")
    assert result["action"] == "deleted"
    assert sub_path not in client.subscriptions


def test_delete_subscription_already_absent_is_noop():
    client = FakeSubscriberClient()
    r = _resource(subscriber=client)
    result = r.delete_subscription("proj", "orders-worker")
    assert result["action"] == "none"


def test_real_client_topic_and_subscription_path_format():
    """Not mocked -- confirms the real google-cloud-pubsub client's path formats (static methods)."""
    assert pubsub_v1.PublisherClient.topic_path("proj", "orders") == "projects/proj/topics/orders"
    assert pubsub_v1.SubscriberClient.subscription_path("proj", "orders-worker") == "projects/proj/subscriptions/orders-worker"


# --- Publish ---

def test_publish_encodes_str_data_and_returns_message_id():
    client = FakeFuturePublisherClient(message_id="msg-42")
    r = _resource(publisher=client)

    message_id = r.publish("proj", "orders", "hello world", attributes={"source": "test"})

    assert message_id == "msg-42"
    name, topic, data, attrs = client.calls[0]
    assert name == "publish"
    assert topic == "projects/proj/topics/orders"
    assert data == b"hello world"
    assert attrs == {"source": "test"}


def test_publish_passes_bytes_data_through_unchanged():
    client = FakeFuturePublisherClient(message_id="msg-7")
    r = _resource(publisher=client)

    r.publish("proj", "orders", b"\x00\x01binary")

    _, _, data, _ = client.calls[0]
    assert data == b"\x00\x01binary"


def test_publish_passes_ordering_key():
    client = FakeFuturePublisherClient(message_id="msg-9")
    r = _resource(publisher=client)

    r.publish("proj", "orders", "x", ordering_key="customer-123")

    _, _, _, attrs = client.calls[0]
    assert attrs["ordering_key"] == "customer-123"


def test_publish_propagates_future_failure():
    client = FakeFuturePublisherClient(publish_exc=RuntimeError("broker unavailable"))
    r = _resource(publisher=client)

    with pytest.raises(RuntimeError, match="broker unavailable"):
        r.publish("proj", "orders", "x")


# --- Component registration ---

def test_component_registers_resource_under_default_key():
    component = mod.PubsubResourceComponent()
    defs = component.build_defs(None)
    assert "pubsub_resource" in defs.resources
    assert isinstance(defs.resources["pubsub_resource"], mod.PubsubResource)


def test_component_registers_resource_under_custom_key():
    component = mod.PubsubResourceComponent(resource_key="my_pubsub")
    defs = component.build_defs(None)
    assert "my_pubsub" in defs.resources


def test_resource_usable_from_a_custom_asset():
    """Confirms the resource is a real injectable ConfigurableResource, not just a standalone helper."""
    client = FakePublisherClient()

    @dg.asset
    def ensure_topic_asset(pubsub: mod.PubsubResource) -> dg.MaterializeResult:
        pubsub._client_cache["publisher"] = client
        result = pubsub.ensure_topic("proj", "orders")
        return dg.MaterializeResult(metadata={"action": result["action"]})

    result = dg.materialize([ensure_topic_asset], resources={"pubsub": mod.PubsubResource()})
    assert result.success
    assert any(c[0] == "create_topic" for c in client.calls)
