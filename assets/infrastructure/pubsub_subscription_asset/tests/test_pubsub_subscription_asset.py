"""Real, committed tests for PubsubSubscriptionAssetComponent.

Mocks only the external call: a fake SubscriberClient whose
create_subscription/get_subscription/delete_subscription responses and
errors match the real google-cloud-pubsub client shapes
(AlreadyExists/NotFound from google.api_core.exceptions). Everything else
(idempotency logic, Dagster asset wiring) is real.
"""
import dagster as dg
import pytest
from google.api_core import exceptions as gax_exceptions
from google.cloud import pubsub_v1

from .conftest import load_component_module

mod = load_component_module()


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


def _patch_client(monkeypatch, client):
    monkeypatch.setattr(mod, "_make_subscriber_client", lambda credentials_env_var: client)


def _comp(**kw):
    args = dict(asset_name="out", project_id="proj", topic_id="orders", subscription_id="orders-worker")
    args.update(kw)
    return mod.PubsubSubscriptionAssetComponent(**args)


def test_create_subscription_when_absent_pull(monkeypatch):
    client = FakeSubscriberClient()
    _patch_client(monkeypatch, client)
    comp = _comp()
    result = dg.materialize(list(comp.build_defs(None).assets))
    assert result.success
    create_request = next(c[1] for c in client.calls if c[0] == "create_subscription")
    assert "push_config" not in create_request
    assert create_request["topic"] == "projects/proj/topics/orders"
    meta = next(e for e in result.all_events if e.event_type_value == "ASSET_MATERIALIZATION").event_specific_data.materialization.metadata
    assert meta["action"].value == "created"
    assert meta["type"].value == "pull"


def test_create_subscription_with_push_endpoint(monkeypatch):
    client = FakeSubscriberClient()
    _patch_client(monkeypatch, client)
    comp = _comp(push_endpoint="https://example.com/hook")
    result = dg.materialize(list(comp.build_defs(None).assets))
    assert result.success
    create_request = next(c[1] for c in client.calls if c[0] == "create_subscription")
    assert create_request["push_config"] == {"push_endpoint": "https://example.com/hook"}
    meta = next(e for e in result.all_events if e.event_type_value == "ASSET_MATERIALIZATION").event_specific_data.materialization.metadata
    assert meta["type"].value == "push"


def test_create_subscription_already_exists_is_noop(monkeypatch):
    sub_path = "projects/proj/subscriptions/orders-worker"
    topic_path = "projects/proj/topics/orders"
    client = FakeSubscriberClient(existing_subscriptions={sub_path: FakeSubscription(sub_path, topic_path)})
    _patch_client(monkeypatch, client)
    comp = _comp()
    result = dg.materialize(list(comp.build_defs(None).assets))
    assert result.success
    meta = next(e for e in result.all_events if e.event_type_value == "ASSET_MATERIALIZATION").event_specific_data.materialization.metadata
    assert meta["action"].value == "none"
    create_calls = [c for c in client.calls if c[0] == "create_subscription"]
    get_calls = [c for c in client.calls if c[0] == "get_subscription"]
    assert len(create_calls) == 1 and len(get_calls) == 1


def test_delete_subscription_when_present(monkeypatch):
    sub_path = "projects/proj/subscriptions/orders-worker"
    topic_path = "projects/proj/topics/orders"
    client = FakeSubscriberClient(existing_subscriptions={sub_path: FakeSubscription(sub_path, topic_path)})
    _patch_client(monkeypatch, client)
    comp = _comp(desired_state="absent")
    result = dg.materialize(list(comp.build_defs(None).assets))
    assert result.success
    assert sub_path not in client.subscriptions
    meta = next(e for e in result.all_events if e.event_type_value == "ASSET_MATERIALIZATION").event_specific_data.materialization.metadata
    assert meta["action"].value == "deleted"


def test_delete_subscription_already_absent_is_noop(monkeypatch):
    client = FakeSubscriberClient()
    _patch_client(monkeypatch, client)
    comp = _comp(desired_state="absent")
    result = dg.materialize(list(comp.build_defs(None).assets))
    assert result.success
    meta = next(e for e in result.all_events if e.event_type_value == "ASSET_MATERIALIZATION").event_specific_data.materialization.metadata
    assert meta["action"].value == "none"


def test_invalid_desired_state_raises():
    comp = _comp(desired_state="bogus")
    with pytest.raises(ValueError, match="desired_state"):
        comp.build_defs(None)


def test_ack_deadline_passed_through(monkeypatch):
    client = FakeSubscriberClient()
    _patch_client(monkeypatch, client)
    comp = _comp(ack_deadline_seconds=42)
    dg.materialize(list(comp.build_defs(None).assets))
    create_request = next(c[1] for c in client.calls if c[0] == "create_subscription")
    assert create_request["ack_deadline_seconds"] == 42


def test_real_client_subscription_path_format():
    """Not mocked -- confirms subscription_path's real format against the installed google-cloud-pubsub client (a staticmethod)."""
    assert pubsub_v1.SubscriberClient.subscription_path("proj", "orders-worker") == "projects/proj/subscriptions/orders-worker"
