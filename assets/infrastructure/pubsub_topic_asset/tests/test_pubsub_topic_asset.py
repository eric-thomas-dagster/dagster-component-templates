"""Real, committed tests for PubsubTopicAssetComponent.

Mocks only the external call: a fake PublisherClient whose
create_topic/get_topic/delete_topic responses and errors match the real
google-cloud-pubsub client shapes (AlreadyExists/NotFound from
google.api_core.exceptions). Everything else (idempotency logic, Dagster
asset wiring) is real.
"""
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


def _patch_client(monkeypatch, client):
    monkeypatch.setattr(mod, "_make_publisher_client", lambda credentials_env_var: client)


def _comp(**kw):
    args = dict(asset_name="out", project_id="proj", topic_id="orders")
    args.update(kw)
    return mod.PubsubTopicAssetComponent(**args)


def test_create_topic_when_absent(monkeypatch):
    client = FakePublisherClient()
    _patch_client(monkeypatch, client)
    comp = _comp(labels={"team": "data"})
    result = dg.materialize(list(comp.build_defs(None).assets))
    assert result.success
    assert any(c[0] == "create_topic" for c in client.calls)
    meta = next(e for e in result.all_events if e.event_type_value == "ASSET_MATERIALIZATION").event_specific_data.materialization.metadata
    assert meta["action"].value == "created"
    assert meta["topic_path"].value == "projects/proj/topics/orders"


def test_create_topic_already_exists_is_noop(monkeypatch):
    topic_path = "projects/proj/topics/orders"
    client = FakePublisherClient(existing_topics={topic_path: FakeTopic(topic_path, {"env": "prod"})})
    _patch_client(monkeypatch, client)
    comp = _comp()
    result = dg.materialize(list(comp.build_defs(None).assets))
    assert result.success
    meta = next(e for e in result.all_events if e.event_type_value == "ASSET_MATERIALIZATION").event_specific_data.materialization.metadata
    assert meta["action"].value == "none"
    assert meta["labels"].data == {"env": "prod"}
    create_calls = [c for c in client.calls if c[0] == "create_topic"]
    get_calls = [c for c in client.calls if c[0] == "get_topic"]
    assert len(create_calls) == 1 and len(get_calls) == 1


def test_delete_topic_when_present(monkeypatch):
    topic_path = "projects/proj/topics/orders"
    client = FakePublisherClient(existing_topics={topic_path: FakeTopic(topic_path)})
    _patch_client(monkeypatch, client)
    comp = _comp(desired_state="absent")
    result = dg.materialize(list(comp.build_defs(None).assets))
    assert result.success
    assert topic_path not in client.topics
    meta = next(e for e in result.all_events if e.event_type_value == "ASSET_MATERIALIZATION").event_specific_data.materialization.metadata
    assert meta["action"].value == "deleted"


def test_delete_topic_already_absent_is_noop(monkeypatch):
    client = FakePublisherClient()
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


def test_message_retention_duration_passed_as_timedelta(monkeypatch):
    client = FakePublisherClient()
    _patch_client(monkeypatch, client)
    comp = _comp(message_retention_duration_seconds=3600)
    dg.materialize(list(comp.build_defs(None).assets))
    create_request = next(c[1] for c in client.calls if c[0] == "create_topic")
    from datetime import timedelta
    assert create_request["message_retention_duration"] == timedelta(seconds=3600)


def test_real_client_topic_path_format():
    """Not mocked -- confirms topic_path's real format against the installed google-cloud-pubsub client (a staticmethod)."""
    assert pubsub_v1.PublisherClient.topic_path("proj", "orders") == "projects/proj/topics/orders"
