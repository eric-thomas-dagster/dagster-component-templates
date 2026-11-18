"""Real, committed tests for SlackReplyComponent -- real dg.materialize() for the success
path, a real failed run (not mocked) feeding a real RunStatusSensorContext for the failure
path. Mocks only `_get_slack_client`."""
import dagster as dg
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


class _FakeClient:
    def __init__(self, ok=True, error=None):
        self.calls = []
        self._ok = ok
        self._error = error

    def chat_postMessage(self, **kwargs):
        self.calls.append(kwargs)
        if not self._ok:
            return {"ok": False, "error": self._error}
        return {"ok": True, "ts": "999.0001"}


def _upstream_asset(output):
    @dg.asset(key=dg.AssetKey(["chat_agent"]))
    def chat_agent():
        return output
    return chat_agent


def _materialize(agent_output, component, monkeypatch, *, tags=None, client=None):
    client = client or _FakeClient()
    monkeypatch.setattr(mod_cache["mod"], "_get_slack_client", lambda env: client)
    upstream = _upstream_asset(agent_output)
    defs = component.build_defs(None)
    result = dg.materialize(
        [upstream, *defs.assets], resources={"io_manager": dg.InMemoryIOManager()}, tags=tags or {},
        raise_on_error=False,
    )
    return result, client


mod_cache = {}


@pytest.fixture(autouse=True)
def _cache_mod(mod):
    mod_cache["mod"] = mod
    yield


def _component(**overrides):
    defaults = dict(asset_name="chat_reply", upstream_asset_key="chat_agent")
    defaults.update(overrides)
    return mod_cache["mod"].SlackReplyComponent(**defaults)


# --- success path ----------------------------------------------------------------------

def test_posts_final_answer_to_the_tagged_thread(monkeypatch):
    component = _component()
    result, client = _materialize(
        {"final_answer": "The nightly etl failed twice this week.", "iterations": 1, "tool_calls_made": 0, "stopped_reason": "end_turn", "tool_call_details": []},
        component, monkeypatch, tags={"slack/channel": "C1", "slack/thread_ts": "100.001"},
    )
    assert result.success
    assert client.calls == [{"channel": "C1", "thread_ts": "100.001", "text": "The nightly etl failed twice this week."}]


def test_missing_slack_tags_fails_the_run_clearly(monkeypatch):
    component = _component()
    result, client = _materialize({"final_answer": "hi"}, component, monkeypatch, tags={})
    assert not result.success
    assert client.calls == []


def test_answer_field_override(monkeypatch):
    component = _component(answer_field="summary")
    result, client = _materialize(
        {"summary": "custom field text", "final_answer": "ignored"},
        component, monkeypatch, tags={"slack/channel": "C1", "slack/thread_ts": "100.001"},
    )
    assert result.success
    assert client.calls[0]["text"] == "custom field text"


def test_plain_string_upstream_output_is_used_directly(monkeypatch):
    component = _component()
    result, client = _materialize("just a plain string answer", component, monkeypatch, tags={"slack/channel": "C1", "slack/thread_ts": "100.001"})
    assert result.success
    assert client.calls[0]["text"] == "just a plain string answer"


def test_long_answer_is_split_into_multiple_messages(monkeypatch):
    component = _component(max_chars_per_message=10, on_long_answer="split")
    long_text = "0123456789" * 3  # 30 chars
    result, client = _materialize({"final_answer": long_text}, component, monkeypatch, tags={"slack/channel": "C1", "slack/thread_ts": "100.001"})
    assert result.success
    assert len(client.calls) == 3
    assert "".join(c["text"].split("\n\n")[0] for c in client.calls) == long_text


def test_long_answer_truncate_mode_posts_one_message(monkeypatch):
    component = _component(max_chars_per_message=10, on_long_answer="truncate")
    result, client = _materialize({"final_answer": "0123456789" * 3}, component, monkeypatch, tags={"slack/channel": "C1", "slack/thread_ts": "100.001"})
    assert result.success
    assert len(client.calls) == 1
    assert "truncated" in client.calls[0]["text"]


def test_run_link_appended_when_webserver_base_url_set(monkeypatch):
    component = _component(webserver_base_url="http://localhost:3000")
    result, client = _materialize({"final_answer": "hi"}, component, monkeypatch, tags={"slack/channel": "C1", "slack/thread_ts": "100.001"})
    assert result.success
    assert "http://localhost:3000/runs/" in client.calls[0]["text"]
    assert "View run" in client.calls[0]["text"]


def test_no_run_link_when_webserver_base_url_unset(monkeypatch):
    component = _component()
    result, client = _materialize({"final_answer": "hi"}, component, monkeypatch, tags={"slack/channel": "C1", "slack/thread_ts": "100.001"})
    assert result.success
    assert client.calls[0]["text"] == "hi"


def test_secrets_in_the_answer_are_redacted(monkeypatch):
    component = _component()
    result, client = _materialize(
        {"final_answer": "here's a token: xoxb-1234567890-abcdefghij"},
        component, monkeypatch, tags={"slack/channel": "C1", "slack/thread_ts": "100.001"},
    )
    assert result.success
    assert "xoxb-1234567890" not in client.calls[0]["text"]
    assert "[REDACTED]" in client.calls[0]["text"]


def test_slack_post_failure_fails_the_run(monkeypatch):
    component = _component()
    result, client = _materialize(
        {"final_answer": "hi"}, component, monkeypatch, tags={"slack/channel": "C1", "slack/thread_ts": "100.001"},
        client=_FakeClient(ok=False, error="channel_not_found"),
    )
    assert not result.success


# --- failure-path run-status sensor -----------------------------------------------------

def test_component_emits_a_failure_watcher_sensor_by_default(mod):
    component = _component()
    defs = component.build_defs(None)
    assert [s.name for s in defs.sensors] == ["chat_reply_failure_watcher"]


def test_monitor_failures_false_emits_no_sensor(mod):
    component = _component(monitor_failures=False)
    defs = component.build_defs(None)
    assert not defs.sensors


def _real_failed_run_context(tags):
    @dg.asset
    def boom():
        raise ValueError("deliberate failure for a test")

    instance = dg.DagsterInstance.ephemeral()
    result = dg.materialize([boom], instance=instance, raise_on_error=False, tags=tags)
    run = instance.get_run_by_id(result.run_id)
    rec = instance.get_records_for_run(run.run_id, of_type=dg.DagsterEventType.RUN_FAILURE).records[0]
    ev = rec.event_log_entry.dagster_event
    return dg.build_run_status_sensor_context(sensor_name="chat_reply_failure_watcher", dagster_event=ev, dagster_instance=instance, dagster_run=run)


def test_failure_watcher_posts_an_error_reply_for_a_tagged_failed_run(monkeypatch, mod):
    component = _component()
    defs = component.build_defs(None)
    (sensor_def,) = defs.sensors
    client = _FakeClient()
    monkeypatch.setattr(mod, "_get_slack_client", lambda env: client)

    ctx = _real_failed_run_context({"slack/channel": "C1", "slack/thread_ts": "100.001"})
    sensor_def(ctx)
    assert len(client.calls) == 1
    assert client.calls[0]["channel"] == "C1" and client.calls[0]["thread_ts"] == "100.001"
    assert "Something went wrong" in client.calls[0]["text"]


def test_failure_watcher_ignores_a_failed_run_with_no_slack_tags(monkeypatch, mod):
    component = _component()
    defs = component.build_defs(None)
    (sensor_def,) = defs.sensors
    client = _FakeClient()
    monkeypatch.setattr(mod, "_get_slack_client", lambda env: client)

    ctx = _real_failed_run_context({})  # not a chat-agent run
    sensor_def(ctx)
    assert client.calls == []
