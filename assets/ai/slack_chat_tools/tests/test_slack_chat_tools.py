"""get_thread() is the chat agent's only window onto the conversation -- always reads THIS run's
own channel/thread (no per-call argument), fetches fresh every time. Mocks only `_get_slack_client`
(same convention as slack_run_request_sensor's / slack_approval_gate's own tests)."""
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


class _FakeClient:
    def __init__(self, messages=None, ok=True, error=None):
        self._messages = messages or []
        self._ok = ok
        self._error = error
        self.calls = []

    def conversations_replies(self, channel, ts, limit):
        self.calls.append({"channel": channel, "ts": ts, "limit": limit})
        if not self._ok:
            return {"ok": False, "error": self._error}
        return {"ok": True, "messages": self._messages}


def _msg(user, text, ts):
    return {"user": user, "text": text, "ts": ts}


def test_get_thread_returns_messages_oldest_first_as_given(mod, monkeypatch):
    client = _FakeClient(messages=[_msg("U1", "can someone run nightly etl", "100.001"), _msg("UBOT", "Got it", "100.002")])
    monkeypatch.setattr(mod, "_get_slack_client", lambda env: client)

    tools = mod.SlackChatTools(channel="C1", thread_ts="100.001")
    result = tools.call("get_thread", {})
    assert result["channel"] == "C1"
    assert result["thread_ts"] == "100.001"
    assert result["message_count"] == 2
    assert [m["text"] for m in result["messages"]] == ["can someone run nightly etl", "Got it"]
    assert client.calls == [{"channel": "C1", "ts": "100.001", "limit": 20}]


def test_get_thread_works_for_a_solo_non_threaded_message(mod, monkeypatch):
    """conversations.replies on a message with no actual thread just returns that one message --
    confirmed against Slack's own docs -- so this component never needs to special-case it."""
    client = _FakeClient(messages=[_msg("U1", "what jobs failed today?", "200.001")])
    monkeypatch.setattr(mod, "_get_slack_client", lambda env: client)

    tools = mod.SlackChatTools(channel="C1", thread_ts="200.001")
    result = tools.call("get_thread", {})
    assert result["message_count"] == 1


def test_missing_channel_errors_clearly(mod):
    tools = mod.SlackChatTools(channel=None, thread_ts="100.001")
    result = tools.call("get_thread", {})
    assert "no channel configured" in result["error"]


def test_missing_thread_ts_errors_clearly(mod):
    tools = mod.SlackChatTools(channel="C1", thread_ts=None)
    result = tools.call("get_thread", {})
    assert "no thread/message identifier" in result["error"]


def test_slack_api_failure_surfaces_as_error_not_exception(mod, monkeypatch):
    client = _FakeClient(ok=False, error="channel_not_found")
    monkeypatch.setattr(mod, "_get_slack_client", lambda env: client)
    tools = mod.SlackChatTools(channel="C1", thread_ts="100.001")
    result = tools.call("get_thread", {})
    assert "channel_not_found" in result["error"]


def test_max_messages_truncates_to_most_recent(mod, monkeypatch):
    messages = [_msg("U1", f"msg {i}", f"100.00{i}") for i in range(5)]
    client = _FakeClient(messages=messages)
    monkeypatch.setattr(mod, "_get_slack_client", lambda env: client)
    tools = mod.SlackChatTools(channel="C1", thread_ts="100.000", max_messages=2)
    result = tools.call("get_thread", {})
    assert [m["text"] for m in result["messages"]] == ["msg 3", "msg 4"]
    assert client.calls[0]["limit"] == 2


def test_result_size_is_bounded_by_dropping_oldest_messages_first(mod, monkeypatch):
    long_text = "x" * 500
    messages = [_msg("U1", long_text, f"100.00{i}") for i in range(10)]
    client = _FakeClient(messages=messages)
    monkeypatch.setattr(mod, "_get_slack_client", lambda env: client)
    tools = mod.SlackChatTools(channel="C1", thread_ts="100.000", max_messages=10, max_result_chars=1000)
    result = tools.call("get_thread", {})
    assert len(result["messages"]) < 10
    assert result["messages"][-1]["text"] == long_text  # the most recent message always survives


def test_unknown_tool_name_returns_error_not_exception(mod):
    tools = mod.SlackChatTools(channel="C1", thread_ts="100.001")
    result = tools.call("not_a_real_tool", {})
    assert "unknown tool" in result["error"]


def test_secrets_in_message_text_are_redacted(mod, monkeypatch):
    client = _FakeClient(messages=[_msg("U1", "here's the token: xoxb-1234567890-abcdefghij", "100.001")])
    monkeypatch.setattr(mod, "_get_slack_client", lambda env: client)
    tools = mod.SlackChatTools(channel="C1", thread_ts="100.001")
    result = tools.call("get_thread", {})
    assert "xoxb-1234567890" not in str(result)
    assert "[REDACTED]" in result["messages"][0]["text"]


def test_get_slack_client_raises_clearly_when_token_missing(monkeypatch):
    import importlib.util
    spec = importlib.util.spec_from_file_location("slack_chat_tools_under_test2", "assets/ai/slack_chat_tools/component.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    monkeypatch.delenv("SLACK_BOT_TOKEN", raising=False)
    with pytest.raises(RuntimeError, match="not set"):
        mod._get_slack_client("SLACK_BOT_TOKEN")


def test_component_registers_resource_under_resource_key(mod):
    component = mod.SlackChatToolsComponent(resource_key="chat_tools")
    defs = component.build_defs(None)
    assert "chat_tools" in defs.resources


def test_mcp_server_resource_key_passes_templates_through_verbatim(mod):
    component = mod.SlackChatToolsComponent(mcp_server_resource_key="slack_chat_mcp")
    defs = component.build_defs(None)
    resource = defs.resources["slack_chat_mcp"]
    dumped = resource.model_dump()
    assert dumped["env"]["SLACK_CHAT_TOOLS_CHANNEL"] == "{tag.slack/channel}"
    assert dumped["env"]["SLACK_CHAT_TOOLS_THREAD_TS"] == "{tag.slack/thread_ts}"
