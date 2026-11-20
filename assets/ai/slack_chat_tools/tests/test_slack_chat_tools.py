"""get_thread() is the chat agent's only window onto the conversation -- always reads THIS run's
own channel/thread (no per-call argument), fetches fresh every time. Mocks only `_get_slack_client`
(same convention as slack_run_request_sensor's / slack_approval_gate's own tests)."""
import time

import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


class _FakeClient:
    def __init__(self, messages=None, ok=True, error=None, upload_ok=True, rate_limited_calls=0, retry_after=2):
        self._messages = messages or []
        self._ok = ok
        self._error = error
        self._upload_ok = upload_ok
        self._rate_limited_calls = rate_limited_calls  # how many leading calls return ratelimited
        self._retry_after = retry_after
        self.calls = []
        self.uploaded = []

    def conversations_replies(self, channel, ts, limit):
        self.calls.append({"channel": channel, "ts": ts, "limit": limit})
        if len(self.calls) <= self._rate_limited_calls:
            return {"ok": False, "error": "ratelimited", "response_metadata": {"retry_after": self._retry_after}}
        if not self._ok:
            return {"ok": False, "error": self._error}
        return {"ok": True, "messages": self._messages}

    def files_upload_v2(self, **kwargs):
        if not self._upload_ok:
            raise RuntimeError("upload failed")
        self.uploaded.append(kwargs)
        return {"ok": True}


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


def test_get_thread_retries_once_after_rate_limit_then_succeeds(mod, monkeypatch):
    client = _FakeClient(messages=[_msg("U1", "hi", "100.001")], rate_limited_calls=1, retry_after=0.001)
    monkeypatch.setattr(mod, "_get_slack_client", lambda env: client)
    monkeypatch.setattr(time, "sleep", lambda s: None)

    tools = mod.SlackChatTools(channel="C1", thread_ts="100.001")
    result = tools.call("get_thread", {})
    assert result["message_count"] == 1
    assert len(client.calls) == 2  # one rate-limited attempt, one retry


def test_get_thread_rate_limited_twice_surfaces_the_error_after_one_retry(mod, monkeypatch):
    client = _FakeClient(rate_limited_calls=5, retry_after=0.001)  # rate-limited well past the one retry
    monkeypatch.setattr(mod, "_get_slack_client", lambda env: client)
    monkeypatch.setattr(time, "sleep", lambda s: None)

    tools = mod.SlackChatTools(channel="C1", thread_ts="100.001")
    result = tools.call("get_thread", {})
    assert "ratelimited" in result["error"]
    assert len(client.calls) == 2  # exactly one retry -- not a retry loop


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


def test_mcp_server_resource_key_includes_receipts_db_path_when_set(mod):
    component = mod.SlackChatToolsComponent(mcp_server_resource_key="slack_chat_mcp", receipts_db_path="/tmp/r.sqlite3")
    defs = component.build_defs(None)
    dumped = defs.resources["slack_chat_mcp"].model_dump()
    assert dumped["env"]["SLACK_CHAT_TOOLS_RECEIPTS_DB_PATH"] == "/tmp/r.sqlite3"


def test_mcp_server_resource_key_omits_receipts_db_path_when_unset(mod):
    component = mod.SlackChatToolsComponent(mcp_server_resource_key="slack_chat_mcp")
    defs = component.build_defs(None)
    dumped = defs.resources["slack_chat_mcp"].model_dump()
    assert "SLACK_CHAT_TOOLS_RECEIPTS_DB_PATH" not in dumped["env"]


# --- receipts_db_path: attaching receipt_ids to bot messages ---------------------------

def _bot_msg(text, ts, bot_id="BBOT"):
    return {"user": "UBOT", "text": text, "ts": ts, "bot_id": bot_id}


def test_bot_message_gets_receipt_ids_attached(mod, monkeypatch, tmp_path):
    db_path = str(tmp_path / "r.sqlite3")
    mod.record_message_links(db_path, ["r1", "r2"], platform="slack", channel="C1", message_ts="100.002")

    client = _FakeClient(messages=[_msg("U1", "what's our per diem policy?", "100.001"), _bot_msg("It's $50/day.", "100.002")])
    monkeypatch.setattr(mod, "_get_slack_client", lambda env: client)
    tools = mod.SlackChatTools(channel="C1", thread_ts="100.001", receipts_db_path=db_path)
    result = tools.call("get_thread", {})

    user_row, bot_row = result["messages"]
    assert "receipt_ids" not in user_row  # never attached to a user message
    assert set(bot_row["receipt_ids"]) == {"r1", "r2"}


def test_bot_message_with_no_linked_receipts_gets_an_empty_list(mod, monkeypatch, tmp_path):
    db_path = str(tmp_path / "r.sqlite3")
    client = _FakeClient(messages=[_bot_msg("hi", "100.002")])
    monkeypatch.setattr(mod, "_get_slack_client", lambda env: client)
    tools = mod.SlackChatTools(channel="C1", thread_ts="100.001", receipts_db_path=db_path)
    result = tools.call("get_thread", {})
    assert result["messages"][0]["receipt_ids"] == []


def test_receipts_db_path_unset_never_attaches_receipt_ids(mod, monkeypatch):
    client = _FakeClient(messages=[_bot_msg("hi", "100.002")])
    monkeypatch.setattr(mod, "_get_slack_client", lambda env: client)
    tools = mod.SlackChatTools(channel="C1", thread_ts="100.001")
    result = tools.call("get_thread", {})
    assert "receipt_ids" not in result["messages"][0]


# --- upload_files (report_files_dir) ----------------------------------------------------

def test_upload_files_posts_each_resolved_file(mod, monkeypatch, tmp_path):
    run_dir = tmp_path / "run1"
    run_dir.mkdir()
    (run_dir / "manifest.json").write_text(
        '[{"file_id": "f1", "name": "chart.png", "path": "' + str(run_dir / "chart.png").replace("\\", "\\\\") + '"}]'
    )
    client = _FakeClient()
    monkeypatch.setattr(mod, "_get_slack_client", lambda env: client)
    tools = mod.SlackChatTools(channel="C1", thread_ts="100.001", report_files_dir=str(tmp_path), run_id="run1")
    result = tools.call("upload_files", {"file_ids": ["f1"]})
    assert result == {"posted": ["chart.png"]}
    assert len(client.uploaded) == 1
    assert client.uploaded[0]["channel"] == "C1" and client.uploaded[0]["thread_ts"] == "100.001"


def test_upload_files_unknown_file_id_is_reported_not_raised(mod, monkeypatch, tmp_path):
    client = _FakeClient()
    monkeypatch.setattr(mod, "_get_slack_client", lambda env: client)
    tools = mod.SlackChatTools(channel="C1", thread_ts="100.001", report_files_dir=str(tmp_path), run_id="run1")
    result = tools.call("upload_files", {"file_ids": ["nope"]})
    assert result == {"posted": [], "missing_file_ids": ["nope"]}


def test_upload_files_without_report_files_dir_is_a_clear_error(mod):
    tools = mod.SlackChatTools(channel="C1", thread_ts="100.001")
    result = tools.call("upload_files", {"file_ids": ["f1"]})
    assert "report_files_dir" in result["error"]


def test_upload_files_without_channel_is_a_clear_error(mod):
    tools = mod.SlackChatTools(report_files_dir="/tmp/whatever")
    result = tools.call("upload_files", {"file_ids": ["f1"]})
    assert "error" in result


def test_upload_files_empty_list_is_a_clear_error(mod):
    tools = mod.SlackChatTools(channel="C1", thread_ts="100.001", report_files_dir="/tmp/whatever")
    result = tools.call("upload_files", {"file_ids": []})
    assert "error" in result


def test_mcp_server_resource_key_includes_report_files_dir_and_run_id_template_when_set(mod):
    component = mod.SlackChatToolsComponent(mcp_server_resource_key="slack_chat_mcp", report_files_dir="/tmp/files")
    cfg = component.build_defs(None).resources["slack_chat_mcp"].model_dump()
    assert cfg["env"]["SLACK_CHAT_TOOLS_REPORT_FILES_DIR"] == "/tmp/files"
    assert cfg["env"]["SLACK_CHAT_TOOLS_RUN_ID"] == "{run_id}"


def test_mcp_server_resource_key_omits_report_files_dir_when_unset(mod):
    component = mod.SlackChatToolsComponent(mcp_server_resource_key="slack_chat_mcp")
    cfg = component.build_defs(None).resources["slack_chat_mcp"].model_dump()
    assert "SLACK_CHAT_TOOLS_REPORT_FILES_DIR" not in cfg["env"]
    assert "SLACK_CHAT_TOOLS_RUN_ID" not in cfg["env"]


def test_read_run_manifest_reads_a_real_manifest_file(mod, tmp_path):
    run_dir = tmp_path / "run_abc"
    run_dir.mkdir()
    (run_dir / "manifest.json").write_text('[{"file_id": "f1", "name": "x.png"}]')
    assert mod.read_run_manifest(str(tmp_path), "run_abc") == [{"file_id": "f1", "name": "x.png"}]


def test_lookup_report_file_finds_by_id(mod, tmp_path):
    run_dir = tmp_path / "run_abc"
    run_dir.mkdir()
    (run_dir / "manifest.json").write_text('[{"file_id": "f1", "name": "x.png"}, {"file_id": "f2", "name": "y.pdf"}]')
    assert mod.lookup_report_file(str(tmp_path), "run_abc", "f2")["name"] == "y.pdf"
    assert mod.lookup_report_file(str(tmp_path), "run_abc", "nope") is None


# --- create_canvas (enable_canvas_tool) --------------------------------------------------

class _CanvasFakeClient:
    def __init__(self, create_ok=True, team_ok=True):
        self._create_ok = create_ok
        self._team_ok = team_ok
        self.calls = []

    def canvases_create(self, **kwargs):
        self.calls.append(kwargs)
        if not self._create_ok:
            return {"ok": False, "error": "canvas_disabled_team"}
        return {"ok": True, "canvas_id": "F123ABC"}

    def team_info(self):
        if not self._team_ok:
            return {"ok": False}
        return {"ok": True, "team": {"id": "T000", "domain": "my-workspace"}}


def test_create_canvas_returns_canvas_id_and_link(mod, monkeypatch):
    client = _CanvasFakeClient()
    monkeypatch.setattr(mod, "_get_slack_client", lambda env: client)
    tools = mod.SlackChatTools(channel="C1", thread_ts="100.001", enable_canvas_tool=True)
    result = tools.call("create_canvas", {"title": "Notes", "markdown": "# hi"})
    assert result == {"canvas_id": "F123ABC", "link": "https://my-workspace.slack.com/docs/T000/F123ABC"}
    assert client.calls[0]["channel_id"] == "C1"
    assert client.calls[0]["document_content"] == {"type": "markdown", "markdown": "# hi"}


def test_create_canvas_disabled_by_default(mod, monkeypatch):
    client = _CanvasFakeClient()
    monkeypatch.setattr(mod, "_get_slack_client", lambda env: client)
    tools = mod.SlackChatTools(channel="C1", thread_ts="100.001")
    result = tools.call("create_canvas", {"title": "Notes", "markdown": "# hi"})
    assert "enable_canvas_tool" in result["error"]


def test_create_canvas_api_failure_surfaces_as_error(mod, monkeypatch):
    client = _CanvasFakeClient(create_ok=False)
    monkeypatch.setattr(mod, "_get_slack_client", lambda env: client)
    tools = mod.SlackChatTools(channel="C1", thread_ts="100.001", enable_canvas_tool=True)
    result = tools.call("create_canvas", {"title": "Notes", "markdown": "# hi"})
    assert "error" in result


def test_create_canvas_missing_link_when_team_info_fails(mod, monkeypatch):
    client = _CanvasFakeClient(team_ok=False)
    monkeypatch.setattr(mod, "_get_slack_client", lambda env: client)
    tools = mod.SlackChatTools(channel="C1", thread_ts="100.001", enable_canvas_tool=True)
    result = tools.call("create_canvas", {"title": "Notes", "markdown": "# hi"})
    assert result == {"canvas_id": "F123ABC", "link": None}


def test_create_canvas_requires_title_and_markdown(mod, monkeypatch):
    client = _CanvasFakeClient()
    monkeypatch.setattr(mod, "_get_slack_client", lambda env: client)
    tools = mod.SlackChatTools(channel="C1", thread_ts="100.001", enable_canvas_tool=True)
    assert "error" in tools.call("create_canvas", {"title": "", "markdown": "# hi"})


def test_mcp_server_resource_key_sets_enable_canvas_env_when_true(mod):
    component = mod.SlackChatToolsComponent(mcp_server_resource_key="slack_chat_mcp", enable_canvas_tool=True)
    cfg = component.build_defs(None).resources["slack_chat_mcp"].model_dump()
    assert cfg["env"]["SLACK_CHAT_TOOLS_ENABLE_CANVAS"] == "1"


def test_mcp_server_resource_key_omits_enable_canvas_env_by_default(mod):
    component = mod.SlackChatToolsComponent(mcp_server_resource_key="slack_chat_mcp")
    cfg = component.build_defs(None).resources["slack_chat_mcp"].model_dump()
    assert "SLACK_CHAT_TOOLS_ENABLE_CANVAS" not in cfg["env"]
