"""get_thread() is the chat agent's only window onto the Teams conversation -- always reads
THIS run's own team/channel/thread (no per-call argument), fetches fresh every time. Mocks
only the unavoidable external calls (requests.get/requests.post for Graph + OAuth), per this
repo's test convention."""
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


class _Resp:
    def __init__(self, body):
        self._body = body

    def raise_for_status(self):
        pass

    def json(self):
        return self._body


def _install_fakes(monkeypatch, *, root=None, replies=None, get_fail=None):
    import requests

    calls = {"get": [], "post": []}

    def fake_post(url, **kwargs):
        calls["post"].append((url, kwargs))
        if "oauth2/v2.0/token" in url:
            return _Resp({"access_token": "BEARER123"})
        return _Resp({})

    def fake_get(url, **kwargs):
        calls["get"].append((url, kwargs))
        if get_fail:
            raise RuntimeError(get_fail)
        if url.endswith("/replies"):
            return _Resp({"value": replies or []})
        return _Resp(root or {})

    monkeypatch.setattr(requests, "post", fake_post)
    monkeypatch.setattr(requests, "get", fake_get)
    monkeypatch.setenv("TEAMS_TENANT_ID", "tid")
    monkeypatch.setenv("TEAMS_CLIENT_ID", "cid")
    monkeypatch.setenv("TEAMS_CLIENT_SECRET", "sec")
    monkeypatch.setenv("TEAMS_REFRESH_TOKEN", "seed")
    return calls


def _user_msg(id_, created, aad_id, text):
    return {"id": id_, "createdDateTime": created, "from": {"user": {"id": aad_id}}, "body": {"content": f"<p>{text}</p>"}}


def _bot_msg(id_, created, text):
    return {"id": id_, "createdDateTime": created, "from": {"application": {"id": "BOT"}}, "body": {"content": f"<p>{text}</p>"}}


def _tools(mod, tmp_path, **overrides):
    defaults = dict(oauth_state_dir=str(tmp_path / "state"), team_id="T1", channel_id="Ch1", thread_id="root1")
    defaults.update(overrides)
    return mod.TeamsChatTools(**defaults)


def test_get_thread_merges_root_and_replies_oldest_first(mod, monkeypatch, tmp_path):
    root = _user_msg("root1", "2026-01-01T00:00:00Z", "U1", "can someone run nightly etl")
    reply = _bot_msg("r1", "2026-01-01T00:00:05Z", "Got it")
    _install_fakes(monkeypatch, root=root, replies=[reply])
    tools = _tools(mod, tmp_path)
    result = tools.call("get_thread", {})
    assert result["team_id"] == "T1" and result["channel_id"] == "Ch1" and result["thread_id"] == "root1"
    assert result["message_count"] == 2
    assert [m["text"] for m in result["messages"]] == ["can someone run nightly etl", "Got it"]


def test_get_thread_works_for_a_solo_message_with_no_replies(mod, monkeypatch, tmp_path):
    root = _user_msg("root1", "2026-01-01T00:00:00Z", "U1", "what jobs failed today?")
    _install_fakes(monkeypatch, root=root, replies=[])
    tools = _tools(mod, tmp_path)
    result = tools.call("get_thread", {})
    assert result["message_count"] == 1


def test_missing_team_id_errors_clearly(mod, tmp_path):
    tools = _tools(mod, tmp_path, team_id=None)
    result = tools.call("get_thread", {})
    assert "no team configured" in result["error"]


def test_missing_channel_id_errors_clearly(mod, tmp_path):
    tools = _tools(mod, tmp_path, channel_id=None)
    result = tools.call("get_thread", {})
    assert "no channel configured" in result["error"]


def test_missing_thread_id_errors_clearly(mod, tmp_path):
    tools = _tools(mod, tmp_path, thread_id=None)
    result = tools.call("get_thread", {})
    assert "no thread configured" in result["error"]


def test_graph_failure_surfaces_as_error_not_exception(mod, monkeypatch, tmp_path):
    _install_fakes(monkeypatch, get_fail="boom")
    tools = _tools(mod, tmp_path)
    result = tools.call("get_thread", {})
    assert "error" in result and "boom" in result["error"]


def test_max_messages_truncates_to_most_recent(mod, monkeypatch, tmp_path):
    root = _user_msg("root1", "2026-01-01T00:00:00Z", "U1", "msg0")
    replies = [_user_msg(f"r{i}", f"2026-01-01T00:0{i}:00Z", "U1", f"msg{i}") for i in range(1, 5)]
    _install_fakes(monkeypatch, root=root, replies=replies)
    tools = _tools(mod, tmp_path, max_messages=2)
    result = tools.call("get_thread", {})
    assert [m["text"] for m in result["messages"]] == ["msg3", "msg4"]


def test_result_size_is_bounded_by_dropping_oldest_messages_first(mod, monkeypatch, tmp_path):
    long_text = "x" * 500
    root = _user_msg("root1", "2026-01-01T00:00:00Z", "U1", long_text)
    replies = [_user_msg(f"r{i}", f"2026-01-01T00:0{i}:00Z", "U1", long_text) for i in range(1, 9)]
    _install_fakes(monkeypatch, root=root, replies=replies)
    tools = _tools(mod, tmp_path, max_messages=9, max_result_chars=1000)
    result = tools.call("get_thread", {})
    assert len(result["messages"]) < 9
    assert result["messages"][-1]["text"] == long_text  # the most recent message always survives


def test_unknown_tool_name_returns_error_not_exception(mod, tmp_path):
    tools = _tools(mod, tmp_path)
    result = tools.call("not_a_real_tool", {})
    assert "unknown tool" in result["error"]


def test_secrets_in_message_text_are_redacted(mod, monkeypatch, tmp_path):
    root = _user_msg("root1", "2026-01-01T00:00:00Z", "U1", "here's the token: xoxb-1234567890-abcdefghij")
    _install_fakes(monkeypatch, root=root, replies=[])
    tools = _tools(mod, tmp_path)
    result = tools.call("get_thread", {})
    assert "xoxb-1234567890" not in str(result)
    assert "[REDACTED]" in result["messages"][0]["text"]


def test_get_teams_bearer_raises_clearly_when_env_vars_missing(mod, monkeypatch, tmp_path):
    monkeypatch.delenv("TEAMS_TENANT_ID", raising=False)
    fs = mod._StateFS(str(tmp_path))
    with pytest.raises(RuntimeError, match="not all set"):
        mod._get_teams_bearer(fs, "TEAMS_TENANT_ID", "TEAMS_CLIENT_ID", "TEAMS_CLIENT_SECRET", "TEAMS_REFRESH_TOKEN")


# --- receipts_db_path: attaching receipt_ids to bot messages ---------------------------

def test_bot_message_gets_receipt_ids_attached(mod, monkeypatch, tmp_path):
    db_path = str(tmp_path / "r.sqlite3")
    mod.record_message_links(db_path, ["r1", "r2"], platform="teams", channel="T1|Ch1", message_ts="root1|reply1")
    root = _user_msg("root1", "2026-01-01T00:00:00Z", "U1", "what's our per diem policy?")
    reply = _bot_msg("reply1", "2026-01-01T00:00:05Z", "It's $50/day.")
    _install_fakes(monkeypatch, root=root, replies=[reply])
    tools = _tools(mod, tmp_path, receipts_db_path=db_path)
    result = tools.call("get_thread", {})
    user_row, bot_row = result["messages"]
    assert "receipt_ids" not in user_row
    assert set(bot_row["receipt_ids"]) == {"r1", "r2"}


def test_bot_message_with_no_linked_receipts_gets_an_empty_list(mod, monkeypatch, tmp_path):
    db_path = str(tmp_path / "r.sqlite3")
    root = _user_msg("root1", "2026-01-01T00:00:00Z", "U1", "hi")
    reply = _bot_msg("reply1", "2026-01-01T00:00:05Z", "hi back")
    _install_fakes(monkeypatch, root=root, replies=[reply])
    tools = _tools(mod, tmp_path, receipts_db_path=db_path)
    result = tools.call("get_thread", {})
    assert result["messages"][1]["receipt_ids"] == []


def test_receipts_db_path_unset_never_attaches_receipt_ids(mod, monkeypatch, tmp_path):
    root = _user_msg("root1", "2026-01-01T00:00:00Z", "U1", "hi")
    reply = _bot_msg("reply1", "2026-01-01T00:00:05Z", "hi back")
    _install_fakes(monkeypatch, root=root, replies=[reply])
    tools = _tools(mod, tmp_path)
    result = tools.call("get_thread", {})
    assert "receipt_ids" not in result["messages"][1]


# --- Component -----------------------------------------------------------------------

def test_component_registers_resource_under_resource_key(mod, tmp_path):
    component = mod.TeamsChatToolsComponent(resource_key="chat_tools", oauth_state_dir=str(tmp_path))
    defs = component.build_defs(None)
    assert "chat_tools" in defs.resources


def test_mcp_server_resource_key_passes_templates_through_verbatim(mod, tmp_path):
    component = mod.TeamsChatToolsComponent(mcp_server_resource_key="teams_chat_mcp", oauth_state_dir=str(tmp_path))
    defs = component.build_defs(None)
    dumped = defs.resources["teams_chat_mcp"].model_dump()
    assert dumped["env"]["TEAMS_CHAT_TOOLS_TEAM_ID"] == "{tag.teams/team_id}"
    assert dumped["env"]["TEAMS_CHAT_TOOLS_CHANNEL_ID"] == "{tag.teams/channel_id}"
    assert dumped["env"]["TEAMS_CHAT_TOOLS_THREAD_ID"] == "{tag.teams/thread_id}"


def test_mcp_server_resource_key_includes_receipts_db_path_when_set(mod, tmp_path):
    component = mod.TeamsChatToolsComponent(mcp_server_resource_key="teams_chat_mcp", oauth_state_dir=str(tmp_path), receipts_db_path="/tmp/r.sqlite3")
    defs = component.build_defs(None)
    dumped = defs.resources["teams_chat_mcp"].model_dump()
    assert dumped["env"]["TEAMS_CHAT_TOOLS_RECEIPTS_DB_PATH"] == "/tmp/r.sqlite3"


def test_mcp_server_resource_key_omits_receipts_db_path_when_unset(mod, tmp_path):
    component = mod.TeamsChatToolsComponent(mcp_server_resource_key="teams_chat_mcp", oauth_state_dir=str(tmp_path))
    defs = component.build_defs(None)
    dumped = defs.resources["teams_chat_mcp"].model_dump()
    assert "TEAMS_CHAT_TOOLS_RECEIPTS_DB_PATH" not in dumped["env"]
