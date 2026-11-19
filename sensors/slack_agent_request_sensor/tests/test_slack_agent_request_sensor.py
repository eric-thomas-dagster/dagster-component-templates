"""Real, committed tests for SlackAgentRequestSensorComponent -- mocks only the Slack client
(same convention as slack_run_request_sensor's own tests), exercises the real sensor function
via dg.build_sensor_context()."""
import importlib.util
import json
import pathlib

import dagster as dg
import pytest

_HERE = pathlib.Path(__file__).resolve().parent.parent
_spec = importlib.util.spec_from_file_location("slack_agent_request_sensor_component", _HERE / "component.py")
mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(mod)

SlackAgentRequestSensorComponent = mod.SlackAgentRequestSensorComponent


@pytest.fixture()
def chat_agent_asset():
    @dg.asset(key=dg.AssetKey(["chat_agent"]))
    def chat_agent():
        return "ok"
    return chat_agent


def _component(**overrides):
    defaults = dict(
        sensor_name="ask_dagster", slack_channel_id="C1", asset_selection=["chat_agent"], default_status="RUNNING",
    )
    defaults.update(overrides)
    return SlackAgentRequestSensorComponent(**defaults)


def _msg(ts, user, text, thread_ts=None, subtype=None, bot_id=None):
    m = {"ts": ts, "user": user, "text": text}
    if thread_ts:
        m["thread_ts"] = thread_ts
    if subtype:
        m["subtype"] = subtype
    if bot_id:
        m["bot_id"] = bot_id
    return m


class _FakeClient:
    def __init__(self, *, root_messages=None, replies_by_thread=None, bot_user_id="UBOT", email_by_user=None, history_fail=None, chat_post_fail=None):
        self.root_messages = root_messages or []
        self.replies_by_thread = replies_by_thread or {}
        self.bot_user_id = bot_user_id
        self.email_by_user = email_by_user or {}
        self.history_fail = history_fail
        self.chat_post_fail = chat_post_fail
        self._ts_counter = 0
        self.calls = {"history": [], "replies": [], "reactions": [], "chat_post": []}

    def auth_test(self):
        return {"ok": True, "user_id": self.bot_user_id}

    def conversations_history(self, channel, oldest, limit, cursor=None):
        self.calls["history"].append({"channel": channel, "oldest": oldest})
        if self.history_fail:
            return {"ok": False, "error": self.history_fail}
        return {"ok": True, "messages": self.root_messages}

    def conversations_replies(self, channel, ts, oldest, limit):
        self.calls["replies"].append({"ts": ts, "oldest": oldest})
        return {"ok": True, "messages": self.replies_by_thread.get(ts, [])}

    def users_info(self, user):
        return {"ok": True, "user": {"profile": {"email": self.email_by_user.get(user)}}}

    def reactions_add(self, **kwargs):
        self.calls["reactions"].append(kwargs)
        return {"ok": True}

    def chat_postMessage(self, **kwargs):
        self.calls["chat_post"].append(kwargs)
        if self.chat_post_fail:
            return {"ok": False, "error": self.chat_post_fail}
        self._ts_counter += 1
        return {"ok": True, "ts": f"placeholder.{self._ts_counter}"}


def _ctx(cursor_dict=None):
    return dg.build_sensor_context(cursor=json.dumps(cursor_dict) if cursor_dict is not None else None)


# --- construction / validation -----------------------------------------------------------

def test_asset_selection_empty_match_raises_clear_error(chat_agent_asset):
    component = _component(asset_selection="tag:nope=nope")  # a DSL string with no sibling context resolves to no matches
    with pytest.raises(ValueError, match="matched no assets"):
        component.build_defs(context=None)


def test_invalid_default_status_rejected(chat_agent_asset):
    component = _component(default_status="bogus")
    with pytest.raises(ValueError, match="default_status"):
        component.build_defs(context=None)


# --- trigger_mode: mention (default) ------------------------------------------------------

def test_mention_triggers_a_run_unmentioned_message_does_not(monkeypatch, chat_agent_asset):
    component = _component()
    sensor_def = component.build_defs(context=None).sensors[0]
    client = _FakeClient(root_messages=[
        _msg("100.001", "U1", "<@UBOT> what jobs failed today?"),
        _msg("100.002", "U2", "just chatting, no mention"),
    ], email_by_user={"U1": "jane@co.com"})
    monkeypatch.setattr(mod, "_get_slack_client", lambda env: client)

    result = sensor_def(_ctx({"last_ts": "100.000", "threads": {}}))
    assert [(r.run_key, r.tags["slack/user"]) for r in result.run_requests] == [("ask_dagster:100.001", "jane@co.com")]
    cursor = json.loads(result.cursor)
    assert cursor["last_ts"] == "100.002"
    assert cursor["threads"] == {"100.001": "100.001"}


def test_bot_and_subtype_and_self_messages_are_never_candidates(monkeypatch, chat_agent_asset):
    component = _component()
    sensor_def = component.build_defs(context=None).sensors[0]
    client = _FakeClient(root_messages=[
        _msg("100.001", "UBOT", "<@UBOT> I am the bot, ignore me"),  # from the bot itself
        _msg("100.002", "U1", "<@UBOT> hi", bot_id="B1"),  # bot_id set -- integration message
        _msg("100.003", "U1", "<@UBOT> hi", subtype="channel_join"),
    ])
    monkeypatch.setattr(mod, "_get_slack_client", lambda env: client)
    result = sensor_def(_ctx({"last_ts": "100.000", "threads": {}}))
    assert result.run_requests == []


# --- trigger_mode: dm / all ----------------------------------------------------------------

def test_all_mode_matches_every_message_without_a_mention(monkeypatch, chat_agent_asset):
    component = _component(trigger_mode="all")
    sensor_def = component.build_defs(context=None).sensors[0]
    client = _FakeClient(root_messages=[_msg("100.001", "U1", "no mention here at all")])
    monkeypatch.setattr(mod, "_get_slack_client", lambda env: client)
    result = sensor_def(_ctx({"last_ts": "100.000", "threads": {}}))
    assert len(result.run_requests) == 1


# --- follow_up_threads ----------------------------------------------------------------------

def test_follow_up_reply_in_a_tracked_thread_triggers_without_a_mention(monkeypatch, chat_agent_asset):
    component = _component()
    sensor_def = component.build_defs(context=None).sensors[0]
    client = _FakeClient(
        root_messages=[],
        replies_by_thread={"100.001": [_msg("100.005", "U1", "a follow-up question", thread_ts="100.001")]},
        email_by_user={"U1": "jane@co.com"},
    )
    monkeypatch.setattr(mod, "_get_slack_client", lambda env: client)

    result = sensor_def(_ctx({"last_ts": "100.002", "threads": {"100.001": "100.001"}}))
    assert [(r.run_key, r.tags["slack/thread_ts"], r.tags["slack/message_ts"]) for r in result.run_requests] == [
        ("ask_dagster:100.005", "100.001", "100.005"),
    ]
    assert client.calls["replies"] == [{"ts": "100.001", "oldest": "100.001"}]
    cursor = json.loads(result.cursor)
    assert cursor["threads"]["100.001"] == "100.005"  # advanced so the same reply isn't reprocessed


def test_follow_up_threads_false_ignores_replies_entirely(monkeypatch, chat_agent_asset):
    component = _component(follow_up_threads=False)
    sensor_def = component.build_defs(context=None).sensors[0]
    client = _FakeClient(root_messages=[])
    monkeypatch.setattr(mod, "_get_slack_client", lambda env: client)
    result = sensor_def(_ctx({"last_ts": "100.002", "threads": {"100.001": "100.001"}}))
    assert result.run_requests == []
    assert client.calls["replies"] == []


def test_known_threads_limit_bounds_the_cursor(monkeypatch, chat_agent_asset):
    component = _component(known_threads_limit=1)
    sensor_def = component.build_defs(context=None).sensors[0]
    client = _FakeClient(root_messages=[
        _msg("100.001", "U1", "<@UBOT> first"),
        _msg("100.002", "U2", "<@UBOT> second"),
    ])
    monkeypatch.setattr(mod, "_get_slack_client", lambda env: client)
    result = sensor_def(_ctx({"last_ts": "100.000", "threads": {}}))
    assert len(result.run_requests) == 2  # both still fire this tick
    cursor = json.loads(result.cursor)
    assert len(cursor["threads"]) == 1  # but only the most-recently-active thread survives


# --- cursor seeding / auth / error handling --------------------------------------------------

def test_first_tick_seeds_cursor_to_now_and_processes_nothing(monkeypatch, chat_agent_asset):
    component = _component()
    sensor_def = component.build_defs(context=None).sensors[0]
    client = _FakeClient(root_messages=[_msg("1.001", "U1", "<@UBOT> hi")])
    monkeypatch.setattr(mod, "_get_slack_client", lambda env: client)
    result = sensor_def(_ctx(None))
    assert result.run_requests == []
    assert "seeding cursor to now" in result.skip_reason.skip_message


def test_process_backlog_on_first_run_true_processes_history(monkeypatch, chat_agent_asset):
    component = _component(process_backlog_on_first_run=True)
    sensor_def = component.build_defs(context=None).sensors[0]
    client = _FakeClient(root_messages=[_msg("1.001", "U1", "<@UBOT> hi")])
    monkeypatch.setattr(mod, "_get_slack_client", lambda env: client)
    result = sensor_def(_ctx(None))
    assert len(result.run_requests) == 1


def test_history_api_failure_skips_cleanly(monkeypatch, chat_agent_asset):
    component = _component()
    sensor_def = component.build_defs(context=None).sensors[0]
    client = _FakeClient(history_fail="not_in_channel")
    monkeypatch.setattr(mod, "_get_slack_client", lambda env: client)
    result = sensor_def(_ctx({"last_ts": "100.000", "threads": {}}))
    assert "conversations_history failed" in result.skip_message


def test_corrupt_cursor_restarts_clean_instead_of_crashing(monkeypatch, chat_agent_asset):
    component = _component()
    sensor_def = component.build_defs(context=None).sensors[0]
    client = _FakeClient(root_messages=[])
    monkeypatch.setattr(mod, "_get_slack_client", lambda env: client)
    ctx = dg.build_sensor_context(cursor="not valid json{{{")
    result = sensor_def(ctx)  # must not raise
    assert isinstance(result, (dg.SensorResult,))


def test_post_acknowledgement_reacts_to_matched_messages(monkeypatch, chat_agent_asset):
    component = _component()
    sensor_def = component.build_defs(context=None).sensors[0]
    client = _FakeClient(root_messages=[_msg("100.001", "U1", "<@UBOT> hi")])
    monkeypatch.setattr(mod, "_get_slack_client", lambda env: client)
    sensor_def(_ctx({"last_ts": "100.000", "threads": {}}))
    assert client.calls["reactions"] == [{"channel": "C1", "timestamp": "100.001", "name": "eyes"}]


def test_resolve_requester_email_false_uses_raw_user_id(monkeypatch, chat_agent_asset):
    component = _component(resolve_requester_email=False)
    sensor_def = component.build_defs(context=None).sensors[0]
    client = _FakeClient(root_messages=[_msg("100.001", "U1", "<@UBOT> hi")], email_by_user={"U1": "jane@co.com"})
    monkeypatch.setattr(mod, "_get_slack_client", lambda env: client)
    result = sensor_def(_ctx({"last_ts": "100.000", "threads": {}}))
    assert result.run_requests[0].tags["slack/user"] == "U1"


def test_set_isolation_disabled_tag_false_omits_the_tag(monkeypatch, chat_agent_asset):
    component = _component(set_isolation_disabled_tag=False)
    sensor_def = component.build_defs(context=None).sensors[0]
    client = _FakeClient(root_messages=[_msg("100.001", "U1", "<@UBOT> hi")])
    monkeypatch.setattr(mod, "_get_slack_client", lambda env: client)
    result = sensor_def(_ctx({"last_ts": "100.000", "threads": {}}))
    assert "dagster/isolation" not in result.run_requests[0].tags


def test_use_in_process_executor_defaults_to_true(monkeypatch, chat_agent_asset):
    component = _component()
    sensor_def = component.build_defs(context=None).sensors[0]
    client = _FakeClient(root_messages=[_msg("100.001", "U1", "<@UBOT> hi")])
    monkeypatch.setattr(mod, "_get_slack_client", lambda env: client)
    result = sensor_def(_ctx({"last_ts": "100.000", "threads": {}}))
    assert result.run_requests[0].run_config == {"execution": {"config": {"in_process": {}}}}


def test_use_in_process_executor_false_leaves_run_config_empty(monkeypatch, chat_agent_asset):
    component = _component(use_in_process_executor=False)
    sensor_def = component.build_defs(context=None).sensors[0]
    client = _FakeClient(root_messages=[_msg("100.001", "U1", "<@UBOT> hi")])
    monkeypatch.setattr(mod, "_get_slack_client", lambda env: client)
    result = sensor_def(_ctx({"last_ts": "100.000", "threads": {}}))
    assert result.run_requests[0].run_config == {}


# --- placeholder_text --------------------------------------------------------------------

def test_placeholder_text_posts_a_message_and_tags_its_ts(monkeypatch, chat_agent_asset):
    component = _component(placeholder_text="Looking into it…")
    sensor_def = component.build_defs(context=None).sensors[0]
    client = _FakeClient(root_messages=[_msg("100.001", "U1", "<@UBOT> hi")])
    monkeypatch.setattr(mod, "_get_slack_client", lambda env: client)
    result = sensor_def(_ctx({"last_ts": "100.000", "threads": {}}))
    assert client.calls["chat_post"] == [{"channel": "C1", "thread_ts": "100.001", "text": "Looking into it…"}]
    assert result.run_requests[0].tags["slack/placeholder_ts"] == "placeholder.1"


def test_placeholder_text_unset_never_posts_or_tags(monkeypatch, chat_agent_asset):
    component = _component()
    sensor_def = component.build_defs(context=None).sensors[0]
    client = _FakeClient(root_messages=[_msg("100.001", "U1", "<@UBOT> hi")])
    monkeypatch.setattr(mod, "_get_slack_client", lambda env: client)
    result = sensor_def(_ctx({"last_ts": "100.000", "threads": {}}))
    assert client.calls["chat_post"] == []
    assert "slack/placeholder_ts" not in result.run_requests[0].tags


def test_placeholder_text_post_failure_does_not_block_the_launch(monkeypatch, chat_agent_asset):
    component = _component(placeholder_text="Looking into it…")
    sensor_def = component.build_defs(context=None).sensors[0]
    client = _FakeClient(root_messages=[_msg("100.001", "U1", "<@UBOT> hi")], chat_post_fail="channel_not_found")
    monkeypatch.setattr(mod, "_get_slack_client", lambda env: client)
    result = sensor_def(_ctx({"last_ts": "100.000", "threads": {}}))
    assert len(result.run_requests) == 1
    assert "slack/placeholder_ts" not in result.run_requests[0].tags
