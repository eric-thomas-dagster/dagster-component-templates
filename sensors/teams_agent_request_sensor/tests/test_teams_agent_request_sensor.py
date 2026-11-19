"""Real, committed tests for TeamsAgentRequestSensorComponent -- mocks only the unavoidable
external calls (requests.post/requests.get for Graph + OAuth), per this repo's test
convention, and exercises the real sensor function via dg.build_sensor_context()."""
import importlib.util
import json
import pathlib

import dagster as dg
import pytest

_HERE = pathlib.Path(__file__).resolve().parent.parent
_spec = importlib.util.spec_from_file_location("teams_agent_request_sensor_component", _HERE / "component.py")
mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(mod)

TeamsAgentRequestSensorComponent = mod.TeamsAgentRequestSensorComponent


@pytest.fixture()
def chat_agent_asset():
    @dg.asset(key=dg.AssetKey(["chat_agent"]))
    def chat_agent():
        return "ok"
    return chat_agent


def _component(tmp_path, **overrides):
    defaults = dict(
        sensor_name="ask_dagster_teams", teams_team_id="T1", teams_channel_id="Ch1",
        asset_selection=["chat_agent"], oauth_state_dir=str(tmp_path / "state"), default_status="RUNNING",
    )
    defaults.update(overrides)
    return TeamsAgentRequestSensorComponent(**defaults)


def _msg(id_, created, aad_id, mentions=None):
    m = {"id": id_, "createdDateTime": created, "from": {"user": {"id": aad_id}}}
    if mentions is not None:
        m["mentions"] = mentions
    return m


def _mention(app_id):
    return [{"id": 0, "mentionText": "bot", "mentioned": {"application": {"id": app_id}}}]


class _Resp:
    def __init__(self, body):
        self._body = body

    def raise_for_status(self):
        pass

    def json(self):
        return self._body


def _install_fakes(monkeypatch, *, root_messages=None, replies_by_thread=None, email_by_aad_id=None, list_fail=None):
    import requests

    root_messages = root_messages if root_messages is not None else []
    replies_by_thread = replies_by_thread or {}
    email_by_aad_id = email_by_aad_id or {}
    calls = {"post": [], "get": []}

    def fake_post(url, **kwargs):
        calls["post"].append((url, kwargs))
        if "oauth2/v2.0/token" in url:
            return _Resp({"access_token": "BEARER123"})
        return _Resp({})

    def fake_get(url, **kwargs):
        calls["get"].append((url, kwargs))
        if "/users/" in url:
            aad_id = url.rsplit("/", 1)[-1]
            return _Resp({"mail": email_by_aad_id.get(aad_id), "userPrincipalName": None})
        if url.endswith("/messages"):
            if list_fail:
                raise RuntimeError(list_fail)
            return _Resp({"value": root_messages})
        if "/replies" in url:
            thread_id = url.split("/messages/")[1].split("/replies")[0]
            return _Resp({"value": replies_by_thread.get(thread_id, [])})
        return _Resp({"value": []})

    monkeypatch.setattr(requests, "post", fake_post)
    monkeypatch.setattr(requests, "get", fake_get)
    monkeypatch.setenv("TEAMS_TENANT_ID", "tid")
    monkeypatch.setenv("TEAMS_CLIENT_ID", "BOTAPPID")
    monkeypatch.setenv("TEAMS_CLIENT_SECRET", "sec")
    monkeypatch.setenv("TEAMS_REFRESH_TOKEN", "seed")
    return calls


def _ctx(cursor_dict=None):
    return dg.build_sensor_context(cursor=json.dumps(cursor_dict) if cursor_dict is not None else None)


# --- construction / validation -----------------------------------------------------------

def test_asset_selection_empty_match_raises_clear_error(tmp_path, chat_agent_asset):
    component = _component(tmp_path, asset_selection="tag:nope=nope")
    with pytest.raises(ValueError, match="matched no assets"):
        component.build_defs(context=None)


def test_invalid_default_status_rejected(tmp_path, chat_agent_asset):
    component = _component(tmp_path, default_status="bogus")
    with pytest.raises(ValueError, match="default_status"):
        component.build_defs(context=None)


# --- mention / all trigger modes ----------------------------------------------------------

def test_mention_triggers_a_run(tmp_path, monkeypatch, chat_agent_asset):
    component = _component(tmp_path)
    sensor_def = component.build_defs(context=None).sensors[0]
    _install_fakes(monkeypatch, root_messages=[_msg("m1", "2026-01-01T00:00:01Z", "U1", mentions=_mention("BOTAPPID"))])
    result = sensor_def(_ctx({"last_ts": "2026-01-01T00:00:00Z", "threads": {}}))
    assert len(result.run_requests) == 1
    assert result.run_requests[0].tags["teams/message_id"] == "m1"
    assert result.run_requests[0].tags["teams/thread_id"] == "m1"


def test_unmentioned_message_does_not_trigger_in_mention_mode(tmp_path, monkeypatch, chat_agent_asset):
    component = _component(tmp_path)
    sensor_def = component.build_defs(context=None).sensors[0]
    _install_fakes(monkeypatch, root_messages=[_msg("m1", "2026-01-01T00:00:01Z", "U1")])
    result = sensor_def(_ctx({"last_ts": "2026-01-01T00:00:00Z", "threads": {}}))
    assert not result.run_requests


def test_all_mode_matches_every_message(tmp_path, monkeypatch, chat_agent_asset):
    component = _component(tmp_path, trigger_mode="all")
    sensor_def = component.build_defs(context=None).sensors[0]
    _install_fakes(monkeypatch, root_messages=[_msg("m1", "2026-01-01T00:00:01Z", "U1")])
    result = sensor_def(_ctx({"last_ts": "2026-01-01T00:00:00Z", "threads": {}}))
    assert len(result.run_requests) == 1


def test_system_message_with_no_user_from_is_excluded(tmp_path, monkeypatch, chat_agent_asset):
    component = _component(tmp_path, trigger_mode="all")
    sensor_def = component.build_defs(context=None).sensors[0]
    system_msg = {"id": "m1", "createdDateTime": "2026-01-01T00:00:01Z", "from": {"application": {"id": "SYS"}}}
    _install_fakes(monkeypatch, root_messages=[system_msg])
    result = sensor_def(_ctx({"last_ts": "2026-01-01T00:00:00Z", "threads": {}}))
    assert not result.run_requests


# --- follow-up replies (no delta query -- full re-fetch + client-side diff) --------------

def test_follow_up_reply_triggers_without_mention(tmp_path, monkeypatch, chat_agent_asset):
    component = _component(tmp_path)
    sensor_def = component.build_defs(context=None).sensors[0]
    reply = _msg("r1", "2026-01-01T00:05:00Z", "U2")
    _install_fakes(monkeypatch, root_messages=[], replies_by_thread={"thread1": [reply]})
    result = sensor_def(_ctx({"last_ts": "2026-01-01T00:00:00Z", "threads": {"thread1": "2026-01-01T00:00:00Z"}}))
    assert len(result.run_requests) == 1
    assert result.run_requests[0].tags["teams/thread_id"] == "thread1"
    assert result.run_requests[0].tags["teams/message_id"] == "r1"


def test_follow_up_threads_false_ignores_replies(tmp_path, monkeypatch, chat_agent_asset):
    component = _component(tmp_path, follow_up_threads=False)
    sensor_def = component.build_defs(context=None).sensors[0]
    reply = _msg("r1", "2026-01-01T00:05:00Z", "U2")
    _install_fakes(monkeypatch, root_messages=[], replies_by_thread={"thread1": [reply]})
    result = sensor_def(_ctx({"last_ts": "2026-01-01T00:00:00Z", "threads": {"thread1": "2026-01-01T00:00:00Z"}}))
    assert not result.run_requests


def test_already_seen_reply_is_not_reprocessed(tmp_path, monkeypatch, chat_agent_asset):
    component = _component(tmp_path)
    sensor_def = component.build_defs(context=None).sensors[0]
    reply = _msg("r1", "2026-01-01T00:05:00Z", "U2")
    _install_fakes(monkeypatch, root_messages=[], replies_by_thread={"thread1": [reply]})
    result = sensor_def(_ctx({"last_ts": "2026-01-01T00:00:00Z", "threads": {"thread1": "2026-01-01T00:05:00Z"}}))
    assert not result.run_requests


def test_known_threads_limit_bounds_the_cursor(tmp_path, monkeypatch, chat_agent_asset):
    component = _component(tmp_path, known_threads_limit=1, follow_up_threads=False)
    sensor_def = component.build_defs(context=None).sensors[0]
    _install_fakes(monkeypatch, root_messages=[])
    threads = {"old": "2026-01-01T00:00:00Z", "newer": "2026-01-01T00:01:00Z"}
    result = sensor_def(_ctx({"last_ts": "2026-01-01T00:00:00Z", "threads": threads}))
    new_state = json.loads(result.cursor)
    assert list(new_state["threads"].keys()) == ["newer"]


# --- cursor / backlog behavior -------------------------------------------------------------

def test_first_tick_seeds_cursor_to_now_and_processes_nothing(tmp_path, monkeypatch, chat_agent_asset):
    component = _component(tmp_path)
    sensor_def = component.build_defs(context=None).sensors[0]
    calls = _install_fakes(monkeypatch, root_messages=[_msg("m1", "2026-01-01T00:00:01Z", "U1", mentions=_mention("BOTAPPID"))])
    result = sensor_def(_ctx(None))
    assert not getattr(result, "run_requests", None)
    assert not calls["get"]  # never even listed messages


def test_process_backlog_on_first_run_true_processes_history(tmp_path, monkeypatch, chat_agent_asset):
    component = _component(tmp_path, process_backlog_on_first_run=True)
    sensor_def = component.build_defs(context=None).sensors[0]
    _install_fakes(monkeypatch, root_messages=[_msg("m1", "2026-01-01T00:00:01Z", "U1", mentions=_mention("BOTAPPID"))])
    result = sensor_def(_ctx(None))
    assert len(result.run_requests) == 1


def test_messages_fetch_failure_skips_cleanly(tmp_path, monkeypatch, chat_agent_asset):
    component = _component(tmp_path)
    sensor_def = component.build_defs(context=None).sensors[0]
    _install_fakes(monkeypatch, list_fail="boom")
    result = sensor_def(_ctx({"last_ts": "2026-01-01T00:00:00Z", "threads": {}}))
    assert isinstance(result, dg.SkipReason)


def test_corrupt_cursor_restarts_clean(tmp_path, monkeypatch, chat_agent_asset):
    component = _component(tmp_path)
    sensor_def = component.build_defs(context=None).sensors[0]
    _install_fakes(monkeypatch, root_messages=[])
    result = sensor_def(dg.build_sensor_context(cursor="not json"))
    assert isinstance(result, dg.SensorResult)


# --- acknowledgement reactions, email resolution, tags ------------------------------------

def test_post_acknowledgement_sets_a_reaction(tmp_path, monkeypatch, chat_agent_asset):
    component = _component(tmp_path)
    sensor_def = component.build_defs(context=None).sensors[0]
    calls = _install_fakes(monkeypatch, root_messages=[_msg("m1", "2026-01-01T00:00:01Z", "U1", mentions=_mention("BOTAPPID"))])
    sensor_def(_ctx({"last_ts": "2026-01-01T00:00:00Z", "threads": {}}))
    reaction_posts = [c for c in calls["post"] if "setReaction" in c[0]]
    assert len(reaction_posts) == 1
    assert reaction_posts[0][0].endswith("/messages/m1/setReaction")
    assert reaction_posts[0][1]["json"] == {"reactionType": "👀"}


def test_reply_acknowledgement_reacts_on_the_reply_not_the_root(tmp_path, monkeypatch, chat_agent_asset):
    component = _component(tmp_path)
    sensor_def = component.build_defs(context=None).sensors[0]
    reply = _msg("r1", "2026-01-01T00:05:00Z", "U2")
    calls = _install_fakes(monkeypatch, root_messages=[], replies_by_thread={"thread1": [reply]})
    sensor_def(_ctx({"last_ts": "2026-01-01T00:00:00Z", "threads": {"thread1": "2026-01-01T00:00:00Z"}}))
    reaction_posts = [c for c in calls["post"] if "setReaction" in c[0]]
    assert reaction_posts[0][0].endswith("/messages/thread1/replies/r1/setReaction")


def test_resolve_requester_email_false_uses_raw_aad_id(tmp_path, monkeypatch, chat_agent_asset):
    component = _component(tmp_path, resolve_requester_email=False)
    sensor_def = component.build_defs(context=None).sensors[0]
    _install_fakes(monkeypatch, root_messages=[_msg("m1", "2026-01-01T00:00:01Z", "U1", mentions=_mention("BOTAPPID"))], email_by_aad_id={"U1": "jane@co.com"})
    result = sensor_def(_ctx({"last_ts": "2026-01-01T00:00:00Z", "threads": {}}))
    assert result.run_requests[0].tags["teams/user"] == "U1"


def test_resolve_requester_email_true_uses_resolved_email(tmp_path, monkeypatch, chat_agent_asset):
    component = _component(tmp_path)
    sensor_def = component.build_defs(context=None).sensors[0]
    _install_fakes(monkeypatch, root_messages=[_msg("m1", "2026-01-01T00:00:01Z", "U1", mentions=_mention("BOTAPPID"))], email_by_aad_id={"U1": "jane@co.com"})
    result = sensor_def(_ctx({"last_ts": "2026-01-01T00:00:00Z", "threads": {}}))
    assert result.run_requests[0].tags["teams/user"] == "jane@co.com"


def test_set_isolation_disabled_tag_false_omits_the_tag(tmp_path, monkeypatch, chat_agent_asset):
    component = _component(tmp_path, set_isolation_disabled_tag=False)
    sensor_def = component.build_defs(context=None).sensors[0]
    _install_fakes(monkeypatch, root_messages=[_msg("m1", "2026-01-01T00:00:01Z", "U1", mentions=_mention("BOTAPPID"))])
    result = sensor_def(_ctx({"last_ts": "2026-01-01T00:00:00Z", "threads": {}}))
    assert "dagster/isolation" not in result.run_requests[0].tags


def test_use_in_process_executor_defaults_to_true(tmp_path, monkeypatch, chat_agent_asset):
    component = _component(tmp_path)
    sensor_def = component.build_defs(context=None).sensors[0]
    _install_fakes(monkeypatch, root_messages=[_msg("m1", "2026-01-01T00:00:01Z", "U1", mentions=_mention("BOTAPPID"))])
    result = sensor_def(_ctx({"last_ts": "2026-01-01T00:00:00Z", "threads": {}}))
    assert result.run_requests[0].run_config == {"execution": {"config": {"in_process": {}}}}


def test_use_in_process_executor_false_leaves_run_config_empty(tmp_path, monkeypatch, chat_agent_asset):
    component = _component(tmp_path, use_in_process_executor=False)
    sensor_def = component.build_defs(context=None).sensors[0]
    _install_fakes(monkeypatch, root_messages=[_msg("m1", "2026-01-01T00:00:01Z", "U1", mentions=_mention("BOTAPPID"))])
    result = sensor_def(_ctx({"last_ts": "2026-01-01T00:00:00Z", "threads": {}}))
    assert result.run_requests[0].run_config == {}


def test_bot_application_id_can_be_set_explicitly(tmp_path, monkeypatch, chat_agent_asset):
    component = _component(tmp_path, bot_application_id="OTHERAPPID")
    sensor_def = component.build_defs(context=None).sensors[0]
    # The env var (BOTAPPID) no longer matches -- only the explicit bot_application_id should.
    _install_fakes(monkeypatch, root_messages=[_msg("m1", "2026-01-01T00:00:01Z", "U1", mentions=_mention("BOTAPPID"))])
    result = sensor_def(_ctx({"last_ts": "2026-01-01T00:00:00Z", "threads": {}}))
    assert not result.run_requests

    _install_fakes(monkeypatch, root_messages=[_msg("m2", "2026-01-01T00:00:02Z", "U1", mentions=_mention("OTHERAPPID"))])
    result = sensor_def(_ctx({"last_ts": "2026-01-01T00:00:00Z", "threads": {}}))
    assert len(result.run_requests) == 1
