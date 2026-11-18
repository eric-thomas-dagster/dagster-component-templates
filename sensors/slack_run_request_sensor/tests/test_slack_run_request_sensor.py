"""Real, committed tests for SlackRunRequestSensorComponent -- mocks only
`_get_slack_client` (same convention as slack_approval_gate's own tests), never the
real slack_sdk network calls."""
import importlib.util
import pathlib

import dagster as dg
import pytest

_HERE = pathlib.Path(__file__).resolve().parent.parent
_spec = importlib.util.spec_from_file_location("slack_run_request_sensor_component", _HERE / "component.py")
mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(mod)

SlackRunRequestSensorComponent = mod.SlackRunRequestSensorComponent


def _component(**overrides):
    defaults = dict(
        sensor_name="slack_runner",
        slack_channel_id="C123CHANNEL",
        triggers=[dict(phrases=["run nightly etl", "kick off nightly"], job_name="nightly_etl_job", owners=["owner@co.com"])],
        default_status="RUNNING",
    )
    defaults.update(overrides)
    return SlackRunRequestSensorComponent(**defaults)


def _msg(ts, text, user, subtype=None, bot_id=None):
    m = {"ts": ts, "text": text, "user": user, "type": "message"}
    if subtype:
        m["subtype"] = subtype
    if bot_id:
        m["bot_id"] = bot_id
    return m


def _install_fake_client(monkeypatch, *, messages, email_by_user=None, fail_history=None, fail_post=None):
    email_by_user = email_by_user or {}
    calls = {"history": [], "post": [], "users_info": []}

    class _FakeClient:
        def conversations_history(self, channel, oldest, limit, cursor=None):
            calls["history"].append({"channel": channel, "oldest": oldest, "limit": limit, "cursor": cursor})
            if fail_history:
                return {"ok": False, "error": fail_history}
            return {"ok": True, "messages": messages, "response_metadata": {}}

        def users_info(self, user):
            calls["users_info"].append(user)
            return {"ok": True, "user": {"profile": {"email": email_by_user.get(user)}}}

        def chat_postMessage(self, channel, text, thread_ts=None):
            calls["post"].append({"channel": channel, "text": text, "thread_ts": thread_ts})
            if fail_post:
                return {"ok": False, "error": fail_post}
            return {"ok": True, "ts": "9999.0001"}

    monkeypatch.setattr(mod, "_get_slack_client", lambda token_env_var: _FakeClient())
    return calls


# --- TriggerSpec validation (identical contract to the Teams version) -----------------------

def test_trigger_requires_exactly_one_of_asset_selection_or_job_name():
    with pytest.raises(Exception, match="exactly one"):
        _component(triggers=[dict(phrases=["x"], job_name="j", asset_selection="*", owners=["a"])])
    with pytest.raises(Exception, match="exactly one"):
        _component(triggers=[dict(phrases=["x"])])


def test_job_name_trigger_requires_owners():
    with pytest.raises(Exception, match="owners"):
        _component(triggers=[dict(phrases=["x"], job_name="j")])


def test_trigger_accepts_bare_string_phrases_and_owners():
    comp = _component(triggers=[dict(phrases="run it", job_name="j", owners="a@b.com")])
    assert comp.triggers[0].phrases == ["run it"]
    assert comp.triggers[0].owners == ["a@b.com"]


def test_empty_triggers_rejected_at_build():
    comp = _component()
    comp.triggers = []
    with pytest.raises(ValueError, match="non-empty"):
        comp.build_defs(context=None)


def test_invalid_default_status_rejected():
    comp = _component(default_status="bogus")
    with pytest.raises(ValueError, match="default_status"):
        comp.build_defs(context=None)


# --- end-to-end sensor_fn: the authorization gate is the point ------------------------------

def test_authorized_owner_fires_run_request_unauthorized_sender_is_skipped(monkeypatch):
    comp = _component()
    sensor_def = comp.build_defs(context=None).sensors[0]
    messages = [
        _msg("100.001", "Hey can someone run nightly etl please", "U-OWNER"),
        _msg("100.002", "run nightly etl now!!", "U-INTRUDER"),
    ]
    calls = _install_fake_client(monkeypatch, messages=messages, email_by_user={"U-OWNER": "owner@co.com"})

    ctx = dg.build_sensor_context(cursor="100.000")
    result = sensor_def(ctx)

    assert [(r.run_key, r.job_name) for r in result.run_requests] == [("slack_runner:100.001", "nightly_etl_job")]
    assert result.cursor == "100.002"
    assert len(calls["post"]) == 2  # one ack, one unauthorized reply
    assert calls["post"][0]["thread_ts"] == "100.001"
    assert calls["post"][1]["thread_ts"] == "100.002"


def test_unmatched_message_text_is_ignored(monkeypatch):
    comp = _component()
    sensor_def = comp.build_defs(context=None).sensors[0]
    messages = [_msg("100.001", "good morning team", "U-OWNER")]
    _install_fake_client(monkeypatch, messages=messages)

    ctx = dg.build_sensor_context(cursor="100.000")
    result = sensor_def(ctx)
    assert result.run_requests == []
    assert result.cursor == "100.001"  # still advances past the seen-but-unmatched message


def test_bot_and_subtype_messages_are_never_candidates(monkeypatch):
    comp = _component()
    sensor_def = comp.build_defs(context=None).sensors[0]
    messages = [
        _msg("100.001", "run nightly etl", "U-BOT", bot_id="B123"),
        _msg("100.002", "run nightly etl", "U-OWNER", subtype="channel_join"),
    ]
    _install_fake_client(monkeypatch, messages=messages, email_by_user={"U-OWNER": "owner@co.com"})

    ctx = dg.build_sensor_context(cursor="100.000")
    result = sensor_def(ctx)
    assert result.run_requests == []


def test_first_tick_seeds_cursor_to_now_and_processes_nothing(monkeypatch):
    comp = _component()
    sensor_def = comp.build_defs(context=None).sensors[0]
    messages = [_msg("1.001", "run nightly etl", "U-OWNER")]
    _install_fake_client(monkeypatch, messages=messages, email_by_user={"U-OWNER": "owner@co.com"})

    ctx = dg.build_sensor_context()  # no cursor at all -- first-ever tick
    result = sensor_def(ctx)
    assert result.run_requests == []
    assert "seeding cursor to now" in result.skip_reason.skip_message


def test_process_backlog_on_first_run_true_processes_the_whole_history(monkeypatch):
    comp = _component(process_backlog_on_first_run=True)
    sensor_def = comp.build_defs(context=None).sensors[0]
    messages = [_msg("1.001", "run nightly etl", "U-OWNER")]
    _install_fake_client(monkeypatch, messages=messages, email_by_user={"U-OWNER": "owner@co.com"})

    ctx = dg.build_sensor_context()
    result = sensor_def(ctx)
    assert len(result.run_requests) == 1


def test_require_owner_authorization_false_lets_anyone_trigger(monkeypatch):
    comp = _component(require_owner_authorization=False)
    sensor_def = comp.build_defs(context=None).sensors[0]
    messages = [_msg("100.001", "run nightly etl", "U-STRANGER")]
    _install_fake_client(monkeypatch, messages=messages)

    ctx = dg.build_sensor_context(cursor="100.000")
    result = sensor_def(ctx)
    assert len(result.run_requests) == 1


def test_resolve_requester_email_false_only_matches_literal_user_id(monkeypatch):
    comp = _component(
        resolve_requester_email=False,
        triggers=[dict(phrases=["run nightly etl"], job_name="nightly_etl_job", owners=["U-OWNER"])],
    )
    sensor_def = comp.build_defs(context=None).sensors[0]
    messages = [_msg("100.001", "run nightly etl", "U-OWNER")]
    calls = _install_fake_client(monkeypatch, messages=messages)

    ctx = dg.build_sensor_context(cursor="100.000")
    result = sensor_def(ctx)
    assert len(result.run_requests) == 1
    assert calls["users_info"] == []  # no email-resolution call made


def test_on_unauthorized_skip_posts_no_reply(monkeypatch):
    comp = _component(on_unauthorized="skip")
    sensor_def = comp.build_defs(context=None).sensors[0]
    messages = [_msg("100.001", "run nightly etl", "U-INTRUDER")]
    calls = _install_fake_client(monkeypatch, messages=messages)

    ctx = dg.build_sensor_context(cursor="100.000")
    result = sensor_def(ctx)
    assert result.run_requests == []
    assert calls["post"] == []


def test_post_acknowledgement_false_posts_no_reply_on_success(monkeypatch):
    comp = _component(post_acknowledgement=False)
    sensor_def = comp.build_defs(context=None).sensors[0]
    messages = [_msg("100.001", "run nightly etl", "U-OWNER")]
    calls = _install_fake_client(monkeypatch, messages=messages, email_by_user={"U-OWNER": "owner@co.com"})

    ctx = dg.build_sensor_context(cursor="100.000")
    result = sensor_def(ctx)
    assert len(result.run_requests) == 1
    assert calls["post"] == []


def test_history_api_failure_skips_cleanly(monkeypatch):
    comp = _component()
    sensor_def = comp.build_defs(context=None).sensors[0]
    _install_fake_client(monkeypatch, messages=[], fail_history="not_in_channel")

    ctx = dg.build_sensor_context(cursor="100.000")
    result = sensor_def(ctx)
    assert "Slack conversations_history failed" in result.skip_message


# --- asset_selection trigger: owners come from the matched asset's own metadata -------------

def test_owners_by_key_reads_live_asset_spec_owners():
    @dg.asset(key=dg.AssetKey(["nightly", "etl"]), owners=["team-lead@co.com"])
    def nightly_etl():
        ...

    defs = dg.Definitions(assets=[nightly_etl])
    owners_map = mod._owners_by_key(defs)
    assert owners_map == {"nightly/etl": ["team-lead@co.com"]}


def test_asset_selection_trigger_fails_closed_with_no_discoverable_owners(monkeypatch):
    comp = _component(triggers=[dict(phrases=["run nightly etl"], asset_selection=["nightly/etl"])])
    sensor_def = comp.build_defs(context=None).sensors[0]
    messages = [_msg("100.001", "run nightly etl", "U-OWNER")]
    _install_fake_client(monkeypatch, messages=messages, email_by_user={"U-OWNER": "owner@co.com"})

    ctx = dg.build_sensor_context(cursor="100.000")
    result = sensor_def(ctx)
    assert result.run_requests == []  # context=None -> no sibling owners discoverable -> fails closed


# --- helpers ---------------------------------------------------------------------------

def test_list_new_messages_follows_pagination(monkeypatch):
    pages = [
        {"ok": True, "messages": [{"ts": "1"}], "response_metadata": {"next_cursor": "abc"}},
        {"ok": True, "messages": [{"ts": "2"}], "response_metadata": {}},
    ]
    seen_cursors = []

    class _FakeClient:
        def conversations_history(self, channel, oldest, limit, cursor=None):
            seen_cursors.append(cursor)
            return pages.pop(0)

    out = mod._list_new_messages(_FakeClient(), "C1", "0", 200, 3)
    assert [m["ts"] for m in out] == ["1", "2"]
    assert seen_cursors == [None, "abc"]


def test_get_slack_client_raises_clearly_when_token_missing(monkeypatch):
    monkeypatch.delenv("SLACK_BOT_TOKEN", raising=False)
    with pytest.raises(RuntimeError, match="not set"):
        mod._get_slack_client("SLACK_BOT_TOKEN")
