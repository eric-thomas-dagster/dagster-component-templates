"""Real, committed tests for TeamsRunRequestSensorComponent -- mocks only the unavoidable
external calls (requests.post/requests.get for Graph + OAuth), per this repo's test
convention, and exercises the real sensor function via dg.build_sensor_context()."""
import importlib.util
import pathlib

import dagster as dg
import pytest

_HERE = pathlib.Path(__file__).resolve().parent.parent
_spec = importlib.util.spec_from_file_location("teams_run_request_sensor_component", _HERE / "component.py")
mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(mod)

TeamsRunRequestSensorComponent = mod.TeamsRunRequestSensorComponent


def _component(**overrides):
    defaults = dict(
        sensor_name="teams_runner",
        teams_team_id="T",
        teams_channel_id="Ch",
        oauth_state_dir="/tmp/teams_run_request_sensor_test_state",
        triggers=[dict(phrases=["run nightly etl", "kick off nightly"], job_name="nightly_etl_job", owners=["owner@co.com"])],
        default_status="RUNNING",
    )
    defaults.update(overrides)
    return TeamsRunRequestSensorComponent(**defaults)


def _install_fakes(monkeypatch, *, messages, email_by_aad_id=None):
    """Fakes requests.post (token refresh + reply posting) and requests.get (list messages +
    user email lookup). Returns the calls lists so a test can assert on them."""
    import requests

    email_by_aad_id = email_by_aad_id or {}
    calls = {"post": [], "get": []}

    class _Resp:
        def __init__(self, body):
            self._body = body

        def raise_for_status(self):
            pass

        def json(self):
            return self._body

    def fake_post(url, **kwargs):
        calls["post"].append((url, kwargs))
        if "oauth2/v2.0/token" in url:
            return _Resp({"access_token": "BEARER123"})
        return _Resp({})

    def fake_get(url, **kwargs):
        calls["get"].append((url, kwargs))
        if "/users/" in url:
            aad_id = url.rsplit("/", 1)[-1]
            mail = email_by_aad_id.get(aad_id)
            return _Resp({"mail": mail, "userPrincipalName": None})
        if url.endswith("/messages"):
            return _Resp({"value": messages})
        return _Resp({"value": []})

    monkeypatch.setattr(requests, "post", fake_post)
    monkeypatch.setattr(requests, "get", fake_get)
    monkeypatch.setenv("TEAMS_TENANT_ID", "tid")
    monkeypatch.setenv("TEAMS_CLIENT_ID", "cid")
    monkeypatch.setenv("TEAMS_CLIENT_SECRET", "sec")
    monkeypatch.setenv("TEAMS_REFRESH_TOKEN", "seed")
    return calls


def _msg(id_, created, text, aad_id):
    return {
        "id": id_,
        "createdDateTime": created,
        "from": {"user": {"id": aad_id}},
        "body": {"content": f"<p>{text}</p>"},
    }


# --- TriggerSpec validation -------------------------------------------------------------

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
    comp = _component(triggers=[dict(phrases=["x"], job_name="j", owners=["a"])])
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
        _msg("m1", "2026-01-01T00:00:01.0000000Z", "Hey can someone run nightly etl please", "aad-owner"),
        _msg("m2", "2026-01-01T00:00:02.0000000Z", "run nightly etl now!!", "aad-intruder"),
    ]
    calls = _install_fakes(monkeypatch, messages=messages, email_by_aad_id={"aad-owner": "owner@co.com"})

    ctx = dg.build_sensor_context(cursor="2026-01-01T00:00:00.0000000Z")
    result = sensor_def(ctx)

    assert [(r.run_key, r.job_name) for r in result.run_requests] == [("teams_runner:m1", "nightly_etl_job")]
    assert result.cursor == "2026-01-01T00:00:02.0000000Z"
    reply_posts = [c for c in calls["post"] if "/replies" in c[0]]
    assert len(reply_posts) == 2  # one ack for m1, one unauthorized-reply for m2


def test_unmatched_message_text_is_ignored(monkeypatch):
    comp = _component()
    sensor_def = comp.build_defs(context=None).sensors[0]
    messages = [_msg("m1", "2026-01-01T00:00:01.0000000Z", "good morning team", "aad-owner")]
    _install_fakes(monkeypatch, messages=messages)

    ctx = dg.build_sensor_context(cursor="2026-01-01T00:00:00.0000000Z")
    result = sensor_def(ctx)
    assert result.run_requests == []
    assert result.cursor == "2026-01-01T00:00:01.0000000Z"  # still advances past the seen-but-unmatched message


def test_messages_at_or_before_cursor_are_not_reprocessed(monkeypatch):
    comp = _component()
    sensor_def = comp.build_defs(context=None).sensors[0]
    messages = [_msg("m1", "2026-01-01T00:00:01.0000000Z", "run nightly etl", "aad-owner")]
    _install_fakes(monkeypatch, messages=messages, email_by_aad_id={"aad-owner": "owner@co.com"})

    ctx = dg.build_sensor_context(cursor="2026-01-01T00:00:01.0000000Z")  # == the message's own createdDateTime
    result = sensor_def(ctx)
    assert result.run_requests == []


def test_first_tick_seeds_cursor_to_now_and_processes_nothing(monkeypatch):
    comp = _component()
    sensor_def = comp.build_defs(context=None).sensors[0]
    messages = [_msg("m1", "2020-01-01T00:00:01.0000000Z", "run nightly etl", "aad-owner")]
    _install_fakes(monkeypatch, messages=messages, email_by_aad_id={"aad-owner": "owner@co.com"})

    ctx = dg.build_sensor_context()  # no cursor at all -- first-ever tick
    result = sensor_def(ctx)
    assert result.run_requests == []
    assert "seeding cursor to now" in result.skip_reason.skip_message


def test_process_backlog_on_first_run_true_processes_the_whole_history(monkeypatch):
    comp = _component(process_backlog_on_first_run=True)
    sensor_def = comp.build_defs(context=None).sensors[0]
    messages = [_msg("m1", "2020-01-01T00:00:01.0000000Z", "run nightly etl", "aad-owner")]
    _install_fakes(monkeypatch, messages=messages, email_by_aad_id={"aad-owner": "owner@co.com"})

    ctx = dg.build_sensor_context()
    result = sensor_def(ctx)
    assert len(result.run_requests) == 1


def test_require_owner_authorization_false_lets_anyone_trigger(monkeypatch):
    comp = _component(require_owner_authorization=False)
    sensor_def = comp.build_defs(context=None).sensors[0]
    messages = [_msg("m1", "2026-01-01T00:00:01.0000000Z", "run nightly etl", "aad-stranger")]
    _install_fakes(monkeypatch, messages=messages)

    ctx = dg.build_sensor_context(cursor="2026-01-01T00:00:00.0000000Z")
    result = sensor_def(ctx)
    assert len(result.run_requests) == 1


def test_resolve_requester_email_false_only_matches_literal_aad_id(monkeypatch):
    comp = _component(
        resolve_requester_email=False,
        triggers=[dict(phrases=["run nightly etl"], job_name="nightly_etl_job", owners=["aad-owner"])],
    )
    sensor_def = comp.build_defs(context=None).sensors[0]
    messages = [_msg("m1", "2026-01-01T00:00:01.0000000Z", "run nightly etl", "aad-owner")]
    calls = _install_fakes(monkeypatch, messages=messages)

    ctx = dg.build_sensor_context(cursor="2026-01-01T00:00:00.0000000Z")
    result = sensor_def(ctx)
    assert len(result.run_requests) == 1
    assert not any("/users/" in c[0] for c in calls["get"])  # no email-resolution call made


def test_on_unauthorized_skip_posts_no_reply(monkeypatch):
    comp = _component(on_unauthorized="skip")
    sensor_def = comp.build_defs(context=None).sensors[0]
    messages = [_msg("m1", "2026-01-01T00:00:01.0000000Z", "run nightly etl", "aad-intruder")]
    calls = _install_fakes(monkeypatch, messages=messages)

    ctx = dg.build_sensor_context(cursor="2026-01-01T00:00:00.0000000Z")
    result = sensor_def(ctx)
    assert result.run_requests == []
    assert not any("/replies" in c[0] for c in calls["post"])


def test_post_acknowledgement_false_posts_no_reply_on_success(monkeypatch):
    comp = _component(post_acknowledgement=False)
    sensor_def = comp.build_defs(context=None).sensors[0]
    messages = [_msg("m1", "2026-01-01T00:00:01.0000000Z", "run nightly etl", "aad-owner")]
    calls = _install_fakes(monkeypatch, messages=messages, email_by_aad_id={"aad-owner": "owner@co.com"})

    ctx = dg.build_sensor_context(cursor="2026-01-01T00:00:00.0000000Z")
    result = sensor_def(ctx)
    assert len(result.run_requests) == 1
    assert not any("/replies" in c[0] for c in calls["post"])


# --- asset_selection trigger: owners come from the matched asset's own metadata -------------

def test_owners_by_key_reads_live_asset_spec_owners():
    @dg.asset(key=dg.AssetKey(["nightly", "etl"]), owners=["team-lead@co.com"])
    def nightly_etl():
        ...

    defs = dg.Definitions(assets=[nightly_etl])
    owners_map = mod._owners_by_key(defs)
    assert owners_map == {"nightly/etl": ["team-lead@co.com"]}


def test_asset_selection_trigger_authorizes_against_the_assets_own_owners(monkeypatch):
    comp = _component(triggers=[dict(phrases=["run nightly etl"], asset_selection=["nightly/etl"])])
    sensor_def = comp.build_defs(context=None).sensors[0]
    messages = [_msg("m1", "2026-01-01T00:00:01.0000000Z", "run nightly etl", "aad-owner")]
    # No sibling discovery in this unit test (context=None) -- owners_by_key is empty, so the
    # explicit-list target has NO recorded owners and must fail closed (nobody is authorized).
    _install_fakes(monkeypatch, messages=messages, email_by_aad_id={"aad-owner": "owner@co.com"})

    ctx = dg.build_sensor_context(cursor="2026-01-01T00:00:00.0000000Z")
    result = sensor_def(ctx)
    assert result.run_requests == []  # fails closed: no asset metadata was discoverable to authorize against


# --- helpers ---------------------------------------------------------------------------

def test_strip_html():
    assert mod._strip_html("<p>hello <b>world</b></p>") == "hello  world"


def test_list_channel_messages_follows_pagination(monkeypatch):
    import requests
    pages = {
        "https://graph.microsoft.com/v1.0/teams/T/channels/Ch/messages": {
            "value": [{"id": "a"}], "@odata.nextLink": "https://next",
        },
        "https://next": {"value": [{"id": "b"}]},
    }

    def fake_get(url, **kwargs):
        class R:
            def raise_for_status(self2): pass
            def json(self2): return pages[url]
        return R()

    monkeypatch.setattr(requests, "get", fake_get)
    out = mod._list_channel_messages("BEARER", "T", "Ch", top=50, max_pages=3)
    assert [m["id"] for m in out] == ["a", "b"]


def test_get_teams_bearer_raises_clearly_when_env_vars_missing(monkeypatch, tmp_path):
    for var in ("TEAMS_TENANT_ID", "TEAMS_CLIENT_ID", "TEAMS_CLIENT_SECRET", "TEAMS_REFRESH_TOKEN"):
        monkeypatch.delenv(var, raising=False)
    fs = mod._StateFS(str(tmp_path))
    with pytest.raises(RuntimeError, match="not all set"):
        mod._get_teams_bearer(fs, "TEAMS_TENANT_ID", "TEAMS_CLIENT_ID", "TEAMS_CLIENT_SECRET", "TEAMS_REFRESH_TOKEN")


def test_get_teams_bearer_persists_a_rotated_refresh_token(monkeypatch, tmp_path):
    import requests
    monkeypatch.setenv("TEAMS_TENANT_ID", "tid")
    monkeypatch.setenv("TEAMS_CLIENT_ID", "cid")
    monkeypatch.setenv("TEAMS_CLIENT_SECRET", "sec")
    monkeypatch.setenv("TEAMS_REFRESH_TOKEN", "seed-token")

    seen_refresh_tokens = []

    def fake_post(url, **kwargs):
        seen_refresh_tokens.append(kwargs["data"]["refresh_token"])
        class R:
            def raise_for_status(self2): pass
            def json(self2): return {"access_token": "BEARER", "refresh_token": "rotated-token"}
        return R()

    monkeypatch.setattr(requests, "post", fake_post)
    fs = mod._StateFS(str(tmp_path))

    mod._get_teams_bearer(fs, "TEAMS_TENANT_ID", "TEAMS_CLIENT_ID", "TEAMS_CLIENT_SECRET", "TEAMS_REFRESH_TOKEN")
    mod._get_teams_bearer(fs, "TEAMS_TENANT_ID", "TEAMS_CLIENT_ID", "TEAMS_CLIENT_SECRET", "TEAMS_REFRESH_TOKEN")

    assert seen_refresh_tokens == ["seed-token", "rotated-token"]  # second call preferred the persisted rotation
