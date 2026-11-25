"""Real tests for ScheduledSlackAgentSensorComponent: only the Slack client is faked; the real sensor
function runs through dg.build_sensor_context()."""
import importlib.util
import json
import pathlib

import dagster as dg
import pytest

_HERE = pathlib.Path(__file__).resolve().parent.parent
_spec = importlib.util.spec_from_file_location("scheduled_slack_agent_sensor_component", _HERE / "component.py")
mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(mod)


@pytest.fixture()
def agent_asset():
    @dg.asset(key=dg.AssetKey(["review_agent"]))
    def review_agent():
        return "ok"
    return review_agent


class _FakeClient:
    def __init__(self, fail=None, raise_exc=None):
        self.posts = []
        self.fail = fail
        self.raise_exc = raise_exc

    def chat_postMessage(self, **kw):
        if self.raise_exc:
            raise self.raise_exc
        if self.fail:
            return {"ok": False, "error": self.fail}
        self.posts.append(kw)
        return {"ok": True, "ts": f"1700000000.00000{len(self.posts)}", "channel": kw.get("channel")}


def _sensor(**overrides):
    kw = dict(sensor_name="weekly_review", slack_channel_id="C1", asset_selection=["review_agent"],
              announce_text=":bar_chart: Improvement review started", period_hours=168, default_status="RUNNING")
    kw.update(overrides)
    return mod.ScheduledSlackAgentSensorComponent(**kw).build_defs(context=None).sensors[0]


def _run(sensor, cursor=None):
    return sensor(dg.build_sensor_context(cursor=json.dumps(cursor) if cursor is not None else None))


def test_a_due_tick_posts_one_message_and_starts_one_run_tagged_with_its_ts(monkeypatch, agent_asset):
    client = _FakeClient()
    monkeypatch.setattr(mod, "_get_slack_client_for_announcement", lambda env: client)
    result = _run(_sensor(), {"due_at": 0, "last_ts": None})
    assert [p["text"] for p in client.posts] == [":bar_chart: Improvement review started"]
    assert "thread_ts" not in client.posts[0]  # top-level message, the thread root
    assert len(result.run_requests) == 1
    ts = "1700000000.000001"
    tags = result.run_requests[0].tags
    assert tags["slack/channel"] == "C1" and tags["slack/thread_ts"] == ts and tags["slack/message_ts"] == ts
    assert result.run_requests[0].run_key == f"weekly_review:{ts}"
    state = json.loads(result.cursor)
    assert state["last_ts"] == ts and state["due_at"] > 0


def test_a_tick_within_the_period_skips_with_the_time_left(monkeypatch, agent_asset):
    client = _FakeClient()
    monkeypatch.setattr(mod, "_get_slack_client_for_announcement", lambda env: client)
    import time
    result = _run(_sensor(), {"due_at": time.time() + 2 * 86400 + 3 * 3600 + 120, "last_ts": "1.0"})
    assert isinstance(result, dg.SkipReason)
    assert "2d 3h" in result.skip_message
    assert client.posts == []


def test_first_tick_starts_the_period_now_and_does_not_fire(monkeypatch, agent_asset):
    client = _FakeClient()
    monkeypatch.setattr(mod, "_get_slack_client_for_announcement", lambda env: client)
    result = _run(_sensor())
    assert not result.run_requests and client.posts == []
    import time
    due = json.loads(result.cursor)["due_at"]
    assert abs(due - (time.time() + 168 * 3600)) < 60  # default delay = one full period


def test_first_run_delay_overrides_the_default(monkeypatch, agent_asset):
    monkeypatch.setattr(mod, "_get_slack_client_for_announcement", lambda env: _FakeClient())
    import time
    result = _run(_sensor(first_run_delay_hours=2))
    assert abs(json.loads(result.cursor)["due_at"] - (time.time() + 2 * 3600)) < 60


def test_first_run_delay_zero_fires_on_the_first_tick(monkeypatch, agent_asset):
    client = _FakeClient()
    monkeypatch.setattr(mod, "_get_slack_client_for_announcement", lambda env: client)
    result = _run(_sensor(first_run_delay_hours=0))
    assert len(result.run_requests) == 1 and len(client.posts) == 1


@pytest.mark.parametrize("client", [_FakeClient(fail="channel_not_found"), _FakeClient(raise_exc=RuntimeError("boom"))])
def test_a_failed_post_starts_no_run_and_leaves_the_cursor_alone(monkeypatch, agent_asset, client):
    monkeypatch.setattr(mod, "_get_slack_client_for_announcement", lambda env: client)
    result = _run(_sensor(), {"due_at": 0, "last_ts": None})
    assert isinstance(result, dg.SkipReason)
    assert "will retry" in result.skip_message


def test_after_a_run_the_next_tick_skips(monkeypatch, agent_asset):
    client = _FakeClient()
    monkeypatch.setattr(mod, "_get_slack_client_for_announcement", lambda env: client)
    sensor = _sensor()
    first = _run(sensor, {"due_at": 0, "last_ts": None})
    second = _run(sensor, json.loads(first.cursor))
    assert isinstance(second, dg.SkipReason) and len(client.posts) == 1


def test_branch_deployment_skips(monkeypatch, agent_asset):
    monkeypatch.setenv("DAGSTER_CLOUD_IS_BRANCH_DEPLOYMENT", "1")
    monkeypatch.setattr(mod, "_get_slack_client_for_announcement", lambda env: (_ for _ in ()).throw(AssertionError("no Slack")))
    assert isinstance(_run(_sensor(), {"due_at": 0}), dg.SkipReason)


def test_extra_tags_cannot_override_slack_tags(monkeypatch, agent_asset):
    monkeypatch.setattr(mod, "_get_slack_client_for_announcement", lambda env: _FakeClient())
    result = _run(_sensor(extra_tags={"team": "de", "slack/thread_ts": "evil"}), {"due_at": 0})
    tags = result.run_requests[0].tags
    assert tags["team"] == "de" and tags["slack/thread_ts"] == "1700000000.000001"


def test_validation(agent_asset):
    with pytest.raises(ValueError, match="period_hours"):
        _sensor(period_hours=0)
    with pytest.raises(ValueError, match="first_run_delay_hours"):
        _sensor(first_run_delay_hours=-1)
    with pytest.raises(ValueError, match="matched no assets"):
        _sensor(asset_selection="tag:nope=nope")
    with pytest.raises(ValueError, match="teams_team_id"):
        _sensor(platform="teams", slack_channel_id=None)
    with pytest.raises(ValueError, match="slack_channel_id"):
        _sensor(slack_channel_id=None)


def test_platform_teams_posts_via_graph_and_tags_the_run(monkeypatch, agent_asset):
    import requests

    class _Resp:
        def __init__(self, body):
            self._body = body

        def raise_for_status(self):
            pass

        def json(self):
            return self._body

    posts = []

    def fake_post(url, **kwargs):
        posts.append(url)
        if "oauth2/v2.0/token" in url:
            return _Resp({"access_token": "BEARER123"})
        return _Resp({"id": "msg1"})

    monkeypatch.setattr(requests, "post", fake_post)
    monkeypatch.setenv("TEAMS_TENANT_ID", "tid")
    monkeypatch.setenv("TEAMS_CLIENT_ID", "cid")
    monkeypatch.setenv("TEAMS_CLIENT_SECRET", "sec")
    monkeypatch.setenv("TEAMS_REFRESH_TOKEN", "seed")

    result = _run(_sensor(platform="teams", slack_channel_id=None, teams_team_id="T1", teams_channel_id="Ch1"), {"due_at": 0, "last_ts": None})
    assert len(result.run_requests) == 1
    tags = result.run_requests[0].tags
    assert tags["teams/team_id"] == "T1" and tags["teams/channel_id"] == "Ch1" and tags["teams/thread_id"] == "msg1"
    assert "slack/channel" not in tags
    assert any(not u.endswith("/token") for u in posts)
