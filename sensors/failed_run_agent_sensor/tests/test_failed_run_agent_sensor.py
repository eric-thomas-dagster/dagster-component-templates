"""Committed tests for FailedRunAgentSensorComponent, against a REAL temp-dir DagsterInstance.

A real asset genuinely fails (`raise ConnectionError(...)`), then the real `run_failure_sensor`-built
SensorDefinition is evaluated with `SensorDefinition.evaluate_tick` -- the same path the daemon uses, cursor and
all. Only `_get_slack_client` is faked; the cooldown rate-limiter writes real files under tmp_path.
"""
import json

import dagster as dg
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def inst():
    with dg.instance_for_test() as i:
        yield i


class FakeSlackClient:
    def __init__(self):
        self.posted = []

    def chat_postMessage(self, channel, text):
        ts = f"{len(self.posted) + 1}.000"
        self.posted.append({"channel": channel, "text": text, "ts": ts})
        return {"ok": True, "ts": ts, "channel": channel}


@dg.asset
def upstream():
    return 1


@dg.asset(deps=[upstream])
def flaky():
    raise ConnectionError("connection refused to db.internal")


nightly = dg.define_asset_job("nightly", selection=[upstream, flaky])


@dg.asset
def bug_fixer():
    return 1


def _env(mod, tmp_path, monkeypatch, slack, **overrides):
    kwargs = dict(
        sensor_name="failed_run_agent_sensor", asset_keys=["bug_fixer"], slack_channel="C1",
        evidence_dir=str(tmp_path / "evidence"), cooldown_minutes=0, default_status="RUNNING",
    )
    kwargs.update(overrides)
    comp = mod.FailedRunAgentSensorComponent(**kwargs)
    monkeypatch.setattr(mod, "_get_slack_client_for_announcement", lambda token_env_var: slack)
    cdefs = comp.build_defs(None)
    defs = dg.Definitions.merge(cdefs, dg.Definitions(assets=[upstream, flaky, bug_fixer], jobs=[nightly]))
    return defs.get_sensor_def(kwargs["sensor_name"]), defs


def _fail(inst, defs):
    r = defs.get_job_def("nightly").execute_in_process(instance=inst, raise_on_error=False)
    assert not r.success
    return inst.get_run_by_id(r.run_id)


def _tick(sensor_def, inst, defs, cursor=None):
    return sensor_def.evaluate_tick(dg.build_sensor_context(instance=inst, definitions=defs, cursor=cursor, instance_ref=inst.get_ref()))


def _primed(mod, tmp_path, monkeypatch, inst, slack, **overrides):
    """A run_failure_sensor's first-ever tick only initializes its cursor (to "now") -- it can't see a failure
    that already happened before the cursor existed. Prime it once (no failure yet) before each test's real
    failure, same as real deployments experience (the sensor is already running when a run later fails)."""
    sensor_def, defs = _env(mod, tmp_path, monkeypatch, slack, **overrides)
    primed = _tick(sensor_def, inst, defs)
    return sensor_def, defs, primed.cursor


def test_a_real_failure_posts_to_slack_and_requests_a_run(mod, tmp_path, monkeypatch, inst):
    slack = FakeSlackClient()
    sensor_def, defs, cursor = _primed(mod, tmp_path, monkeypatch, inst, slack)
    run = _fail(inst, defs)
    res = _tick(sensor_def, inst, defs, cursor=cursor)

    assert len(res.run_requests) == 1
    rr = res.run_requests[0]
    assert rr.run_key == run.run_id
    assert rr.tags["eng/failed_run_id"] == run.run_id
    assert rr.tags["slack/channel"] == "C1"
    assert rr.tags["slack/thread_ts"] == slack.posted[0]["ts"]
    assert len(slack.posted) == 1
    assert run.run_id[:8] in slack.posted[0]["text"]


def test_cooldown_suppresses_the_same_signature(mod, tmp_path, monkeypatch, inst):
    slack = FakeSlackClient()
    sensor_def, defs, cursor = _primed(mod, tmp_path, monkeypatch, inst, slack, cooldown_minutes=60)
    _fail(inst, defs)
    first = _tick(sensor_def, inst, defs, cursor=cursor)
    assert len(first.run_requests) == 1

    _fail(inst, defs)  # same job, same error signature -- flapping
    second = _tick(sensor_def, inst, defs, cursor=first.cursor)
    assert second.run_requests == []
    assert len(slack.posted) == 1  # no second Slack post


def test_rate_limit_max_triage_per_hour(mod, tmp_path, monkeypatch, inst):
    slack = FakeSlackClient()
    sensor_def, defs, cursor = _primed(mod, tmp_path, monkeypatch, inst, slack, cooldown_minutes=0, max_triage_per_hour=1)
    _fail(inst, defs)
    first = _tick(sensor_def, inst, defs, cursor=cursor)
    assert len(first.run_requests) == 1

    _fail(inst, defs)
    second = _tick(sensor_def, inst, defs, cursor=first.cursor)
    assert second.run_requests == []


def test_job_names_filter_skips_unmonitored_jobs(mod, tmp_path, monkeypatch, inst):
    slack = FakeSlackClient()
    sensor_def, defs, cursor = _primed(mod, tmp_path, monkeypatch, inst, slack, job_names=["some_other_job"])
    _fail(inst, defs)
    res = _tick(sensor_def, inst, defs, cursor=cursor)
    assert res.run_requests == []
    assert not slack.posted


def test_required_tags_filter_skips_runs_missing_the_tag(mod, tmp_path, monkeypatch, inst):
    slack = FakeSlackClient()
    sensor_def, defs, cursor = _primed(mod, tmp_path, monkeypatch, inst, slack, required_tags={"team": "data-eng"})
    _fail(inst, defs)
    res = _tick(sensor_def, inst, defs, cursor=cursor)
    assert res.run_requests == []


def test_code_location_names_filter_skips_a_run_with_no_matching_origin(mod, tmp_path, monkeypatch, inst):
    slack = FakeSlackClient()
    sensor_def, defs, cursor = _primed(mod, tmp_path, monkeypatch, inst, slack, code_location_names=["some_other_location"])
    _fail(inst, defs)
    res = _tick(sensor_def, inst, defs, cursor=cursor)
    assert res.run_requests == []


def test_exclude_code_location_names_still_processes_a_run_whose_location_is_not_excluded(mod, tmp_path, monkeypatch, inst):
    """Hooli feedback: 'every code location except these' (e.g. excluding the agents' own
    location, so a fixer run's own failures don't trigger more fixer runs) -- a run whose
    location is NOT in exclude_code_location_names is processed normally."""
    slack = FakeSlackClient()
    sensor_def, defs, cursor = _primed(mod, tmp_path, monkeypatch, inst, slack, exclude_code_location_names=["hooli_agents"])
    _fail(inst, defs)
    res = _tick(sensor_def, inst, defs, cursor=cursor)
    assert len(res.run_requests) == 1


def test_message_template_and_dagster_url_build_the_run_link(mod, tmp_path, monkeypatch, inst):
    slack = FakeSlackClient()
    sensor_def, defs, cursor = _primed(mod, tmp_path, monkeypatch, inst, slack, dagster_url="https://acme.dagster.cloud/prod")
    run = _fail(inst, defs)
    _tick(sensor_def, inst, defs, cursor=cursor)
    assert f"https://acme.dagster.cloud/prod/runs/{run.run_id}" in slack.posted[0]["text"]


def test_skips_in_branch_deployment_by_default(mod, tmp_path, monkeypatch, inst):
    slack = FakeSlackClient()
    monkeypatch.setenv("DAGSTER_CLOUD_IS_BRANCH_DEPLOYMENT", "1")
    sensor_def, defs, cursor = _primed(mod, tmp_path, monkeypatch, inst, slack)
    run = _fail(inst, defs)
    res = _tick(sensor_def, inst, defs, cursor=cursor)
    assert not res.run_requests
    assert not slack.posted


def test_cooldown_rate_limiter_writes_files_under_evidence_dir(mod, tmp_path, monkeypatch, inst):
    slack = FakeSlackClient()
    sensor_def, defs, cursor = _primed(mod, tmp_path, monkeypatch, inst, slack, cooldown_minutes=60)
    _fail(inst, defs)
    _tick(sensor_def, inst, defs, cursor=cursor)
    rl_dir = tmp_path / "evidence" / "_state" / "ratelimit"
    assert rl_dir.exists()
    assert any(f.name.endswith("-triage.json") for f in rl_dir.iterdir())
    assert any(f.name.endswith("-cooldown.json") for f in rl_dir.iterdir())


def test_platform_teams_posts_via_graph_and_tags_the_run(mod, tmp_path, monkeypatch, inst):
    import requests

    posts = []

    class _Resp:
        def __init__(self, body):
            self._body = body

        def raise_for_status(self):
            pass

        def json(self):
            return self._body

    def fake_post(url, **kwargs):
        posts.append((url, kwargs))
        if "oauth2/v2.0/token" in url:
            return _Resp({"access_token": "BEARER123"})
        return _Resp({"id": "msg1"})

    monkeypatch.setattr(requests, "post", fake_post)
    monkeypatch.setenv("TEAMS_TENANT_ID", "tid")
    monkeypatch.setenv("TEAMS_CLIENT_ID", "cid")
    monkeypatch.setenv("TEAMS_CLIENT_SECRET", "sec")
    monkeypatch.setenv("TEAMS_REFRESH_TOKEN", "seed")

    slack = FakeSlackClient()
    sensor_def, defs, cursor = _primed(
        mod, tmp_path, monkeypatch, inst, slack,
        platform="teams", slack_channel=None, teams_team_id="T1", teams_channel_id="Ch1",
    )
    run = _fail(inst, defs)
    res = _tick(sensor_def, inst, defs, cursor=cursor)

    assert not slack.posted
    assert len(res.run_requests) == 1
    rr = res.run_requests[0]
    assert rr.tags["teams/team_id"] == "T1"
    assert rr.tags["teams/channel_id"] == "Ch1"
    assert rr.tags["teams/thread_id"] == "msg1"
    assert any(not url.endswith("/token") for url, _ in posts)  # the actual channel-message post happened


def test_platform_teams_requires_team_and_channel_id(mod):
    with pytest.raises(ValueError, match="teams_team_id"):
        mod.FailedRunAgentSensorComponent(asset_keys=["bug_fixer"], platform="teams")


def test_platform_slack_requires_slack_channel(mod):
    with pytest.raises(ValueError, match="slack_channel"):
        mod.FailedRunAgentSensorComponent(asset_keys=["bug_fixer"])
