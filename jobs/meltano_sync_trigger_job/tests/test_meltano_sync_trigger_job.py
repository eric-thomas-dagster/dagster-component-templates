"""Tests for MeltanoSyncTriggerJobComponent.

Mocks only the network boundary (`requests.post`/`requests.get`/`requests.put`),
exercising the real module-level trigger/poll helpers plus the real
`build_defs` wiring (job + optional schedule) and a full
`job.execute_in_process()` run with a fake `requests` module standing in for
Meltano Cloud's API.
"""
import os

import dagster as dg
import pytest

from .conftest import load_component_module


@pytest.fixture
def mod():
    return load_component_module()


class FakeResponse:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code
        self.text = str(payload)

    def json(self):
        return self._payload


# ── _auth_headers ───────────────────────────────────────────────────────

def test_auth_headers_raises_without_token_env(mod, monkeypatch):
    monkeypatch.delenv("MELTANO_JOB_MISSING_TOKEN", raising=False)
    with pytest.raises(RuntimeError, match="MELTANO_JOB_MISSING_TOKEN"):
        mod._auth_headers("MELTANO_JOB_MISSING_TOKEN")


def test_auth_headers_builds_bearer_header(mod, monkeypatch):
    monkeypatch.setenv("MELTANO_JOB_TEST_TOKEN", "sekret")
    headers = mod._auth_headers("MELTANO_JOB_TEST_TOKEN")
    assert headers == {"Authorization": "Bearer sekret"}


# ── _trigger_pipeline_job / _get_job ────────────────────────────────────

def test_trigger_pipeline_job_posts_and_returns_job(mod, monkeypatch):
    calls = []

    def fake_post(url, headers=None, timeout=None):
        calls.append(url)
        return FakeResponse({"id": "job-1", "status": "QUEUED"}, status_code=201)

    monkeypatch.setattr(mod.requests, "post", fake_post)
    job = mod._trigger_pipeline_job("https://app.meltano.com/api", "pipe-1", {"Authorization": "Bearer x"})
    assert job["id"] == "job-1"
    assert calls == ["https://app.meltano.com/api/pipelines/pipe-1/jobs"]


def test_trigger_pipeline_job_raises_on_error_status(mod, monkeypatch):
    monkeypatch.setattr(mod.requests, "post", lambda *a, **k: FakeResponse({"error": "nope"}, status_code=409))
    with pytest.raises(Exception, match="meltano cloud trigger failed"):
        mod._trigger_pipeline_job("https://x", "pipe-1", {})


def test_get_job_raises_on_error_status(mod, monkeypatch):
    monkeypatch.setattr(mod.requests, "get", lambda *a, **k: FakeResponse({}, status_code=404))
    with pytest.raises(Exception, match="meltano cloud job lookup failed"):
        mod._get_job("https://x", "job-1", {})


# ── _poll_until_terminal ─────────────────────────────────────────────────

class _FakeLog:
    def __init__(self):
        self.messages = []

    def info(self, msg):
        self.messages.append(msg)


def test_poll_until_terminal_returns_on_complete(mod, monkeypatch):
    responses = iter([
        {"id": "job-1", "status": "RUNNING"},
        {"id": "job-1", "status": "COMPLETE", "exitCode": 0},
    ])
    monkeypatch.setattr(mod.requests, "get", lambda *a, **k: FakeResponse(next(responses)))
    monkeypatch.setattr(mod.time, "sleep", lambda _s: None)

    log = _FakeLog()
    result = mod._poll_until_terminal("https://x", "job-1", {}, poll_interval_seconds=1, timeout_seconds=60, log=log)
    assert result["status"] == "COMPLETE"
    assert any("RUNNING" in m for m in log.messages)


def test_poll_until_terminal_raises_on_error_status(mod, monkeypatch):
    monkeypatch.setattr(mod.requests, "get", lambda *a, **k: FakeResponse({"id": "job-1", "status": "ERROR", "exitCode": 1}))
    monkeypatch.setattr(mod.time, "sleep", lambda _s: None)
    with pytest.raises(Exception, match="status=ERROR"):
        mod._poll_until_terminal("https://x", "job-1", {}, poll_interval_seconds=1, timeout_seconds=60, log=_FakeLog())


def test_poll_until_terminal_times_out_and_cancels_when_configured(mod, monkeypatch):
    monkeypatch.setattr(mod.requests, "get", lambda *a, **k: FakeResponse({"id": "job-1", "status": "RUNNING"}))
    monkeypatch.setattr(mod.time, "sleep", lambda _s: None)

    put_calls = []
    monkeypatch.setattr(mod.requests, "put", lambda url, **k: put_calls.append(url))

    # Force the deadline to already be in the past so the loop body never runs.
    times = iter([0, 1])
    monkeypatch.setattr(mod.time, "time", lambda: next(times, 1))

    with pytest.raises(Exception, match="timed out waiting for meltano cloud job"):
        mod._poll_until_terminal(
            "https://x", "job-1", {}, poll_interval_seconds=1, timeout_seconds=0, log=_FakeLog(), cancel_on_timeout=True,
        )
    assert put_calls == ["https://x/jobs/job-1/stopped"]


# ── Component wiring (build_defs) ───────────────────────────────────────

def test_build_defs_wires_job_name_and_op(mod):
    component = mod.MeltanoSyncTriggerJobComponent(job_name="sync_job", pipeline_id="pipe-1")
    defs = component.build_defs(None)
    job_def = next(iter(defs.jobs))
    assert job_def.name == "sync_job"
    assert f"{component.job_name}_op" in [node.name for node in job_def.nodes]


def test_build_defs_attaches_schedule_when_configured(mod):
    component = mod.MeltanoSyncTriggerJobComponent(
        job_name="sync_job", pipeline_id="pipe-1", schedule="0 3 * * *", default_status="RUNNING",
    )
    defs = component.build_defs(None)
    schedule_def = next(iter(defs.schedules))
    assert schedule_def.cron_schedule == "0 3 * * *"
    assert schedule_def.default_status == dg.DefaultScheduleStatus.RUNNING


def test_build_defs_omits_schedule_when_not_configured(mod):
    component = mod.MeltanoSyncTriggerJobComponent(job_name="sync_job", pipeline_id="pipe-1")
    defs = component.build_defs(None)
    assert not (defs.schedules or [])


def test_job_executes_and_triggers_without_waiting(mod, monkeypatch):
    monkeypatch.setenv("MELTANO_CLOUD_API_TOKEN", "sekret")
    posted = []

    def fake_post(url, headers=None, timeout=None):
        posted.append((url, headers))
        return FakeResponse({"id": "job-1", "status": "QUEUED"}, status_code=201)

    monkeypatch.setattr(mod.requests, "post", fake_post)

    component = mod.MeltanoSyncTriggerJobComponent(job_name="sync_job", pipeline_id="pipe-1")
    defs = component.build_defs(None)
    job_def = next(iter(defs.jobs))
    result = job_def.execute_in_process()
    assert result.success
    assert posted[0][0] == f"{component.meltano_cloud_api_url}/pipelines/pipe-1/jobs"
    assert posted[0][1] == {"Authorization": "Bearer sekret"}


def test_job_executes_and_waits_for_completion(mod, monkeypatch):
    monkeypatch.setenv("MELTANO_CLOUD_API_TOKEN", "sekret")
    monkeypatch.setattr(mod.time, "sleep", lambda _s: None)
    monkeypatch.setattr(mod.requests, "post", lambda *a, **k: FakeResponse({"id": "job-1", "status": "QUEUED"}, status_code=201))

    get_responses = iter([
        {"id": "job-1", "status": "RUNNING"},
        {"id": "job-1", "status": "COMPLETE", "exitCode": 0},
    ])
    monkeypatch.setattr(mod.requests, "get", lambda *a, **k: FakeResponse(next(get_responses)))

    component = mod.MeltanoSyncTriggerJobComponent(
        job_name="sync_job", pipeline_id="pipe-1", wait_for_completion=True, poll_interval_seconds=0,
    )
    defs = component.build_defs(None)
    job_def = next(iter(defs.jobs))
    result = job_def.execute_in_process()
    assert result.success


def test_job_executes_and_fails_when_job_errors(mod, monkeypatch):
    monkeypatch.setenv("MELTANO_CLOUD_API_TOKEN", "sekret")
    monkeypatch.setattr(mod.time, "sleep", lambda _s: None)
    monkeypatch.setattr(mod.requests, "post", lambda *a, **k: FakeResponse({"id": "job-1", "status": "QUEUED"}, status_code=201))
    monkeypatch.setattr(mod.requests, "get", lambda *a, **k: FakeResponse({"id": "job-1", "status": "ERROR", "exitCode": 1}))

    component = mod.MeltanoSyncTriggerJobComponent(
        job_name="sync_job", pipeline_id="pipe-1", wait_for_completion=True, poll_interval_seconds=0,
    )
    defs = component.build_defs(None)
    job_def = next(iter(defs.jobs))
    result = job_def.execute_in_process(raise_on_error=False)
    assert not result.success
