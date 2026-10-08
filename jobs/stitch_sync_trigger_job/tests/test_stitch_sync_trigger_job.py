"""Tests for StitchSyncTriggerJobComponent.

Mocks only the network boundary (`requests.post`/`requests.get`). Since
Stitch's Connect API gives no single run-id to poll (see module docstring),
coverage focuses on: the defensive extraction-record matching/terminal
detection (`_find_latest_matching_extraction`, `_is_finished`,
`_is_successful` across several plausible field-name shapes), the real
trigger/list HTTP helpers, and `build_defs` wiring plus a full
`job.execute_in_process()` run.
"""
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
    monkeypatch.delenv("STITCH_JOB_MISSING_TOKEN", raising=False)
    with pytest.raises(RuntimeError, match="STITCH_JOB_MISSING_TOKEN"):
        mod._auth_headers("STITCH_JOB_MISSING_TOKEN")


def test_auth_headers_builds_bearer_header(mod, monkeypatch):
    monkeypatch.setenv("STITCH_JOB_TEST_TOKEN", "sekret")
    headers = mod._auth_headers("STITCH_JOB_TEST_TOKEN")
    assert headers["Authorization"] == "Bearer sekret"
    assert headers["Content-Type"] == "application/json"


# ── _trigger_sync / _list_extractions ───────────────────────────────────

def test_trigger_sync_posts_to_source_sync_endpoint(mod, monkeypatch):
    calls = []

    def fake_post(url, headers=None, timeout=None):
        calls.append(url)
        return FakeResponse({})

    monkeypatch.setattr(mod.requests, "post", fake_post)
    mod._trigger_sync("https://api.stitchdata.com", "48291", {})
    assert calls == ["https://api.stitchdata.com/v4/sources/48291/sync"]


def test_trigger_sync_raises_on_error_status(mod, monkeypatch):
    monkeypatch.setattr(mod.requests, "post", lambda *a, **k: FakeResponse({"error": "nope"}, status_code=403))
    with pytest.raises(Exception, match="stitch sync trigger failed"):
        mod._trigger_sync("https://x", "48291", {})


def test_list_extractions_unwraps_dict_with_data_key(mod, monkeypatch):
    monkeypatch.setattr(mod.requests, "get", lambda *a, **k: FakeResponse({"data": [{"source_id": 1}]}))
    records = mod._list_extractions("https://x", "116078", {})
    assert records == [{"source_id": 1}]


def test_list_extractions_accepts_bare_list_response(mod, monkeypatch):
    monkeypatch.setattr(mod.requests, "get", lambda *a, **k: FakeResponse([{"source_id": 1}]))
    records = mod._list_extractions("https://x", "116078", {})
    assert records == [{"source_id": 1}]


def test_list_extractions_raises_on_error_status(mod, monkeypatch):
    monkeypatch.setattr(mod.requests, "get", lambda *a, **k: FakeResponse({}, status_code=429))
    with pytest.raises(Exception, match="stitch extractions list failed"):
        mod._list_extractions("https://x", "116078", {})


# ── defensive field-name matching ───────────────────────────────────────

def test_matches_source_handles_int_and_str_ids(mod):
    assert mod._matches_source({"source_id": 48291}, "48291")
    assert mod._matches_source({"source_id": "48291"}, 48291)
    assert not mod._matches_source({"source_id": 1}, "48291")


def test_is_finished_true_for_any_end_field_variant(mod):
    assert mod._is_finished({"job_finished_at": "2026-01-01T00:00:00Z"})
    assert mod._is_finished({"end_time": "2026-01-01T00:00:00Z"})
    assert mod._is_finished({"finished_at": "2026-01-01T00:00:00Z"})
    assert not mod._is_finished({"job_started_at": "2026-01-01T00:00:00Z"})


def test_is_successful_true_when_no_exit_fields_present(mod):
    # Stitch's docs don't guarantee exit-status fields -- "finished" with no
    # failure signal is treated as success, not silently assumed to fail.
    assert mod._is_successful({"job_finished_at": "x"})


def test_is_successful_false_on_nonzero_tap_exit_status(mod):
    assert not mod._is_successful({"tap_exit_status": 1, "target_exit_status": 0})


def test_is_successful_true_on_zero_exit_statuses(mod):
    assert mod._is_successful({"tap_exit_status": 0, "target_exit_status": 0})


def test_find_latest_matching_extraction_picks_most_recent_by_start_field(mod):
    records = [
        {"source_id": 1, "job_started_at": "2026-01-01T00:00:00Z"},
        {"source_id": 1, "job_started_at": "2026-01-02T00:00:00Z"},
        {"source_id": 2, "job_started_at": "2026-01-03T00:00:00Z"},
    ]
    latest = mod._find_latest_matching_extraction(records, 1)
    assert latest["job_started_at"] == "2026-01-02T00:00:00Z"


def test_find_latest_matching_extraction_returns_none_when_no_match(mod):
    assert mod._find_latest_matching_extraction([{"source_id": 9}], 1) is None


# ── _poll_until_terminal ─────────────────────────────────────────────────

class _FakeLog:
    def __init__(self):
        self.messages = []

    def info(self, msg):
        self.messages.append(msg)


def test_poll_until_terminal_returns_once_finished_and_successful(mod, monkeypatch):
    responses = iter([
        [{"source_id": "48291", "job_started_at": "t1"}],
        [{"source_id": "48291", "job_started_at": "t1", "job_finished_at": "t2", "tap_exit_status": 0, "target_exit_status": 0}],
    ])
    monkeypatch.setattr(mod.requests, "get", lambda *a, **k: FakeResponse(next(responses)))
    monkeypatch.setattr(mod.time, "sleep", lambda _s: None)

    result = mod._poll_until_terminal("https://x", "116078", "48291", {}, poll_interval_seconds=1, timeout_seconds=60, log=_FakeLog())
    assert result["job_finished_at"] == "t2"


def test_poll_until_terminal_raises_when_exit_status_nonzero(mod, monkeypatch):
    monkeypatch.setattr(
        mod.requests, "get",
        lambda *a, **k: FakeResponse([{"source_id": "48291", "job_finished_at": "t2", "tap_exit_status": 1}]),
    )
    monkeypatch.setattr(mod.time, "sleep", lambda _s: None)
    with pytest.raises(Exception, match="stitch extraction for source 48291 failed"):
        mod._poll_until_terminal("https://x", "116078", "48291", {}, poll_interval_seconds=1, timeout_seconds=60, log=_FakeLog())


# ── Component wiring (build_defs) ───────────────────────────────────────

def test_build_defs_wires_job_name_and_op(mod):
    component = mod.StitchSyncTriggerJobComponent(job_name="sync_job", client_id="116078", source_id="48291")
    defs = component.build_defs(None)
    job_def = next(iter(defs.jobs))
    assert job_def.name == "sync_job"
    assert f"{component.job_name}_op" in [node.name for node in job_def.nodes]


def test_build_defs_attaches_schedule_when_configured(mod):
    component = mod.StitchSyncTriggerJobComponent(
        job_name="sync_job", client_id="116078", source_id="48291", schedule="0 3 * * *", default_status="RUNNING",
    )
    defs = component.build_defs(None)
    schedule_def = next(iter(defs.schedules))
    assert schedule_def.cron_schedule == "0 3 * * *"


def test_build_defs_omits_schedule_when_not_configured(mod):
    component = mod.StitchSyncTriggerJobComponent(job_name="sync_job", client_id="116078", source_id="48291")
    defs = component.build_defs(None)
    assert not (defs.schedules or [])


def test_job_executes_and_triggers_without_waiting(mod, monkeypatch):
    monkeypatch.setenv("STITCH_API_TOKEN", "sekret")
    posted = []

    def fake_post(url, headers=None, timeout=None):
        posted.append(url)
        return FakeResponse({})

    monkeypatch.setattr(mod.requests, "post", fake_post)

    component = mod.StitchSyncTriggerJobComponent(job_name="sync_job", client_id="116078", source_id="48291")
    defs = component.build_defs(None)
    job_def = next(iter(defs.jobs))
    result = job_def.execute_in_process()
    assert result.success
    assert posted == ["https://api.stitchdata.com/v4/sources/48291/sync"]


def test_job_executes_and_waits_for_completion(mod, monkeypatch):
    monkeypatch.setenv("STITCH_API_TOKEN", "sekret")
    monkeypatch.setattr(mod.requests, "post", lambda *a, **k: FakeResponse({}))
    monkeypatch.setattr(mod.time, "sleep", lambda _s: None)

    get_responses = iter([
        [{"source_id": "48291", "job_started_at": "t1"}],
        [{"source_id": "48291", "job_started_at": "t1", "job_finished_at": "t2"}],
    ])
    monkeypatch.setattr(mod.requests, "get", lambda *a, **k: FakeResponse(next(get_responses)))

    component = mod.StitchSyncTriggerJobComponent(
        job_name="sync_job", client_id="116078", source_id="48291", wait_for_completion=True, poll_interval_seconds=0,
    )
    defs = component.build_defs(None)
    job_def = next(iter(defs.jobs))
    result = job_def.execute_in_process()
    assert result.success
