"""Real, committed tests for McpTokenRefresherComponent.

Mocks the external call (`requests.post` to the OAuth token endpoint) and `DefsStateStorage.get`
(Dagster's own storage boundary), per this repo's "mock only the paid/external call" convention.
"""
import time

import dagster as dg
import pytest


class _FakeStorage:
    def __init__(self, blob):
        self._version = "v1"
        self._blob = blob
        self.uploaded = None

    def get_latest_version(self, key):
        return self._version

    def get_latest_defs_state_info(self):
        # Dagster's own DefinitionsLoadContext.get() calls this during repository construction,
        # independent of this component's own explicit get_latest_version/download/upload calls.
        return None

    def download_state_to_path(self, key, version, path):
        import json
        path.write_text(json.dumps(self._blob))

    def upload_state_from_path(self, key, version, path):
        import json
        self.uploaded = (key, json.loads(path.read_text()))


class _FakeResp:
    def __init__(self, status_code=200, payload=None):
        self.status_code = status_code
        self._payload = payload or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        return self._payload


def _patch_storage(monkeypatch, storage):
    import dagster._core.storage.defs_state.base as defs_state_base
    monkeypatch.setattr(defs_state_base.DefsStateStorage, "get", classmethod(lambda cls: storage))


def test_refresh_job_reads_refreshes_and_uploads_new_blob(mod, monkeypatch):
    old_blob = {
        "access_token": "old-at", "refresh_token": "old-rt", "expires_at": time.time() - 10,
        "client_id": "cid", "client_secret": "sec", "token_endpoint": "https://auth.example.com/token",
        "mcp_url": "https://mcp.example.com/mcp",
    }
    storage = _FakeStorage(old_blob)
    _patch_storage(monkeypatch, storage)
    monkeypatch.setattr("requests.post", lambda *a, **k: _FakeResp(payload={"access_token": "new-at", "refresh_token": "new-rt", "expires_in": 3600}))

    component = mod.McpTokenRefresherComponent(job_name="linear_token_refresh", state_key="linear")
    job_def = component.build_defs(context=None).jobs[0]
    result = job_def.execute_in_process()
    assert result.success

    key, new_blob = storage.uploaded
    assert key == "linear"
    assert new_blob["access_token"] == "new-at"
    assert new_blob["refresh_token"] == "new-rt"
    assert new_blob["expires_at"] > time.time()
    assert new_blob["client_id"] == "cid"  # unrelated fields preserved


def test_refresh_rejected_refresh_token_fails_with_sign_in_again_message(mod, monkeypatch):
    blob = {"access_token": "old", "refresh_token": "bad", "token_endpoint": "https://auth.example.com/token", "client_id": "cid"}
    storage = _FakeStorage(blob)
    _patch_storage(monkeypatch, storage)
    monkeypatch.setattr("requests.post", lambda *a, **k: _FakeResp(status_code=401))

    component = mod.McpTokenRefresherComponent(job_name="linear_token_refresh", state_key="linear")
    job_def = component.build_defs(context=None).jobs[0]
    result = job_def.execute_in_process(raise_on_error=False)
    assert not result.success
    failure = result.failure_data_for_node("linear_token_refresh_op")
    assert "sign in again" in failure.error.message.lower() or "mcp_oauth_login" in failure.error.message


def test_refresh_missing_refresh_token_fails_clearly(mod, monkeypatch):
    blob = {"access_token": "old", "token_endpoint": "https://auth.example.com/token", "client_id": "cid"}  # no refresh_token
    storage = _FakeStorage(blob)
    _patch_storage(monkeypatch, storage)

    component = mod.McpTokenRefresherComponent(job_name="linear_token_refresh", state_key="linear")
    job_def = component.build_defs(context=None).jobs[0]
    result = job_def.execute_in_process(raise_on_error=False)
    assert not result.success
    failure = result.failure_data_for_node("linear_token_refresh_op")
    assert "No refresh_token" in failure.error.message


def test_refresh_missing_token_endpoint_fails_clearly(mod, monkeypatch):
    blob = {"access_token": "old", "refresh_token": "rt", "client_id": "cid"}  # no token_endpoint
    storage = _FakeStorage(blob)
    _patch_storage(monkeypatch, storage)

    component = mod.McpTokenRefresherComponent(job_name="linear_token_refresh", state_key="linear")
    job_def = component.build_defs(context=None).jobs[0]
    result = job_def.execute_in_process(raise_on_error=False)
    assert not result.success
    failure = result.failure_data_for_node("linear_token_refresh_op")
    assert "No token_endpoint" in failure.error.message


def test_read_mcp_state_token_blob_raises_clearly_with_no_storage_configured(mod, monkeypatch):
    import dagster._core.storage.defs_state.base as defs_state_base
    monkeypatch.setattr(defs_state_base.DefsStateStorage, "get", classmethod(lambda cls: None))
    with pytest.raises(RuntimeError, match="defs_state_storage"):
        mod._read_mcp_state_token_blob("linear")


def test_write_mcp_state_token_blob_round_trips(mod, monkeypatch):
    storage = _FakeStorage({})
    _patch_storage(monkeypatch, storage)
    mod._write_mcp_state_token_blob("linear", {"access_token": "x"})
    key, blob = storage.uploaded
    assert key == "linear"
    assert blob["access_token"] == "x"


# --- job/schedule shape (not an asset -- see module docstring) ---------------------------------

def test_build_defs_emits_a_job_not_an_asset(mod):
    component = mod.McpTokenRefresherComponent(job_name="linear_token_refresh", state_key="linear")
    defs = component.build_defs(context=None)
    assert len(defs.jobs) == 1
    assert defs.jobs[0].name == "linear_token_refresh"
    assert not defs.assets


def test_no_schedule_emitted_when_cron_schedule_unset(mod):
    component = mod.McpTokenRefresherComponent(job_name="linear_token_refresh", state_key="linear")
    defs = component.build_defs(context=None)
    assert defs.schedules == []


def test_schedule_emitted_and_running_by_default_when_cron_schedule_set(mod):
    component = mod.McpTokenRefresherComponent(job_name="linear_token_refresh", state_key="linear", cron_schedule="0 */4 * * *")
    defs = component.build_defs(context=None)
    schedule = defs.schedules[0]
    assert schedule.cron_schedule == "0 */4 * * *"
    assert schedule.default_status == dg.DefaultScheduleStatus.RUNNING


def test_schedule_can_be_set_to_stopped(mod):
    component = mod.McpTokenRefresherComponent(job_name="linear_token_refresh", state_key="linear", cron_schedule="0 */4 * * *", default_status="stopped")
    defs = component.build_defs(context=None)
    schedule = defs.schedules[0]
    assert schedule.default_status == dg.DefaultScheduleStatus.STOPPED
