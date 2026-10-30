"""Real local HTTP server fake shaped like StreamSets Control Hub's Job Runner
REST surface (not a mocked function) -- same convention `mirth_connect_workspace`
/ `qlik_cloud_workspace` use in this registry. Covers the verified auth headers
and the start -> poll-currentStatus-until-ACTIVE reconcile-to-running flow."""
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import dagster as dg
import pytest

from .conftest import load_component_module

mod = load_component_module()
StreamSetsJobSpec = mod.StreamSetsJobSpec
StreamSetsTransport = mod.StreamSetsTransport
StreamSetsWorkspaceComponent = mod.StreamSetsWorkspaceComponent

_COMPONENT_ID = "component-id-1"
_AUTH_TOKEN = "auth-token-1"


class _FakeControlHub(BaseHTTPRequestHandler):
    statuses = {}  # job_id -> {"status": ..., "color": ...}
    start_calls = []
    stop_calls = []

    def _authorized(self) -> bool:
        return (
            self.headers.get("X-SS-REST-CALL") == "true"
            and self.headers.get("X-Requested-By") is not None
            and self.headers.get("X-SS-App-Component-Id") == _COMPONENT_ID
            and self.headers.get("X-SS-App-Auth-Token") == _AUTH_TOKEN
        )

    def _write_json(self, code: int, payload) -> None:
        body = json.dumps(payload).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if not self._authorized():
            self.send_response(401)
            self.end_headers()
            return
        if self.path.startswith("/jobrunner/rest/v1/job/") and self.path.endswith("/currentStatus"):
            job_id = self.path[len("/jobrunner/rest/v1/job/"):-len("/currentStatus")]
            self._write_json(200, self.statuses.get(job_id, {"status": "INACTIVE"}))
            return
        self.send_response(404)
        self.end_headers()

    def do_POST(self):
        if not self._authorized():
            self.send_response(401)
            self.end_headers()
            return
        if self.path.startswith("/jobrunner/rest/v1/job/") and self.path.endswith("/start"):
            job_id = self.path[len("/jobrunner/rest/v1/job/"):-len("/start")]
            self.start_calls.append(job_id)
            self.statuses[job_id] = {"status": "ACTIVE", "color": "GREEN"}
            self._write_json(200, self.statuses[job_id])
            return
        if self.path.startswith("/jobrunner/rest/v1/job/") and self.path.endswith("/stop"):
            job_id = self.path[len("/jobrunner/rest/v1/job/"):-len("/stop")]
            self.stop_calls.append(job_id)
            self.statuses[job_id] = {"status": "INACTIVE", "color": "GRAY"}
            self._write_json(200, self.statuses[job_id])
            return
        self.send_response(404)
        self.end_headers()

    def log_message(self, *args):
        pass


@pytest.fixture
def fake_server():
    _FakeControlHub.statuses = {
        "job-1": {"status": "INACTIVE", "color": "GRAY"},
        "job-2": {"status": "ACTIVE", "color": "GREEN"},
    }
    _FakeControlHub.start_calls = []
    _FakeControlHub.stop_calls = []

    server = ThreadingHTTPServer(("127.0.0.1", 0), _FakeControlHub)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        thread.join()


def _component(base_url: str, monkeypatch, **overrides):
    monkeypatch.setenv("STREAMSETS_CONTROL_HUB_URL", base_url)
    monkeypatch.setenv("STREAMSETS_COMPONENT_ID", _COMPONENT_ID)
    monkeypatch.setenv("STREAMSETS_AUTH_TOKEN", _AUTH_TOKEN)
    attrs = dict(
        jobs=[
            StreamSetsJobSpec(job_id="job-1", asset_key="streamsets/orders_cdc"),
            StreamSetsJobSpec(job_id="job-2", asset_key="streamsets/clickstream"),
        ],
    )
    attrs.update(overrides)
    return StreamSetsWorkspaceComponent(**attrs)


def test_one_asset_per_job_from_one_component_instance(fake_server, monkeypatch):
    comp = _component(fake_server, monkeypatch)
    defs = comp.build_defs(None)
    keys = {a.key for a in defs.assets}
    assert keys == {dg.AssetKey(["streamsets", "orders_cdc"]), dg.AssetKey(["streamsets", "clickstream"])}


def test_noop_action_materializes_without_calling_start(fake_server, monkeypatch):
    comp = _component(fake_server, monkeypatch, action="noop")
    defs = comp.build_defs(None)
    result = dg.materialize(list(defs.assets), raise_on_error=True)
    assert result.success
    assert _FakeControlHub.start_calls == []


def test_start_action_starts_job_and_confirms_active_status(fake_server, monkeypatch):
    comp = _component(fake_server, monkeypatch, action="start", poll_interval_seconds=0, timeout_seconds=5)
    defs = comp.build_defs(None)
    result = dg.materialize(list(defs.assets), raise_on_error=True)
    assert result.success
    assert sorted(_FakeControlHub.start_calls) == ["job-1", "job-2"]
    assert _FakeControlHub.statuses["job-1"]["status"] == "ACTIVE"
    events = result.get_asset_materialization_events()
    assert all(e.materialization.metadata["streamsets/status"].value == "ACTIVE" for e in events)


def test_fake_server_rejects_missing_or_wrong_credentials(fake_server):
    import requests

    resp = requests.get(f"{fake_server}/jobrunner/rest/v1/job/job-1/currentStatus", timeout=5)
    assert resp.status_code == 401

    resp = requests.get(
        f"{fake_server}/jobrunner/rest/v1/job/job-1/currentStatus",
        headers={
            "X-Requested-By": "x",
            "X-SS-REST-CALL": "true",
            "X-SS-App-Component-Id": _COMPONENT_ID,
            "X-SS-App-Auth-Token": "wrong",
        },
        timeout=5,
    )
    assert resp.status_code == 401

    resp = requests.get(
        f"{fake_server}/jobrunner/rest/v1/job/job-1/currentStatus",
        headers={
            "X-Requested-By": "x",
            "X-SS-REST-CALL": "true",
            "X-SS-App-Component-Id": _COMPONENT_ID,
            "X-SS-App-Auth-Token": _AUTH_TOKEN,
        },
        timeout=5,
    )
    assert resp.status_code == 200


def test_assets_by_job_overrides_group_name_deps_and_tags(fake_server, monkeypatch):
    comp = _component(
        fake_server,
        monkeypatch,
        assets_by_job={"job-1": {"group_name": "cdc_prod", "deps": ["warehouse/raw_orders"], "tags": {"tier": "critical"}}},
    )
    defs = comp.build_defs(None)
    spec = next(a for a in defs.assets if a.key == dg.AssetKey(["streamsets", "orders_cdc"])).get_asset_spec(
        dg.AssetKey(["streamsets", "orders_cdc"])
    )
    assert spec.group_name == "cdc_prod"
    assert dg.AssetKey(["warehouse", "raw_orders"]) in {d.asset_key for d in spec.deps}
    assert spec.tags.get("tier") == "critical"


def test_polling_sensor_only_emits_for_jobs_whose_status_changed(fake_server, monkeypatch):
    comp = _component(fake_server, monkeypatch, polling_sensor=True)
    defs = comp.build_defs(None)
    sensor_def = next(iter(defs.sensors))

    # Tick 1: no prior cursor -- baseline only, no events yet.
    result1 = sensor_def(dg.build_sensor_context())
    assert result1.asset_events == []

    # Tick 2: job-1 flips from INACTIVE to ACTIVE; job-2 unchanged.
    _FakeControlHub.statuses["job-1"] = {"status": "ACTIVE", "color": "GREEN"}
    result2 = sensor_def(dg.build_sensor_context(cursor=result1.cursor))
    assert len(result2.asset_events) == 1
    assert result2.asset_events[0].asset_key == dg.AssetKey(["streamsets", "orders_cdc"])
    assert result2.asset_events[0].metadata["streamsets/status"].value == "ACTIVE"
    assert result2.asset_events[0].metadata["streamsets/previous_status"].value == "INACTIVE"

    # Tick 3: nothing changed -- no events.
    result3 = sensor_def(dg.build_sensor_context(cursor=result2.cursor))
    assert result3.asset_events == []


def test_require_active_check_fails_when_job_inactive(fake_server, monkeypatch):
    comp = _component(fake_server, monkeypatch, require_active=True)
    defs = comp.build_defs(None)
    check = next(c for c in defs.asset_checks if c.check_key.asset_key == dg.AssetKey(["streamsets", "orders_cdc"]))
    result = check(dg.build_asset_check_context())
    assert result.passed is False  # fixture starts job-1 INACTIVE

    _FakeControlHub.statuses["job-1"] = {"status": "ACTIVE", "color": "GREEN"}
    result = check(dg.build_asset_check_context())
    assert result.passed is True


def test_start_action_raises_when_job_lands_on_inactive(fake_server, monkeypatch):
    # Simulate a failed start: force start_job to leave the status INACTIVE
    # instead of ACTIVE, so wait_for_status must raise rather than hang.
    orig_start_job = StreamSetsTransport.start_job

    def _broken_start_job(self, job_id):
        import requests

        resp = requests.post(f"{self._base}/jobrunner/rest/v1/job/{job_id}/start", headers=self._headers, timeout=60)
        resp.raise_for_status()
        _FakeControlHub.statuses[job_id] = {"status": "INACTIVE", "color": "RED"}

    StreamSetsTransport.start_job = _broken_start_job
    try:
        comp = _component(fake_server, monkeypatch, jobs=[StreamSetsJobSpec(job_id="job-1", asset_key="streamsets/orders_cdc")], action="start", poll_interval_seconds=0, timeout_seconds=5)
        defs = comp.build_defs(None)
        result = dg.materialize(list(defs.assets), raise_on_error=False)
        assert not result.success
    finally:
        StreamSetsTransport.start_job = orig_start_job


def test_demo_mode_needs_no_real_control_hub():
    comp = StreamSetsWorkspaceComponent(
        jobs=[StreamSetsJobSpec(job_id="demo-1", asset_key="streamsets/demo_pipeline")],
        action="start",
        demo_mode=True,
        polling_sensor=True,
        require_active=True,
    )
    defs = comp.build_defs(None)
    result = dg.materialize(list(defs.assets), raise_on_error=True)
    assert result.success

    sensor_def = next(iter(defs.sensors))
    sensor_result = sensor_def(dg.build_sensor_context())
    assert sensor_result is not None  # ran with no network access at all

    for check in defs.asset_checks:
        assert check(dg.build_asset_check_context()).passed is True


def test_transport_seam_is_a_swappable_class(fake_server):
    transport = StreamSetsTransport(fake_server, _COMPONENT_ID, _AUTH_TOKEN)
    transport.start_job("job-1")
    assert transport.get_job_status("job-1")["status"] == "ACTIVE"
    transport.stop_job("job-1")
    assert transport.get_job_status("job-1")["status"] == "INACTIVE"


def test_missing_env_vars_raise_clear_error(monkeypatch):
    monkeypatch.delenv("STREAMSETS_CONTROL_HUB_URL", raising=False)
    monkeypatch.delenv("STREAMSETS_COMPONENT_ID", raising=False)
    monkeypatch.delenv("STREAMSETS_AUTH_TOKEN", raising=False)
    comp = StreamSetsWorkspaceComponent(
        jobs=[StreamSetsJobSpec(job_id="job-1", asset_key="streamsets/orders_cdc")],
        action="start",
    )
    defs = comp.build_defs(None)
    result = dg.materialize(list(defs.assets), raise_on_error=False)
    assert not result.success
