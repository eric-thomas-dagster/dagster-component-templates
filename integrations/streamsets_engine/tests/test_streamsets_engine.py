"""Real local HTTP server fake shaped like a standalone StreamSets Data
Collector engine's REST surface (not a mocked function) -- same convention
`streamsets_workspace` uses. Covers HTTP Basic auth and the
start -> poll-status-until-RUNNING reconcile-to-running flow."""
import base64
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import dagster as dg
import pytest

from .conftest import load_component_module

mod = load_component_module()
StreamSetsPipelineSpec = mod.StreamSetsPipelineSpec
StreamSetsEngineTransport = mod.StreamSetsEngineTransport
StreamSetsEngineComponent = mod.StreamSetsEngineComponent

_USERNAME = "admin"
_PASSWORD = "admin"


class _FakeEngine(BaseHTTPRequestHandler):
    statuses = {}  # pipeline_id -> {"status": ...}
    start_calls = []
    stop_calls = []
    metrics = {}
    force_start_status = {}  # pipeline_id -> status to land on instead of RUNNING after /start

    def _authorized(self) -> bool:
        auth = self.headers.get("Authorization", "")
        if not auth.startswith("Basic "):
            return False
        decoded = base64.b64decode(auth[len("Basic "):]).decode()
        return decoded == f"{_USERNAME}:{_PASSWORD}"

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
        if self.path == "/rest/v1/pipelines":
            self._write_json(200, list(self.statuses.keys()))
            return
        if self.path.startswith("/rest/v1/pipeline/") and self.path.endswith("/status"):
            pid = self.path[len("/rest/v1/pipeline/"):-len("/status")]
            self._write_json(200, self.statuses.get(pid, {}))
            return
        if self.path.startswith("/rest/v1/pipeline/") and self.path.endswith("/metrics"):
            pid = self.path[len("/rest/v1/pipeline/"):-len("/metrics")]
            self._write_json(200, self.metrics.get(pid, {}))
            return
        self.send_response(404)
        self.end_headers()

    def do_POST(self):
        if not self._authorized():
            self.send_response(401)
            self.end_headers()
            return
        if self.path.startswith("/rest/v1/pipeline/") and self.path.endswith("/start"):
            pid = self.path[len("/rest/v1/pipeline/"):-len("/start")]
            self.start_calls.append(pid)
            self.statuses[pid] = {"status": self.force_start_status.get(pid, "RUNNING")}
            self._write_json(200, self.statuses[pid])
            return
        if self.path.startswith("/rest/v1/pipeline/") and self.path.endswith("/stop"):
            pid = self.path[len("/rest/v1/pipeline/"):-len("/stop")]
            self.stop_calls.append(pid)
            self.statuses[pid] = {"status": "STOPPED"}
            self._write_json(200, self.statuses[pid])
            return
        self.send_response(404)
        self.end_headers()

    def log_message(self, *args):
        pass


@pytest.fixture
def fake_server():
    _FakeEngine.statuses = {
        "pipeline-1": {"status": "STOPPED"},
        "pipeline-2": {"status": "RUNNING"},
    }
    _FakeEngine.start_calls = []
    _FakeEngine.stop_calls = []
    _FakeEngine.metrics = {"pipeline-2": {"counters": {"pipeline.batchCount.counter": {"count": 5}}}}
    _FakeEngine.force_start_status = {}

    server = ThreadingHTTPServer(("127.0.0.1", 0), _FakeEngine)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        thread.join()


def _component(base_url: str, monkeypatch, **overrides):
    monkeypatch.setenv("STREAMSETS_ENGINE_URL", base_url)
    monkeypatch.setenv("STREAMSETS_USERNAME", _USERNAME)
    monkeypatch.setenv("STREAMSETS_PASSWORD", _PASSWORD)
    attrs = dict(
        pipelines=[
            StreamSetsPipelineSpec(pipeline_id="pipeline-1", asset_key="streamsets_engine/orders_cdc"),
            StreamSetsPipelineSpec(pipeline_id="pipeline-2", asset_key="streamsets_engine/clickstream"),
        ],
    )
    attrs.update(overrides)
    return StreamSetsEngineComponent(**attrs)


def test_one_asset_per_pipeline_from_one_component_instance(fake_server, monkeypatch):
    comp = _component(fake_server, monkeypatch)
    defs = comp.build_defs(None)
    keys = {a.key for a in defs.assets}
    assert keys == {dg.AssetKey(["streamsets_engine", "orders_cdc"]), dg.AssetKey(["streamsets_engine", "clickstream"])}


def test_noop_action_materializes_without_calling_start(fake_server, monkeypatch):
    comp = _component(fake_server, monkeypatch, action="noop")
    defs = comp.build_defs(None)
    result = dg.materialize(list(defs.assets), raise_on_error=True)
    assert result.success
    assert _FakeEngine.start_calls == []


def test_start_action_starts_pipeline_and_confirms_running_status(fake_server, monkeypatch):
    comp = _component(fake_server, monkeypatch, action="start", poll_interval_seconds=0, timeout_seconds=5)
    defs = comp.build_defs(None)
    result = dg.materialize(list(defs.assets), raise_on_error=True)
    assert result.success
    assert sorted(_FakeEngine.start_calls) == ["pipeline-1", "pipeline-2"]
    assert _FakeEngine.statuses["pipeline-1"]["status"] == "RUNNING"
    events = result.get_asset_materialization_events()
    assert all(e.materialization.metadata["streamsets/status"].value == "RUNNING" for e in events)


def test_stop_action_stops_pipeline_and_confirms_stopped_status(fake_server, monkeypatch):
    comp = _component(fake_server, monkeypatch, action="stop", poll_interval_seconds=0, timeout_seconds=5)
    defs = comp.build_defs(None)
    result = dg.materialize(list(defs.assets), raise_on_error=True)
    assert result.success
    assert sorted(_FakeEngine.stop_calls) == ["pipeline-1", "pipeline-2"]
    assert _FakeEngine.statuses["pipeline-2"]["status"] == "STOPPED"


def test_expose_metrics_attaches_metrics_metadata(fake_server, monkeypatch):
    comp = _component(
        fake_server, monkeypatch,
        pipelines=[StreamSetsPipelineSpec(pipeline_id="pipeline-2", asset_key="streamsets_engine/clickstream")],
        action="start", poll_interval_seconds=0, timeout_seconds=5, expose_metrics=True,
    )
    defs = comp.build_defs(None)
    result = dg.materialize(list(defs.assets), raise_on_error=True)
    assert result.success
    events = result.get_asset_materialization_events()
    metrics_md = events[0].materialization.metadata["streamsets/metrics"]
    assert metrics_md.data["counters"]["pipeline.batchCount.counter"]["count"] == 5


def test_error_status_raises_instead_of_hanging(fake_server, monkeypatch):
    _FakeEngine.force_start_status["pipeline-1"] = "START_ERROR"

    comp = _component(
        fake_server, monkeypatch,
        pipelines=[StreamSetsPipelineSpec(pipeline_id="pipeline-1", asset_key="streamsets_engine/orders_cdc")],
        action="start", poll_interval_seconds=0, timeout_seconds=5,
    )
    defs = comp.build_defs(None)
    result = dg.materialize(list(defs.assets), raise_on_error=False)
    assert not result.success


def test_demo_mode_requires_no_real_connection():
    comp = StreamSetsEngineComponent(
        demo_mode=True,
        action="start",
        pipelines=[StreamSetsPipelineSpec(pipeline_id="pipeline-1", asset_key="streamsets_engine/orders_cdc")],
    )
    defs = comp.build_defs(None)
    result = dg.materialize(list(defs.assets), raise_on_error=True)
    assert result.success
    events = result.get_asset_materialization_events()
    assert events[0].materialization.metadata["streamsets/status"].value == "RUNNING"


def test_polling_sensor_emits_observation_on_status_change(fake_server, monkeypatch):
    comp = _component(fake_server, monkeypatch, polling_sensor=True)
    defs = comp.build_defs(None)
    sensor = defs.sensors[0]

    ctx1 = dg.build_sensor_context(cursor=None)
    result1 = sensor(ctx1)
    assert result1.asset_events == []  # first sighting, baseline only

    _FakeEngine.statuses["pipeline-1"] = {"status": "RUNNING"}
    ctx2 = dg.build_sensor_context(cursor=result1.cursor)
    result2 = sensor(ctx2)
    assert len(result2.asset_events) == 1
    assert result2.asset_events[0].asset_key == dg.AssetKey(["streamsets_engine", "orders_cdc"])


def test_fake_server_rejects_missing_or_wrong_credentials(fake_server):
    import requests

    resp = requests.get(f"{fake_server}/rest/v1/pipeline/pipeline-1/status", timeout=5)
    assert resp.status_code == 401

    resp = requests.get(
        f"{fake_server}/rest/v1/pipeline/pipeline-1/status",
        auth=(_USERNAME, "wrong"),
        timeout=5,
    )
    assert resp.status_code == 401

    resp = requests.get(
        f"{fake_server}/rest/v1/pipeline/pipeline-1/status",
        auth=(_USERNAME, _PASSWORD),
        timeout=5,
    )
    assert resp.status_code == 200


def test_observe_metrics_in_sensor_fetches_metrics_every_tick_while_running(fake_server, monkeypatch):
    # pipeline-2 starts RUNNING per the fake_server fixture; pipeline-1 starts STOPPED.
    comp = _component(fake_server, monkeypatch, polling_sensor=True, observe_metrics_in_sensor=True)
    defs = comp.build_defs(None)
    sensor = defs.sensors[0]

    ctx1 = dg.build_sensor_context(cursor=None)
    result1 = sensor(ctx1)
    # Unlike status-change-only observation, a RUNNING pipeline's metrics are observed
    # on the very first tick too -- there's nothing to "change" from, but there IS a
    # live metrics snapshot worth reporting.
    assert len(result1.asset_events) == 1
    ev1 = result1.asset_events[0]
    assert ev1.asset_key == dg.AssetKey(["streamsets_engine", "clickstream"])
    assert ev1.metadata["streamsets/metrics"].data["counters"]["pipeline.batchCount.counter"]["count"] == 5
    assert "streamsets/previous_status" not in ev1.metadata  # no status change happened

    # Bump the live metrics between ticks -- proves each tick does a real fetch, not a cached one.
    _FakeEngine.metrics["pipeline-2"] = {"counters": {"pipeline.batchCount.counter": {"count": 42}}}
    ctx2 = dg.build_sensor_context(cursor=result1.cursor)
    result2 = sensor(ctx2)
    assert len(result2.asset_events) == 1
    assert result2.asset_events[0].metadata["streamsets/metrics"].data["counters"]["pipeline.batchCount.counter"]["count"] == 42

    # A STOPPED pipeline with unchanged status and observe_metrics_in_sensor on should NOT be
    # observed every tick -- metrics are only fetched/reported for RUNNING pipelines.
    assert all(e.asset_key != dg.AssetKey(["streamsets_engine", "orders_cdc"]) for e in result2.asset_events)


def test_observe_metrics_in_sensor_off_by_default_matches_status_only_behavior(fake_server, monkeypatch):
    comp = _component(fake_server, monkeypatch, polling_sensor=True)  # observe_metrics_in_sensor defaults False
    defs = comp.build_defs(None)
    sensor = defs.sensors[0]

    ctx1 = dg.build_sensor_context(cursor=None)
    result1 = sensor(ctx1)
    # Default behavior unchanged: first tick is baseline-only, even for an already-RUNNING pipeline.
    assert result1.asset_events == []
