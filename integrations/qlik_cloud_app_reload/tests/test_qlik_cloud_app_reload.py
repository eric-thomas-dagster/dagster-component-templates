"""Real, committed tests for QlikCloudAppReloadComponent, the first registry
component covering Qlik Cloud (a different product from Qlik Compose, which
this registry already had components for) -- see
component-feedback/2026-09-03-qlik-cloud-export.md in dagster-demo-factory.

Endpoints verified directly against https://qlik.dev/apis/rest/reloads/
before writing any code:
  POST /api/v1/reloads             {appId, partial, resourceType, resourceId}
  GET  /api/v1/reloads/{reloadId}  -> {id, appId, status, startTime, endTime, errorMessage, ...}
  GET  /api/v1/reloads?appId=...   -> {data: [...]}
Status enum: QUEUED, RELOADING, CANCELING (active) / SUCCEEDED, FAILED,
CANCELED, EXCEEDED_LIMIT (terminal).

A real local HTTP server plays the Qlik Cloud reloads API for real (not
mocked at the Python-function level) -- QUEUED -> RELOADING -> SUCCEEDED
across successive polls, and a separate FAILED-terminal-status case.
"""
import http.server
import json
import threading
from http import HTTPStatus
from urllib.parse import urlparse, parse_qs

import dagster as dg
import pytest

from .conftest import load_component_module


@pytest.fixture
def mod():
    return load_component_module()


class _FakeQlikServer(http.server.BaseHTTPRequestHandler):
    """Class-level state so each test configures its own reload-status sequence."""
    reload_sequence = []  # list of status dicts, consumed one GET at a time (last one repeats)
    reload_history = []   # for the GET /api/v1/reloads?appId=... list endpoint
    _poll_index = {}
    triggered_bodies = []
    received_auth = []

    def _send_json(self, code, payload):
        body = json.dumps(payload).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length)) if length else {}
        type(self).triggered_bodies.append(body)
        type(self).received_auth.append(self.headers.get("Authorization"))
        self._send_json(HTTPStatus.CREATED, {"id": "reload-1", "appId": body.get("appId"), "status": "QUEUED"})

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/api/v1/reloads":
            qs = parse_qs(parsed.query)
            self._send_json(HTTPStatus.OK, {"data": type(self).reload_history, "links": {}})
            return
        # /api/v1/reloads/{id}
        reload_id = parsed.path.rsplit("/", 1)[-1]
        idx = type(self)._poll_index.get(reload_id, 0)
        seq = type(self).reload_sequence
        status = seq[min(idx, len(seq) - 1)]
        type(self)._poll_index[reload_id] = idx + 1
        self._send_json(HTTPStatus.OK, {**status, "id": reload_id})

    def log_message(self, *a):
        pass


@pytest.fixture
def server():
    _FakeQlikServer.reload_sequence = [{"status": "QUEUED"}]
    _FakeQlikServer.reload_history = []
    _FakeQlikServer._poll_index = {}
    _FakeQlikServer.triggered_bodies = []
    _FakeQlikServer.received_auth = []
    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _FakeQlikServer)
    thread = threading.Thread(target=srv.serve_forever, daemon=True)
    thread.start()
    yield _FakeQlikServer, f"http://127.0.0.1:{srv.server_address[1]}"
    srv.shutdown()
    thread.join(timeout=5)


def _comp(mod, tenant_url, **kw):
    args = dict(
        asset_key="bi/finance_app", app_id="app-123",
        tenant_url_env_var="TEST_QLIK_URL", api_key_env_var="TEST_QLIK_KEY",
        poll_interval_seconds=0, timeout_seconds=10,
    )
    args.update(kw)
    return mod.QlikCloudAppReloadComponent(**args)


def test_successful_reload_triggers_and_polls_to_completion(mod, server, monkeypatch):
    handler, url = server
    handler.reload_sequence = [
        {"status": "QUEUED"},
        {"status": "RELOADING"},
        {"status": "SUCCEEDED", "startTime": "2026-10-23T00:00:00.000Z", "endTime": "2026-10-23T00:01:30.000Z"},
    ]
    monkeypatch.setenv("TEST_QLIK_URL", url)
    monkeypatch.setenv("TEST_QLIK_KEY", "fake-key")

    comp = _comp(mod, url)
    defs = comp.build_defs(None)
    result = dg.materialize(defs.assets)
    assert result.success

    mat_event = next(e for e in result.get_asset_materialization_events() if e.asset_key == dg.AssetKey(["bi", "finance_app"]))
    mat = mat_event.materialization
    assert mat.metadata["status"].value == "SUCCEEDED"
    assert mat.metadata["duration_seconds"].value == 90.0

    # Triggered with the resourceId tag that lets the sensor distinguish self-triggers.
    assert handler.triggered_bodies[0]["appId"] == "app-123"
    assert handler.triggered_bodies[0]["resourceId"] == "dagster"
    assert handler.received_auth[0] == "Bearer fake-key"


def test_failed_reload_raises_with_the_real_error_fields(mod, server, monkeypatch):
    handler, url = server
    handler.reload_sequence = [
        {"status": "QUEUED"},
        {"status": "FAILED", "errorCode": "SCRIPT_ERROR", "errorMessage": "Load script failed at line 42"},
    ]
    monkeypatch.setenv("TEST_QLIK_URL", url)
    monkeypatch.setenv("TEST_QLIK_KEY", "fake-key")

    comp = _comp(mod, url)
    defs = comp.build_defs(None)
    with pytest.raises(Exception, match="Load script failed at line 42"):
        dg.materialize(defs.assets)


def test_timeout_raises_when_reload_never_reaches_terminal_status(mod, server, monkeypatch):
    handler, url = server
    handler.reload_sequence = [{"status": "RELOADING"}]  # never terminates
    monkeypatch.setenv("TEST_QLIK_URL", url)
    monkeypatch.setenv("TEST_QLIK_KEY", "fake-key")

    comp = _comp(mod, url, timeout_seconds=0, poll_interval_seconds=0)
    defs = comp.build_defs(None)
    with pytest.raises(Exception, match="did not reach a terminal status"):
        dg.materialize(defs.assets)


def test_no_sensor_by_default(mod, server):
    _, url = server
    comp = _comp(mod, url)
    defs = comp.build_defs(None)
    assert not list(defs.sensors or [])


def test_polling_sensor_observes_externally_triggered_reloads_only(mod, server, monkeypatch):
    handler, url = server
    monkeypatch.setenv("TEST_QLIK_URL", url)
    monkeypatch.setenv("TEST_QLIK_KEY", "fake-key")
    handler.reload_history = [
        {"id": "r-self", "status": "SUCCEEDED", "resourceId": "dagster", "type": "api"},
        {"id": "r-external", "status": "SUCCEEDED", "resourceId": None, "type": "hub"},
    ]

    comp = _comp(mod, url, polling_sensor=True, sensor_name="qlik_reload_observer")
    defs = comp.build_defs(None)
    sensor_def = defs.get_sensor_def("qlik_reload_observer")
    context = dg.build_sensor_context()
    result = sensor_def(context)

    assert len(result.asset_events) == 1
    assert result.asset_events[0].metadata["qlik_reload_id"].value == "r-external"


def test_polling_sensor_skips_cleanly_with_no_new_reloads(mod, server, monkeypatch):
    handler, url = server
    monkeypatch.setenv("TEST_QLIK_URL", url)
    monkeypatch.setenv("TEST_QLIK_KEY", "fake-key")
    handler.reload_history = []

    comp = _comp(mod, url, polling_sensor=True, sensor_name="qlik_reload_observer")
    defs = comp.build_defs(None)
    sensor_def = defs.get_sensor_def("qlik_reload_observer")
    result = sensor_def(dg.build_sensor_context())
    assert result.skip_reason is not None
