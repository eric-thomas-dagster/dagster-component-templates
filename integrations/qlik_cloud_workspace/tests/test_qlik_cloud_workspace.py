"""Real, committed tests for QlikCloudWorkspaceComponent -- the N-app sibling
to QlikCloudAppReloadComponent, closing a real scaling gap found via review
feedback: 30 dashboards used to mean 30 separate component instances (and 30
polling sensors). One QlikCloudWorkspaceComponent instance now wraps any
number of apps with ONE shared observation sensor.

A real local HTTP server plays the Qlik Cloud reloads API for real (not
mocked at the Python-function level), same convention as
qlik_cloud_app_reload's own test suite.
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
    reload_sequence = []
    reload_history_by_app = {}
    _poll_index = {}
    triggered_bodies = []

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
        self._send_json(HTTPStatus.CREATED, {"id": f"reload-{body.get('appId')}", "appId": body.get("appId"), "status": "QUEUED"})

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/api/v1/reloads":
            qs = parse_qs(parsed.query)
            app_id = qs.get("appId", [None])[0]
            self._send_json(HTTPStatus.OK, {"data": type(self).reload_history_by_app.get(app_id, []), "links": {}})
            return
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
    _FakeQlikServer.reload_sequence = [{"status": "SUCCEEDED", "startTime": "2026-10-23T00:00:00.000Z", "endTime": "2026-10-23T00:01:00.000Z"}]
    _FakeQlikServer.reload_history_by_app = {}
    _FakeQlikServer._poll_index = {}
    _FakeQlikServer.triggered_bodies = []
    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _FakeQlikServer)
    thread = threading.Thread(target=srv.serve_forever, daemon=True)
    thread.start()
    yield _FakeQlikServer, f"http://127.0.0.1:{srv.server_address[1]}"
    srv.shutdown()
    thread.join(timeout=5)


def _comp(mod, **kw):
    args = dict(
        tenant_url_env_var="TEST_QLIK_URL", api_key_env_var="TEST_QLIK_KEY",
        poll_interval_seconds=0, timeout_seconds=10,
        apps=[
            {"app_id": "app-finance", "asset_key": "bi/finance_app"},
            {"app_id": "app-sales", "asset_key": "bi/sales_app"},
        ],
    )
    args.update(kw)
    return mod.QlikCloudWorkspaceComponent(**args)


def test_one_component_instance_produces_one_asset_per_app(mod, server, monkeypatch):
    _, url = server
    monkeypatch.setenv("TEST_QLIK_URL", url)
    monkeypatch.setenv("TEST_QLIK_KEY", "fake-key")

    comp = _comp(mod)
    defs = comp.build_defs(None)
    keys = {a.key for ad in defs.assets for a in ad.specs}
    assert keys == {dg.AssetKey(["bi", "finance_app"]), dg.AssetKey(["bi", "sales_app"])}


def test_each_app_materializes_independently_against_the_real_api_shape(mod, server, monkeypatch):
    handler, url = server
    monkeypatch.setenv("TEST_QLIK_URL", url)
    monkeypatch.setenv("TEST_QLIK_KEY", "fake-key")

    comp = _comp(mod)
    defs = comp.build_defs(None)
    result = dg.materialize(defs.assets)
    assert result.success

    mat = next(e for e in result.get_asset_materialization_events() if e.asset_key == dg.AssetKey(["bi", "finance_app"])).materialization
    assert mat.metadata["status"].value == "SUCCEEDED"
    assert mat.metadata["qlik_app_id"].value == "app-finance"

    triggered_app_ids = {b["appId"] for b in handler.triggered_bodies}
    assert triggered_app_ids == {"app-finance", "app-sales"}


def test_assets_by_app_overrides_group_name_deps_and_tags(mod, server, monkeypatch):
    _, url = server
    monkeypatch.setenv("TEST_QLIK_URL", url)
    monkeypatch.setenv("TEST_QLIK_KEY", "fake-key")

    comp = _comp(
        mod,
        apps=[{"app_id": "app-finance", "asset_key": "bi/finance_app", "deps": ["warehouse/finance_fact"]}],
        assets_by_app={
            "app-finance": {
                "group_name": "finance_bi",
                "deps": ["warehouse/extra_dep"],
                "tags": {"team": "finance"},
            },
        },
    )
    defs = comp.build_defs(None)
    spec = next(a for ad in defs.assets for a in ad.specs)
    assert spec.group_name == "finance_bi"
    dep_keys = {d.asset_key for d in spec.deps}
    assert dep_keys == {dg.AssetKey(["warehouse", "finance_fact"]), dg.AssetKey(["warehouse", "extra_dep"])}
    assert spec.tags.get("team") == "finance"


def test_one_shared_sensor_observes_every_app_distinguishing_self_triggers(mod, server, monkeypatch):
    handler, url = server
    monkeypatch.setenv("TEST_QLIK_URL", url)
    monkeypatch.setenv("TEST_QLIK_KEY", "fake-key")
    handler.reload_history_by_app = {
        "app-finance": [
            {"id": "r-finance-self", "status": "SUCCEEDED", "resourceId": "dagster", "type": "api"},
            {"id": "r-finance-external", "status": "SUCCEEDED", "resourceId": None, "type": "hub"},
        ],
        "app-sales": [
            {"id": "r-sales-external", "status": "SUCCEEDED", "resourceId": None, "type": "hub"},
        ],
    }

    comp = _comp(mod, polling_sensor=True, sensor_name="qlik_workspace_observer")
    defs = comp.build_defs(None)

    # ONE sensor object across both apps -- the scaling fix.
    sensor_names = [s.name for s in defs.sensors]
    assert sensor_names == ["qlik_workspace_observer"]

    sensor_def = defs.get_sensor_def("qlik_workspace_observer")
    result = sensor_def(dg.build_sensor_context())

    observed_ids = {e.metadata["qlik_reload_id"].value for e in result.asset_events}
    assert observed_ids == {"r-finance-external", "r-sales-external"}
    observed_keys = {e.asset_key for e in result.asset_events}
    assert observed_keys == {dg.AssetKey(["bi", "finance_app"]), dg.AssetKey(["bi", "sales_app"])}


def test_no_sensor_by_default(mod, server):
    comp = _comp(mod)
    defs = comp.build_defs(None)
    assert not list(defs.sensors or [])


def test_transport_seam_is_a_swappable_class(mod, server):
    """The transport is a real, independently-usable class -- confirms the
    seam exists as something other than inline `requests` calls scattered
    through the component, by exercising it directly against the fake
    server with no QlikCloudWorkspaceComponent involved at all."""
    handler, url = server
    handler.reload_sequence = [{"status": "QUEUED"}, {"status": "SUCCEEDED"}]

    transport = mod.QlikCloudTransport(url, "fake-key")
    triggered = transport.trigger_reload("app-finance", partial=False)
    assert triggered["appId"] == "app-finance"

    final = transport.poll_to_terminal(triggered["id"], poll_interval_seconds=0, timeout_seconds=5)
    assert final["status"] == "SUCCEEDED"
