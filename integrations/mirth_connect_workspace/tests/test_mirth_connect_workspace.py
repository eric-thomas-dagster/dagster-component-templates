"""Real local HTTP server fake shaped like Mirth Connect's documented Administrator
REST API (not a mocked function) -- same convention `qlik_cloud_app_reload` /
`qlik_cloud_workspace` use in this registry. Covers the three real gotchas this
component's transport is built around: the mandatory X-Requested-With header,
Basic auth, and deploy/start/stop always returning 204 regardless of outcome
(so the component must poll /channels/statuses to know what actually happened)."""
import base64
import http.server
import json
import threading

import dagster as dg
import pytest

from .conftest import load_component_module

mod = load_component_module()
MirthChannelSpec = mod.MirthChannelSpec
MirthConnectTransport = mod.MirthConnectTransport
MirthConnectWorkspaceComponent = mod.MirthConnectWorkspaceComponent

_USERNAME = "admin"
_PASSWORD = "admin"


class _FakeMirthServer(http.server.BaseHTTPRequestHandler):
    channels = {}  # channel_id -> {"id": ..., "name": ...}
    statuses = {}  # channel_id -> "STARTED" | "STOPPED" | "UNDEPLOYED"
    statistics = {}  # channel_id -> {received, filtered, queued, sent, error}
    deploy_calls = []
    start_calls = []
    stop_calls = []

    def _authorized(self) -> bool:
        if self.headers.get("X-Requested-With") is None:
            return False
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
            self.send_response(403 if self.headers.get("X-Requested-With") is None else 401)
            self.end_headers()
            return

        if self.path == "/channels":
            self._write_json(200, list(self.channels.values()))
            return
        if self.path == "/channels/statuses":
            self._write_json(200, [{"channelId": cid, "name": self.channels.get(cid, {}).get("name"), "state": state} for cid, state in self.statuses.items()])
            return
        if self.path.startswith("/channels/") and self.path.endswith("/statistics"):
            channel_id = self.path[len("/channels/"):-len("/statistics")]
            self._write_json(200, self.statistics.get(channel_id, {"channelId": channel_id, "received": 0, "filtered": 0, "queued": 0, "sent": 0, "error": 0}))
            return
        self.send_response(404)
        self.end_headers()

    def do_POST(self):
        if not self._authorized():
            self.send_response(403 if self.headers.get("X-Requested-With") is None else 401)
            self.end_headers()
            return

        for suffix, log, new_state in (
            ("/_deploy", self.deploy_calls, "STOPPED"),  # deploy alone doesn't start it
            ("/_start", self.start_calls, "STARTED"),
            ("/_stop", self.stop_calls, "STOPPED"),
            ("/_undeploy", None, "UNDEPLOYED"),
        ):
            if self.path.startswith("/channels/") and self.path.endswith(suffix):
                channel_id = self.path[len("/channels/"):-len(suffix)]
                if log is not None:
                    log.append(channel_id)
                self.statuses[channel_id] = new_state
                self.send_response(204)  # always 204 -- the documented Mirth gotcha
                self.end_headers()
                return

        self.send_response(404)
        self.end_headers()

    def log_message(self, *args):
        pass


@pytest.fixture
def fake_server():
    _FakeMirthServer.channels = {
        "ch-1": {"id": "ch-1", "name": "ADT Feed"},
        "ch-2": {"id": "ch-2", "name": "Lab Results Feed"},
    }
    _FakeMirthServer.statuses = {"ch-1": "STOPPED", "ch-2": "STOPPED"}
    _FakeMirthServer.statistics = {
        "ch-1": {"channelId": "ch-1", "serverId": "s1", "received": 10, "filtered": 1, "queued": 0, "sent": 9, "error": 0},
        "ch-2": {"channelId": "ch-2", "serverId": "s1", "received": 5, "filtered": 0, "queued": 0, "sent": 4, "error": 1},
    }
    _FakeMirthServer.deploy_calls = []
    _FakeMirthServer.start_calls = []
    _FakeMirthServer.stop_calls = []

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _FakeMirthServer)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        thread.join()


def _component(base_url: str, monkeypatch, **overrides):
    monkeypatch.setenv("MIRTH_BASE_URL", base_url)
    monkeypatch.setenv("MIRTH_USERNAME", _USERNAME)
    monkeypatch.setenv("MIRTH_PASSWORD", _PASSWORD)
    attrs = dict(
        channels=[
            MirthChannelSpec(channel_id="ch-1", asset_key="hl7/adt_feed"),
            MirthChannelSpec(channel_id="ch-2", asset_key="hl7/lab_results_feed"),
        ],
    )
    attrs.update(overrides)
    return MirthConnectWorkspaceComponent(**attrs)


def test_one_asset_per_channel_from_one_component_instance(fake_server, monkeypatch):
    comp = _component(fake_server, monkeypatch)
    defs = comp.build_defs(None)
    keys = {a.key for a in defs.assets}
    assert keys == {dg.AssetKey(["hl7", "adt_feed"]), dg.AssetKey(["hl7", "lab_results_feed"])}


def test_noop_action_materializes_without_calling_deploy_or_start(fake_server, monkeypatch):
    comp = _component(fake_server, monkeypatch, action="noop")
    defs = comp.build_defs(None)
    result = dg.materialize(list(defs.assets), raise_on_error=True)
    assert result.success
    assert _FakeMirthServer.deploy_calls == []
    assert _FakeMirthServer.start_calls == []


def test_deploy_action_calls_deploy_then_start_and_confirms_started_state(fake_server, monkeypatch):
    comp = _component(fake_server, monkeypatch, action="deploy", poll_interval_seconds=0, timeout_seconds=5)
    defs = comp.build_defs(None)
    result = dg.materialize(list(defs.assets), raise_on_error=True)
    assert result.success
    assert sorted(_FakeMirthServer.deploy_calls) == ["ch-1", "ch-2"]
    assert sorted(_FakeMirthServer.start_calls) == ["ch-1", "ch-2"]
    assert _FakeMirthServer.statuses["ch-1"] == "STARTED"
    events = result.get_asset_materialization_events()
    assert all(e.materialization.metadata["mirth/state"].value == "STARTED" for e in events)


def test_fake_server_rejects_missing_required_header_and_wrong_credentials(fake_server):
    import requests

    # Missing X-Requested-With -- the documented Mirth gotcha: bare 403, no explanation.
    resp = requests.get(f"{fake_server}/channels", auth=(_USERNAME, _PASSWORD), timeout=5)
    assert resp.status_code == 403

    # Present header, wrong credentials.
    resp = requests.get(f"{fake_server}/channels", headers={"X-Requested-With": "x"}, auth=(_USERNAME, "wrong"), timeout=5)
    assert resp.status_code == 401

    # Present header, correct credentials -- succeeds, proving the transport's
    # own calls (which always send this header) are what makes them work.
    resp = requests.get(f"{fake_server}/channels", headers={"X-Requested-With": "x"}, auth=(_USERNAME, _PASSWORD), timeout=5)
    assert resp.status_code == 200


def test_assets_by_channel_overrides_group_name_deps_and_tags(fake_server, monkeypatch):
    comp = _component(
        fake_server,
        monkeypatch,
        assets_by_channel={"ch-1": {"group_name": "hl7_prod", "deps": ["warehouse/raw_hl7"], "tags": {"tier": "critical"}}},
    )
    defs = comp.build_defs(None)
    spec = next(a for a in defs.assets if a.key == dg.AssetKey(["hl7", "adt_feed"])).get_asset_spec(dg.AssetKey(["hl7", "adt_feed"]))
    assert spec.group_name == "hl7_prod"
    assert dg.AssetKey(["warehouse", "raw_hl7"]) in {d.asset_key for d in spec.deps}
    assert spec.tags.get("tier") == "critical"


def test_polling_sensor_only_emits_for_channels_whose_counters_moved(fake_server, monkeypatch):
    comp = _component(fake_server, monkeypatch, polling_sensor=True)
    defs = comp.build_defs(None)
    sensor_def = next(iter(defs.sensors))

    # Tick 1: no prior cursor -- baseline only, no events yet.
    result1 = sensor_def(dg.build_sensor_context())
    assert result1.asset_events == []

    # Tick 2: ch-1's received count increases; ch-2 unchanged.
    _FakeMirthServer.statistics["ch-1"]["received"] = 20
    _FakeMirthServer.statistics["ch-1"]["sent"] = 19
    result2 = sensor_def(dg.build_sensor_context(cursor=result1.cursor))
    assert len(result2.asset_events) == 1
    assert result2.asset_events[0].asset_key == dg.AssetKey(["hl7", "adt_feed"])
    assert result2.asset_events[0].metadata["mirth/received_delta"].value == 10

    # Tick 3: nothing moved -- no events.
    result3 = sensor_def(dg.build_sensor_context(cursor=result2.cursor))
    assert result3.asset_events == []


def test_require_running_check_fails_when_channel_stopped(fake_server, monkeypatch):
    comp = _component(fake_server, monkeypatch, require_running=True)
    defs = comp.build_defs(None)
    check = next(c for c in defs.asset_checks if c.check_key.asset_key == dg.AssetKey(["hl7", "adt_feed"]))
    result = check(dg.build_asset_check_context())
    assert result.passed is False  # fixture starts both channels STOPPED

    _FakeMirthServer.statuses["ch-1"] = "STARTED"
    result = check(dg.build_asset_check_context())
    assert result.passed is True


def test_error_rate_check_fails_over_threshold(fake_server, monkeypatch):
    comp = _component(fake_server, monkeypatch, error_rate_threshold=0.1)
    defs = comp.build_defs(None)
    check = next(c for c in defs.asset_checks if c.check_key.asset_key == dg.AssetKey(["hl7", "lab_results_feed"]))
    # ch-2 fixture: error=1, received=5 -> 0.2 > 0.1
    result = check(dg.build_asset_check_context())
    assert result.passed is False
    assert result.metadata["mirth/error_rate"].value == pytest.approx(0.2)


def test_demo_mode_needs_no_real_server(monkeypatch):
    comp = MirthConnectWorkspaceComponent(
        channels=[MirthChannelSpec(channel_id="demo-1", asset_key="hl7/demo_feed")],
        action="deploy",
        demo_mode=True,
        polling_sensor=True,
        require_running=True,
        error_rate_threshold=0.1,
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
    transport = MirthConnectTransport(fake_server, _USERNAME, _PASSWORD, verify_ssl=True)
    channels = transport.list_channels()
    assert {c["id"] for c in channels} == {"ch-1", "ch-2"}

    transport.deploy_channel("ch-1")
    transport.start_channel("ch-1")
    assert transport.get_statuses()["ch-1"]["state"] == "STARTED"

    stats = transport.get_statistics("ch-2")
    assert stats["received"] == 5
