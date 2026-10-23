"""Real, committed tests for DruidSqlQueryComponent.

Stands up a real local HTTP server implementing the exact Druid SQL API
shape (verified directly against the official reference, not from memory
-- POST /druid/v2/sql with resultFormat='object' returns a JSON array of
row objects), and points the component at it -- real HTTP requests over
the real `requests` library.
"""
import http.server
import json
import threading

import dagster as dg
import pytest

from .conftest import load_component_module

mod = load_component_module()


class _TestServer(http.server.ThreadingHTTPServer):
    request_queue_size = 64
    daemon_threads = True


class _DruidHandler(http.server.BaseHTTPRequestHandler):
    rows = [{"day": "2024-01-01", "cnt": 10}, {"day": "2024-01-02", "cnt": 20}]
    requests_received = []
    auth_headers_received = []

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length)) if length else {}
        self.__class__.requests_received.append(body)
        self.__class__.auth_headers_received.append(self.headers.get("Authorization"))
        if self.path != "/druid/v2/sql":
            self.send_response(404)
            self.end_headers()
            return
        payload = json.dumps(self.__class__.rows).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, fmt, *args):
        pass


@pytest.fixture
def druid_server():
    _DruidHandler.rows = [{"day": "2024-01-01", "cnt": 10}, {"day": "2024-01-02", "cnt": 20}]
    _DruidHandler.requests_received = []
    _DruidHandler.auth_headers_received = []
    server = _TestServer(("127.0.0.1", 0), _DruidHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        thread.join()


def _url(server):
    return f"http://127.0.0.1:{server.server_address[1]}"


def _comp(server, **kw):
    args = dict(asset_name="daily_counts", broker_url=_url(server), query="SELECT day, cnt FROM counts")
    args.update(kw)
    return mod.DruidSqlQueryComponent(**args)


def test_query_returns_a_real_dataframe(druid_server):
    comp = _comp(druid_server)
    defs = comp.build_defs(None)
    result = dg.materialize(list(defs.assets), resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
    assert _DruidHandler.requests_received[0]["query"] == "SELECT day, cnt FROM counts"
    assert _DruidHandler.requests_received[0]["resultFormat"] == "object"

    meta = next(e for e in result.all_events if e.event_type_value == "ASSET_MATERIALIZATION").event_specific_data.materialization.metadata
    assert meta["dagster/row_count"].value == 2


def test_partition_tokens_substituted_into_the_query(druid_server):
    comp = _comp(druid_server, query="SELECT * FROM counts WHERE day = '{partition_key}'", partition_type="daily", partition_start="2024-01-01")
    defs = comp.build_defs(None)
    dg.materialize(list(defs.assets), partition_key="2024-01-02", resources={"io_manager": dg.InMemoryIOManager()})
    assert "2024-01-02" in _DruidHandler.requests_received[0]["query"]


def test_basic_auth_is_sent_when_username_set(druid_server, monkeypatch):
    monkeypatch.setenv("DRUID_PW", "secret")
    comp = _comp(druid_server, username="admin", password_env_var="DRUID_PW")
    defs = comp.build_defs(None)
    dg.materialize(list(defs.assets), resources={"io_manager": dg.InMemoryIOManager()})
    assert _DruidHandler.auth_headers_received[0] is not None
    assert _DruidHandler.auth_headers_received[0].startswith("Basic ")


def test_query_failure_raises_with_response_body(druid_server):
    class _ErrorHandler(_DruidHandler):
        def do_POST(self):
            payload = json.dumps({"error": "Unknown column"}).encode()
            self.send_response(400)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    druid_server.RequestHandlerClass = _ErrorHandler
    comp = _comp(druid_server)
    result = dg.materialize(list(comp.build_defs(None).assets), raise_on_error=False, resources={"io_manager": dg.InMemoryIOManager()})
    assert not result.success
    failure = next(e for e in result.all_events if e.event_type_value == "STEP_FAILURE")
    assert "Unknown column" in str(failure.event_specific_data.error)


def test_weekday_cron_partition_builds_a_real_time_window_partitions_def(druid_server):
    comp = _comp(druid_server, partition_type="cron", partition_start="2024-01-01", partition_cron_schedule="0 0 * * 1-5")
    pdef = list(comp.build_defs(None).assets)[0].partitions_def
    assert isinstance(pdef, dg.TimeWindowPartitionsDefinition)
