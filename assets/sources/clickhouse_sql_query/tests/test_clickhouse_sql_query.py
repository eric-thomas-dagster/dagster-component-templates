"""Real, committed tests for ClickhouseSqlQueryComponent.

Stands up a real local HTTP server implementing the exact ClickHouse HTTP
interface shape (verified directly against the official reference, not
from memory -- the query is sent as the raw POST body, and 'FORMAT JSON'
makes the server return {"meta": [...], "data": [...], "rows": N,
"statistics": {...}}), and points the component at it -- real HTTP
requests over the real `requests` library.
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


class _ClickhouseHandler(http.server.BaseHTTPRequestHandler):
    rows = [{"day": "2024-01-01", "cnt": 10}, {"day": "2024-01-02", "cnt": 20}]
    queries_received = []
    auth_headers_received = []

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length).decode("utf-8") if length else ""
        self.__class__.queries_received.append(body)
        self.__class__.auth_headers_received.append(self.headers.get("Authorization"))
        payload = json.dumps({
            "meta": [{"name": k, "type": "String"} for k in (self.__class__.rows[0].keys() if self.__class__.rows else [])],
            "data": self.__class__.rows,
            "rows": len(self.__class__.rows),
            "statistics": {"elapsed": 0.001, "rows_read": len(self.__class__.rows), "bytes_read": 100},
        }).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, fmt, *args):
        pass


@pytest.fixture
def clickhouse_server():
    _ClickhouseHandler.rows = [{"day": "2024-01-01", "cnt": 10}, {"day": "2024-01-02", "cnt": 20}]
    _ClickhouseHandler.queries_received = []
    _ClickhouseHandler.auth_headers_received = []
    server = _TestServer(("127.0.0.1", 0), _ClickhouseHandler)
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
    args = dict(asset_name="daily_counts", host_url=_url(server), query="SELECT day, cnt FROM counts")
    args.update(kw)
    return mod.ClickhouseSqlQueryComponent(**args)


def test_query_returns_a_real_dataframe(clickhouse_server):
    comp = _comp(clickhouse_server)
    defs = comp.build_defs(None)
    result = dg.materialize(list(defs.assets), resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
    assert _ClickhouseHandler.queries_received[0] == "SELECT day, cnt FROM counts FORMAT JSON"

    meta = next(e for e in result.all_events if e.event_type_value == "ASSET_MATERIALIZATION").event_specific_data.materialization.metadata
    assert meta["dagster/row_count"].value == 2


def test_format_clause_not_duplicated_when_already_present(clickhouse_server):
    comp = _comp(clickhouse_server, query="SELECT day, cnt FROM counts FORMAT JSONCompact")
    dg.materialize(list(comp.build_defs(None).assets), resources={"io_manager": dg.InMemoryIOManager()})
    assert _ClickhouseHandler.queries_received[0] == "SELECT day, cnt FROM counts FORMAT JSONCompact"


def test_partition_tokens_substituted_into_the_query(clickhouse_server):
    comp = _comp(clickhouse_server, query="SELECT * FROM counts WHERE day = '{partition_key}'", partition_type="daily", partition_start="2024-01-01")
    defs = comp.build_defs(None)
    dg.materialize(list(defs.assets), partition_key="2024-01-02", resources={"io_manager": dg.InMemoryIOManager()})
    assert "2024-01-02" in _ClickhouseHandler.queries_received[0]


def test_basic_auth_is_sent_when_username_set(clickhouse_server, monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_PW", "secret")
    comp = _comp(clickhouse_server, username="default", password_env_var="CLICKHOUSE_PW")
    defs = comp.build_defs(None)
    dg.materialize(list(defs.assets), resources={"io_manager": dg.InMemoryIOManager()})
    assert _ClickhouseHandler.auth_headers_received[0] is not None
    assert _ClickhouseHandler.auth_headers_received[0].startswith("Basic ")


def test_query_failure_raises_with_response_body(clickhouse_server):
    class _ErrorHandler(_ClickhouseHandler):
        def do_POST(self):
            payload = b"Code: 62. DB::Exception: Syntax error (Unknown column)"
            self.send_response(500)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    clickhouse_server.RequestHandlerClass = _ErrorHandler
    comp = _comp(clickhouse_server)
    result = dg.materialize(list(comp.build_defs(None).assets), raise_on_error=False, resources={"io_manager": dg.InMemoryIOManager()})
    assert not result.success
    failure = next(e for e in result.all_events if e.event_type_value == "STEP_FAILURE")
    assert "Unknown column" in str(failure.event_specific_data.error)


def test_mid_stream_failure_with_200_status_and_non_json_body_raises(clickhouse_server):
    """A 200 status does not guarantee success -- ClickHouse can start streaming before an error happens."""
    class _MidStreamErrorHandler(_ClickhouseHandler):
        def do_POST(self):
            payload = b"day\tcnt\n2024-01-01\t10\nCode: 241. DB::Exception: Memory limit exceeded"
            self.send_response(200)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    clickhouse_server.RequestHandlerClass = _MidStreamErrorHandler
    comp = _comp(clickhouse_server)
    result = dg.materialize(list(comp.build_defs(None).assets), raise_on_error=False, resources={"io_manager": dg.InMemoryIOManager()})
    assert not result.success


def test_weekday_cron_partition_builds_a_real_time_window_partitions_def(clickhouse_server):
    comp = _comp(clickhouse_server, partition_type="cron", partition_start="2024-01-01", partition_cron_schedule="0 0 * * 1-5")
    pdef = list(comp.build_defs(None).assets)[0].partitions_def
    assert isinstance(pdef, dg.TimeWindowPartitionsDefinition)
