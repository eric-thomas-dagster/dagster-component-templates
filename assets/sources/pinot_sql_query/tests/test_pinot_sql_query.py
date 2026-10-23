"""Real, committed tests for PinotSqlQueryComponent.

Stands up a real local HTTP server implementing the exact Pinot Broker
Query API shape (verified directly against the official reference, not
from memory -- POST /query/sql returns {"resultTable": {"dataSchema":
{"columnNames": [...]}, "rows": [[...], ...]}, "exceptions": [...]}), and
points the component at it.
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


class _PinotHandler(http.server.BaseHTTPRequestHandler):
    column_names = ["day", "cnt"]
    rows = [["2024-01-01", 10], ["2024-01-02", 20]]
    exceptions = []
    status_code = 200
    requests_received = []
    auth_headers_received = []

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length)) if length else {}
        self.__class__.requests_received.append(body)
        self.__class__.auth_headers_received.append(self.headers.get("Authorization"))
        payload = json.dumps({
            "resultTable": {"dataSchema": {"columnNames": self.__class__.column_names}, "rows": self.__class__.rows},
            "exceptions": self.__class__.exceptions,
        }).encode()
        self.send_response(self.__class__.status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, fmt, *args):
        pass


@pytest.fixture
def pinot_server():
    _PinotHandler.column_names = ["day", "cnt"]
    _PinotHandler.rows = [["2024-01-01", 10], ["2024-01-02", 20]]
    _PinotHandler.exceptions = []
    _PinotHandler.status_code = 200
    _PinotHandler.requests_received = []
    _PinotHandler.auth_headers_received = []
    server = _TestServer(("127.0.0.1", 0), _PinotHandler)
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
    return mod.PinotSqlQueryComponent(**args)


def test_query_returns_a_real_dataframe_with_columns(pinot_server):
    comp = _comp(pinot_server)
    result = dg.materialize(list(comp.build_defs(None).assets), resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
    assert _PinotHandler.requests_received[0]["sql"] == "SELECT day, cnt FROM counts"

    meta = next(e for e in result.all_events if e.event_type_value == "ASSET_MATERIALIZATION").event_specific_data.materialization.metadata
    assert meta["dagster/row_count"].value == 2


def test_partition_tokens_substituted_into_the_query(pinot_server):
    comp = _comp(pinot_server, query="SELECT * FROM counts WHERE day = '{partition_key}'", partition_type="daily", partition_start="2024-01-01")
    dg.materialize(list(comp.build_defs(None).assets), partition_key="2024-01-02", resources={"io_manager": dg.InMemoryIOManager()})
    assert "2024-01-02" in _PinotHandler.requests_received[0]["sql"]


def test_basic_auth_is_sent_when_username_set(pinot_server, monkeypatch):
    monkeypatch.setenv("PINOT_PW", "secret")
    comp = _comp(pinot_server, username="admin", password_env_var="PINOT_PW")
    dg.materialize(list(comp.build_defs(None).assets), resources={"io_manager": dg.InMemoryIOManager()})
    assert _PinotHandler.auth_headers_received[0] is not None
    assert _PinotHandler.auth_headers_received[0].startswith("Basic ")


def test_inline_exceptions_raise_even_with_http_200(pinot_server):
    """The real Pinot gotcha this component handles: a query error can come back
    inside the JSON body alongside HTTP 200, not just via a 4xx/5xx status."""
    _PinotHandler.exceptions = [{"errorCode": 150, "message": "QueryExecutionError"}]
    comp = _comp(pinot_server)
    result = dg.materialize(list(comp.build_defs(None).assets), raise_on_error=False, resources={"io_manager": dg.InMemoryIOManager()})
    assert not result.success
    failure = next(e for e in result.all_events if e.event_type_value == "STEP_FAILURE")
    assert "QueryExecutionError" in str(failure.event_specific_data.error)


def test_http_error_status_also_raises(pinot_server):
    _PinotHandler.status_code = 400
    comp = _comp(pinot_server)
    result = dg.materialize(list(comp.build_defs(None).assets), raise_on_error=False, resources={"io_manager": dg.InMemoryIOManager()})
    assert not result.success


def test_weekday_cron_partition_builds_a_real_time_window_partitions_def(pinot_server):
    comp = _comp(pinot_server, partition_type="cron", partition_start="2024-01-01", partition_cron_schedule="0 0 * * 1-5")
    pdef = list(comp.build_defs(None).assets)[0].partitions_def
    assert isinstance(pdef, dg.TimeWindowPartitionsDefinition)
