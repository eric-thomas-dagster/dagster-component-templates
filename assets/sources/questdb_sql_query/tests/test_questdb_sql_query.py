"""Real, committed tests for QuestdbSqlQueryComponent.

Stands up a real local HTTP server implementing the exact QuestDB REST
API shape (verified directly against the official reference, not from
memory -- GET /api/v1/sql/execute with the query as a URL parameter,
returning {"columns": [...], "dataset": [[...]], ...} with positional row
values), and points the component at it -- real HTTP requests over the
real `requests` library.
"""
import http.server
import json
import threading
from urllib.parse import urlparse, parse_qs

import dagster as dg
import pytest

from .conftest import load_component_module

mod = load_component_module()


class _TestServer(http.server.ThreadingHTTPServer):
    request_queue_size = 64
    daemon_threads = True


class _QuestdbHandler(http.server.BaseHTTPRequestHandler):
    columns = [{"name": "day", "type": "STRING"}, {"name": "cnt", "type": "LONG"}]
    dataset = [["2024-01-01", 10], ["2024-01-02", 20]]
    queries_received = []
    auth_headers_received = []

    def do_GET(self):
        parsed = urlparse(self.path)
        qs = parse_qs(parsed.query)
        self.__class__.queries_received.append(qs.get("query", [""])[0])
        self.__class__.auth_headers_received.append(self.headers.get("Authorization"))
        payload = json.dumps({
            "query": qs.get("query", [""])[0],
            "columns": self.__class__.columns,
            "dataset": self.__class__.dataset,
            "timestamp": -1,
        }).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_POST(self):
        self.send_response(405)
        self.end_headers()

    def log_message(self, fmt, *args):
        pass


@pytest.fixture
def questdb_server():
    _QuestdbHandler.columns = [{"name": "day", "type": "STRING"}, {"name": "cnt", "type": "LONG"}]
    _QuestdbHandler.dataset = [["2024-01-01", 10], ["2024-01-02", 20]]
    _QuestdbHandler.queries_received = []
    _QuestdbHandler.auth_headers_received = []
    server = _TestServer(("127.0.0.1", 0), _QuestdbHandler)
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
    return mod.QuestdbSqlQueryComponent(**args)


def test_query_returns_a_real_dataframe_with_positional_columns(questdb_server):
    comp = _comp(questdb_server)
    defs = comp.build_defs(None)
    result = dg.materialize(list(defs.assets), resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
    assert _QuestdbHandler.queries_received[0] == "SELECT day, cnt FROM counts"

    meta = next(e for e in result.all_events if e.event_type_value == "ASSET_MATERIALIZATION").event_specific_data.materialization.metadata
    assert meta["dagster/row_count"].value == 2


def test_dataframe_columns_match_the_columns_field_not_dataset_order_assumption(questdb_server):
    _QuestdbHandler.columns = [{"name": "cnt", "type": "LONG"}, {"name": "day", "type": "STRING"}]
    _QuestdbHandler.dataset = [[10, "2024-01-01"], [20, "2024-01-02"]]
    comp = _comp(questdb_server)
    df = mod._run_questdb_sql(_url(questdb_server), "SELECT cnt, day FROM counts", None, {}, 30)
    assert list(df.columns) == ["cnt", "day"]
    assert df.iloc[0]["cnt"] == 10
    assert df.iloc[0]["day"] == "2024-01-01"


def test_uses_get_not_post(questdb_server):
    """QuestDB's REST API only supports GET for queries (POST returns 405 there)."""
    comp = _comp(questdb_server)
    result = dg.materialize(list(comp.build_defs(None).assets), resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success


def test_partition_tokens_substituted_into_the_query(questdb_server):
    comp = _comp(questdb_server, query="SELECT * FROM counts WHERE day = '{partition_key}'", partition_type="daily", partition_start="2024-01-01")
    defs = comp.build_defs(None)
    dg.materialize(list(defs.assets), partition_key="2024-01-02", resources={"io_manager": dg.InMemoryIOManager()})
    assert "2024-01-02" in _QuestdbHandler.queries_received[0]


def test_basic_auth_is_sent_when_username_set(questdb_server, monkeypatch):
    monkeypatch.setenv("QUESTDB_PW", "secret")
    comp = _comp(questdb_server, username="admin", password_env_var="QUESTDB_PW")
    defs = comp.build_defs(None)
    dg.materialize(list(defs.assets), resources={"io_manager": dg.InMemoryIOManager()})
    assert _QuestdbHandler.auth_headers_received[0] is not None
    assert _QuestdbHandler.auth_headers_received[0].startswith("Basic ")


def test_query_failure_raises_with_error_message(questdb_server):
    class _ErrorHandler(_QuestdbHandler):
        def do_GET(self):
            payload = json.dumps({"query": "bad sql", "error": "unexpected token: 'FORM'", "position": 16}).encode()
            self.send_response(400)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    questdb_server.RequestHandlerClass = _ErrorHandler
    comp = _comp(questdb_server)
    result = dg.materialize(list(comp.build_defs(None).assets), raise_on_error=False, resources={"io_manager": dg.InMemoryIOManager()})
    assert not result.success
    failure = next(e for e in result.all_events if e.event_type_value == "STEP_FAILURE")
    assert "unexpected token" in str(failure.event_specific_data.error)


def test_weekday_cron_partition_builds_a_real_time_window_partitions_def(questdb_server):
    comp = _comp(questdb_server, partition_type="cron", partition_start="2024-01-01", partition_cron_schedule="0 0 * * 1-5")
    pdef = list(comp.build_defs(None).assets)[0].partitions_def
    assert isinstance(pdef, dg.TimeWindowPartitionsDefinition)
