"""Real, committed tests for QuestdbResource / QuestdbResourceComponent.

Stands up a real local HTTP server implementing the exact QuestDB REST
API shape (GET /api/v1/sql/execute with the query as a URL parameter,
returning {"columns": [...], "dataset": [[...]], ...} with positional row
values -- same convention as assets/sources/questdb_sql_query's tests) and
points the resource at it -- real HTTP requests over the real `requests`
library.
"""
import http.server
import json
import threading
from urllib.parse import parse_qs, urlparse

import pytest

from .conftest import load_component_module

mod = load_component_module()


class _TestServer(http.server.ThreadingHTTPServer):
    request_queue_size = 64
    daemon_threads = True


class _QuestdbHandler(http.server.BaseHTTPRequestHandler):
    columns = [{"name": "day", "type": "STRING"}, {"name": "cnt", "type": "LONG"}]
    dataset = [["2024-01-01", 10], ["2024-01-02", 20]]
    status_code = 200
    queries_received = []
    auth_headers_received = []

    def do_GET(self):
        parsed = urlparse(self.path)
        qs = parse_qs(parsed.query)
        self.__class__.queries_received.append(qs.get("query", [""])[0])
        self.__class__.auth_headers_received.append(self.headers.get("Authorization"))
        if self.__class__.status_code >= 400:
            payload = json.dumps({"query": qs.get("query", [""])[0], "error": "unexpected token", "position": 0}).encode()
            self.send_response(self.__class__.status_code)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return
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

    def log_message(self, fmt, *args):
        pass


@pytest.fixture
def questdb_server():
    _QuestdbHandler.columns = [{"name": "day", "type": "STRING"}, {"name": "cnt", "type": "LONG"}]
    _QuestdbHandler.dataset = [["2024-01-01", 10], ["2024-01-02", 20]]
    _QuestdbHandler.status_code = 200
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


def _resource(server, **kw):
    kw.setdefault("host_url", _url(server))
    return mod.QuestdbResource(**kw)


# ----------------------------------------------------------------- QuestdbResource

def test_query_url_is_the_host_url(questdb_server):
    resource = _resource(questdb_server)
    assert resource.query_url == _url(questdb_server)


def test_execute_query_returns_a_real_dataframe_with_positional_columns(questdb_server):
    resource = _resource(questdb_server)
    df = resource.execute_query("SELECT day, cnt FROM counts")
    assert list(df.columns) == ["day", "cnt"]
    assert len(df) == 2
    assert _QuestdbHandler.queries_received[0] == "SELECT day, cnt FROM counts"


def test_dataframe_columns_match_the_columns_field_not_dataset_order_assumption(questdb_server):
    _QuestdbHandler.columns = [{"name": "cnt", "type": "LONG"}, {"name": "day", "type": "STRING"}]
    _QuestdbHandler.dataset = [[10, "2024-01-01"], [20, "2024-01-02"]]
    resource = _resource(questdb_server)
    df = resource.execute_query("SELECT cnt, day FROM counts")
    assert list(df.columns) == ["cnt", "day"]
    assert df.iloc[0]["cnt"] == 10
    assert df.iloc[0]["day"] == "2024-01-01"


def test_basic_auth_sent_when_username_set(questdb_server):
    resource = _resource(questdb_server, username="admin", password="secret")
    resource.execute_query("SELECT 1")
    assert _QuestdbHandler.auth_headers_received[0] is not None
    assert _QuestdbHandler.auth_headers_received[0].startswith("Basic ")


def test_no_auth_header_when_username_unset(questdb_server):
    resource = _resource(questdb_server)
    resource.execute_query("SELECT 1")
    assert _QuestdbHandler.auth_headers_received[0] is None


def test_query_failure_raises_with_error_message(questdb_server):
    _QuestdbHandler.status_code = 400
    resource = _resource(questdb_server)
    with pytest.raises(RuntimeError, match="unexpected token"):
        resource.execute_query("bad sql")


# ----------------------------------------------------------- QuestdbResourceComponent

def test_component_registers_resource_under_resource_key():
    component = mod.QuestdbResourceComponent(resource_key="questdb_custom", host_url="http://questdb:9000")
    defs = component.build_defs(context=None)
    assert "questdb_custom" in defs.resources
    registered = defs.resources["questdb_custom"]
    assert isinstance(registered, mod.QuestdbResource)
    assert registered.host_url == "http://questdb:9000"


def test_component_defaults_resource_key_to_questdb_resource():
    component = mod.QuestdbResourceComponent(host_url="http://questdb:9000")
    defs = component.build_defs(context=None)
    assert "questdb_resource" in defs.resources


def test_component_resolves_password_env_var(monkeypatch):
    monkeypatch.setenv("QUESTDB_PW", "from-env")
    component = mod.QuestdbResourceComponent(host_url="http://questdb:9000", username="admin", password_env_var="QUESTDB_PW")
    defs = component.build_defs(context=None)
    resource = defs.resources["questdb_resource"]
    assert resource.password.get_value() == "from-env"
