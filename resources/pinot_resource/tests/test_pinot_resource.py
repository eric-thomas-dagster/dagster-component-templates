"""Real, committed tests for PinotResource / PinotResourceComponent.

Stands up a real local HTTP server implementing the exact Pinot Broker
Query API shape (POST /query/sql returns {"resultTable": {"dataSchema":
{"columnNames": [...]}, "rows": [[...], ...]}, "exceptions": [...]} --
same convention as assets/sources/pinot_sql_query's tests) and points the
resource at it -- real HTTP requests over the real `requests` library.
"""
import http.server
import json
import threading

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


def _resource(server, **kw):
    kw.setdefault("broker_url", _url(server))
    return mod.PinotResource(**kw)


# ----------------------------------------------------------------- PinotResource

def test_query_url_is_the_broker_url(pinot_server):
    resource = _resource(pinot_server)
    assert resource.query_url == _url(pinot_server)


def test_execute_query_returns_a_real_dataframe(pinot_server):
    resource = _resource(pinot_server)
    df = resource.execute_query("SELECT day, cnt FROM counts")
    assert list(df.columns) == ["day", "cnt"]
    assert len(df) == 2
    assert _PinotHandler.requests_received[0]["sql"] == "SELECT day, cnt FROM counts"


def test_basic_auth_sent_when_username_set(pinot_server):
    resource = _resource(pinot_server, username="admin", password="secret")
    resource.execute_query("SELECT 1")
    assert _PinotHandler.auth_headers_received[0] is not None
    assert _PinotHandler.auth_headers_received[0].startswith("Basic ")


def test_no_auth_header_when_username_unset(pinot_server):
    resource = _resource(pinot_server)
    resource.execute_query("SELECT 1")
    assert _PinotHandler.auth_headers_received[0] is None


def test_inline_exceptions_raise_even_with_http_200(pinot_server):
    """The real Pinot gotcha: a query error can come back inside the JSON body
    alongside HTTP 200, not just via a 4xx/5xx status."""
    _PinotHandler.exceptions = [{"errorCode": 150, "message": "QueryExecutionError"}]
    resource = _resource(pinot_server)
    with pytest.raises(RuntimeError, match="QueryExecutionError"):
        resource.execute_query("SELECT * FROM counts")


def test_http_error_status_also_raises(pinot_server):
    _PinotHandler.status_code = 400
    resource = _resource(pinot_server)
    with pytest.raises(RuntimeError, match="HTTP 400"):
        resource.execute_query("SELECT * FROM counts")


# ----------------------------------------------------------- PinotResourceComponent

def test_component_registers_resource_under_resource_key():
    component = mod.PinotResourceComponent(resource_key="pinot_custom", broker_url="http://pinot-broker:8099")
    defs = component.build_defs(context=None)
    assert "pinot_custom" in defs.resources
    registered = defs.resources["pinot_custom"]
    assert isinstance(registered, mod.PinotResource)
    assert registered.broker_url == "http://pinot-broker:8099"


def test_component_defaults_resource_key_to_pinot_resource():
    component = mod.PinotResourceComponent(broker_url="http://pinot-broker:8099")
    defs = component.build_defs(context=None)
    assert "pinot_resource" in defs.resources


def test_component_resolves_password_env_var(monkeypatch):
    monkeypatch.setenv("PINOT_PW", "from-env")
    component = mod.PinotResourceComponent(broker_url="http://pinot-broker:8099", username="admin", password_env_var="PINOT_PW")
    defs = component.build_defs(context=None)
    resource = defs.resources["pinot_resource"]
    assert resource.password.get_value() == "from-env"
