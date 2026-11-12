"""Real, committed tests for DruidResource / DruidResourceComponent.

Stands up a real local HTTP server implementing the exact Druid SQL API
shape (POST /druid/v2/sql with resultFormat='object' returns a JSON array
of row objects -- same convention as assets/sources/druid_sql_query's
tests) and points the resource at it -- real HTTP requests over the real
`requests` library.
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


class _DruidHandler(http.server.BaseHTTPRequestHandler):
    rows = [{"day": "2024-01-01", "cnt": 10}, {"day": "2024-01-02", "cnt": 20}]
    status_code = 200
    requests_received = []
    auth_headers_received = []

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length)) if length else {}
        self.__class__.requests_received.append(body)
        self.__class__.auth_headers_received.append(self.headers.get("Authorization"))
        if self.__class__.status_code >= 400:
            payload = json.dumps({"error": "Unknown column"}).encode()
            self.send_response(self.__class__.status_code)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
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
    _DruidHandler.status_code = 200
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


def _resource(server, **kw):
    kw.setdefault("broker_url", _url(server))
    return mod.DruidResource(**kw)


# ----------------------------------------------------------------- DruidResource

def test_query_url_is_the_broker_url(druid_server):
    resource = _resource(druid_server)
    assert resource.query_url == _url(druid_server)


def test_execute_query_returns_the_row_objects(druid_server):
    resource = _resource(druid_server)
    rows = resource.execute_query("SELECT day, cnt FROM counts")
    assert rows == [{"day": "2024-01-01", "cnt": 10}, {"day": "2024-01-02", "cnt": 20}]
    assert _DruidHandler.requests_received[0]["query"] == "SELECT day, cnt FROM counts"
    assert _DruidHandler.requests_received[0]["resultFormat"] == "object"


def test_basic_auth_sent_when_username_set(druid_server):
    resource = _resource(druid_server, username="admin", password="secret")
    resource.execute_query("SELECT 1")
    assert _DruidHandler.auth_headers_received[0] is not None
    assert _DruidHandler.auth_headers_received[0].startswith("Basic ")


def test_no_auth_header_when_username_unset(druid_server):
    resource = _resource(druid_server)
    resource.execute_query("SELECT 1")
    assert _DruidHandler.auth_headers_received[0] is None


def test_query_failure_raises_with_response_body(druid_server):
    _DruidHandler.status_code = 400
    resource = _resource(druid_server)
    with pytest.raises(RuntimeError, match="Unknown column"):
        resource.execute_query("SELECT bogus FROM counts")


# ----------------------------------------------------------- DruidResourceComponent

def test_component_registers_resource_under_resource_key():
    component = mod.DruidResourceComponent(resource_key="druid_custom", broker_url="http://druid-broker:8082")
    defs = component.build_defs(context=None)
    assert "druid_custom" in defs.resources
    registered = defs.resources["druid_custom"]
    assert isinstance(registered, mod.DruidResource)
    assert registered.broker_url == "http://druid-broker:8082"


def test_component_defaults_resource_key_to_druid_resource():
    component = mod.DruidResourceComponent(broker_url="http://druid-broker:8082")
    defs = component.build_defs(context=None)
    assert "druid_resource" in defs.resources


def test_component_resolves_password_env_var(monkeypatch):
    monkeypatch.setenv("DRUID_PW", "from-env")
    component = mod.DruidResourceComponent(broker_url="http://druid-broker:8082", username="admin", password_env_var="DRUID_PW")
    defs = component.build_defs(context=None)
    resource = defs.resources["druid_resource"]
    assert resource.password.get_value() == "from-env"
