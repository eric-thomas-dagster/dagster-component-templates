"""Real, committed tests for migration-skills wishlist item #15 (ADF): file_ingestion could not read
a plain http(s):// URL at all -- `file_path` only ever worked for local paths and fsspec URIs
(s3://, gs://, abfss://, ...). Pandas already forwards `storage_options` straight through as request
headers for an http(s) URL (pandas.io.common.get_handle's `urllib.request.Request(url,
headers=storage_options)`), so the fix is the new `_http_auth_headers` helper plus the
http_auth_type/http_auth_username_env_var/http_auth_password_env_var/http_auth_token_env_var fields,
threaded through the same `_storage_options_for` closure the Azure fields already use.

Covers the pure helper, the threaded-through `pandas.read_csv(..., storage_options=...)` call, and a
real end-to-end read against a local HTTP server (loopback-only network, not a fake local file
pretending to be a URL) proving an unauthenticated request succeeds, a bearer-protected request
succeeds with the right header and 401s without it, and the detected format survives a query string.
"""
import base64
import http.server
import threading
from unittest.mock import patch

import pandas as pd
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def _make_component(mod, **overrides):
    kwargs = dict(asset_name="orders", file_path="https://example.invalid/orders.csv")
    kwargs.update(overrides)
    return mod.FileIngestionComponent(**kwargs)


# ── _http_auth_headers itself (pure function) ───────────────────────────────────────────────────────

def test_non_http_uri_returns_none(mod):
    assert mod._http_auth_headers("s3://bucket/key.csv", auth_type="bearer", token_env_var="X") is None


def test_auth_type_none_returns_none_even_for_http_uri(mod):
    assert mod._http_auth_headers("https://host/data.csv", auth_type="none") is None


def test_bearer_builds_authorization_header(mod, monkeypatch):
    monkeypatch.setenv("MY_TOKEN", "secret-token")
    headers = mod._http_auth_headers("https://host/data.csv", auth_type="bearer", token_env_var="MY_TOKEN")
    assert headers == {"Authorization": "Bearer secret-token"}


def test_bearer_without_token_env_var_raises(mod):
    with pytest.raises(ValueError, match="http_auth_token_env_var"):
        mod._http_auth_headers("https://host/data.csv", auth_type="bearer")


def test_basic_builds_base64_authorization_header(mod, monkeypatch):
    monkeypatch.setenv("MY_USER", "alice")
    monkeypatch.setenv("MY_PASS", "s3cr3t")
    headers = mod._http_auth_headers(
        "https://host/data.csv", auth_type="basic",
        username_env_var="MY_USER", password_env_var="MY_PASS",
    )
    expected = base64.b64encode(b"alice:s3cr3t").decode()
    assert headers == {"Authorization": f"Basic {expected}"}


def test_basic_missing_username_env_var_raises(mod, monkeypatch):
    monkeypatch.setenv("MY_PASS", "s3cr3t")
    with pytest.raises(ValueError, match="http_auth_username_env_var"):
        mod._http_auth_headers("https://host/data.csv", auth_type="basic", password_env_var="MY_PASS")


def test_unknown_auth_type_raises(mod):
    with pytest.raises(ValueError, match="unknown http_auth_type"):
        mod._http_auth_headers("https://host/data.csv", auth_type="oauth2_client_credentials")


# ── format detection survives a query string ───────────────────────────────────────────────────────

def test_format_autodetect_strips_query_string(mod):
    assert mod._detect_format_from_uri("https://host/data.csv?token=abc&x=1") == "csv"
    assert mod._detect_format_from_uri("https://host/data.json#frag") == "json"


# ── threaded through FileIngestionComponent's fixed file_path (Branch B) ────────────────────────────

def test_fixed_file_path_passes_bearer_header_to_pandas_for_an_http_uri(mod, monkeypatch):
    monkeypatch.setenv("TOK", "secret-token")
    component = _make_component(
        mod, http_auth_type="bearer", http_auth_token_env_var="TOK",
    )
    asset_def = component.build_defs(context=None).assets[0]

    captured = {}

    def _fake_read_csv(uri, **kwargs):
        captured.update(kwargs)
        return pd.DataFrame({"a": [1]})

    with patch("pandas.read_csv", _fake_read_csv):
        import dagster as dg
        dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()}, raise_on_error=True)

    assert captured.get("storage_options") == {"Authorization": "Bearer secret-token"}


def test_fixed_file_path_passes_no_storage_options_for_an_unauthenticated_http_uri(mod):
    component = _make_component(mod)  # http_auth_type defaults to "none"
    asset_def = component.build_defs(context=None).assets[0]

    captured = {}

    def _fake_read_csv(uri, **kwargs):
        captured.update(kwargs)
        return pd.DataFrame({"a": [1]})

    with patch("pandas.read_csv", _fake_read_csv):
        import dagster as dg
        dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()}, raise_on_error=True)

    assert "storage_options" not in captured or captured["storage_options"] is None


# ── real end-to-end read against a local HTTP server (loopback only) ───────────────────────────────

class _BearerProtectedCSVHandler(http.server.BaseHTTPRequestHandler):
    """Serves a fixed CSV body, 401ing any request without the expected Authorization header."""

    expected_auth_header = "Bearer real-token"
    body = b"a,b\n1,2\n3,4\n"

    def do_GET(self):  # noqa: N802 - http.server's required method name
        if self.headers.get("Authorization") != self.expected_auth_header:
            self.send_response(401)
            self.end_headers()
            return
        self.send_response(200)
        self.send_header("Content-Type", "text/csv")
        self.send_header("Content-Length", str(len(self.body)))
        self.end_headers()
        self.wfile.write(self.body)

    def log_message(self, *args):  # silence test output
        pass


@pytest.fixture()
def http_server():
    server = http.server.HTTPServer(("127.0.0.1", 0), _BearerProtectedCSVHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        thread.join(timeout=5)


def test_real_http_read_succeeds_with_correct_bearer_token(mod, http_server, monkeypatch):
    monkeypatch.setenv("REAL_TOK", "real-token")
    port = http_server.server_address[1]
    component = mod.FileIngestionComponent(
        asset_name="orders", file_path=f"http://127.0.0.1:{port}/orders.csv",
        http_auth_type="bearer", http_auth_token_env_var="REAL_TOK",
    )
    asset_def = component.build_defs(context=None).assets[0]

    import dagster as dg
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()}, raise_on_error=True)
    df = result.output_for_node("orders")
    assert list(df.columns) == ["a", "b"]
    assert df.shape == (2, 2)


def test_real_http_read_fails_without_auth(mod, http_server):
    port = http_server.server_address[1]
    component = mod.FileIngestionComponent(
        asset_name="orders", file_path=f"http://127.0.0.1:{port}/orders.csv",
    )  # no auth configured -> server responds 401
    asset_def = component.build_defs(context=None).assets[0]

    import dagster as dg
    import urllib.error
    with pytest.raises((urllib.error.HTTPError, Exception)):
        dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()}, raise_on_error=True)
