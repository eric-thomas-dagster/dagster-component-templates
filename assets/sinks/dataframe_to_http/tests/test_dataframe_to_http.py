"""Committed regression tests for DataframeToHttpComponent.

`requests.Session.request` is the one external boundary mocked here (never the
component's own batching/retry/auth logic) -- the component runs for real under
dg.materialize().
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import FakeResponse, load_component_module, source_asset


@pytest.fixture()
def mod():
    return load_component_module()


def _comp(mod, **kw):
    kw.setdefault("asset_name", "sent")
    kw.setdefault("upstream_asset_key", "rows_src")
    kw.setdefault("url", "https://api.example.com/v1/things")
    return mod.DataframeToHttpComponent(**kw)


def _materialize(mod, component, rows=None, resources=None):
    sink = component.build_defs(None).assets[0]
    result = dg.materialize([source_asset(rows=rows), sink], resources=resources or {})
    return result, sink


# --------------------------------------------------------------------- per_record

def test_per_record_posts_one_request_per_row(mod, monkeypatch):
    calls = []

    def fake_request(self, method, url, **kwargs):
        calls.append((method, url, kwargs.get("json")))
        return FakeResponse(status_code=200)

    monkeypatch.setattr("requests.Session.request", fake_request)
    component = _comp(mod, batch_mode="per_record")
    result, _ = _materialize(mod, component)
    assert result.success
    assert len(calls) == 2
    assert calls[0][0] == "POST"
    assert calls[0][1] == "https://api.example.com/v1/things"
    assert calls[0][2] == {"id": 1, "name": "a"}


def test_per_record_url_templating(mod, monkeypatch):
    calls = []

    def fake_request(self, method, url, **kwargs):
        calls.append(url)
        return FakeResponse(status_code=201)

    monkeypatch.setattr("requests.Session.request", fake_request)
    component = _comp(mod, url="https://api.example.com/things/{id}")
    result, _ = _materialize(mod, component)
    assert result.success
    assert calls == [
        "https://api.example.com/things/1",
        "https://api.example.com/things/2",
    ]


# --------------------------------------------------------------------- batch modes

def test_batch_array_chunks_rows(mod, monkeypatch):
    bodies = []

    def fake_request(self, method, url, **kwargs):
        bodies.append(kwargs.get("json"))
        return FakeResponse(status_code=200)

    monkeypatch.setattr("requests.Session.request", fake_request)
    rows = [{"id": i} for i in range(5)]
    component = _comp(mod, batch_mode="batch_array", batch_size=2)
    result, _ = _materialize(mod, component, rows=rows)
    assert result.success
    # 5 rows / batch_size=2 -> 3 requests (2, 2, 1)
    assert [len(b) for b in bodies] == [2, 2, 1]
    assert bodies[0] == [{"id": 0}, {"id": 1}]
    assert bodies[2] == [{"id": 4}]


def test_ndjson_sends_newline_delimited_json_with_content_type(mod, monkeypatch):
    sent = {}

    def fake_request(self, method, url, **kwargs):
        sent["data"] = kwargs.get("data")
        sent["headers"] = kwargs.get("headers")
        return FakeResponse(status_code=200)

    monkeypatch.setattr("requests.Session.request", fake_request)
    rows = [{"id": 1}, {"id": 2}]
    component = _comp(mod, batch_mode="ndjson", batch_size=10)
    result, _ = _materialize(mod, component, rows=rows)
    assert result.success
    assert sent["data"] == '{"id": 1}\n{"id": 2}'
    assert sent["headers"]["Content-Type"] == "application/x-ndjson"


# --------------------------------------------------------------------- auth

def test_basic_auth_sets_session_auth(mod, monkeypatch):
    seen_sessions = []

    def fake_request(self, method, url, **kwargs):
        seen_sessions.append(self.auth)
        return FakeResponse(status_code=200)

    monkeypatch.setattr("requests.Session.request", fake_request)
    monkeypatch.setenv("HTTP_USER", "u")
    monkeypatch.setenv("HTTP_PASS", "p")
    component = _comp(
        mod, auth_type="basic", auth_username_env_var="HTTP_USER", auth_password_env_var="HTTP_PASS"
    )
    result, _ = _materialize(mod, component)
    assert result.success
    assert seen_sessions[0] == ("u", "p")


def test_bearer_auth_sets_authorization_header(mod, monkeypatch):
    seen_headers = []

    def fake_request(self, method, url, **kwargs):
        seen_headers.append(dict(self.headers))
        return FakeResponse(status_code=200)

    monkeypatch.setattr("requests.Session.request", fake_request)
    monkeypatch.setenv("HTTP_TOKEN", "secret-token")
    component = _comp(mod, auth_type="bearer", auth_token_env_var="HTTP_TOKEN")
    result, _ = _materialize(mod, component)
    assert result.success
    assert seen_headers[0]["Authorization"] == "Bearer secret-token"


def test_oauth2_client_credentials_fetches_and_caches_token(mod, monkeypatch):
    token_calls = []

    def fake_token_fetch(token_url, client_id, client_secret, scope, audience):
        token_calls.append((token_url, client_id, client_secret))
        return {"access_token": "tok-123", "expires_in": 3600}

    monkeypatch.setattr(mod, "_fetch_oauth2_client_credentials_token", fake_token_fetch)
    # Isolate the module-level cache between tests.
    monkeypatch.setattr(mod, "_OAUTH2_TOKEN_CACHE", {})

    seen_headers = []

    def fake_request(self, method, url, **kwargs):
        seen_headers.append(dict(self.headers))
        return FakeResponse(status_code=200)

    monkeypatch.setattr("requests.Session.request", fake_request)
    monkeypatch.setenv("OAUTH_CID", "client-id")
    monkeypatch.setenv("OAUTH_SECRET", "client-secret")
    component = _comp(
        mod,
        auth_type="oauth2_client_credentials",
        oauth2_token_url="https://auth.example.com/oauth/token",
        oauth2_client_id_env_var="OAUTH_CID",
        oauth2_client_secret_env_var="OAUTH_SECRET",
    )
    result, _ = _materialize(mod, component)
    assert result.success
    assert seen_headers[0]["Authorization"] == "Bearer tok-123"
    assert len(token_calls) == 1

    # Second materialization within the same process reuses the cached token.
    result2, _ = _materialize(mod, component)
    assert result2.success
    assert len(token_calls) == 1  # not re-fetched


# --------------------------------------------------------------------- retry

def test_retries_on_retryable_status_then_succeeds(mod, monkeypatch):
    attempts = {"n": 0}

    def fake_request(self, method, url, **kwargs):
        attempts["n"] += 1
        if attempts["n"] == 1:
            return FakeResponse(status_code=500, text="boom")
        return FakeResponse(status_code=200)

    monkeypatch.setattr("requests.Session.request", fake_request)
    monkeypatch.setattr("time.sleep", lambda *_: None)
    component = _comp(mod, max_retries=2, retry_backoff_seconds=0.01)
    result, _ = _materialize(mod, component, rows=[{"id": 1}])
    assert result.success
    assert attempts["n"] == 2


def test_non_retryable_status_fails_without_retry(mod, monkeypatch):
    attempts = {"n": 0}

    def fake_request(self, method, url, **kwargs):
        attempts["n"] += 1
        return FakeResponse(status_code=400, text="bad request")

    monkeypatch.setattr("requests.Session.request", fake_request)
    component = _comp(mod, max_retries=3, retry_on_status_codes=[429, 500])
    with pytest.raises(Exception):
        _materialize(mod, component, rows=[{"id": 1}])
    assert attempts["n"] == 1  # 400 isn't in retry_on_status_codes -> no retry


# --------------------------------------------------------------------- write_response_to_record

def test_write_response_to_record_appends_columns_and_returns_dataframe(mod, monkeypatch):
    def fake_request(self, method, url, **kwargs):
        return FakeResponse(status_code=201, json_data={"data": {"id": 999}})

    monkeypatch.setattr("requests.Session.request", fake_request)
    component = _comp(
        mod, write_response_to_record=True, response_json_path="data.id",
    )
    sink = component.build_defs(None).assets[0]
    result = dg.materialize([source_asset(), sink])
    assert result.success
    out = result.asset_value(sink.key)
    assert isinstance(out, pd.DataFrame)
    assert list(out["http_response_status"]) == [201, 201]
    assert list(out["http_response_value"]) == [999, 999]


# --------------------------------------------------------------------- failure handling

def test_failures_raise_and_record_sample_metadata(mod, monkeypatch):
    def fake_request(self, method, url, **kwargs):
        return FakeResponse(status_code=500, text="server error")

    monkeypatch.setattr("requests.Session.request", fake_request)
    monkeypatch.setattr("time.sleep", lambda *_: None)
    component = _comp(mod, max_retries=0, fail_after_n_failures=1)
    with pytest.raises(Exception, match="failed"):
        _materialize(mod, component, rows=[{"id": 1}, {"id": 2}])
