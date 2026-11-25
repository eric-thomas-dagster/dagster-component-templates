"""Committed regression tests for PerRowHttpFetcherComponent.

`requests.Session.request` is the one external boundary mocked here (never the
component's own fetch/retry/auth/templating logic) -- the component runs for real
under dg.materialize().
"""
import dagster as dg
import pytest

from .conftest import FakeResponse, load_component_module, source_asset


@pytest.fixture()
def mod():
    return load_component_module()


def _comp(mod, **kw):
    kw.setdefault("asset_name", "fetched")
    kw.setdefault("upstream_asset_key", "rows_src")
    kw.setdefault("url_column", "url")
    return mod.PerRowHttpFetcherComponent(**kw)


def _materialize(mod, component, rows=None, resources=None):
    fetch_asset = component.build_defs(None).assets[0]
    result = dg.materialize([source_asset(rows=rows), fetch_asset], resources=resources or {})
    return result, fetch_asset


def test_get_fetches_one_request_per_row(mod, monkeypatch):
    calls = []

    def fake_request(self, method, url, **kwargs):
        calls.append((method, url))
        return FakeResponse(status_code=200, text="ok")

    monkeypatch.setattr("requests.Session.request", fake_request)
    component = _comp(mod)
    result, _ = _materialize(mod, component)
    assert result.success
    assert len(calls) == 2
    assert calls[0] == ("GET", "https://api.example.com/things/1")
    out = result.output_for_node("fetched")
    assert list(out["response_status"]) == [200, 200]


def test_body_template_renders_per_row_and_sets_json_content_type(mod, monkeypatch):
    sent = []

    def fake_request(self, method, url, **kwargs):
        sent.append((method, kwargs.get("data"), kwargs.get("headers")))
        return FakeResponse(status_code=201)

    monkeypatch.setattr("requests.Session.request", fake_request)
    component = _comp(mod, method="POST", body_template='{{"thing_id": {id}}}')
    result, _ = _materialize(mod, component)
    assert result.success
    assert sent[0][0] == "POST"
    assert sent[0][1] == b'{"thing_id": 1}'
    assert sent[0][2] == {"Content-Type": "application/json"}
    assert sent[1][1] == b'{"thing_id": 2}'


def test_body_template_ignored_for_get(mod, monkeypatch):
    sent = []

    def fake_request(self, method, url, **kwargs):
        sent.append(kwargs)
        return FakeResponse(status_code=200)

    monkeypatch.setattr("requests.Session.request", fake_request)
    component = _comp(mod, method="GET", body_template='{{"thing_id": {id}}}')
    result, _ = _materialize(mod, component)
    assert result.success
    assert "data" not in sent[0]


def test_retry_on_status_codes_retries_429_then_succeeds(mod, monkeypatch):
    attempts_by_url = {}

    def fake_request(self, method, url, **kwargs):
        n = attempts_by_url.get(url, 0)
        attempts_by_url[url] = n + 1
        if n == 0:
            return FakeResponse(status_code=429)
        return FakeResponse(status_code=200)

    monkeypatch.setattr("requests.Session.request", fake_request)
    monkeypatch.setattr("time.sleep", lambda s: None)
    component = _comp(mod, retries=1, retry_on_status_codes=[429])
    result, _ = _materialize(mod, component)
    assert result.success
    out = result.output_for_node("fetched")
    assert list(out["response_status"]) == [200, 200]
    assert all(n == 2 for n in attempts_by_url.values())


def test_retry_not_triggered_for_status_outside_configured_list(mod, monkeypatch):
    calls = {"n": 0}

    def fake_request(self, method, url, **kwargs):
        calls["n"] += 1
        return FakeResponse(status_code=404)

    monkeypatch.setattr("requests.Session.request", fake_request)
    component = _comp(mod, retries=3, retry_on_status_codes=[429, 500])
    result, _ = _materialize(mod, component)
    assert result.success
    # 2 rows, no retries triggered since 404 isn't in retry_on_status_codes
    assert calls["n"] == 2


def test_response_json_path_extracts_nested_value(mod, monkeypatch):
    def fake_request(self, method, url, **kwargs):
        return FakeResponse(status_code=200, json_data={"data": {"id": 42, "name": "x"}})

    monkeypatch.setattr("requests.Session.request", fake_request)
    component = _comp(mod, response_json_path="data.id")
    result, _ = _materialize(mod, component)
    assert result.success
    out = result.output_for_node("fetched")
    assert list(out["response_value"]) == [42, 42]


def test_auth_type_bearer_sets_authorization_header(mod, monkeypatch):
    seen_headers = []

    class FakeSession:
        def __init__(self):
            self.headers = {}
            self.auth = None

        def request(self, method, url, **kwargs):
            seen_headers.append(dict(self.headers))
            return FakeResponse(status_code=200)

    monkeypatch.setattr("requests.Session", FakeSession)
    monkeypatch.setenv("FETCHER_TOKEN", "s3cr3t")
    component = _comp(mod, auth_type="bearer", auth_token_env_var="FETCHER_TOKEN")
    result, _ = _materialize(mod, component)
    assert result.success
    assert seen_headers[0]["Authorization"] == "Bearer s3cr3t"


def test_auth_type_basic_requires_both_env_vars(mod, monkeypatch):
    component = _comp(mod, auth_type="basic", auth_username_env_var="U")
    with pytest.raises(ValueError, match="auth_type='basic' requires"):
        _materialize(mod, component)


def test_missing_url_column_emits_empty_columns_without_requests(mod, monkeypatch):
    calls = {"n": 0}

    def fake_request(self, method, url, **kwargs):
        calls["n"] += 1
        return FakeResponse(status_code=200)

    monkeypatch.setattr("requests.Session.request", fake_request)
    component = _comp(mod, url_column="missing_col", response_json_path="data.id")
    result, _ = _materialize(mod, component)
    assert result.success
    assert calls["n"] == 0
    out = result.output_for_node("fetched")
    assert list(out["response_value"]) == [None, None]
