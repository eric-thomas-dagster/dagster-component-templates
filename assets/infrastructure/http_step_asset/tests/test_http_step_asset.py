"""Real, committed tests for HttpStepAssetComponent.

Mocks only the actual network boundary (`requests.Session.request`, returning
a fake response object with the real `requests.Response` surface this
component touches: status_code/text/headers/json()) -- everything else
(status/value success checks, auth header assembly, Dagster asset wiring) is
real.
"""
import dagster as dg
import pytest

from .conftest import load_component_module

mod = load_component_module()


class _FakeResponse:
    def __init__(self, status_code=200, json_data=None, text=None, headers=None):
        self.status_code = status_code
        self._json_data = json_data
        self.text = text if text is not None else ("" if json_data is None else "")
        self.headers = headers or ({"Content-Type": "application/json"} if json_data is not None else {})

    def json(self):
        if self._json_data is None:
            raise ValueError("no JSON")
        return self._json_data


class FakeSession:
    def __init__(self, response=None):
        self.calls = []
        self.headers = {}
        self.auth = None
        self._response = response or _FakeResponse()

    def request(self, method, url, **kwargs):
        self.calls.append({"method": method, "url": url, **kwargs})
        return self._response


def _patch_requests(monkeypatch, response=None):
    fake_session = FakeSession(response=response)
    fake_requests = type("FakeRequestsModule", (), {"Session": lambda: fake_session})
    monkeypatch.setitem(__import__("sys").modules, "requests", fake_requests)
    return fake_session


def _comp(**kw):
    args = dict(asset_name="out", url="https://api.example.com/x")
    args.update(kw)
    return mod.HttpStepAssetComponent(**args)


def test_successful_get_materializes_with_metadata(monkeypatch):
    session = _patch_requests(monkeypatch, _FakeResponse(status_code=200, json_data={"ok": True}))
    comp = _comp()
    defs = comp.build_defs(None)
    result = dg.materialize(list(defs.assets))
    assert result.success

    mats = [e for e in result.all_events if e.event_type_value == "ASSET_MATERIALIZATION"]
    meta = mats[0].event_specific_data.materialization.metadata
    assert meta["status_code"].value == 200
    assert meta["response_json"].data == {"ok": True}

    assert session.calls[0]["method"] == "GET"
    assert session.calls[0]["url"] == "https://api.example.com/x"


def test_non_2xx_status_raises_by_default(monkeypatch):
    _patch_requests(monkeypatch, _FakeResponse(status_code=500, text="server error"))
    comp = _comp()
    defs = comp.build_defs(None)
    result = dg.materialize(list(defs.assets), raise_on_error=False)
    assert not result.success
    failure = next(e for e in result.all_events if e.event_type_value == "STEP_FAILURE")
    assert "server error" in str(failure.event_specific_data.error)
    assert "500" in str(failure.event_specific_data.error)


def test_success_status_codes_restricts_the_check(monkeypatch):
    _patch_requests(monkeypatch, _FakeResponse(status_code=201))
    comp = _comp(success_status_codes=[200])
    defs = comp.build_defs(None)
    result = dg.materialize(list(defs.assets), raise_on_error=False)
    assert not result.success


def test_response_json_path_success_value_match(monkeypatch):
    _patch_requests(monkeypatch, _FakeResponse(status_code=200, json_data={"status": "ok"}))
    comp = _comp(response_json_path="status", response_json_success_value="ok")
    defs = comp.build_defs(None)
    result = dg.materialize(list(defs.assets))
    assert result.success
    mats = [e for e in result.all_events if e.event_type_value == "ASSET_MATERIALIZATION"]
    meta = mats[0].event_specific_data.materialization.metadata
    assert meta["response_value"].text == "ok"


def test_response_json_path_success_value_mismatch_fails(monkeypatch):
    _patch_requests(monkeypatch, _FakeResponse(status_code=200, json_data={"status": "pending"}))
    comp = _comp(response_json_path="status", response_json_success_value="ok")
    defs = comp.build_defs(None)
    result = dg.materialize(list(defs.assets), raise_on_error=False)
    assert not result.success
    failure = next(e for e in result.all_events if e.event_type_value == "STEP_FAILURE")
    assert "pending" in str(failure.event_specific_data.error)


def test_json_body_sent_as_json(monkeypatch):
    session = _patch_requests(monkeypatch)
    comp = _comp(method="POST", json_body={"a": 1})
    defs = comp.build_defs(None)
    dg.materialize(list(defs.assets))
    assert session.calls[0]["json"] == {"a": 1}


def test_body_sent_as_data(monkeypatch):
    session = _patch_requests(monkeypatch)
    comp = _comp(method="POST", body="raw=1")
    defs = comp.build_defs(None)
    dg.materialize(list(defs.assets))
    assert session.calls[0]["data"] == "raw=1"


def test_body_and_json_body_together_is_rejected():
    comp = _comp(body="a", json_body={"a": 1})
    with pytest.raises(ValueError, match="json_body"):
        comp.build_defs(None)


def test_bearer_auth_sets_authorization_header(monkeypatch):
    session = _patch_requests(monkeypatch)
    monkeypatch.setenv("MY_TOKEN", "secret123")
    comp = _comp(auth_type="bearer", auth_token_env_var="MY_TOKEN")
    defs = comp.build_defs(None)
    dg.materialize(list(defs.assets))
    assert session.headers["Authorization"] == "Bearer secret123"


def test_basic_auth_requires_username_and_password_env_vars():
    comp = _comp(auth_type="basic")
    with pytest.raises(ValueError, match="auth_username_env_var"):
        comp.build_defs(None)


def test_basic_auth_sets_session_auth_tuple(monkeypatch):
    session = _patch_requests(monkeypatch)
    monkeypatch.setenv("U", "user")
    monkeypatch.setenv("P", "pass")
    comp = _comp(auth_type="basic", auth_username_env_var="U", auth_password_env_var="P")
    defs = comp.build_defs(None)
    dg.materialize(list(defs.assets))
    assert session.auth == ("user", "pass")


def test_bearer_auth_without_token_env_var_is_rejected():
    comp = _comp(auth_type="bearer")
    with pytest.raises(ValueError, match="auth_token_env_var"):
        comp.build_defs(None)


def test_non_json_response_falls_back_to_body_metadata(monkeypatch):
    _patch_requests(monkeypatch, _FakeResponse(status_code=200, text="plain text ok", headers={"Content-Type": "text/plain"}))
    comp = _comp()
    defs = comp.build_defs(None)
    result = dg.materialize(list(defs.assets))
    assert result.success
    mats = [e for e in result.all_events if e.event_type_value == "ASSET_MATERIALIZATION"]
    meta = mats[0].event_specific_data.materialization.metadata
    assert meta["response_body"].text == "plain text ok"
    assert "response_json" not in meta
