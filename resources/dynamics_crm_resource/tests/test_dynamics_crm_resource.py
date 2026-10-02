"""Committed regression tests for DynamicsCrmResource.

Monkeypatches `requests.post` (token endpoint) and `requests.request` (every
other call -- GET/POST/PATCH/DELETE all funnel through `_request`, which
calls `requests.request` so status-code inspection works uniformly) -- the
one external, paid-API boundary. Token caching, URL building, header
construction, retry/backoff, and the alternate-key upsert logic (including
created-vs-updated detection and GUID extraction) are all exercised for
real.
"""
import time

import pytest

from .conftest import FakeResponse, load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def env(monkeypatch):
    monkeypatch.setenv("DYN_CLIENT_ID", "client-123")
    monkeypatch.setenv("DYN_CLIENT_SECRET", "secret-456")


@pytest.fixture()
def resource(mod, env):
    return mod.DynamicsCrmResource(
        org_url="https://myorg.crm.dynamics.com",
        tenant_id="tenant-abc",
        client_id_env_var="DYN_CLIENT_ID",
        client_secret_env_var="DYN_CLIENT_SECRET",
    )


def _token_response(token="tok-1", expires_in=3600):
    return FakeResponse({"access_token": token, "expires_in": expires_in})


# --- token acquisition ----------------------------------------------------

def test_get_access_token_fetches_and_caches(mod, resource, monkeypatch):
    import requests

    calls = []

    def fake_post(url, data=None, timeout=None):
        calls.append({"url": url, "data": data})
        return _token_response()

    monkeypatch.setattr(requests, "post", fake_post)

    token1 = resource._get_access_token()
    token2 = resource._get_access_token()

    assert token1 == "tok-1"
    assert token2 == "tok-1"
    assert len(calls) == 1
    assert calls[0]["url"] == "https://login.microsoftonline.com/tenant-abc/oauth2/v2.0/token"
    assert calls[0]["data"]["grant_type"] == "client_credentials"
    assert calls[0]["data"]["client_id"] == "client-123"
    assert calls[0]["data"]["client_secret"] == "secret-456"
    # scope must be {org_url}/.default -- the documented Dataverse convention.
    assert calls[0]["data"]["scope"] == "https://myorg.crm.dynamics.com/.default"


def test_get_access_token_refetches_once_expired(mod, resource, monkeypatch):
    import requests

    calls = []

    def fake_post(url, data=None, timeout=None):
        calls.append(url)
        return _token_response(token=f"tok-{len(calls)}")

    monkeypatch.setattr(requests, "post", fake_post)

    token1 = resource._get_access_token()
    resource._token_cache[resource.org_url]["expires"] = time.time() - 1
    token2 = resource._get_access_token()

    assert token1 == "tok-1"
    assert token2 == "tok-2"
    assert len(calls) == 2


def test_get_access_token_missing_env_vars_raises(mod, monkeypatch):
    monkeypatch.delenv("DYN_CLIENT_ID", raising=False)
    monkeypatch.delenv("DYN_CLIENT_SECRET", raising=False)
    resource = mod.DynamicsCrmResource(
        org_url="https://myorg.crm.dynamics.com",
        tenant_id="tenant-abc",
        client_id_env_var="DYN_CLIENT_ID",
        client_secret_env_var="DYN_CLIENT_SECRET",
    )
    with pytest.raises(RuntimeError, match="Missing Dynamics CRM OAuth"):
        resource._get_access_token()


# --- .get() / .post() ------------------------------------------------------

def test_get_builds_url_and_sends_bearer_token_and_odata_headers(mod, resource, monkeypatch):
    import requests

    monkeypatch.setattr(requests, "post", lambda *a, **k: _token_response())

    captured = {}

    def fake_request(method, url, headers=None, params=None, json=None, timeout=None):
        captured["method"] = method
        captured["url"] = url
        captured["headers"] = headers
        captured["params"] = params
        return FakeResponse({"value": [{"accountid": "1"}]})

    monkeypatch.setattr(requests, "request", fake_request)

    result = resource.get("accounts", params={"$select": "name"})

    assert result == {"value": [{"accountid": "1"}]}
    assert captured["method"] == "GET"
    assert captured["url"] == "https://myorg.crm.dynamics.com/api/data/v9.2/accounts"
    assert captured["headers"]["Authorization"] == "Bearer tok-1"
    assert captured["headers"]["OData-MaxVersion"] == "4.0"
    assert captured["headers"]["OData-Version"] == "4.0"
    assert captured["params"] == {"$select": "name"}


def test_post_sends_json_body(mod, resource, monkeypatch):
    import requests

    monkeypatch.setattr(requests, "post", lambda *a, **k: _token_response())

    captured = {}

    def fake_request(method, url, headers=None, params=None, json=None, timeout=None):
        captured["method"] = method
        captured["json"] = json
        return FakeResponse({"accountid": "new-id"}, status_code=201)

    monkeypatch.setattr(requests, "request", fake_request)

    result = resource.post("accounts", json_body={"name": "Acme"})

    assert result == {"accountid": "new-id"}
    assert captured["method"] == "POST"
    assert captured["json"] == {"name": "Acme"}


# --- .patch() ----------------------------------------------------------

def test_patch_with_prefer_representation_sends_header(mod, resource, monkeypatch):
    import requests

    monkeypatch.setattr(requests, "post", lambda *a, **k: _token_response())

    captured = {}

    def fake_request(method, url, headers=None, params=None, json=None, timeout=None):
        captured["headers"] = headers
        return FakeResponse(
            {"accountid": "00000000-0000-0000-0000-000000000001"},
            status_code=201,
            headers={"OData-EntityId": f"{url}"},
        )

    monkeypatch.setattr(requests, "request", fake_request)

    result = resource.patch(
        "accounts(ext_id='abc')", json_body={"name": "Acme"}, prefer_representation=True
    )

    assert captured["headers"]["Prefer"] == "return=representation"
    assert result["status_code"] == 201
    assert result["body"] == {"accountid": "00000000-0000-0000-0000-000000000001"}
    assert result["odata_entity_id"].endswith("accounts(ext_id='abc')")


def test_patch_without_prefer_representation_omits_header_and_gets_204(mod, resource, monkeypatch):
    import requests

    monkeypatch.setattr(requests, "post", lambda *a, **k: _token_response())

    captured = {}

    def fake_request(method, url, headers=None, params=None, json=None, timeout=None):
        captured["headers"] = headers
        return FakeResponse(status_code=204, headers={"OData-EntityId": f"{url}"})

    monkeypatch.setattr(requests, "request", fake_request)

    result = resource.patch(
        "accounts(ext_id='abc')", json_body={"name": "Acme"}, prefer_representation=False
    )

    assert "Prefer" not in captured["headers"]
    assert result["status_code"] == 204
    assert result["body"] is None


# --- retry behavior ------------------------------------------------------

def test_401_triggers_token_refresh_and_retry(mod, resource, monkeypatch):
    import requests

    token_calls = []

    def fake_post(url, data=None, timeout=None):
        token_calls.append(1)
        return _token_response(token=f"tok-{len(token_calls)}")

    monkeypatch.setattr(requests, "post", fake_post)

    request_calls = []

    def fake_request(method, url, headers=None, params=None, json=None, timeout=None):
        request_calls.append(headers["Authorization"])
        if len(request_calls) == 1:
            return FakeResponse(status_code=401)
        return FakeResponse({"ok": True}, status_code=200)

    monkeypatch.setattr(requests, "request", fake_request)

    result = resource.get("accounts")

    assert result == {"ok": True}
    # First attempt used the original token; after the 401 the cache was
    # invalidated so the retry re-fetched a new one.
    assert request_calls == ["Bearer tok-1", "Bearer tok-2"]
    assert len(token_calls) == 2


def test_5xx_retries_then_raises_on_exhaustion(mod, env, monkeypatch):
    import requests

    monkeypatch.setattr(requests, "post", lambda *a, **k: _token_response())
    monkeypatch.setattr(time, "sleep", lambda *_: None)

    attempts = []

    def fake_request(method, url, headers=None, params=None, json=None, timeout=None):
        attempts.append(1)
        return FakeResponse(status_code=503)

    monkeypatch.setattr(requests, "request", fake_request)

    # max_retries is a frozen pydantic field -- construct with the value
    # under test rather than mutating an existing instance.
    resource = mod.DynamicsCrmResource(
        org_url="https://myorg.crm.dynamics.com",
        tenant_id="tenant-abc",
        client_id_env_var="DYN_CLIENT_ID",
        client_secret_env_var="DYN_CLIENT_SECRET",
        max_retries=2,
    )
    with pytest.raises(Exception):
        resource.get("accounts")
    assert len(attempts) == 2


# --- _format_key_value / _build_key_expr (pure logic) ---------------------

def test_format_key_value_string_is_quoted_and_escaped(mod):
    assert mod.DynamicsCrmResource._format_key_value("abc") == "'abc'"
    assert mod.DynamicsCrmResource._format_key_value("o'brien") == "'o''brien'"


def test_format_key_value_numeric_and_bool_unquoted(mod):
    assert mod.DynamicsCrmResource._format_key_value(42) == "42"
    assert mod.DynamicsCrmResource._format_key_value(3.14) == "3.14"
    assert mod.DynamicsCrmResource._format_key_value(True) == "true"
    assert mod.DynamicsCrmResource._format_key_value(False) == "false"


def test_build_key_expr_simple(mod, resource):
    assert resource._build_key_expr("external_id", "abc-123") == "external_id='abc-123'"


def test_build_key_expr_composite(mod, resource):
    expr = resource._build_key_expr(["k1", "k2"], [1, "two"])
    assert expr == "k1=1,k2='two'"


def test_build_key_expr_mismatched_lengths_raises(mod, resource):
    with pytest.raises(ValueError, match="same length"):
        resource._build_key_expr(["k1", "k2"], [1])


# --- upsert_by_key ---------------------------------------------------------

def test_upsert_by_key_created(mod, resource, monkeypatch):
    import requests

    monkeypatch.setattr(requests, "post", lambda *a, **k: _token_response())

    captured = {}

    def fake_request(method, url, headers=None, params=None, json=None, timeout=None):
        captured["url"] = url
        captured["json"] = json
        return FakeResponse(
            {"accountid": "00000000-0000-0000-0000-000000000001", "name": "Acme"},
            status_code=201,
        )

    monkeypatch.setattr(requests, "request", fake_request)

    result = resource.upsert_by_key(
        "accounts", "external_account_id", "ext-1", {"name": "Acme"}
    )

    assert result == {"action": "created", "id": "00000000-0000-0000-0000-000000000001"}
    assert captured["url"] == (
        "https://myorg.crm.dynamics.com/api/data/v9.2/"
        "accounts(external_account_id='ext-1')"
    )
    assert captured["json"] == {"name": "Acme"}


def test_upsert_by_key_updated(mod, resource, monkeypatch):
    import requests

    monkeypatch.setattr(requests, "post", lambda *a, **k: _token_response())

    def fake_request(method, url, headers=None, params=None, json=None, timeout=None):
        return FakeResponse(
            {"accountid": "11111111-1111-1111-1111-111111111111"}, status_code=200
        )

    monkeypatch.setattr(requests, "request", fake_request)

    result = resource.upsert_by_key(
        "accounts", "external_account_id", "ext-1", {"name": "Acme"}
    )
    assert result == {"action": "updated", "id": "11111111-1111-1111-1111-111111111111"}


def test_upsert_by_key_composite(mod, resource, monkeypatch):
    import requests

    monkeypatch.setattr(requests, "post", lambda *a, **k: _token_response())

    captured = {}

    def fake_request(method, url, headers=None, params=None, json=None, timeout=None):
        captured["url"] = url
        return FakeResponse({"contactid": "22222222-2222-2222-2222-222222222222"}, status_code=201)

    monkeypatch.setattr(requests, "request", fake_request)

    result = resource.upsert_by_key(
        "contacts", ["key1", "key2"], [1, "two"], {"firstname": "Jane"}
    )
    assert "contacts(key1=1,key2='two')" in captured["url"]
    assert result["action"] == "created"


def test_upsert_by_key_without_prefer_representation_is_unknown(mod, resource, monkeypatch):
    import requests

    monkeypatch.setattr(requests, "post", lambda *a, **k: _token_response())

    def fake_request(method, url, headers=None, params=None, json=None, timeout=None):
        return FakeResponse(
            status_code=204,
            headers={"OData-EntityId": f"{url}"},
        )

    monkeypatch.setattr(requests, "request", fake_request)

    result = resource.upsert_by_key(
        "accounts", "external_account_id", "ext-1", {"name": "Acme"},
        prefer_representation=False,
    )
    assert result["action"] == "unknown"
    assert "accounts(external_account_id='ext-1')" in result["id"]


# --- DynamicsCrmResourceComponent registration ----------------------------

def test_component_registers_resource_under_resource_key(mod):
    component = mod.DynamicsCrmResourceComponent(
        resource_key="dynamics_custom",
        org_url="https://myorg.crm.dynamics.com",
        tenant_id="tenant-abc",
        client_id_env_var="DYN_CLIENT_ID",
        client_secret_env_var="DYN_CLIENT_SECRET",
    )
    defs = component.build_defs(context=None)
    assert "dynamics_custom" in defs.resources
    registered = defs.resources["dynamics_custom"]
    assert isinstance(registered, mod.DynamicsCrmResource)
    assert registered.org_url == "https://myorg.crm.dynamics.com"


def test_component_defaults_resource_key_to_dynamics_crm(mod):
    component = mod.DynamicsCrmResourceComponent(
        org_url="https://myorg.crm.dynamics.com",
        tenant_id="tenant-abc",
    )
    defs = component.build_defs(context=None)
    assert "dynamics_crm" in defs.resources
