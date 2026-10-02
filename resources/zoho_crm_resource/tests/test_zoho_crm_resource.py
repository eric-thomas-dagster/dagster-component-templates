"""Committed regression tests for ZohoCrmResource.

Monkeypatches `requests.post` (token refresh) and `requests.request` (the
generic .get()/.post()/.patch()/.upsert() HTTP core) -- the one external,
paid-API boundary. Token caching, regional accounts-host construction,
api_domain resolution, retry-on-401/429/5xx, and the upsert request/response
shape are all exercised for real.
"""
import time

import pytest

from .conftest import FakeResponse, load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def env(monkeypatch):
    monkeypatch.setenv("ZOHO_CLIENT_ID", "client-123")
    monkeypatch.setenv("ZOHO_CLIENT_SECRET", "secret-456")
    monkeypatch.setenv("ZOHO_REFRESH_TOKEN", "refresh-789")


@pytest.fixture()
def resource(mod, env):
    return mod.ZohoCrmResource(
        client_id_env_var="ZOHO_CLIENT_ID",
        client_secret_env_var="ZOHO_CLIENT_SECRET",
        refresh_token_env_var="ZOHO_REFRESH_TOKEN",
    )


def _token_response(access_token="tok-1", expires_in=3600, api_domain="https://www.zohoapis.com"):
    return FakeResponse(
        {"access_token": access_token, "expires_in": expires_in, "api_domain": api_domain, "token_type": "Bearer"}
    )


# --- regional accounts host -------------------------------------------------

def test_accounts_host_default_us(mod):
    assert mod._accounts_host("com") == "accounts.zoho.com"


def test_accounts_host_eu(mod):
    assert mod._accounts_host("eu") == "accounts.zoho.eu"


def test_accounts_host_canada_special_cased(mod):
    # Canada is accounts.zohocloud.ca, NOT accounts.zoho.ca.
    assert mod._accounts_host("ca") == "accounts.zohocloud.ca"


def test_accounts_host_defaults_when_blank(mod):
    assert mod._accounts_host("") == "accounts.zoho.com"


# --- token acquisition -------------------------------------------------------

def test_get_access_fetches_and_caches(mod, resource, monkeypatch):
    import requests

    calls = []

    def fake_post(url, data=None, timeout=None):
        calls.append({"url": url, "data": data})
        return _token_response()

    monkeypatch.setattr(requests, "post", fake_post)

    access1 = resource._get_access()
    access2 = resource._get_access()

    assert access1["access_token"] == "tok-1"
    assert access2["access_token"] == "tok-1"
    # Second call hit the cache -- only one HTTP call to the token endpoint.
    assert len(calls) == 1
    assert calls[0]["url"] == "https://accounts.zoho.com/oauth/v2/token"
    assert calls[0]["data"] == {
        "grant_type": "refresh_token",
        "client_id": "client-123",
        "client_secret": "secret-456",
        "refresh_token": "refresh-789",
    }


def test_get_access_uses_accounts_domain_for_token_url(mod, env, monkeypatch):
    import requests

    calls = []

    def fake_post(url, data=None, timeout=None):
        calls.append(url)
        return _token_response()

    monkeypatch.setattr(requests, "post", fake_post)

    resource = mod.ZohoCrmResource(
        client_id_env_var="ZOHO_CLIENT_ID",
        client_secret_env_var="ZOHO_CLIENT_SECRET",
        refresh_token_env_var="ZOHO_REFRESH_TOKEN",
        accounts_domain="eu",
    )
    resource._get_access()
    assert calls[0] == "https://accounts.zoho.eu/oauth/v2/token"


def test_get_access_derives_api_domain_from_token_response(mod, resource, monkeypatch):
    import requests

    monkeypatch.setattr(
        requests, "post",
        lambda url, data=None, timeout=None: _token_response(api_domain="https://www.zohoapis.eu"),
    )
    access = resource._get_access()
    assert access["api_domain"] == "https://www.zohoapis.eu"


def test_api_domain_override_wins_over_token_response(mod, env, monkeypatch):
    import requests

    monkeypatch.setattr(
        requests, "post",
        lambda url, data=None, timeout=None: _token_response(api_domain="https://www.zohoapis.com"),
    )
    resource = mod.ZohoCrmResource(
        client_id_env_var="ZOHO_CLIENT_ID",
        client_secret_env_var="ZOHO_CLIENT_SECRET",
        refresh_token_env_var="ZOHO_REFRESH_TOKEN",
        api_domain="https://custom.example.com",
    )
    access = resource._get_access()
    assert access["api_domain"] == "https://custom.example.com"


def test_get_access_refetches_once_expired(mod, resource, monkeypatch):
    import requests

    calls = []

    def fake_post(url, data=None, timeout=None):
        calls.append(url)
        return _token_response(access_token=f"tok-{len(calls)}")

    monkeypatch.setattr(requests, "post", fake_post)

    access1 = resource._get_access()
    resource._token_cache[resource.client_id_env_var]["expires"] = time.time() - 1
    access2 = resource._get_access()

    assert access1["access_token"] == "tok-1"
    assert access2["access_token"] == "tok-2"
    assert len(calls) == 2


def test_get_access_missing_env_vars_raises(mod, monkeypatch):
    monkeypatch.delenv("ZOHO_CLIENT_ID", raising=False)
    monkeypatch.delenv("ZOHO_CLIENT_SECRET", raising=False)
    monkeypatch.delenv("ZOHO_REFRESH_TOKEN", raising=False)
    resource = mod.ZohoCrmResource(
        client_id_env_var="ZOHO_CLIENT_ID",
        client_secret_env_var="ZOHO_CLIENT_SECRET",
        refresh_token_env_var="ZOHO_REFRESH_TOKEN",
    )
    with pytest.raises(RuntimeError, match="Missing Zoho OAuth"):
        resource._get_access()


# --- .get() / .post() / .patch() --------------------------------------------

def test_get_builds_url_and_sends_oauthtoken_header(mod, resource, monkeypatch):
    import requests

    monkeypatch.setattr(requests, "post", lambda url, data=None, timeout=None: _token_response())

    req_calls = []

    def fake_request(method, url, headers=None, params=None, json=None, timeout=None):
        req_calls.append({"method": method, "url": url, "headers": headers, "params": params})
        return FakeResponse({"data": [{"id": "1"}]})

    monkeypatch.setattr(requests, "request", fake_request)

    result = resource.get("Leads/1", params={"fields": "Email"})

    assert result == {"data": [{"id": "1"}]}
    assert len(req_calls) == 1
    call = req_calls[0]
    assert call["method"] == "GET"
    assert call["url"] == "https://www.zohoapis.com/crm/v8/Leads/1"
    assert call["headers"]["Authorization"] == "Zoho-oauthtoken tok-1"
    assert call["params"] == {"fields": "Email"}


def test_post_sends_json_body(mod, resource, monkeypatch):
    import requests

    monkeypatch.setattr(requests, "post", lambda url, data=None, timeout=None: _token_response())

    req_calls = []

    def fake_request(method, url, headers=None, params=None, json=None, timeout=None):
        req_calls.append({"method": method, "json": json})
        return FakeResponse({"data": []})

    monkeypatch.setattr(requests, "request", fake_request)

    resource.post("Leads", json_body={"data": [{"Last_Name": "X"}]})
    assert req_calls[0]["method"] == "POST"
    assert req_calls[0]["json"] == {"data": [{"Last_Name": "X"}]}


def test_patch_sends_json_body(mod, resource, monkeypatch):
    import requests

    monkeypatch.setattr(requests, "post", lambda url, data=None, timeout=None: _token_response())

    req_calls = []

    def fake_request(method, url, headers=None, params=None, json=None, timeout=None):
        req_calls.append({"method": method, "json": json})
        return FakeResponse({"data": []})

    monkeypatch.setattr(requests, "request", fake_request)

    resource.patch("Leads/1", json_body={"Last_Name": "Y"})
    assert req_calls[0]["method"] == "PATCH"
    assert req_calls[0]["json"] == {"Last_Name": "Y"}


# --- retry behavior ----------------------------------------------------------

def test_401_forces_refresh_and_retries(mod, resource, monkeypatch):
    import requests

    token_calls = []

    def fake_post(url, data=None, timeout=None):
        token_calls.append(url)
        return _token_response(access_token=f"tok-{len(token_calls)}")

    monkeypatch.setattr(requests, "post", fake_post)
    monkeypatch.setattr(time, "sleep", lambda *_: None)

    req_calls = []

    def fake_request(method, url, headers=None, params=None, json=None, timeout=None):
        req_calls.append(headers["Authorization"])
        if len(req_calls) == 1:
            return FakeResponse({"error": "unauthorized"}, status_code=401)
        return FakeResponse({"data": [{"id": "1"}]})

    monkeypatch.setattr(requests, "request", fake_request)

    result = resource.get("Leads/1")
    assert result == {"data": [{"id": "1"}]}
    # First attempt used the originally-cached token; second attempt used a
    # freshly-refreshed one (forced by the 401).
    assert req_calls[0] == "Zoho-oauthtoken tok-1"
    assert req_calls[1] == "Zoho-oauthtoken tok-2"
    assert len(token_calls) == 2


def test_429_retries_with_backoff(mod, resource, monkeypatch):
    import requests

    monkeypatch.setattr(requests, "post", lambda url, data=None, timeout=None: _token_response())
    sleeps = []
    monkeypatch.setattr(time, "sleep", lambda s: sleeps.append(s))

    attempts = []

    def fake_request(method, url, headers=None, params=None, json=None, timeout=None):
        attempts.append(1)
        if len(attempts) < 3:
            return FakeResponse({}, status_code=429)
        return FakeResponse({"data": []})

    monkeypatch.setattr(requests, "request", fake_request)

    result = resource.get("Leads")
    assert result == {"data": []}
    assert len(attempts) == 3
    assert len(sleeps) == 2  # slept before attempt 2 and attempt 3


def test_5xx_exhausts_retries_then_raises(mod, resource, monkeypatch):
    import requests

    monkeypatch.setattr(requests, "post", lambda url, data=None, timeout=None: _token_response())
    monkeypatch.setattr(time, "sleep", lambda *_: None)

    def fake_request(method, url, headers=None, params=None, json=None, timeout=None):
        return FakeResponse({"error": "boom"}, status_code=500)

    monkeypatch.setattr(requests, "request", fake_request)

    with pytest.raises(requests.HTTPError):
        resource.get("Leads")


# --- .upsert() ---------------------------------------------------------------

def test_upsert_builds_correct_body_and_path(mod, resource, monkeypatch):
    import requests

    monkeypatch.setattr(requests, "post", lambda url, data=None, timeout=None: _token_response())

    req_calls = []

    def fake_request(method, url, headers=None, params=None, json=None, timeout=None):
        req_calls.append({"method": method, "url": url, "json": json})
        return FakeResponse({
            "data": [
                {"code": "SUCCESS", "duplicate_field": "Email", "action": "update",
                 "status": "success", "message": "record updated", "details": {"id": "1"}},
            ]
        })

    monkeypatch.setattr(requests, "request", fake_request)

    records = [{"Email": "a@b.com", "Last_Name": "A"}]
    result = resource.upsert("Leads", records, duplicate_check_fields=["Email"])

    assert len(req_calls) == 1
    assert req_calls[0]["method"] == "POST"
    assert req_calls[0]["url"] == "https://www.zohoapis.com/crm/v8/Leads/upsert"
    assert req_calls[0]["json"] == {"data": records, "duplicate_check_fields": ["Email"]}
    assert result == [
        {"code": "SUCCESS", "duplicate_field": "Email", "action": "update",
         "status": "success", "message": "record updated", "details": {"id": "1"}},
    ]


def test_upsert_omits_duplicate_check_fields_key_when_not_given(mod, resource, monkeypatch):
    import requests

    monkeypatch.setattr(requests, "post", lambda url, data=None, timeout=None: _token_response())

    req_calls = []

    def fake_request(method, url, headers=None, params=None, json=None, timeout=None):
        req_calls.append(json)
        return FakeResponse({"data": []})

    monkeypatch.setattr(requests, "request", fake_request)

    resource.upsert("Leads", [{"Email": "a@b.com"}])
    assert "duplicate_check_fields" not in req_calls[0]


def test_upsert_raises_over_100_records(mod, resource):
    records = [{"Email": f"user{i}@example.com"} for i in range(101)]
    with pytest.raises(ValueError, match="100-record"):
        resource.upsert("Leads", records, duplicate_check_fields=["Email"])


# --- ZohoCrmResourceComponent registration -----------------------------------

def test_component_registers_resource_under_resource_key(mod):
    component = mod.ZohoCrmResourceComponent(
        resource_key="zoho_custom",
        client_id_env_var="ZOHO_CLIENT_ID",
        client_secret_env_var="ZOHO_CLIENT_SECRET",
        refresh_token_env_var="ZOHO_REFRESH_TOKEN",
    )
    defs = component.build_defs(context=None)
    assert "zoho_custom" in defs.resources
    registered = defs.resources["zoho_custom"]
    assert isinstance(registered, mod.ZohoCrmResource)
    assert registered.client_id_env_var == "ZOHO_CLIENT_ID"


def test_component_defaults_resource_key_to_zoho_crm(mod):
    component = mod.ZohoCrmResourceComponent()
    defs = component.build_defs(context=None)
    assert "zoho_crm" in defs.resources
    assert defs.resources["zoho_crm"].api_version == "v8"
