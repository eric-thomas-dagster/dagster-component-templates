"""Committed regression tests for SigmaResource.

Monkeypatches `requests.get` / `requests.post` wholesale (the one external,
paid-API boundary) -- token caching, URL building, and header construction
are all exercised for real.
"""
import time

import pytest

from .conftest import FakeResponse, load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def env(monkeypatch):
    monkeypatch.setenv("SIGMA_CLIENT_ID", "client-123")
    monkeypatch.setenv("SIGMA_CLIENT_SECRET", "secret-456")


@pytest.fixture()
def resource(mod, env):
    return mod.SigmaResource(
        client_id_env_var="SIGMA_CLIENT_ID",
        client_secret_env_var="SIGMA_CLIENT_SECRET",
    )


# --- token acquisition ----------------------------------------------------

def test_get_access_token_fetches_and_caches(mod, resource, monkeypatch):
    import requests

    calls = []

    def fake_post(url, data=None, headers=None, timeout=None):
        calls.append({"url": url, "data": data, "headers": headers})
        return FakeResponse({"access_token": "tok-1", "token_type": "bearer", "expires_in": 3600})

    monkeypatch.setattr(requests, "post", fake_post)

    token1 = resource._get_access_token()
    token2 = resource._get_access_token()

    assert token1 == "tok-1"
    assert token2 == "tok-1"
    # Second call hit the cache -- only one HTTP call to the token endpoint.
    assert len(calls) == 1
    assert calls[0]["url"] == "https://api.sigmacomputing.com/v2/auth/token"
    assert calls[0]["data"] == {
        "grant_type": "client_credentials",
        "client_id": "client-123",
        "client_secret": "secret-456",
    }
    assert calls[0]["headers"] == {"Content-Type": "application/x-www-form-urlencoded"}


def test_get_access_token_refetches_once_expired(mod, resource, monkeypatch):
    import requests

    calls = []

    def fake_post(url, data=None, headers=None, timeout=None):
        calls.append(url)
        return FakeResponse({"access_token": f"tok-{len(calls)}", "expires_in": 3600})

    monkeypatch.setattr(requests, "post", fake_post)

    token1 = resource._get_access_token()
    resource._token_cache[resource.base_url]["expires"] = time.time() - 1
    token2 = resource._get_access_token()

    assert token1 == "tok-1"
    assert token2 == "tok-2"
    assert len(calls) == 2


def test_get_access_token_missing_env_vars_raises(mod, monkeypatch):
    monkeypatch.delenv("SIGMA_CLIENT_ID", raising=False)
    monkeypatch.delenv("SIGMA_CLIENT_SECRET", raising=False)
    resource = mod.SigmaResource(
        client_id_env_var="SIGMA_CLIENT_ID",
        client_secret_env_var="SIGMA_CLIENT_SECRET",
    )
    with pytest.raises(RuntimeError, match="Missing Sigma OAuth"):
        resource._get_access_token()


# --- .get() ----------------------------------------------------------------

def test_get_builds_url_and_sends_bearer_token(mod, resource, monkeypatch):
    import requests

    def fake_post(url, data=None, headers=None, timeout=None):
        return FakeResponse({"access_token": "tok-get", "expires_in": 3600})

    get_calls = []

    def fake_get(url, params=None, headers=None, timeout=None):
        get_calls.append({"url": url, "params": params, "headers": headers})
        return FakeResponse({"entries": [{"workbookId": "wb1"}]})

    monkeypatch.setattr(requests, "post", fake_post)
    monkeypatch.setattr(requests, "get", fake_get)

    result = resource.get("v2/workbooks", params={"limit": 100})

    assert result == {"entries": [{"workbookId": "wb1"}]}
    assert len(get_calls) == 1
    assert get_calls[0]["url"] == "https://api.sigmacomputing.com/v2/workbooks"
    assert get_calls[0]["params"] == {"limit": 100}
    assert get_calls[0]["headers"] == {"Authorization": "Bearer tok-get"}


# --- .post() -----------------------------------------------------------------

def test_post_builds_url_sends_json_body_and_bearer_token(mod, resource, monkeypatch):
    import requests

    post_calls = []

    def fake_post_dispatch(url, data=None, json=None, headers=None, timeout=None):
        if "auth/token" in url:
            return FakeResponse({"access_token": "tok-post", "expires_in": 3600})
        post_calls.append({"url": url, "json": json, "headers": headers})
        return FakeResponse({"traceId": "trace-1"})

    monkeypatch.setattr(requests, "post", fake_post_dispatch)

    body = {"rows": [{"a": 1}]}
    result = resource.post("v2/webhooks/wb1/seq1", json_body=body)

    assert result == {"traceId": "trace-1"}
    assert len(post_calls) == 1
    assert post_calls[0]["url"] == "https://api.sigmacomputing.com/v2/webhooks/wb1/seq1"
    assert post_calls[0]["json"] == body
    assert post_calls[0]["headers"] == {"Authorization": "Bearer tok-post"}


def test_post_reuses_cached_token_from_prior_get(mod, resource, monkeypatch):
    import requests

    token_fetches = []

    def fake_post(url, data=None, json=None, headers=None, timeout=None):
        if "auth/token" in url:
            token_fetches.append(url)
            return FakeResponse({"access_token": "shared-tok", "expires_in": 3600})
        return FakeResponse({"traceId": "t"})

    monkeypatch.setattr(requests, "post", fake_post)

    resource._get_access_token()  # primes the cache
    resource.post("v2/webhooks/wb1/seq1", json_body={"rows": []})

    # Only the priming call hit the token endpoint -- post() reused the cache.
    assert len(token_fetches) == 1


def test_post_raises_on_http_error(mod, resource, monkeypatch):
    import requests

    def fake_post(url, data=None, json=None, headers=None, timeout=None):
        if "auth/token" in url:
            return FakeResponse({"access_token": "tok", "expires_in": 3600})
        return FakeResponse({"error": "bad request"}, status_code=400)

    monkeypatch.setattr(requests, "post", fake_post)

    with pytest.raises(RuntimeError, match="HTTP 400"):
        resource.post("v2/webhooks/wb1/seq1", json_body={"rows": []})


def test_post_handles_empty_response_content(mod, resource, monkeypatch):
    import requests

    def fake_post(url, data=None, json=None, headers=None, timeout=None):
        if "auth/token" in url:
            return FakeResponse({"access_token": "tok", "expires_in": 3600})
        return FakeResponse({}, content=b"")

    monkeypatch.setattr(requests, "post", fake_post)

    result = resource.post("v2/webhooks/wb1/seq1", json_body={"rows": []})
    assert result == {}


# --- SigmaResourceComponent registration ------------------------------------

def test_component_registers_resource_under_resource_key(mod):
    component = mod.SigmaResourceComponent(
        resource_key="sigma_custom",
        client_id_env_var="SIGMA_CLIENT_ID",
        client_secret_env_var="SIGMA_CLIENT_SECRET",
        base_url="https://api.eu.aws.sigmacomputing.com",
    )
    defs = component.build_defs(context=None)
    assert "sigma_custom" in defs.resources
    registered = defs.resources["sigma_custom"]
    assert isinstance(registered, mod.SigmaResource)
    assert registered.base_url == "https://api.eu.aws.sigmacomputing.com"


def test_component_defaults_resource_key_to_sigma_resource(mod):
    component = mod.SigmaResourceComponent(
        client_id_env_var="SIGMA_CLIENT_ID",
        client_secret_env_var="SIGMA_CLIENT_SECRET",
    )
    defs = component.build_defs(context=None)
    assert "sigma_resource" in defs.resources
