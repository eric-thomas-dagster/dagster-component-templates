"""Committed regression tests for MarketoResource.

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
    monkeypatch.setenv("MKTO_CLIENT_ID", "client-123")
    monkeypatch.setenv("MKTO_CLIENT_SECRET", "secret-456")


@pytest.fixture()
def resource(mod, env):
    return mod.MarketoResource(
        rest_url="https://123-ABC-456.mktorest.com",
        client_id_env_var="MKTO_CLIENT_ID",
        client_secret_env_var="MKTO_CLIENT_SECRET",
    )


# --- token acquisition ----------------------------------------------------

def test_get_access_token_fetches_and_caches(mod, resource, monkeypatch):
    import requests

    calls = []

    def fake_get(url, params=None, headers=None, timeout=None):
        calls.append(url)
        return FakeResponse({"access_token": "tok-1", "expires_in": 3600})

    monkeypatch.setattr(requests, "get", fake_get)

    token1 = resource._get_access_token()
    token2 = resource._get_access_token()

    assert token1 == "tok-1"
    assert token2 == "tok-1"
    # Second call hit the cache -- only one HTTP call to the token endpoint.
    assert len(calls) == 1
    assert calls[0] == "https://123-ABC-456.mktorest.com/identity/oauth/token"


def test_get_access_token_refetches_once_expired(mod, resource, monkeypatch):
    import requests

    calls = []

    def fake_get(url, params=None, headers=None, timeout=None):
        calls.append(url)
        return FakeResponse({"access_token": f"tok-{len(calls)}", "expires_in": 3600})

    monkeypatch.setattr(requests, "get", fake_get)

    token1 = resource._get_access_token()
    # Force expiry.
    resource._token_cache[resource.rest_url]["expires"] = time.time() - 1
    token2 = resource._get_access_token()

    assert token1 == "tok-1"
    assert token2 == "tok-2"
    assert len(calls) == 2


def test_get_access_token_missing_env_vars_raises(mod, monkeypatch):
    monkeypatch.delenv("MKTO_CLIENT_ID", raising=False)
    monkeypatch.delenv("MKTO_CLIENT_SECRET", raising=False)
    resource = mod.MarketoResource(
        rest_url="https://123-ABC-456.mktorest.com",
        client_id_env_var="MKTO_CLIENT_ID",
        client_secret_env_var="MKTO_CLIENT_SECRET",
    )
    with pytest.raises(RuntimeError, match="Missing Marketo OAuth"):
        resource._get_access_token()


# --- .get() ----------------------------------------------------------------

def test_get_builds_url_and_sends_bearer_token(mod, resource, monkeypatch):
    import requests

    token_calls = []
    get_calls = []

    def fake_get(url, params=None, headers=None, timeout=None):
        if "identity/oauth/token" in url:
            token_calls.append(url)
            return FakeResponse({"access_token": "tok-get", "expires_in": 3600})
        get_calls.append({"url": url, "params": params, "headers": headers})
        return FakeResponse({"result": [{"id": 1}]})

    monkeypatch.setattr(requests, "get", fake_get)

    result = resource.get("rest/v1/leads.json", params={"filterType": "email"})

    assert result == {"result": [{"id": 1}]}
    assert len(get_calls) == 1
    assert get_calls[0]["url"] == "https://123-ABC-456.mktorest.com/rest/v1/leads.json"
    assert get_calls[0]["params"] == {"filterType": "email"}
    assert get_calls[0]["headers"] == {"Authorization": "Bearer tok-get"}


# --- .post() (the new write method) -----------------------------------------

def test_post_builds_url_sends_json_body_and_bearer_token(mod, resource, monkeypatch):
    import requests

    def fake_get(url, params=None, headers=None, timeout=None):
        return FakeResponse({"access_token": "tok-post", "expires_in": 3600})

    post_calls = []

    def fake_post(url, json=None, headers=None, timeout=None):
        post_calls.append({"url": url, "json": json, "headers": headers})
        return FakeResponse({"success": True, "result": [{"id": 42, "status": "created"}]})

    monkeypatch.setattr(requests, "get", fake_get)
    monkeypatch.setattr(requests, "post", fake_post)

    body = {"action": "createOrUpdate", "lookupField": "email", "input": [{"email": "a@b.com"}]}
    result = resource.post("rest/v1/leads.json", json_body=body)

    assert result == {"success": True, "result": [{"id": 42, "status": "created"}]}
    assert len(post_calls) == 1
    assert post_calls[0]["url"] == "https://123-ABC-456.mktorest.com/rest/v1/leads.json"
    assert post_calls[0]["json"] == body
    assert post_calls[0]["headers"] == {"Authorization": "Bearer tok-post"}


def test_post_reuses_cached_token_from_prior_get(mod, resource, monkeypatch):
    import requests

    token_fetches = []

    def fake_get(url, params=None, headers=None, timeout=None):
        token_fetches.append(url)
        return FakeResponse({"access_token": "shared-tok", "expires_in": 3600})

    def fake_post(url, json=None, headers=None, timeout=None):
        return FakeResponse({"success": True})

    monkeypatch.setattr(requests, "get", fake_get)
    monkeypatch.setattr(requests, "post", fake_post)

    resource._get_access_token()  # primes the cache
    resource.post("rest/v1/leads.json", json_body={"input": []})

    # Only the priming call hit the token endpoint -- post() reused the cache.
    assert len(token_fetches) == 1


def test_post_raises_on_http_error(mod, resource, monkeypatch):
    import requests

    def fake_get(url, params=None, headers=None, timeout=None):
        return FakeResponse({"access_token": "tok", "expires_in": 3600})

    def fake_post(url, json=None, headers=None, timeout=None):
        return FakeResponse({"error": "bad request"}, status_code=400)

    monkeypatch.setattr(requests, "get", fake_get)
    monkeypatch.setattr(requests, "post", fake_post)

    with pytest.raises(RuntimeError, match="HTTP 400"):
        resource.post("rest/v1/leads.json", json_body={"input": []})


# --- MarketoResourceComponent registration ----------------------------------

def test_component_registers_resource_under_resource_key(mod):
    component = mod.MarketoResourceComponent(
        resource_key="marketo_custom",
        rest_url="https://123-ABC-456.mktorest.com",
        client_id_env_var="MKTO_CLIENT_ID",
        client_secret_env_var="MKTO_CLIENT_SECRET",
    )
    defs = component.build_defs(context=None)
    assert "marketo_custom" in defs.resources
    registered = defs.resources["marketo_custom"]
    assert isinstance(registered, mod.MarketoResource)
    assert registered.rest_url == "https://123-ABC-456.mktorest.com"


def test_component_defaults_resource_key_to_marketo(mod):
    component = mod.MarketoResourceComponent(
        rest_url="https://123-ABC-456.mktorest.com",
        client_id_env_var="MKTO_CLIENT_ID",
        client_secret_env_var="MKTO_CLIENT_SECRET",
    )
    defs = component.build_defs(context=None)
    assert "marketo" in defs.resources
