"""Committed regression tests for ZocdocResource / ZocdocResourceComponent.

Monkeypatches `_fetch_zocdoc_access_token` wholesale (the one external,
paid-API boundary this resource owns) -- token caching, environment-based
URL selection, and the get_client()/get_base_url() surface are all
exercised for real.
"""
import time

import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def env(monkeypatch):
    monkeypatch.setenv("ZOCDOC_CLIENT_ID", "client-123")
    monkeypatch.setenv("ZOCDOC_CLIENT_SECRET", "secret-456")


@pytest.fixture()
def resource(mod, env):
    return mod.ZocdocResource(
        environment="sandbox",
        client_id_env_var="ZOCDOC_CLIENT_ID",
        client_secret_env_var="ZOCDOC_CLIENT_SECRET",
    )


# --- environment-based URL selection -----------------------------------------

def test_sandbox_urls(resource):
    urls = resource._urls()
    assert urls["token_url"] == "https://auth-api-developer-sandbox.zocdoc.com/oauth/token"
    assert urls["base_url"] == "https://api-developer-sandbox.zocdoc.com/"


def test_production_urls(mod, env):
    resource = mod.ZocdocResource(
        environment="production",
        client_id_env_var="ZOCDOC_CLIENT_ID",
        client_secret_env_var="ZOCDOC_CLIENT_SECRET",
    )
    urls = resource._urls()
    assert urls["token_url"] == "https://auth.zocdoc.com/oauth/token"
    assert urls["base_url"] == "https://api-developer.zocdoc.com/"


def test_invalid_environment_raises(mod, env):
    resource = mod.ZocdocResource(
        environment="staging",
        client_id_env_var="ZOCDOC_CLIENT_ID",
        client_secret_env_var="ZOCDOC_CLIENT_SECRET",
    )
    with pytest.raises(ValueError, match="sandbox.*production"):
        resource._urls()


def test_get_base_url(resource):
    assert resource.get_base_url() == "https://api-developer-sandbox.zocdoc.com/"


# --- token acquisition --------------------------------------------------------

def test_get_access_token_fetches_and_caches(mod, resource, monkeypatch):
    calls = []

    def fake_fetch(token_url, client_id, client_secret, audience, scope):
        calls.append((token_url, client_id, client_secret, audience, scope))
        return {"access_token": "tok-1", "expires_in": 3600}

    monkeypatch.setattr(mod, "_fetch_zocdoc_access_token", fake_fetch)

    token1 = resource._get_access_token()
    token2 = resource._get_access_token()

    assert token1 == token2 == "tok-1"
    assert len(calls) == 1  # second call hit the cache
    assert calls[0][0] == "https://auth-api-developer-sandbox.zocdoc.com/oauth/token"
    assert calls[0][3] == "https://api-developer-sandbox.zocdoc.com/"


def test_get_access_token_refetches_once_expired(mod, resource, monkeypatch):
    calls = []

    def fake_fetch(token_url, client_id, client_secret, audience, scope):
        calls.append(1)
        return {"access_token": f"tok-{len(calls)}", "expires_in": 3600}

    monkeypatch.setattr(mod, "_fetch_zocdoc_access_token", fake_fetch)

    token1 = resource._get_access_token()
    cache_key = f"{resource.environment}:{resource.client_id_env_var}"
    resource._token_cache[cache_key]["expires"] = time.time() - 1
    token2 = resource._get_access_token()

    assert token1 == "tok-1"
    assert token2 == "tok-2"
    assert len(calls) == 2


def test_get_access_token_missing_env_vars_raises(mod, monkeypatch):
    monkeypatch.delenv("ZOCDOC_CLIENT_ID", raising=False)
    monkeypatch.delenv("ZOCDOC_CLIENT_SECRET", raising=False)
    resource = mod.ZocdocResource(
        environment="sandbox",
        client_id_env_var="ZOCDOC_CLIENT_ID",
        client_secret_env_var="ZOCDOC_CLIENT_SECRET",
    )
    with pytest.raises(RuntimeError, match="Missing Zocdoc OAuth2 credentials"):
        resource._get_access_token()


def test_fetch_zocdoc_access_token_posts_client_credentials_body(mod, monkeypatch):
    import requests

    posted = {}

    def fake_post(url, json=None, timeout=None):
        posted["url"] = url
        posted["json"] = json
        from .conftest import FakeResponse
        return FakeResponse({"access_token": "tok-x", "expires_in": 3600})

    monkeypatch.setattr(requests, "post", fake_post)

    data = mod._fetch_zocdoc_access_token(
        token_url="https://auth-api-developer-sandbox.zocdoc.com/oauth/token",
        client_id="cid",
        client_secret="csecret",
        audience="https://api-developer-sandbox.zocdoc.com/",
        scope=None,
    )
    assert data["access_token"] == "tok-x"
    assert posted["json"]["grant_type"] == "client_credentials"
    assert posted["json"]["client_id"] == "cid"
    assert posted["json"]["client_secret"] == "csecret"
    assert posted["json"]["audience"] == "https://api-developer-sandbox.zocdoc.com/"
    assert "scope" not in posted["json"]


def test_fetch_zocdoc_access_token_includes_scope_when_set(mod, monkeypatch):
    import requests

    posted = {}

    def fake_post(url, json=None, timeout=None):
        posted["json"] = json
        from .conftest import FakeResponse
        return FakeResponse({"access_token": "tok-x", "expires_in": 3600})

    monkeypatch.setattr(requests, "post", fake_post)

    mod._fetch_zocdoc_access_token(
        token_url="https://auth-api-developer-sandbox.zocdoc.com/oauth/token",
        client_id="cid",
        client_secret="csecret",
        audience="https://api-developer-sandbox.zocdoc.com/",
        scope="offline_access",
    )
    assert posted["json"]["scope"] == "offline_access"


# --- get_client() --------------------------------------------------------------

def test_get_client_sets_bearer_header(mod, resource, monkeypatch):
    def fake_fetch(token_url, client_id, client_secret, audience, scope):
        return {"access_token": "tok-bearer", "expires_in": 3600}

    monkeypatch.setattr(mod, "_fetch_zocdoc_access_token", fake_fetch)

    session = resource.get_client()
    assert session.headers["Authorization"] == "Bearer tok-bearer"
    assert session.headers["Content-Type"] == "application/json"


# --- component registration -----------------------------------------------------

def test_component_registers_resource_under_resource_key(mod):
    component = mod.ZocdocResourceComponent(
        resource_key="zocdoc_custom",
        environment="production",
        client_id_env_var="ZOCDOC_CLIENT_ID",
        client_secret_env_var="ZOCDOC_CLIENT_SECRET",
    )
    defs = component.build_defs(context=None)
    assert "zocdoc_custom" in defs.resources
    registered = defs.resources["zocdoc_custom"]
    assert isinstance(registered, mod.ZocdocResource)
    assert registered.environment == "production"


def test_component_defaults_resource_key(mod):
    component = mod.ZocdocResourceComponent(
        environment="sandbox",
        client_id_env_var="ZOCDOC_CLIENT_ID",
        client_secret_env_var="ZOCDOC_CLIENT_SECRET",
    )
    defs = component.build_defs(context=None)
    assert "zocdoc_resource" in defs.resources


def test_component_rejects_invalid_environment(mod):
    component = mod.ZocdocResourceComponent(
        environment="not-a-real-env",
        client_id_env_var="ZOCDOC_CLIENT_ID",
        client_secret_env_var="ZOCDOC_CLIENT_SECRET",
    )
    with pytest.raises(ValueError, match="sandbox.*production"):
        component.build_defs(context=None)
