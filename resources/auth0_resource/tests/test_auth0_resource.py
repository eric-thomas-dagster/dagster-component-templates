"""Committed regression tests for Auth0Resource.

Monkeypatches `requests.get`/`requests.post`/`requests.patch` wholesale
(the one external, paid-API boundary) -- token caching, URL building, and
the structural separation between update_user (profile) and set_blocked
(the ONLY path that can touch `blocked`) are all exercised for real.
"""
import time

import pytest

from .conftest import FakeResponse, load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def env(monkeypatch):
    monkeypatch.setenv("AUTH0_CLIENT_ID", "client-123")
    monkeypatch.setenv("AUTH0_CLIENT_SECRET", "secret-456")


@pytest.fixture()
def resource(mod, env):
    return mod.Auth0Resource(
        tenant_domain="mycompany.us.auth0.com",
        client_id_env_var="AUTH0_CLIENT_ID",
        client_secret_env_var="AUTH0_CLIENT_SECRET",
    )


def _patch_token(monkeypatch, token="tok-1", expires_in=3600):
    import requests

    def fake_post(url, json=None, headers=None, timeout=None, data=None):
        assert url == "https://mycompany.us.auth0.com/oauth/token"
        assert json["grant_type"] == "client_credentials"
        assert json["audience"] == "https://mycompany.us.auth0.com/api/v2/"
        return FakeResponse({"access_token": token, "expires_in": expires_in})

    monkeypatch.setattr(requests, "post", fake_post)
    return fake_post


# --- token acquisition ------------------------------------------------------

def test_get_access_token_fetches_and_caches(resource, monkeypatch):
    import requests

    calls = []

    def fake_post(url, json=None, headers=None, timeout=None):
        calls.append(url)
        return FakeResponse({"access_token": "tok-1", "expires_in": 3600})

    monkeypatch.setattr(requests, "post", fake_post)

    token1 = resource._get_access_token()
    token2 = resource._get_access_token()

    assert token1 == token2 == "tok-1"
    assert len(calls) == 1  # second call hit the cache


def test_get_access_token_refetches_once_expired(resource, monkeypatch):
    import requests

    calls = []

    def fake_post(url, json=None, headers=None, timeout=None):
        calls.append(url)
        return FakeResponse({"access_token": f"tok-{len(calls)}", "expires_in": 3600})

    monkeypatch.setattr(requests, "post", fake_post)

    token1 = resource._get_access_token()
    cache_key = f"{resource.tenant_domain}:{resource.client_id_env_var}"
    resource._token_cache[cache_key]["expires"] = time.time() - 1
    token2 = resource._get_access_token()

    assert token1 == "tok-1"
    assert token2 == "tok-2"
    assert len(calls) == 2


def test_get_access_token_missing_env_vars_raises(mod, monkeypatch):
    monkeypatch.delenv("AUTH0_CLIENT_ID", raising=False)
    monkeypatch.delenv("AUTH0_CLIENT_SECRET", raising=False)
    resource = mod.Auth0Resource(
        tenant_domain="mycompany.us.auth0.com",
        client_id_env_var="AUTH0_CLIENT_ID",
        client_secret_env_var="AUTH0_CLIENT_SECRET",
    )
    with pytest.raises(RuntimeError, match="Missing Auth0 M2M credentials"):
        resource._get_access_token()


# Auth0's convenience methods (find_user_by_email/create_user/update_user/
# set_blocked) call `session.get/post/patch(...)` on the `requests.Session`
# returned by `get_client()` -- NOT the top-level `requests.get/post/patch`
# functions (those are only used by `_get_access_token`). So exercising
# them for real means patching `requests.Session.get/post/patch` instead.

# --- find_user_by_email -----------------------------------------------------

def test_find_user_by_email_builds_request(resource, monkeypatch):
    import requests

    _patch_token(monkeypatch)
    get_calls = []

    def fake_get(self, url, params=None, timeout=None):
        get_calls.append({"url": url, "params": params, "headers": dict(self.headers)})
        return FakeResponse([{"user_id": "auth0|abc", "email": "jane@example.com"}])

    monkeypatch.setattr(requests.Session, "get", fake_get)

    result = resource.find_user_by_email("jane@example.com")

    assert result == [{"user_id": "auth0|abc", "email": "jane@example.com"}]
    assert get_calls[0]["url"] == "https://mycompany.us.auth0.com/api/v2/users-by-email"
    assert get_calls[0]["params"] == {"email": "jane@example.com"}
    assert get_calls[0]["headers"]["Authorization"] == "Bearer tok-1"


# --- create_user -------------------------------------------------------------

def test_create_user_posts_payload(resource, monkeypatch):
    import requests

    _patch_token(monkeypatch)
    post_calls = []

    def fake_post(self, url, json=None, timeout=None):
        post_calls.append({"url": url, "json": json})
        return FakeResponse({"user_id": "auth0|new", **json})

    monkeypatch.setattr(requests.Session, "post", fake_post)

    payload = {"connection": "Username-Password-Authentication", "email": "new@example.com", "name": "New User"}
    result = resource.create_user(payload)

    assert result["user_id"] == "auth0|new"
    assert post_calls[0]["url"] == "https://mycompany.us.auth0.com/api/v2/users"
    assert post_calls[0]["json"] == payload


# --- update_user vs set_blocked: structurally distinct code paths -----------

def test_update_user_patches_only_given_payload(resource, monkeypatch):
    import requests

    _patch_token(monkeypatch)
    patch_calls = []

    def fake_patch(self, url, json=None, timeout=None):
        patch_calls.append({"url": url, "json": json})
        return FakeResponse({"user_id": "auth0|abc", **json})

    monkeypatch.setattr(requests.Session, "patch", fake_patch)

    resource.update_user("auth0|abc", {"name": "Updated Name"})

    assert patch_calls[0]["url"] == "https://mycompany.us.auth0.com/api/v2/users/auth0|abc"
    assert patch_calls[0]["json"] == {"name": "Updated Name"}
    assert "blocked" not in patch_calls[0]["json"]


def test_set_blocked_sends_only_blocked_field(resource, monkeypatch):
    import requests

    _patch_token(monkeypatch)
    patch_calls = []

    def fake_patch(self, url, json=None, timeout=None):
        patch_calls.append({"url": url, "json": json})
        return FakeResponse({"user_id": "auth0|abc", "blocked": json["blocked"]})

    monkeypatch.setattr(requests.Session, "patch", fake_patch)

    resource.set_blocked("auth0|abc", True)

    assert patch_calls[0]["url"] == "https://mycompany.us.auth0.com/api/v2/users/auth0|abc"
    # Exactly one key -- set_blocked can never smuggle other profile fields
    # through the same call as a deactivation.
    assert patch_calls[0]["json"] == {"blocked": True}


def test_resource_has_no_delete_method():
    """Structural guarantee: there is no code path on this resource that can
    permanently delete an Auth0 user, regardless of how a caller configures
    anything upstream. (The word "DELETE" legitimately appears in the
    module's own docstrings, explaining why it's NOT implemented -- so this
    checks for an actual outbound `.delete(` call / a `delete_user` method
    in the source, not for the substring.)"""
    import pathlib

    mod = load_component_module()
    assert not hasattr(mod.Auth0Resource, "delete_user")
    source = pathlib.Path(__file__).resolve().parent.parent.joinpath("component.py").read_text()
    assert ".delete(" not in source
    assert "def delete" not in source


# --- component registration --------------------------------------------------

def test_component_registers_resource_under_resource_key(mod):
    component = mod.Auth0ResourceComponent(
        resource_key="auth0_custom",
        tenant_domain="mycompany.us.auth0.com",
        client_id_env_var="AUTH0_CLIENT_ID",
        client_secret_env_var="AUTH0_CLIENT_SECRET",
    )
    defs = component.build_defs(context=None)
    assert "auth0_custom" in defs.resources
    registered = defs.resources["auth0_custom"]
    assert isinstance(registered, mod.Auth0Resource)
    assert registered.tenant_domain == "mycompany.us.auth0.com"


def test_component_defaults_resource_key(mod):
    component = mod.Auth0ResourceComponent(
        tenant_domain="mycompany.us.auth0.com",
        client_id_env_var="AUTH0_CLIENT_ID",
        client_secret_env_var="AUTH0_CLIENT_SECRET",
    )
    defs = component.build_defs(context=None)
    assert "auth0_resource" in defs.resources
