"""Committed regression tests for OktaResource.

Monkeypatches `requests.get`/`requests.post` wholesale (the one external,
paid-API boundary) -- token caching, URL building, 404-as-None semantics
on find_user, and the structural separation between update_user (profile
merge) and deactivate_user (the distinct lifecycle endpoint) are all
exercised for real.
"""
import base64
import time

import pytest

from .conftest import FakeResponse, load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def env(monkeypatch):
    monkeypatch.setenv("OKTA_CLIENT_ID", "client-123")
    monkeypatch.setenv("OKTA_CLIENT_SECRET", "secret-456")


@pytest.fixture()
def resource(mod, env):
    return mod.OktaResource(
        org_url="https://mycompany.okta.com",
        client_id_env_var="OKTA_CLIENT_ID",
        client_secret_env_var="OKTA_CLIENT_SECRET",
    )


def _patch_token(monkeypatch, token="tok-1", expires_in=3600):
    import requests

    def fake_post(url, headers=None, data=None, timeout=None, params=None, json=None):
        if url == "https://mycompany.okta.com/oauth2/v1/token":
            expected_basic = "Basic " + base64.b64encode(b"client-123:secret-456").decode("ascii")
            assert headers["Authorization"] == expected_basic
            assert data["grant_type"] == "client_credentials"
            assert data["scope"] == "okta.users.manage"
            return FakeResponse({"access_token": token, "expires_in": expires_in})
        raise AssertionError(f"unexpected POST to {url}")

    monkeypatch.setattr(requests, "post", fake_post)


# --- token acquisition --------------------------------------------------

def test_get_access_token_fetches_and_caches(resource, monkeypatch):
    import requests

    calls = []

    def fake_post(url, headers=None, data=None, timeout=None):
        calls.append(url)
        return FakeResponse({"access_token": "tok-1", "expires_in": 3600})

    monkeypatch.setattr(requests, "post", fake_post)

    token1 = resource._get_access_token()
    token2 = resource._get_access_token()

    assert token1 == token2 == "tok-1"
    assert len(calls) == 1


def test_get_access_token_refetches_once_expired(resource, monkeypatch):
    import requests

    calls = []

    def fake_post(url, headers=None, data=None, timeout=None):
        calls.append(url)
        return FakeResponse({"access_token": f"tok-{len(calls)}", "expires_in": 3600})

    monkeypatch.setattr(requests, "post", fake_post)

    token1 = resource._get_access_token()
    cache_key = f"{resource.org_url}:{resource.client_id_env_var}"
    resource._token_cache[cache_key]["expires"] = time.time() - 1
    token2 = resource._get_access_token()

    assert token1 == "tok-1"
    assert token2 == "tok-2"
    assert len(calls) == 2


def test_get_access_token_missing_env_vars_raises(mod, monkeypatch):
    monkeypatch.delenv("OKTA_CLIENT_ID", raising=False)
    monkeypatch.delenv("OKTA_CLIENT_SECRET", raising=False)
    resource = mod.OktaResource(
        org_url="https://mycompany.okta.com",
        client_id_env_var="OKTA_CLIENT_ID",
        client_secret_env_var="OKTA_CLIENT_SECRET",
    )
    with pytest.raises(RuntimeError, match="Missing Okta Service Integration credentials"):
        resource._get_access_token()


def test_get_access_token_uses_basic_auth_header(resource, monkeypatch):
    _patch_token(monkeypatch)
    token = resource._get_access_token()
    assert token == "tok-1"


# Okta's convenience methods (find_user/create_user/update_user/
# deactivate_user) call `session.get/post(...)` on the `requests.Session`
# returned by `get_client()` -- NOT the top-level `requests.get/post`
# functions (those are only used by `_get_access_token`). So exercising
# them for real means patching `requests.Session.get/post` instead.

# --- find_user: 404 -> None, not an exception ---------------------------

def test_find_user_returns_dict_when_found(resource, monkeypatch):
    import requests

    _patch_token(monkeypatch)
    get_calls = []

    def fake_get(self, url, timeout=None):
        get_calls.append(url)
        return FakeResponse({"id": "00u123", "status": "ACTIVE", "profile": {"login": "jane@example.com"}})

    monkeypatch.setattr(requests.Session, "get", fake_get)

    result = resource.find_user("jane@example.com")

    assert result["id"] == "00u123"
    assert get_calls[0] == "https://mycompany.okta.com/api/v1/users/jane@example.com"


def test_find_user_returns_none_on_404(resource, monkeypatch):
    import requests

    _patch_token(monkeypatch)

    def fake_get(self, url, timeout=None):
        return FakeResponse({"errorSummary": "Not found"}, status_code=404)

    monkeypatch.setattr(requests.Session, "get", fake_get)

    assert resource.find_user("ghost@example.com") is None


def test_find_user_raises_on_other_errors(resource, monkeypatch):
    import requests

    _patch_token(monkeypatch)

    def fake_get(self, url, timeout=None):
        return FakeResponse({"errorSummary": "boom"}, status_code=500)

    monkeypatch.setattr(requests.Session, "get", fake_get)

    with pytest.raises(RuntimeError, match="HTTP 500"):
        resource.find_user("jane@example.com")


# --- create_user ----------------------------------------------------------

def test_create_user_posts_profile_and_activate_param(resource, monkeypatch):
    import requests

    _patch_token(monkeypatch)
    post_calls = []

    def fake_post(self, url, params=None, json=None, timeout=None):
        post_calls.append({"url": url, "params": params, "json": json})
        return FakeResponse({"id": "00u-new", **json})

    monkeypatch.setattr(requests.Session, "post", fake_post)

    profile = {"login": "new@example.com", "email": "new@example.com", "firstName": "New"}
    result = resource.create_user(profile, activate=True)

    assert result["id"] == "00u-new"
    call = post_calls[0]
    assert call["url"] == "https://mycompany.okta.com/api/v1/users"
    assert call["params"] == {"activate": "true"}
    assert call["json"] == {"profile": profile}


# --- update_user (merge) vs deactivate_user: distinct endpoints ----------

def test_update_user_posts_to_user_id_with_profile_wrapper(resource, monkeypatch):
    import requests

    _patch_token(monkeypatch)
    post_calls = []

    def fake_post(self, url, params=None, json=None, timeout=None):
        post_calls.append({"url": url, "json": json, "params": params})
        return FakeResponse({"id": "00u123", **json})

    monkeypatch.setattr(requests.Session, "post", fake_post)

    resource.update_user("00u123", {"title": "Senior Engineer"})

    call = post_calls[0]
    assert call["url"] == "https://mycompany.okta.com/api/v1/users/00u123"
    assert call["json"] == {"profile": {"title": "Senior Engineer"}}
    # update_user must never hit the lifecycle path.
    assert "lifecycle" not in call["url"]


def test_deactivate_user_hits_distinct_lifecycle_endpoint(resource, monkeypatch):
    import requests

    _patch_token(monkeypatch)
    post_calls = []

    def fake_post(self, url, params=None, json=None, timeout=None):
        post_calls.append({"url": url, "params": params})
        return FakeResponse(None)

    monkeypatch.setattr(requests.Session, "post", fake_post)

    resource.deactivate_user("00u123", send_email=False)

    call = post_calls[0]
    assert call["url"] == "https://mycompany.okta.com/api/v1/users/00u123/lifecycle/deactivate"
    assert call["params"] == {"sendEmail": "false"}


def test_resource_has_no_delete_method():
    """Structural guarantee: there is no code path on this resource that can
    permanently delete an Okta user. (The word "DELETE" legitimately appears
    in the module's own docstrings, explaining why it's NOT implemented --
    so this checks for an actual outbound `.delete(` call / a `delete_user`
    method in the source, not for the substring.)"""
    import pathlib

    mod = load_component_module()
    assert not hasattr(mod.OktaResource, "delete_user")
    source = pathlib.Path(__file__).resolve().parent.parent.joinpath("component.py").read_text()
    assert ".delete(" not in source
    assert "def delete" not in source


# --- component registration ------------------------------------------------

def test_component_registers_resource_under_resource_key(mod):
    component = mod.OktaResourceComponent(
        resource_key="okta_custom",
        org_url="https://mycompany.okta.com",
        client_id_env_var="OKTA_CLIENT_ID",
        client_secret_env_var="OKTA_CLIENT_SECRET",
    )
    defs = component.build_defs(context=None)
    assert "okta_custom" in defs.resources
    registered = defs.resources["okta_custom"]
    assert isinstance(registered, mod.OktaResource)
    assert registered.org_url == "https://mycompany.okta.com"


def test_component_defaults_resource_key(mod):
    component = mod.OktaResourceComponent(
        org_url="https://mycompany.okta.com",
        client_id_env_var="OKTA_CLIENT_ID",
        client_secret_env_var="OKTA_CLIENT_SECRET",
    )
    defs = component.build_defs(context=None)
    assert "okta_resource" in defs.resources
