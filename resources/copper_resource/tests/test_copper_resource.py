"""Committed regression tests for CopperResource.

Monkeypatches `requests.get` / `requests.post` / `requests.put` wholesale
(the one external, paid-API boundary) -- header construction, URL
building, retry/backoff, search()/upsert() orchestration are all
exercised for real.
"""
import time

import pytest

from .conftest import FakeResponse, load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def env(monkeypatch):
    monkeypatch.setenv("COPPER_API_KEY", "key-123")


@pytest.fixture()
def resource(mod, env):
    return mod.CopperResource(
        api_key_env_var="COPPER_API_KEY",
        user_email="svc@company.com",
    )


# --- headers ----------------------------------------------------------------

def test_headers_include_all_four_required(resource):
    headers = resource._headers()
    assert headers == {
        "X-PW-AccessToken": "key-123",
        "X-PW-Application": "developer_api",
        "X-PW-UserEmail": "svc@company.com",
        "Content-Type": "application/json",
    }


# --- get() --------------------------------------------------------------

def test_get_builds_url_sends_headers_and_params(mod, resource, monkeypatch):
    import requests

    calls = []

    def fake_get(url, params=None, headers=None, timeout=None):
        calls.append({"url": url, "params": params, "headers": headers, "timeout": timeout})
        return FakeResponse({"id": 1, "name": "Jane"})

    monkeypatch.setattr(requests, "get", fake_get)

    result = resource.get("/people/1")

    assert result == {"id": 1, "name": "Jane"}
    assert len(calls) == 1
    assert calls[0]["url"] == "https://api.copper.com/developer_api/v1/people/1"
    assert calls[0]["headers"]["X-PW-AccessToken"] == "key-123"
    assert calls[0]["headers"]["X-PW-Application"] == "developer_api"
    assert calls[0]["headers"]["X-PW-UserEmail"] == "svc@company.com"


def test_get_normalizes_path_without_leading_slash(mod, resource, monkeypatch):
    import requests
    calls = []

    def fake_get(url, params=None, headers=None, timeout=None):
        calls.append(url)
        return FakeResponse({})

    monkeypatch.setattr(requests, "get", fake_get)
    resource.get("people/1")
    assert calls[0] == "https://api.copper.com/developer_api/v1/people/1"


# --- post() / put() ----------------------------------------------------------

def test_post_sends_json_body(mod, resource, monkeypatch):
    import requests
    calls = []

    def fake_post(url, json=None, headers=None, timeout=None):
        calls.append({"url": url, "json": json})
        return FakeResponse({"id": 42})

    monkeypatch.setattr(requests, "post", fake_post)
    result = resource.post("/people", json_body={"name": "Jane"})

    assert result == {"id": 42}
    assert calls[0]["url"] == "https://api.copper.com/developer_api/v1/people"
    assert calls[0]["json"] == {"name": "Jane"}


def test_put_sends_json_body(mod, resource, monkeypatch):
    import requests
    calls = []

    def fake_put(url, json=None, headers=None, timeout=None):
        calls.append({"url": url, "json": json})
        return FakeResponse({"id": 42})

    monkeypatch.setattr(requests, "put", fake_put)
    result = resource.put("/people/42", json_body={"name": "Jane Updated"})

    assert result == {"id": 42}
    assert calls[0]["url"] == "https://api.copper.com/developer_api/v1/people/42"
    assert calls[0]["json"] == {"name": "Jane Updated"}


# --- retry / backoff ----------------------------------------------------------

def test_retries_on_429_then_succeeds(mod, resource, monkeypatch):
    import requests
    monkeypatch.setattr(mod.time, "sleep", lambda s: None)
    responses = [FakeResponse(status_code=429, headers={}), FakeResponse({"id": 1})]
    calls = []

    def fake_get(url, params=None, headers=None, timeout=None):
        calls.append(url)
        return responses.pop(0)

    monkeypatch.setattr(requests, "get", fake_get)

    result = resource.get("/people/1")
    assert result == {"id": 1}
    assert len(calls) == 2


def test_retries_exhausted_raises(mod, resource, monkeypatch):
    import requests
    monkeypatch.setattr(mod.time, "sleep", lambda s: None)

    def fake_get(url, params=None, headers=None, timeout=None):
        return FakeResponse(status_code=500, headers={})

    monkeypatch.setattr(requests, "get", fake_get)
    with pytest.raises(RuntimeError, match="HTTP 500"):
        resource.get("/people/1")


def test_retry_after_header_used_as_delay(mod, resource, monkeypatch):
    import requests
    sleep_calls = []
    monkeypatch.setattr(mod.time, "sleep", lambda s: sleep_calls.append(s))

    responses = [
        FakeResponse(status_code=429, headers={"Retry-After": "7"}),
        FakeResponse({"id": 1}),
    ]

    def fake_get(url, params=None, headers=None, timeout=None):
        return responses.pop(0)

    monkeypatch.setattr(requests, "get", fake_get)
    result = resource.get("/people/1")
    assert result == {"id": 1}
    assert sleep_calls == [7.0]


# --- search() -----------------------------------------------------------------

def test_search_returns_list(mod, resource, monkeypatch):
    import requests

    def fake_post(url, json=None, headers=None, timeout=None):
        assert url == "https://api.copper.com/developer_api/v1/people/search"
        assert json == {"emails": ["a@b.com"]}
        return FakeResponse([{"id": 1, "name": "Jane"}])

    monkeypatch.setattr(requests, "post", fake_post)
    result = resource.search("people", {"emails": ["a@b.com"]})
    assert result == [{"id": 1, "name": "Jane"}]


def test_search_returns_empty_list_when_no_match(mod, resource, monkeypatch):
    import requests

    def fake_post(url, json=None, headers=None, timeout=None):
        return FakeResponse([])

    monkeypatch.setattr(requests, "post", fake_post)
    result = resource.search("people", {"emails": ["nobody@b.com"]})
    assert result == []


def test_search_coerces_non_list_response_to_empty_list(mod, resource, monkeypatch):
    import requests

    def fake_post(url, json=None, headers=None, timeout=None):
        return FakeResponse({"unexpected": "shape"})

    monkeypatch.setattr(requests, "post", fake_post)
    result = resource.search("people", {"emails": ["a@b.com"]})
    assert result == []


# --- upsert() (search-then-write orchestration) --------------------------------

def test_upsert_creates_when_no_match(mod, resource, monkeypatch):
    import requests
    post_calls = []

    def fake_post(url, json=None, headers=None, timeout=None):
        post_calls.append({"url": url, "json": json})
        if url.endswith("/search"):
            return FakeResponse([])
        return FakeResponse({"id": 99})

    monkeypatch.setattr(requests, "post", fake_post)
    result = resource.upsert("people", {"emails": ["new@b.com"]}, {"name": "New Person"})

    assert result == {"action": "created", "id": 99}
    assert len(post_calls) == 2
    assert post_calls[0]["url"].endswith("/people/search")
    assert post_calls[1]["url"].endswith("/people")
    assert post_calls[1]["json"] == {"name": "New Person"}


def test_upsert_updates_when_match_found(mod, resource, monkeypatch):
    import requests
    put_calls = []

    def fake_post(url, json=None, headers=None, timeout=None):
        return FakeResponse([{"id": 7, "name": "Existing"}])

    def fake_put(url, json=None, headers=None, timeout=None):
        put_calls.append({"url": url, "json": json})
        return FakeResponse({"id": 7})

    monkeypatch.setattr(requests, "post", fake_post)
    monkeypatch.setattr(requests, "put", fake_put)
    result = resource.upsert("people", {"emails": ["existing@b.com"]}, {"name": "Existing Updated"})

    assert result == {"action": "updated", "id": 7}
    assert len(put_calls) == 1
    assert put_calls[0]["url"].endswith("/people/7")
    assert put_calls[0]["json"] == {"name": "Existing Updated"}


def test_upsert_uses_update_body_when_provided(mod, resource, monkeypatch):
    import requests
    put_calls = []

    def fake_post(url, json=None, headers=None, timeout=None):
        return FakeResponse([{"id": 7}])

    def fake_put(url, json=None, headers=None, timeout=None):
        put_calls.append(json)
        return FakeResponse({"id": 7})

    monkeypatch.setattr(requests, "post", fake_post)
    monkeypatch.setattr(requests, "put", fake_put)
    resource.upsert(
        "people", {"emails": ["x@b.com"]},
        create_body={"name": "Create Shape"},
        update_body={"name": "Update Shape"},
    )
    assert put_calls[0] == {"name": "Update Shape"}


# --- CopperResourceComponent registration ----------------------------------

def test_component_registers_resource_under_resource_key(mod):
    component = mod.CopperResourceComponent(
        resource_key="copper_custom",
        api_key_env_var="COPPER_API_KEY",
        user_email="svc@company.com",
    )
    defs = component.build_defs(context=None)
    assert "copper_custom" in defs.resources
    registered = defs.resources["copper_custom"]
    assert isinstance(registered, mod.CopperResource)
    assert registered.user_email == "svc@company.com"


def test_component_defaults_resource_key_to_copper(mod):
    component = mod.CopperResourceComponent(
        api_key_env_var="COPPER_API_KEY",
        user_email="svc@company.com",
    )
    defs = component.build_defs(context=None)
    assert "copper" in defs.resources
