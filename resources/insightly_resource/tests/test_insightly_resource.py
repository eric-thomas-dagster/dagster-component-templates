"""Committed regression tests for InsightlyResource.

Monkeypatches `requests.request` wholesale (the one external, paid-API
boundary) -- URL/pod resolution, auth header construction, retry/backoff
on 429/5xx, search-then-create-or-update orchestration, and id derivation
are all exercised for real.
"""
import pytest

from .conftest import FakeResponse, load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def env(monkeypatch):
    monkeypatch.setenv("INSIGHTLY_API_KEY", "key-abc-123")


@pytest.fixture()
def resource(mod, env):
    return mod.InsightlyResource(api_key_env_var="INSIGHTLY_API_KEY", pod="na1", max_retries=3)


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    # Retry backoff uses time.sleep -- patch it out so retry tests are instant.
    import time as _time
    monkeypatch.setattr(_time, "sleep", lambda *_a, **_k: None)


# --- id derivation ---------------------------------------------------------

def test_derive_id_field_contacts(mod):
    assert mod._derive_id_field("Contacts") == "CONTACT_ID"


def test_derive_id_field_leads(mod):
    assert mod._derive_id_field("Leads") == "LEAD_ID"


def test_derive_id_field_organisations(mod):
    assert mod._derive_id_field("Organisations") == "ORGANISATION_ID"


# --- auth / env var ---------------------------------------------------------

def test_missing_api_key_env_var_raises(mod, monkeypatch):
    monkeypatch.delenv("INSIGHTLY_API_KEY", raising=False)
    resource = mod.InsightlyResource(api_key_env_var="INSIGHTLY_API_KEY")
    with pytest.raises(RuntimeError, match="INSIGHTLY_API_KEY"):
        resource.get("Contacts")


# --- .get() ------------------------------------------------------------------

def test_get_builds_pod_url_and_sends_basic_auth(mod, resource, monkeypatch):
    import requests

    calls = []

    def fake_request(method, url, auth=None, params=None, json=None, timeout=None):
        calls.append({"method": method, "url": url, "auth": auth, "params": params})
        return FakeResponse([{"CONTACT_ID": 1, "FIRST_NAME": "Jane"}])

    monkeypatch.setattr(requests, "request", fake_request)

    result = resource.get("Contacts", params={"top": 10})

    assert result == [{"CONTACT_ID": 1, "FIRST_NAME": "Jane"}]
    assert len(calls) == 1
    assert calls[0]["method"] == "GET"
    assert calls[0]["url"] == "https://api.na1.insightly.com/v3.1/Contacts"
    assert calls[0]["params"] == {"top": 10}
    # HTTPBasicAuth(key, "") -- requests represents it as a (user, pass) tuple-like object.
    assert calls[0]["auth"].username == "key-abc-123"
    assert calls[0]["auth"].password == ""


def test_get_returns_none_on_404(mod, resource, monkeypatch):
    import requests

    def fake_request(method, url, auth=None, params=None, json=None, timeout=None):
        return FakeResponse(None, status_code=404, content=False)

    monkeypatch.setattr(requests, "request", fake_request)
    assert resource.get("Contacts/99999") is None


# --- .post() / .put() --------------------------------------------------------

def test_post_sends_json_body_to_collection_endpoint(mod, resource, monkeypatch):
    import requests

    calls = []

    def fake_request(method, url, auth=None, params=None, json=None, timeout=None):
        calls.append({"method": method, "url": url, "json": json})
        return FakeResponse({"CONTACT_ID": 42, **(json or {})}, status_code=201)

    monkeypatch.setattr(requests, "request", fake_request)

    result = resource.post("/Contacts", {"FIRST_NAME": "New"})

    assert result == {"CONTACT_ID": 42, "FIRST_NAME": "New"}
    assert calls[0]["method"] == "POST"
    assert calls[0]["url"] == "https://api.na1.insightly.com/v3.1/Contacts"
    assert calls[0]["json"] == {"FIRST_NAME": "New"}


def test_put_sends_id_in_path(mod, resource, monkeypatch):
    import requests

    calls = []

    def fake_request(method, url, auth=None, params=None, json=None, timeout=None):
        calls.append({"method": method, "url": url})
        return FakeResponse({"CONTACT_ID": 42, "FIRST_NAME": "Updated"})

    monkeypatch.setattr(requests, "request", fake_request)

    result = resource.put("/Contacts/42", {"FIRST_NAME": "Updated"})

    assert result == {"CONTACT_ID": 42, "FIRST_NAME": "Updated"}
    assert calls[0]["method"] == "PUT"
    assert calls[0]["url"] == "https://api.na1.insightly.com/v3.1/Contacts/42"


# --- retry behavior ----------------------------------------------------------

def test_retries_on_429_then_succeeds(mod, resource, monkeypatch):
    import requests

    responses = [
        FakeResponse(None, status_code=429, headers={"X-RateLimit-Remaining": "0"}, content=False),
        FakeResponse([{"CONTACT_ID": 1}]),
    ]
    calls = []

    def fake_request(method, url, auth=None, params=None, json=None, timeout=None):
        calls.append(url)
        return responses.pop(0)

    monkeypatch.setattr(requests, "request", fake_request)

    result = resource.get("Contacts")
    assert result == [{"CONTACT_ID": 1}]
    assert len(calls) == 2


def test_exhausts_retries_on_persistent_429_and_raises(mod, env, monkeypatch):
    import requests

    resource = mod.InsightlyResource(api_key_env_var="INSIGHTLY_API_KEY", pod="na1", max_retries=2)

    def fake_request(method, url, auth=None, params=None, json=None, timeout=None):
        return FakeResponse(None, status_code=429, content=False)

    monkeypatch.setattr(requests, "request", fake_request)

    with pytest.raises(requests.HTTPError):
        resource.get("Contacts")


# --- .search() -----------------------------------------------------------

def test_search_builds_search_endpoint_and_params(mod, resource, monkeypatch):
    import requests

    calls = []

    def fake_request(method, url, auth=None, params=None, json=None, timeout=None):
        calls.append({"url": url, "params": params})
        return FakeResponse([{"CONTACT_ID": 7, "FIRST_NAME": "Match"}])

    monkeypatch.setattr(requests, "request", fake_request)

    result = resource.search("Contacts", "EMAIL_ADDRESS", "jane@example.com")

    assert result == [{"CONTACT_ID": 7, "FIRST_NAME": "Match"}]
    assert calls[0]["url"] == "https://api.na1.insightly.com/v3.1/Contacts/Search"
    assert calls[0]["params"] == {"field_name": "EMAIL_ADDRESS", "field_value": "jane@example.com"}


def test_search_returns_empty_list_on_no_match(mod, resource, monkeypatch):
    import requests

    def fake_request(method, url, auth=None, params=None, json=None, timeout=None):
        return FakeResponse(None, status_code=404, content=False)

    monkeypatch.setattr(requests, "request", fake_request)
    assert resource.search("Contacts", "EMAIL_ADDRESS", "nobody@example.com") == []


def test_search_wraps_single_dict_result_in_list(mod, resource, monkeypatch):
    import requests

    def fake_request(method, url, auth=None, params=None, json=None, timeout=None):
        return FakeResponse({"CONTACT_ID": 9})

    monkeypatch.setattr(requests, "request", fake_request)
    assert resource.search("Contacts", "EMAIL_ADDRESS", "x@y.com") == [{"CONTACT_ID": 9}]


# --- .upsert() (search-then-create-or-update) ---------------------------

def test_upsert_creates_when_no_match(mod, resource, monkeypatch):
    import requests

    calls = []

    def fake_request(method, url, auth=None, params=None, json=None, timeout=None):
        calls.append(method)
        if method == "GET":
            return FakeResponse([])
        return FakeResponse({"CONTACT_ID": 55, "FIRST_NAME": "Brand New"}, status_code=201)

    monkeypatch.setattr(requests, "request", fake_request)

    result = resource.upsert("Contacts", "EMAIL_ADDRESS", "new@example.com", {"FIRST_NAME": "Brand New"})

    assert calls == ["GET", "POST"]
    assert result == {"action": "created", "id": 55}


def test_upsert_updates_when_match_found(mod, resource, monkeypatch):
    import requests

    calls = []

    def fake_request(method, url, auth=None, params=None, json=None, timeout=None):
        calls.append((method, url))
        if method == "GET":
            return FakeResponse([{"CONTACT_ID": 77, "FIRST_NAME": "Old Name"}])
        return FakeResponse({"CONTACT_ID": 77, "FIRST_NAME": "New Name"})

    monkeypatch.setattr(requests, "request", fake_request)

    result = resource.upsert("Contacts", "EMAIL_ADDRESS", "existing@example.com", {"FIRST_NAME": "New Name"})

    assert [c[0] for c in calls] == ["GET", "PUT"]
    assert calls[1][1] == "https://api.na1.insightly.com/v3.1/Contacts/77"
    assert result == {"action": "updated", "id": 77}


def test_upsert_custom_object_type_id_field_override(mod, resource, monkeypatch):
    import requests

    def fake_request(method, url, auth=None, params=None, json=None, timeout=None):
        if method == "GET":
            return FakeResponse([])
        return FakeResponse({"CUSTOM_THING_ID": 3})

    monkeypatch.setattr(requests, "request", fake_request)

    result = resource.upsert(
        "CustomThings", "NAME", "widget", {"NAME": "widget"}, id_field="CUSTOM_THING_ID"
    )
    assert result == {"action": "created", "id": 3}


# --- InsightlyResourceComponent registration --------------------------------

def test_component_registers_resource_under_resource_key(mod):
    component = mod.InsightlyResourceComponent(
        resource_key="insightly_custom",
        api_key_env_var="INSIGHTLY_API_KEY",
        pod="eu1",
    )
    defs = component.build_defs(context=None)
    assert "insightly_custom" in defs.resources
    registered = defs.resources["insightly_custom"]
    assert isinstance(registered, mod.InsightlyResource)
    assert registered.pod == "eu1"


def test_component_defaults_resource_key_and_pod(mod):
    component = mod.InsightlyResourceComponent(api_key_env_var="INSIGHTLY_API_KEY")
    defs = component.build_defs(context=None)
    assert "insightly" in defs.resources
    assert defs.resources["insightly"].pod == "na1"
