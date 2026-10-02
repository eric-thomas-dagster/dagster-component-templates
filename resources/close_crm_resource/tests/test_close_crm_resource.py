"""Committed regression tests for CloseCrmResource / CloseCrmResourceComponent.

Monkeypatches `requests.request` wholesale (the one external, paid-API
boundary) -- auth, URL building, retry/backoff on 429 / 5xx, the Advanced
Filtering query DSL, and find-then-write upsert orchestration are all
exercised for real.
"""
import pytest

from .conftest import FakeResponse, load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def resource(mod):
    return mod.CloseCrmResource(api_key="sk_test_123", max_retries=3)


# --- auth / URL building ----------------------------------------------------

def test_get_sends_http_basic_auth_with_blank_password(mod, resource, monkeypatch):
    import requests

    calls = []

    def fake_request(method, url, auth=None, params=None, json=None, timeout=None):
        calls.append({"method": method, "url": url, "auth": auth, "params": params})
        return FakeResponse({"ok": True})

    monkeypatch.setattr(requests, "request", fake_request)

    result = resource.get("lead/", params={"_limit": 5})

    assert result == {"ok": True}
    assert len(calls) == 1
    assert calls[0]["method"] == "GET"
    assert calls[0]["url"] == "https://api.close.com/api/v1/lead/"
    # API key as username, blank password -- Close's standard REST auth.
    assert calls[0]["auth"] == ("sk_test_123", "")
    assert calls[0]["params"] == {"_limit": 5}


def test_post_sends_json_body(mod, resource, monkeypatch):
    import requests

    calls = []

    def fake_request(method, url, auth=None, params=None, json=None, timeout=None):
        calls.append({"method": method, "url": url, "json": json})
        return FakeResponse({"id": "lead_123"}, status_code=200)

    monkeypatch.setattr(requests, "request", fake_request)

    body = {"name": "Acme"}
    result = resource.post("lead/", body)

    assert result == {"id": "lead_123"}
    assert calls[0]["method"] == "POST"
    assert calls[0]["url"] == "https://api.close.com/api/v1/lead/"
    assert calls[0]["json"] == body


def test_put_hits_lead_id_path(mod, resource, monkeypatch):
    import requests

    calls = []

    def fake_request(method, url, auth=None, params=None, json=None, timeout=None):
        calls.append({"method": method, "url": url})
        return FakeResponse({"id": "lead_123", "name": "Updated"})

    monkeypatch.setattr(requests, "request", fake_request)

    resource.put("lead/lead_123/", {"name": "Updated"})
    assert calls[0]["method"] == "PUT"
    assert calls[0]["url"] == "https://api.close.com/api/v1/lead/lead_123/"


# --- retry / backoff ---------------------------------------------------------

def test_429_backs_off_using_rate_limit_reset_header_then_succeeds(mod, resource, monkeypatch):
    import requests

    sleeps = []
    monkeypatch.setattr(mod.time, "sleep", lambda s: sleeps.append(s))

    calls = []

    def fake_request(method, url, auth=None, params=None, json=None, timeout=None):
        calls.append(1)
        if len(calls) == 1:
            return FakeResponse(
                {"error": "rate limited"},
                status_code=429,
                headers={"RateLimit": "limit=100, remaining=0, reset=2"},
            )
        return FakeResponse({"ok": True})

    monkeypatch.setattr(requests, "request", fake_request)

    result = resource.get("lead/")
    assert result == {"ok": True}
    assert len(calls) == 2
    # Backed off using the RateLimit header's `reset` value, not a blind default.
    assert sleeps == [2.0]


def test_429_falls_back_to_retry_after_header_when_ratelimit_header_absent(mod, resource, monkeypatch):
    import requests

    sleeps = []
    monkeypatch.setattr(mod.time, "sleep", lambda s: sleeps.append(s))

    calls = []

    def fake_request(method, url, auth=None, params=None, json=None, timeout=None):
        calls.append(1)
        if len(calls) == 1:
            return FakeResponse({}, status_code=429, headers={"retry-after": "3"})
        return FakeResponse({"ok": True})

    monkeypatch.setattr(requests, "request", fake_request)

    result = resource.get("lead/")
    assert result == {"ok": True}
    assert sleeps == [3.0]


def test_5xx_retries_then_succeeds(mod, resource, monkeypatch):
    import requests

    monkeypatch.setattr(mod.time, "sleep", lambda s: None)
    calls = []

    def fake_request(method, url, auth=None, params=None, json=None, timeout=None):
        calls.append(1)
        if len(calls) < 2:
            return FakeResponse({}, status_code=503)
        return FakeResponse({"ok": True})

    monkeypatch.setattr(requests, "request", fake_request)
    result = resource.get("lead/")
    assert result == {"ok": True}
    assert len(calls) == 2


def test_exhausting_retries_on_429_raises(mod, monkeypatch):
    import requests

    resource = mod.CloseCrmResource(api_key="sk_test", max_retries=2)
    monkeypatch.setattr(mod.time, "sleep", lambda s: None)

    def fake_request(method, url, auth=None, params=None, json=None, timeout=None):
        return FakeResponse({}, status_code=429, headers={"retry-after": "1"})

    monkeypatch.setattr(requests, "request", fake_request)
    with pytest.raises(requests.HTTPError):
        resource.get("lead/")


# --- Advanced Filtering query DSL --------------------------------------------

def test_build_dedupe_query_email_uses_has_related_contact_email(mod, resource):
    query = resource._build_dedupe_query("email", "jane@example.com")
    assert query["type"] == "and"
    related = query["queries"][1]
    assert related["type"] == "has_related"
    assert related["related_object_type"] == "contact_email"
    cond = related["related_query"]["condition"]
    assert cond == {"type": "text", "value": "jane@example.com", "mode": "phrase"}


def test_build_dedupe_query_phone_uses_has_related_contact_phone(mod, resource):
    query = resource._build_dedupe_query("phone", "+14155552671")
    related = query["queries"][1]
    assert related["related_object_type"] == "contact_phone"
    assert related["related_query"]["field"]["field_name"] == "phone"


def test_build_dedupe_query_name_is_a_plain_field_condition(mod, resource):
    query = resource._build_dedupe_query("name", "Acme Co")
    inner = query["queries"][1]
    assert inner["type"] == "field_condition"
    assert inner["field"] == {"type": "regular_field", "object_type": "lead", "field_name": "name"}


def test_build_dedupe_query_custom_field_uses_custom_field_id_and_term_match(mod, resource):
    query = resource._build_dedupe_query("custom.cf_abc123", "ext-42")
    inner = query["queries"][1]
    assert inner["field"] == {"type": "custom_field", "custom_field_id": "cf_abc123"}
    assert inner["condition"] == {"type": "term", "values": ["ext-42"]}


def test_build_dedupe_query_unsupported_field_raises(mod, resource):
    with pytest.raises(ValueError, match="unsupported dedupe_field"):
        resource._build_dedupe_query("favorite_color", "blue")


# --- find_lead_by_query -------------------------------------------------------

def test_find_lead_by_query_returns_none_for_empty_value(mod, resource, monkeypatch):
    import requests

    def fake_request(*args, **kwargs):
        raise AssertionError("should not make a network call for an empty value")

    monkeypatch.setattr(requests, "request", fake_request)
    assert resource.find_lead_by_query("email", None) is None
    assert resource.find_lead_by_query("email", "   ") is None


def test_find_lead_by_query_posts_to_data_search_and_returns_first_match(mod, resource, monkeypatch):
    import requests

    calls = []

    def fake_request(method, url, auth=None, params=None, json=None, timeout=None):
        calls.append({"method": method, "url": url, "json": json})
        return FakeResponse({"data": [{"id": "lead_1", "__object_type": "lead"}], "cursor": None})

    monkeypatch.setattr(requests, "request", fake_request)

    match = resource.find_lead_by_query("email", "jane@example.com")
    assert match == {"id": "lead_1", "__object_type": "lead"}
    assert calls[0]["method"] == "POST"
    assert calls[0]["url"] == "https://api.close.com/api/v1/data/search/"
    assert calls[0]["json"]["_limit"] == 1
    assert "query" in calls[0]["json"]


def test_find_lead_by_query_returns_none_on_no_match(mod, resource, monkeypatch):
    import requests

    def fake_request(method, url, auth=None, params=None, json=None, timeout=None):
        return FakeResponse({"data": [], "cursor": None})

    monkeypatch.setattr(requests, "request", fake_request)
    assert resource.find_lead_by_query("email", "nobody@example.com") is None


# --- create_lead / update_lead / upsert_lead ----------------------------------

def test_create_lead_posts_to_lead_path(mod, resource, monkeypatch):
    import requests

    calls = []

    def fake_request(method, url, auth=None, params=None, json=None, timeout=None):
        calls.append({"method": method, "url": url, "json": json})
        return FakeResponse({"id": "lead_new", "name": "Acme"})

    monkeypatch.setattr(requests, "request", fake_request)
    result = resource.create_lead({"name": "Acme"})
    assert result == {"id": "lead_new", "name": "Acme"}
    assert calls[0]["url"] == "https://api.close.com/api/v1/lead/"
    assert calls[0]["method"] == "POST"


def test_update_lead_puts_to_lead_id_path(mod, resource, monkeypatch):
    import requests

    calls = []

    def fake_request(method, url, auth=None, params=None, json=None, timeout=None):
        calls.append({"method": method, "url": url})
        return FakeResponse({"id": "lead_123", "name": "Updated"})

    monkeypatch.setattr(requests, "request", fake_request)
    result = resource.update_lead("lead_123", {"name": "Updated"})
    assert result == {"id": "lead_123", "name": "Updated"}
    assert calls[0]["url"] == "https://api.close.com/api/v1/lead/lead_123/"
    assert calls[0]["method"] == "PUT"


def test_upsert_lead_creates_when_no_match_found(mod, resource, monkeypatch):
    import requests

    calls = []

    def fake_request(method, url, auth=None, params=None, json=None, timeout=None):
        calls.append(method)
        if url.endswith("/data/search/"):
            return FakeResponse({"data": [], "cursor": None})
        return FakeResponse({"id": "lead_new"})

    monkeypatch.setattr(requests, "request", fake_request)
    result = resource.upsert_lead("email", "new@example.com", {"name": "New Co"})
    assert result == {"action": "created", "id": "lead_new"}
    assert calls == ["POST", "POST"]  # search, then create


def test_upsert_lead_updates_when_match_found(mod, resource, monkeypatch):
    import requests

    calls = []

    def fake_request(method, url, auth=None, params=None, json=None, timeout=None):
        calls.append((method, url))
        if url.endswith("/data/search/"):
            return FakeResponse({"data": [{"id": "lead_existing"}], "cursor": None})
        return FakeResponse({"id": "lead_existing", "name": "Updated Co"})

    monkeypatch.setattr(requests, "request", fake_request)
    result = resource.upsert_lead("email", "existing@example.com", {"name": "Updated Co"})
    assert result == {"action": "updated", "id": "lead_existing"}
    assert calls[0][0] == "POST"  # search
    assert calls[1] == ("PUT", "https://api.close.com/api/v1/lead/lead_existing/")


def test_upsert_lead_uses_update_body_over_create_body_when_updating(mod, resource, monkeypatch):
    import requests

    put_bodies = []

    def fake_request(method, url, auth=None, params=None, json=None, timeout=None):
        if url.endswith("/data/search/"):
            return FakeResponse({"data": [{"id": "lead_existing"}], "cursor": None})
        if method == "PUT":
            put_bodies.append(json)
        return FakeResponse({"id": "lead_existing"})

    monkeypatch.setattr(requests, "request", fake_request)
    resource.upsert_lead(
        "email",
        "existing@example.com",
        create_body={"name": "Create Shape", "contacts": []},
        update_body={"name": "Update Shape Only"},
    )
    assert put_bodies == [{"name": "Update Shape Only"}]


# --- CloseCrmResourceComponent registration -----------------------------------

def test_component_registers_resource_under_resource_key(mod, monkeypatch):
    monkeypatch.setenv("CLOSE_API_KEY", "sk_abc")
    component = mod.CloseCrmResourceComponent(
        resource_key="close_custom",
        api_key_env_var="CLOSE_API_KEY",
    )
    defs = component.build_defs(context=None)
    assert "close_custom" in defs.resources
    registered = defs.resources["close_custom"]
    assert isinstance(registered, mod.CloseCrmResource)
    assert registered.api_key == "sk_abc"


def test_component_defaults_resource_key_to_close_crm(mod, monkeypatch):
    monkeypatch.setenv("CLOSE_API_KEY", "sk_abc")
    component = mod.CloseCrmResourceComponent(api_key_env_var="CLOSE_API_KEY")
    defs = component.build_defs(context=None)
    assert "close_crm" in defs.resources
