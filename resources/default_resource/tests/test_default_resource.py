"""Committed regression tests for DefaultResource / DefaultResourceComponent.

No real network/`requests` calls are made here -- `requests.Session.request`
(the one method `DefaultResource._request` calls) is monkeypatched
wholesale, while URL building, auth header construction, the
email-vs-responses request shape, and non-2xx error handling are all
exercised for real.
"""
import pytest

from .conftest import FakeResponse, load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def env(monkeypatch):
    monkeypatch.setenv("DEFAULT_API_KEY", "sk_live_abc123")


@pytest.fixture()
def resource(mod, env):
    return mod.DefaultResource(api_key_env_var="DEFAULT_API_KEY")


def _patch_request(monkeypatch, handler):
    import requests

    def fake_request(self, method, url, timeout=None, **kwargs):
        return handler(method=method, url=url, timeout=timeout, **kwargs)

    monkeypatch.setattr(requests.Session, "request", fake_request)


# --- auth / client construction ------------------------------------------

def test_get_client_sets_bearer_header(resource):
    session = resource.get_client()
    assert session.headers["Authorization"] == "Bearer sk_live_abc123"
    assert session.headers["Content-Type"] == "application/json"


def test_get_client_missing_env_var_raises(mod, monkeypatch):
    monkeypatch.delenv("DEFAULT_API_KEY", raising=False)
    resource = mod.DefaultResource(api_key_env_var="DEFAULT_API_KEY")
    with pytest.raises(RuntimeError, match="is unset"):
        resource.get_client()


def test_default_base_url(resource):
    assert resource.base_url == "https://api.default.com"


# --- list_triggers ----------------------------------------------------------

def test_list_triggers_hits_v1_triggers_and_unwraps(resource, monkeypatch):
    calls = []

    def handler(method, url, timeout, **kwargs):
        calls.append((method, url))
        return FakeResponse({"triggers": [{"id": "t-1", "name": "Route Lead", "fields": []}]})

    _patch_request(monkeypatch, handler)

    triggers = resource.list_triggers()

    assert calls == [("GET", "https://api.default.com/v1/triggers")]
    assert triggers == [{"id": "t-1", "name": "Route Lead", "fields": []}]


# --- fire_trigger: the email-vs-responses shape is the real, confirmed one --

def test_fire_trigger_sends_email_as_top_level_field(resource, monkeypatch):
    calls = []

    def handler(method, url, timeout, **kwargs):
        calls.append({"method": method, "url": url, "json": kwargs.get("json")})
        return FakeResponse({"executionId": "exec-1", "outcome": {"type": "none"}})

    _patch_request(monkeypatch, handler)

    result = resource.fire_trigger("trig-123", email="jane@example.com")

    assert result["executionId"] == "exec-1"
    call = calls[0]
    assert call["method"] == "POST"
    assert call["url"] == "https://api.default.com/v1/triggers/trig-123"
    # email is a separate top-level field -- not nested under responses.
    assert call["json"] == {"email": "jane@example.com"}


def test_fire_trigger_nests_form_values_under_responses(resource, monkeypatch):
    calls = []

    def handler(method, url, timeout, **kwargs):
        calls.append(kwargs.get("json"))
        return FakeResponse({"executionId": "exec-2", "outcome": {"type": "none"}})

    _patch_request(monkeypatch, handler)

    resource.fire_trigger(
        "trig-123",
        email="jane@example.com",
        responses={"Company": "Acme", "Deal Size": 5000},
    )

    assert calls[0] == {
        "email": "jane@example.com",
        "responses": {"Company": "Acme", "Deal Size": 5000},
    }


def test_fire_trigger_includes_context_when_given(resource, monkeypatch):
    calls = []

    def handler(method, url, timeout, **kwargs):
        calls.append(kwargs.get("json"))
        return FakeResponse({"executionId": "exec-3", "outcome": {"type": "none"}})

    _patch_request(monkeypatch, handler)

    resource.fire_trigger(
        "trig-123",
        email="jane@example.com",
        context={"utmParams": {"utm_source": "newsletter"}},
    )

    assert calls[0]["context"] == {"utmParams": {"utm_source": "newsletter"}}


# --- get_available_slots / book_meeting ------------------------------------

def test_get_available_slots_posts_start_end(resource, monkeypatch):
    calls = []

    def handler(method, url, timeout, **kwargs):
        calls.append({"method": method, "url": url, "json": kwargs.get("json")})
        return FakeResponse({
            "reservationId": "res-1",
            "eventId": "evt-1",
            "expiresAt": "2026-10-08T12:00:00Z",
            "slots": ["2026-10-09T10:00:00Z"],
            "activeHost": None,
        })

    _patch_request(monkeypatch, handler)

    result = resource.get_available_slots("evt-1", "2026-10-09T00:00:00Z", "2026-10-10T00:00:00Z")

    assert result["reservationId"] == "res-1"
    call = calls[0]
    assert call["url"] == "https://api.default.com/v1/scheduling/events/evt-1/slots"
    assert call["json"] == {"start": "2026-10-09T00:00:00Z", "end": "2026-10-10T00:00:00Z"}


def test_book_meeting_required_fields_only(resource, monkeypatch):
    calls = []

    def handler(method, url, timeout, **kwargs):
        calls.append(kwargs.get("json"))
        return FakeResponse({"id": "mtg-1", "status": "scheduled"})

    _patch_request(monkeypatch, handler)

    resource.book_meeting(
        event="evt-1",
        start_time="2026-10-09T10:00:00Z",
        reservation_id="res-1",
        person_email="jane@example.com",
    )

    assert calls[0] == {
        "event": "evt-1",
        "startTime": "2026-10-09T10:00:00Z",
        "reservationId": "res-1",
        "personEmail": "jane@example.com",
    }


def test_book_meeting_passes_through_optional_fields(resource, monkeypatch):
    calls = []

    def handler(method, url, timeout, **kwargs):
        calls.append(kwargs.get("json"))
        return FakeResponse({"id": "mtg-2", "status": "scheduled"})

    _patch_request(monkeypatch, handler)

    resource.book_meeting(
        event="evt-1",
        start_time="2026-10-09T10:00:00Z",
        reservation_id="res-1",
        person_email="jane@example.com",
        guestFirstName="Jane",
        guestLastName="Doe",
        irrelevant_kwarg="should be dropped",
    )

    body = calls[0]
    assert body["guestFirstName"] == "Jane"
    assert body["guestLastName"] == "Doe"
    assert "irrelevant_kwarg" not in body


# --- error handling: non-2xx surfaces Default's structured error code -----

def test_request_error_includes_structured_code_and_message(resource, monkeypatch):
    def handler(method, url, timeout, **kwargs):
        return FakeResponse({"code": "MISSING_SCOPE", "message": "This API key is missing the required scope"}, status_code=403)

    _patch_request(monkeypatch, handler)

    with pytest.raises(RuntimeError, match="MISSING_SCOPE"):
        resource.fire_trigger("trig-123", email="jane@example.com")


def test_request_error_falls_back_to_raw_text_when_not_json(resource, monkeypatch):
    def handler(method, url, timeout, **kwargs):
        return FakeResponse(None, status_code=500, text="internal server error")

    _patch_request(monkeypatch, handler)

    with pytest.raises(RuntimeError, match="internal server error"):
        resource.fire_trigger("trig-123", email="jane@example.com")


def test_request_204_returns_empty_dict(resource, monkeypatch):
    def handler(method, url, timeout, **kwargs):
        return FakeResponse(None, status_code=204)

    _patch_request(monkeypatch, handler)

    result = resource._request("POST", "/v1/scheduling/meetings/mtg-1/cancel")
    assert result == {}


# --- component registration ------------------------------------------------

def test_component_registers_resource_under_resource_key(mod):
    component = mod.DefaultResourceComponent(
        resource_key="default_custom",
        api_key_env_var="DEFAULT_API_KEY",
    )
    defs = component.build_defs(context=None)
    assert "default_custom" in defs.resources
    registered = defs.resources["default_custom"]
    assert isinstance(registered, mod.DefaultResource)
    assert registered.api_key_env_var == "DEFAULT_API_KEY"


def test_component_defaults_resource_key_and_base_url(mod):
    component = mod.DefaultResourceComponent(api_key_env_var="DEFAULT_API_KEY")
    defs = component.build_defs(context=None)
    assert "default_resource" in defs.resources
    assert defs.resources["default_resource"].base_url == "https://api.default.com"
