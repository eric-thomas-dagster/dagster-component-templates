"""Committed regression tests for PayscaleResource / PayscaleResourceComponent.

Monkeypatches the component module's one network-boundary function,
`_payscale_http_request` -- token acquisition/caching, the submit-then-poll
report flow, and error handling are all exercised for real.
"""
import time

import pytest

from .conftest import FakeResponse, load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def env(monkeypatch):
    monkeypatch.setenv("PAYSCALE_CLIENT_ID", "client-123")
    monkeypatch.setenv("PAYSCALE_CLIENT_SECRET", "secret-456")


@pytest.fixture()
def resource(mod, env):
    return mod.PayscaleResource(
        customer_id="cust-789",
        client_id_env_var="PAYSCALE_CLIENT_ID",
        client_secret_env_var="PAYSCALE_CLIENT_SECRET",
        poll_interval_seconds=0.0,
        poll_timeout_seconds=1.0,
    )


def _patch_token(monkeypatch, mod, token="tok-1", expires_in=600):
    calls = []

    def fake(method, url, **kwargs):
        if url == mod.DEFAULT_TOKEN_URL:
            calls.append((method, url, kwargs))
            return FakeResponse({"access_token": token, "expires_in": expires_in})
        raise AssertionError(f"unexpected token call to {url}")

    monkeypatch.setattr(mod, "_payscale_http_request", fake)
    return calls


# --- token acquisition -------------------------------------------------------

def test_get_access_token_fetches_and_caches(resource, mod, monkeypatch):
    calls = _patch_token(monkeypatch, mod)

    token1 = resource._get_access_token()
    token2 = resource._get_access_token()

    assert token1 == token2 == "tok-1"
    assert len(calls) == 1  # second call hit the cache


def test_get_access_token_refetches_once_expired(resource, mod, monkeypatch):
    calls = []

    def fake(method, url, **kwargs):
        calls.append(url)
        return FakeResponse({"access_token": f"tok-{len(calls)}", "expires_in": 600})

    monkeypatch.setattr(mod, "_payscale_http_request", fake)

    token1 = resource._get_access_token()
    cache_key = f"{resource.token_url}:{resource.client_id_env_var}"
    resource._token_cache[cache_key]["expires"] = time.time() - 1
    token2 = resource._get_access_token()

    assert token1 == "tok-1"
    assert token2 == "tok-2"
    assert len(calls) == 2


def test_get_access_token_missing_env_vars_raises(mod, monkeypatch):
    monkeypatch.delenv("PAYSCALE_CLIENT_ID", raising=False)
    monkeypatch.delenv("PAYSCALE_CLIENT_SECRET", raising=False)
    resource = mod.PayscaleResource(
        customer_id="cust-789",
        client_id_env_var="PAYSCALE_CLIENT_ID",
        client_secret_env_var="PAYSCALE_CLIENT_SECRET",
    )
    with pytest.raises(RuntimeError, match="Missing PayScale OAuth2 credentials"):
        resource._get_access_token()


def test_token_request_uses_client_credentials_grant(resource, mod, monkeypatch):
    calls = _patch_token(monkeypatch, mod)
    resource._get_access_token()
    _, url, kwargs = calls[0]
    assert url == "https://accounts.payscale.com/connect/token"
    assert kwargs["data"]["grant_type"] == "client_credentials"
    assert kwargs["data"]["scope"] == "jobalyzer"
    assert kwargs["data"]["client_id"] == "client-123"
    assert kwargs["data"]["client_secret"] == "secret-456"


# --- submit_report_request ---------------------------------------------------

def test_submit_report_request_posts_answers_and_customer_id(resource, mod, monkeypatch):
    _patch_token(monkeypatch, mod)
    submit_calls = []

    def fake(method, url, **kwargs):
        if url == mod.DEFAULT_TOKEN_URL:
            return FakeResponse({"access_token": "tok-1", "expires_in": 600})
        submit_calls.append({"method": method, "url": url, "kwargs": kwargs})
        return FakeResponse(
            {
                "Links": {"PayReport": "https://jobalyzer.payscale.com/jobalyzer/v1/reports/abc/pay"},
                "Warnings": None,
                "Errors": None,
            }
        )

    monkeypatch.setattr(mod, "_payscale_http_request", fake)

    answers = {"JobTitle": "Software Developer", "City": "Seattle", "Country": "United States"}
    result = resource.submit_report_request(answers, requested_reports=["pay"])

    assert result["Links"]["PayReport"].endswith("/pay")
    assert len(submit_calls) == 1
    call = submit_calls[0]
    assert call["method"] == "POST"
    assert call["url"] == "https://jobalyzer.payscale.com/jobalyzer/v1/reports"
    assert call["kwargs"]["json_body"]["customerId"] == "cust-789"
    assert call["kwargs"]["json_body"]["user"] == "cust-789"
    assert call["kwargs"]["json_body"]["answers"] == answers
    assert call["kwargs"]["json_body"]["requestedReports"] == ["pay"]
    assert call["kwargs"]["headers"]["Authorization"] == "Bearer tok-1"


# --- poll_until_ready ---------------------------------------------------------

def test_poll_until_ready_retries_on_202_then_returns_200(resource, mod, monkeypatch):
    responses = [FakeResponse({}, status_code=202), FakeResponse({"BasePayReport": {"Percentile50": 95000}})]

    def fake(method, url, **kwargs):
        if url == mod.DEFAULT_TOKEN_URL:
            return FakeResponse({"access_token": "tok-1", "expires_in": 600})
        return responses.pop(0)

    monkeypatch.setattr(mod, "_payscale_http_request", fake)

    result = resource.poll_until_ready("https://jobalyzer.payscale.com/jobalyzer/v1/reports/abc/pay")

    assert result == {"BasePayReport": {"Percentile50": 95000}}
    assert responses == []


def test_poll_until_ready_raises_on_error_status(resource, mod, monkeypatch):
    def fake(method, url, **kwargs):
        if url == mod.DEFAULT_TOKEN_URL:
            return FakeResponse({"access_token": "tok-1", "expires_in": 600})
        return FakeResponse({"error": "bad_request"}, status_code=400)

    monkeypatch.setattr(mod, "_payscale_http_request", fake)

    with pytest.raises(RuntimeError, match="HTTP 400"):
        resource.poll_until_ready("https://jobalyzer.payscale.com/jobalyzer/v1/reports/abc/pay")


def test_poll_until_ready_times_out_when_always_pending(resource, mod, monkeypatch):
    def fake(method, url, **kwargs):
        if url == mod.DEFAULT_TOKEN_URL:
            return FakeResponse({"access_token": "tok-1", "expires_in": 600})
        return FakeResponse({}, status_code=202)

    monkeypatch.setattr(mod, "_payscale_http_request", fake)

    with pytest.raises(TimeoutError):
        resource.poll_until_ready("https://jobalyzer.payscale.com/jobalyzer/v1/reports/abc/pay")


# --- get_pay_report convenience ----------------------------------------------

def test_get_pay_report_submits_then_polls(resource, mod, monkeypatch):
    calls = []

    def fake(method, url, **kwargs):
        calls.append(url)
        if url == mod.DEFAULT_TOKEN_URL:
            return FakeResponse({"access_token": "tok-1", "expires_in": 600})
        if url == "https://jobalyzer.payscale.com/jobalyzer/v1/reports":
            return FakeResponse(
                {
                    "Links": {"PayReport": "https://jobalyzer.payscale.com/jobalyzer/v1/reports/abc/pay"},
                    "Errors": None,
                }
            )
        if url == "https://jobalyzer.payscale.com/jobalyzer/v1/reports/abc/pay":
            return FakeResponse(
                {
                    "BasePayReport": {
                        "Percentile10": 70000,
                        "Percentile50": 95000,
                        "Percentile90": 130000,
                        "CurrencyName": "USD",
                    },
                    "ReportRating": 0.9,
                    "Context": {"MatchedJobTitle": "Software Developer I", "JobTitleRating": 0.95},
                }
            )
        raise AssertionError(f"unexpected url {url}")

    monkeypatch.setattr(mod, "_payscale_http_request", fake)

    report = resource.get_pay_report({"JobTitle": "Software Developer", "Country": "United States"})

    assert report["BasePayReport"]["Percentile50"] == 95000
    assert report["Context"]["MatchedJobTitle"] == "Software Developer I"
    # token, submit, poll -- in that order
    assert calls == [
        mod.DEFAULT_TOKEN_URL,
        "https://jobalyzer.payscale.com/jobalyzer/v1/reports",
        "https://jobalyzer.payscale.com/jobalyzer/v1/reports/abc/pay",
    ]


def test_get_pay_report_raises_on_submit_errors(resource, mod, monkeypatch):
    def fake(method, url, **kwargs):
        if url == mod.DEFAULT_TOKEN_URL:
            return FakeResponse({"access_token": "tok-1", "expires_in": 600})
        return FakeResponse({"Links": None, "Errors": ["Invalid JobTitle"]})

    monkeypatch.setattr(mod, "_payscale_http_request", fake)

    with pytest.raises(RuntimeError, match="Invalid JobTitle"):
        resource.get_pay_report({"JobTitle": ""})


def test_get_pay_report_raises_when_link_missing(resource, mod, monkeypatch):
    def fake(method, url, **kwargs):
        if url == mod.DEFAULT_TOKEN_URL:
            return FakeResponse({"access_token": "tok-1", "expires_in": 600})
        return FakeResponse({"Links": {}, "Errors": None})

    monkeypatch.setattr(mod, "_payscale_http_request", fake)

    with pytest.raises(RuntimeError, match="no 'PayReport' link"):
        resource.get_pay_report({"JobTitle": "Software Developer"})


# --- component registration --------------------------------------------------

def test_component_registers_resource_under_resource_key(mod):
    component = mod.PayscaleResourceComponent(
        resource_key="payscale_custom",
        customer_id="cust-789",
    )
    defs = component.build_defs(context=None)
    assert "payscale_custom" in defs.resources
    registered = defs.resources["payscale_custom"]
    assert isinstance(registered, mod.PayscaleResource)
    assert registered.customer_id == "cust-789"


def test_component_defaults_resource_key(mod):
    component = mod.PayscaleResourceComponent(customer_id="cust-789")
    defs = component.build_defs(context=None)
    assert "payscale_resource" in defs.resources
