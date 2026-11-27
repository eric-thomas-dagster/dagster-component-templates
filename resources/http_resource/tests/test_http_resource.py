"""Real, committed tests for HTTPResource.request() -- the reusable operator logic
extracted from the (now-removed) http_step_asset. Mocks only requests.Session.request /
the one external OAuth2 token-fetch call, matching this repo's "mock only the external
call" convention.
"""
import dagster as dg
import pytest

from .conftest import FakeResponse, run_request


def test_basic_get_success(mod, monkeypatch):
    def fake_request(self, method, url, **kwargs):
        assert method == "GET"
        assert url == "https://api.example.com/v1/ping"
        return FakeResponse(status_code=200)

    monkeypatch.setattr("requests.Session.request", fake_request)
    component = mod.HTTPResourceComponent(base_url="https://api.example.com/v1")
    result, success = run_request(mod, component)
    assert success and result.success and result.status_code == 200


def test_auth_header_value_env_var_sets_header(mod, monkeypatch):
    monkeypatch.setenv("MY_AUTH_HEADER", "Bearer raw-header-token")
    seen = []

    def fake_request(self, method, url, **kwargs):
        seen.append(dict(self.headers))
        return FakeResponse(status_code=200)

    monkeypatch.setattr("requests.Session.request", fake_request)
    component = mod.HTTPResourceComponent(base_url="https://api.example.com", auth_header_env_var="MY_AUTH_HEADER")
    _, success = run_request(mod, component)
    assert success
    assert seen[0]["Authorization"] == "Bearer raw-header-token"


def test_auth_type_basic_sets_session_auth(mod, monkeypatch):
    monkeypatch.setenv("U", "alice")
    monkeypatch.setenv("P", "s3cr3t")
    seen_auth = []

    def fake_request(self, method, url, **kwargs):
        seen_auth.append(self.auth)
        return FakeResponse(status_code=200)

    monkeypatch.setattr("requests.Session.request", fake_request)
    component = mod.HTTPResourceComponent(
        base_url="https://api.example.com", auth_type="basic",
        auth_username_env_var="U", auth_password_env_var="P",
    )
    _, success = run_request(mod, component)
    assert success
    assert seen_auth[0] == ("alice", "s3cr3t")


def test_auth_type_basic_requires_both_env_vars(mod):
    component = mod.HTTPResourceComponent(base_url="https://api.example.com", auth_type="basic", auth_username_env_var="U")
    with pytest.raises(ValueError, match="auth_type='basic'"):
        component.build_defs(context=None)


def test_auth_type_bearer_sets_authorization_header(mod, monkeypatch):
    monkeypatch.setenv("TOK", "s3cr3t-tok")
    seen = []

    def fake_request(self, method, url, **kwargs):
        seen.append(dict(self.headers))
        return FakeResponse(status_code=200)

    monkeypatch.setattr("requests.Session.request", fake_request)
    component = mod.HTTPResourceComponent(base_url="https://api.example.com", auth_type="bearer", auth_token_env_var="TOK")
    _, success = run_request(mod, component)
    assert success
    assert seen[0]["Authorization"] == "Bearer s3cr3t-tok"


def test_header_and_body_mutually_exclusive_auth_config(mod):
    component = mod.HTTPResourceComponent(base_url="https://api.example.com", auth_header_env_var="X", auth_type="bearer", auth_token_env_var="TOK")
    with pytest.raises(ValueError, match="not both"):
        component.build_defs(context=None)


def test_oauth2_client_credentials_fetches_and_caches_token(mod, monkeypatch):
    token_calls = []

    def fake_token_fetch(token_url, client_id, client_secret, scope, audience):
        token_calls.append((token_url, client_id, client_secret))
        return {"access_token": "tok-123", "expires_in": 3600}

    monkeypatch.setattr(mod, "_fetch_oauth2_client_credentials_token", fake_token_fetch)
    monkeypatch.setattr(mod, "_OAUTH2_TOKEN_CACHE", {})

    seen_headers = []

    def fake_request(self, method, url, **kwargs):
        seen_headers.append(dict(self.headers))
        return FakeResponse(status_code=200)

    monkeypatch.setattr("requests.Session.request", fake_request)
    monkeypatch.setenv("OAUTH_CID", "client-id")
    monkeypatch.setenv("OAUTH_SECRET", "client-secret")
    component = mod.HTTPResourceComponent(
        base_url="https://api.example.com", auth_type="oauth2_client_credentials",
        oauth2_token_url="https://auth.example.com/oauth/token",
        oauth2_client_id_env_var="OAUTH_CID", oauth2_client_secret_env_var="OAUTH_SECRET",
    )
    _, success = run_request(mod, component)
    assert success
    assert seen_headers[0]["Authorization"] == "Bearer tok-123"
    assert len(token_calls) == 1

    # Second materialization within the same process reuses the cached token.
    _, success2 = run_request(mod, component)
    assert success2
    assert len(token_calls) == 1


def test_oauth2_requires_all_three_fields(mod):
    component = mod.HTTPResourceComponent(base_url="https://api.example.com", auth_type="oauth2_client_credentials")
    with pytest.raises(ValueError, match="oauth2_client_credentials"):
        component.build_defs(context=None)


def test_retries_on_retryable_status_then_succeeds(mod, monkeypatch):
    attempts = []

    def fake_request(self, method, url, **kwargs):
        attempts.append(1)
        if len(attempts) < 3:
            return FakeResponse(status_code=503)
        return FakeResponse(status_code=200)

    monkeypatch.setattr("requests.Session.request", fake_request)
    monkeypatch.setattr("time.sleep", lambda s: None)
    component = mod.HTTPResourceComponent(base_url="https://api.example.com", retries=3)
    result, success = run_request(mod, component)
    assert success and result.status_code == 200
    assert len(attempts) == 3


def test_retry_exhausted_raises_failure(mod, monkeypatch):
    def fake_request(self, method, url, **kwargs):
        return FakeResponse(status_code=503)

    monkeypatch.setattr("requests.Session.request", fake_request)
    monkeypatch.setattr("time.sleep", lambda s: None)
    component = mod.HTTPResourceComponent(base_url="https://api.example.com", retries=2)
    with pytest.raises(Exception, match="503"):
        run_request(mod, component)


def test_connection_error_retries_then_raises(mod, monkeypatch):
    import requests as real_requests

    def fake_request(self, method, url, **kwargs):
        raise real_requests.exceptions.ConnectionError("boom")

    monkeypatch.setattr("requests.Session.request", fake_request)
    monkeypatch.setattr("time.sleep", lambda s: None)
    component = mod.HTTPResourceComponent(base_url="https://api.example.com", retries=1)
    with pytest.raises(Exception, match="failed after 2 attempt"):
        run_request(mod, component)


def test_custom_headers_merged_into_request(mod, monkeypatch):
    seen = []

    def fake_request(self, method, url, **kwargs):
        seen.append(kwargs["headers"])
        return FakeResponse(status_code=200)

    monkeypatch.setattr("requests.Session.request", fake_request)
    component = mod.HTTPResourceComponent(base_url="https://api.example.com")
    result, success = run_request(mod, component, call_kwargs={"method": "GET", "path": "/ping", "headers": {"X-Custom": "yes"}})
    assert success
    assert seen[0]["X-Custom"] == "yes"


def test_response_json_path_success_check(mod, monkeypatch):
    def fake_request(self, method, url, **kwargs):
        return FakeResponse(status_code=200, json_data={"data": {"status": "ok"}})

    monkeypatch.setattr("requests.Session.request", fake_request)
    component = mod.HTTPResourceComponent(base_url="https://api.example.com")
    result, success = run_request(mod, component, call_kwargs={
        "method": "GET", "path": "/check",
        "response_json_path": "data.status", "response_json_success_value": "ok",
    })
    assert success and result.response_value == "ok"


def test_response_json_path_mismatch_raises(mod, monkeypatch):
    def fake_request(self, method, url, **kwargs):
        return FakeResponse(status_code=200, json_data={"data": {"status": "failed"}})

    monkeypatch.setattr("requests.Session.request", fake_request)
    component = mod.HTTPResourceComponent(base_url="https://api.example.com")
    with pytest.raises(Exception, match="response_value"):
        run_request(mod, component, call_kwargs={
            "method": "GET", "path": "/check",
            "response_json_path": "data.status", "response_json_success_value": "ok",
        })


def test_success_status_codes_custom_list(mod, monkeypatch):
    def fake_request(self, method, url, **kwargs):
        return FakeResponse(status_code=201)

    monkeypatch.setattr("requests.Session.request", fake_request)
    component = mod.HTTPResourceComponent(base_url="https://api.example.com")
    result, success = run_request(mod, component, call_kwargs={"method": "POST", "path": "/x", "success_status_codes": [201]})
    assert success and result.status_code == 201


def test_raise_on_failure_false_returns_result_instead_of_raising(mod, monkeypatch):
    def fake_request(self, method, url, **kwargs):
        return FakeResponse(status_code=500)

    monkeypatch.setattr("requests.Session.request", fake_request)
    component = mod.HTTPResourceComponent(base_url="https://api.example.com")
    result, success = run_request(mod, component, call_kwargs={"method": "GET", "path": "/x", "raise_on_failure": False})
    assert success  # the probe asset itself succeeds
    assert result.success is False
    assert result.status_code == 500


def test_params_passed_through_as_query_string(mod, monkeypatch):
    seen_params = []

    def fake_request(self, method, url, **kwargs):
        seen_params.append(kwargs.get("params"))
        return FakeResponse(status_code=200)

    monkeypatch.setattr("requests.Session.request", fake_request)
    component = mod.HTTPResourceComponent(base_url="https://api.example.com")
    result, success = run_request(mod, component, call_kwargs={
        "method": "GET", "path": "/search", "params": {"since": "2026-01-01", "limit": 50},
    })
    assert success and result.success
    assert seen_params[0] == {"since": "2026-01-01", "limit": 50}


def test_optional_auth_header_env_var_unset_does_not_crash_the_run(mod, monkeypatch):
    """auth_header_env_var is genuinely optional: an unset name must not blow up the whole
    run via DagsterInvalidConfigError -- it should just proceed with no Authorization
    header, same as if auth_header_env_var had never been set."""
    monkeypatch.delenv("DEFINITELY_UNSET_HEADER_VAR_XYZ", raising=False)
    seen_headers = []

    def fake_request(self, method, url, **kwargs):
        seen_headers.append(dict(self.headers))
        return FakeResponse(status_code=200)

    monkeypatch.setattr("requests.Session.request", fake_request)
    component = mod.HTTPResourceComponent(base_url="https://api.example.com", auth_header_env_var="DEFINITELY_UNSET_HEADER_VAR_XYZ")
    result, success = run_request(mod, component)
    assert success and result.success
    assert "Authorization" not in seen_headers[0]


def test_required_bearer_token_env_var_unset_still_fails_clearly(mod, monkeypatch):
    """A credential that IS required for the selected auth_type must still fail clearly
    when its env var is unset -- only the genuinely-optional auth_header_env_var gets the
    graceful-degradation treatment."""
    monkeypatch.delenv("DEFINITELY_UNSET_TOKEN_VAR_XYZ", raising=False)
    component = mod.HTTPResourceComponent(base_url="https://api.example.com", auth_type="bearer", auth_token_env_var="DEFINITELY_UNSET_TOKEN_VAR_XYZ")
    defs = component.build_defs(context=None)
    resource = defs.resources[component.resource_key]

    @dg.asset
    def _probe(http_resource: mod.HTTPResource):
        http_resource.get_session()
        return dg.MaterializeResult()

    with pytest.raises(dg.DagsterInvalidConfigError):
        dg.materialize([_probe], resources={"http_resource": resource})


def test_poke_returns_true_on_default_2xx_success(mod, monkeypatch):
    def fake_request(self, method, url, **kwargs):
        return FakeResponse(status_code=200)

    monkeypatch.setattr("requests.Session.request", fake_request)
    resource = mod.HTTPResource(base_url="https://api.example.com")
    assert resource.poke("GET", "/ready") is True


def test_poke_returns_false_on_non_matching_status(mod, monkeypatch):
    def fake_request(self, method, url, **kwargs):
        return FakeResponse(status_code=404)

    monkeypatch.setattr("requests.Session.request", fake_request)
    resource = mod.HTTPResource(base_url="https://api.example.com")
    assert resource.poke("GET", "/ready") is False


def test_poke_returns_false_on_connection_error_instead_of_raising(mod, monkeypatch):
    import requests

    def fake_request(self, method, url, **kwargs):
        raise requests.exceptions.ConnectionError("refused")

    monkeypatch.setattr("requests.Session.request", fake_request)
    resource = mod.HTTPResource(base_url="https://api.example.com")
    assert resource.poke("GET", "/ready") is False


def test_poke_uses_custom_response_check(mod, monkeypatch):
    def fake_request(self, method, url, **kwargs):
        return FakeResponse(status_code=200, json_data={"status": "pending"})

    monkeypatch.setattr("requests.Session.request", fake_request)
    resource = mod.HTTPResource(base_url="https://api.example.com")
    assert resource.poke("GET", "/job/1", response_check=lambda r: r.json()["status"] == "done") is False
    assert resource.poke("GET", "/job/1", response_check=lambda r: r.json()["status"] == "pending") is True


def test_poke_passes_params(mod, monkeypatch):
    seen_params = []

    def fake_request(self, method, url, **kwargs):
        seen_params.append(kwargs.get("params"))
        return FakeResponse(status_code=200)

    monkeypatch.setattr("requests.Session.request", fake_request)
    resource = mod.HTTPResource(base_url="https://api.example.com")
    resource.poke("GET", "/ready", params={"check": "1"})
    assert seen_params[0] == {"check": "1"}
