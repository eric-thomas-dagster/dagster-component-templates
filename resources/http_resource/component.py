"""HTTP Resource component."""
import time as _time
from typing import Any, Dict, List, Literal, Optional

import dagster as dg
from pydantic import Field


def _require_env(name: str) -> str:
    """Read an env var via dagster.EnvVar, with an actionable message when it is unset."""
    value = dg.EnvVar(name).get_value()
    if value is None:
        raise KeyError(
            f"Environment variable {name!r} is not set. Set it in the environment of the "
            "Dagster code location (or point the *_env_var field at one that is)."
        )
    return value


# Module-level (not instance/class-attribute) OAuth2 token cache, keyed by
# (token_url, client_id_env_var) -- same convention as per_row_http_fetcher's own
# cache: a resource instance can be long-lived across a run, but this still keys
# on the config that actually identifies the token rather than relying on that.
_OAUTH2_TOKEN_CACHE: Dict[str, Dict[str, Any]] = {}


def _fetch_oauth2_client_credentials_token(
    token_url: str,
    client_id: str,
    client_secret: str,
    scope: Optional[str],
    audience: Optional[str],
) -> Dict[str, Any]:
    """Isolates the one external token-request call so it can be monkeypatched
    wholesale in tests. Returns the raw token response dict ({'access_token',
    'expires_in', ...}); caller is responsible for caching."""
    import requests

    body: Dict[str, Any] = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
    }
    if scope:
        body["scope"] = scope
    if audience:
        body["audience"] = audience
    resp = requests.post(token_url, data=body, timeout=30)
    resp.raise_for_status()
    return resp.json()


def _get_oauth2_access_token(
    token_url: str,
    client_id_env_var: str,
    client_secret_env_var: str,
    scope: Optional[str],
    audience: Optional[str],
) -> str:
    cache_key = f"{token_url}:{client_id_env_var}"
    cached = _OAUTH2_TOKEN_CACHE.get(cache_key) or {}
    if cached.get("expires", 0) > _time.time() + 60:
        return cached["access_token"]

    client_id = _require_env(client_id_env_var)
    client_secret = _require_env(client_secret_env_var)
    data = _fetch_oauth2_client_credentials_token(
        token_url=token_url,
        client_id=client_id,
        client_secret=client_secret,
        scope=scope,
        audience=audience,
    )
    _OAUTH2_TOKEN_CACHE[cache_key] = {
        "access_token": data["access_token"],
        # Refresh a little early rather than racing expiry.
        "expires": _time.time() + data.get("expires_in", 3600) - 60,
    }
    return data["access_token"]


def _extract_json_path(data: Any, path: Optional[str]) -> Any:
    """Dotted-path extraction into a parsed JSON value, e.g. 'data.id' for {'data': {'id': 123}}."""
    if not path or data is None:
        return None
    cur = data
    for part in path.split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return None
    return cur


def _status_is_success(status_code: int, success_status_codes: Optional[List[int]]) -> bool:
    if success_status_codes:
        return status_code in success_status_codes
    return 200 <= status_code < 300


class HTTPRequestResult:
    """Result of HTTPResource.request() -- a single HTTP call's outcome."""

    def __init__(self, status_code: int, body: str, json: Any, headers: Dict[str, str], success: bool, response_value: Any = None):
        self.status_code = status_code
        self.body = body
        self.json = json
        self.headers = headers
        self.success = success
        self.response_value = response_value

    def __repr__(self):
        return f"HTTPRequestResult(status_code={self.status_code}, success={self.success})"


class HTTPResource(dg.ConfigurableResource):
    """Provides a configured requests.Session with base URL and auth, plus a one-shot
    `request()` helper for the common "call this endpoint and check success" case --
    the reusable logic behind the Airflow `SimpleHttpOperator` equivalent. Injectable
    into any op/asset, not tied to a single pre-built asset shape.

    Auth: `auth_header_value` (a raw, pre-formed header -- the original mode), or
    `auth_type` (none/basic/bearer/oauth2_client_credentials), matching
    `per_row_http_fetcher`'s exact field names/behavior for registry-wide consistency.
    `oauth2_client_id_env_var`/`oauth2_client_secret_env_var` hold env var NAMES
    (resolved lazily per call, like that sibling), not pre-resolved values -- this
    keeps credential resolution consistent with the token-refresh path.
    """

    base_url: str
    auth_header_value: str = ""
    auth_type: Literal["none", "basic", "bearer", "oauth2_client_credentials"] = "none"
    auth_username: str = ""
    auth_password: str = ""
    auth_token: str = ""
    oauth2_token_url: str = ""
    oauth2_client_id_env_var: str = ""
    oauth2_client_secret_env_var: str = ""
    oauth2_scope: str = ""
    oauth2_audience: str = ""
    timeout_seconds: int = 30
    retries: int = 0
    retry_on_status_codes: List[int] = Field(default_factory=lambda: [429, 500, 502, 503, 504])
    retry_backoff_seconds: float = 0.5

    def get_session(self):
        import requests
        session = requests.Session()
        if self.auth_header_value:
            session.headers["Authorization"] = self.auth_header_value
        elif self.auth_type == "basic":
            session.auth = (self.auth_username, self.auth_password)
        elif self.auth_type == "bearer":
            session.headers["Authorization"] = f"Bearer {self.auth_token}"
        elif self.auth_type == "oauth2_client_credentials":
            token = _get_oauth2_access_token(
                token_url=self.oauth2_token_url,
                client_id_env_var=self.oauth2_client_id_env_var,
                client_secret_env_var=self.oauth2_client_secret_env_var,
                scope=self.oauth2_scope or None,
                audience=self.oauth2_audience or None,
            )
            session.headers["Authorization"] = f"Bearer {token}"
        return session

    def get(self, path: str, **kwargs):
        return self.get_session().get(f"{self.base_url.rstrip('/')}/{path.lstrip('/')}", timeout=self.timeout_seconds, **kwargs)

    def post(self, path: str, **kwargs):
        return self.get_session().post(f"{self.base_url.rstrip('/')}/{path.lstrip('/')}", timeout=self.timeout_seconds, **kwargs)

    def request(
        self,
        method: str,
        path: str,
        *,
        headers: Optional[Dict[str, str]] = None,
        body: Optional[str] = None,
        json_body: Optional[Dict[str, Any]] = None,
        success_status_codes: Optional[List[int]] = None,
        response_json_path: Optional[str] = None,
        response_json_success_value: Optional[str] = None,
        timeout_seconds: Optional[int] = None,
        raise_on_failure: bool = True,
    ) -> HTTPRequestResult:
        """One HTTP call, with a status-code (and optional response-field) success check,
        and retry-with-backoff on a retryable status code or connection error (when
        `retries` > 0) -- the Airflow `SimpleHttpOperator` equivalent as reusable code.
        Raises `dg.Failure` on a non-matching final result unless `raise_on_failure=False`,
        in which case the caller can inspect `result.success` itself."""
        import requests

        session = self.get_session()
        if headers:
            session.headers.update(headers)

        url = f"{self.base_url.rstrip('/')}/{path.lstrip('/')}" if not path.startswith(("http://", "https://")) else path
        request_kwargs: Dict[str, Any] = {"headers": dict(session.headers), "timeout": timeout_seconds or self.timeout_seconds}
        if json_body is not None:
            request_kwargs["json"] = json_body
        elif body is not None:
            request_kwargs["data"] = body

        response = None
        last_exc: Optional[Exception] = None
        for attempt in range(self.retries + 1):
            try:
                response = session.request(method, url, **request_kwargs)
                if response.status_code in self.retry_on_status_codes and attempt < self.retries:
                    _time.sleep(self.retry_backoff_seconds * (2 ** attempt))
                    continue
                last_exc = None
                break
            except requests.RequestException as e:
                last_exc = e
                response = None
                if attempt < self.retries:
                    _time.sleep(self.retry_backoff_seconds * (2 ** attempt))
                    continue
                break

        if response is None:
            if raise_on_failure:
                raise dg.Failure(f"HTTPResource.request: {method} {url} failed after {self.retries + 1} attempt(s): {last_exc}")
            return HTTPRequestResult(status_code=-1, body=f"ERROR: {last_exc}", json=None, headers={}, success=False)

        response_json: Any = None
        content_type = response.headers.get("content-type", "")
        if "json" in content_type.lower():
            try:
                response_json = response.json()
            except ValueError:
                response_json = None

        response_value = _extract_json_path(response_json, response_json_path) if response_json_path else None
        status_ok = _status_is_success(response.status_code, success_status_codes)
        value_ok = True
        if response_json_path and response_json_success_value is not None:
            value_ok = str(response_value) == response_json_success_value
        success = status_ok and value_ok

        result = HTTPRequestResult(
            status_code=response.status_code, body=response.text, json=response_json,
            headers=dict(response.headers), success=success, response_value=response_value,
        )

        if raise_on_failure and not success:
            raise dg.Failure(
                f"HTTPResource.request: {method} {url} did not succeed (status_code={response.status_code}"
                + (f", response_value={response_value!r} != {response_json_success_value!r}" if not value_ok else "")
                + f"): {response.text[:500]}",
                metadata={"status_code": dg.MetadataValue.int(response.status_code)},
            )
        return result


class HTTPResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register an HTTP resource (base URL + auth) for use by other components.

    Supports two auth shapes: a raw `auth_header_env_var` (the original, pre-existing
    mode -- a fully-formed Authorization header value, e.g. 'Bearer token123'), or the
    richer `auth_type` scheme (none/basic/bearer/oauth2_client_credentials, matching
    `per_row_http_fetcher`'s convention). Set one or the other, not both.
    """

    resource_key: str = Field(default="http_resource", description="Key used to register this resource. Other components reference it via resource_key.")
    base_url: str = Field(description="Base URL for all requests, e.g. 'https://api.example.com/v1'")
    auth_header_env_var: Optional[str] = Field(default=None, description="Env var containing a fully-formed Authorization header value, e.g. 'Bearer token123'. Mutually exclusive with auth_type.")
    auth_type: Literal["none", "basic", "bearer", "oauth2_client_credentials"] = Field(default="none", description="Auth scheme, as an alternative to auth_header_env_var: none/basic/bearer/oauth2_client_credentials.")
    auth_username_env_var: Optional[str] = Field(default=None, description="Required for auth_type='basic'.")
    auth_password_env_var: Optional[str] = Field(default=None, description="Required for auth_type='basic'.")
    auth_token_env_var: Optional[str] = Field(default=None, description="Required for auth_type='bearer'.")
    oauth2_token_url: Optional[str] = Field(default=None, description="Token endpoint. Required for auth_type='oauth2_client_credentials'.")
    oauth2_client_id_env_var: Optional[str] = Field(default=None, description="Env var holding the OAuth2 client ID. Required for auth_type='oauth2_client_credentials'.")
    oauth2_client_secret_env_var: Optional[str] = Field(default=None, description="Env var holding the OAuth2 client secret. Required for auth_type='oauth2_client_credentials'.")
    oauth2_scope: Optional[str] = Field(default=None, description="Optional OAuth2 scope to request.")
    oauth2_audience: Optional[str] = Field(default=None, description="Optional OAuth2 audience parameter.")
    timeout_seconds: int = Field(default=30, description="Request timeout in seconds")
    retries: int = Field(default=0, ge=0, le=10, description="Retry count per request on a retryable status code / connection error (with exponential backoff). Only applies to request(), not get()/post().")
    retry_on_status_codes: List[int] = Field(default_factory=lambda: [429, 500, 502, 503, 504], description="Response status codes that trigger a retry (in addition to connection/timeout errors, which always retry). Only takes effect when retries > 0.")
    retry_backoff_seconds: float = Field(default=0.5, description="Base delay for exponential backoff between retries (delay = base * 2**attempt).")

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='HTTP Resource', editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        if self.auth_header_env_var and self.auth_type != "none":
            raise ValueError("Set either auth_header_env_var or auth_type, not both.")
        if self.auth_type == "basic" and not (self.auth_username_env_var and self.auth_password_env_var):
            raise ValueError("auth_type='basic' requires auth_username_env_var + auth_password_env_var.")
        if self.auth_type == "bearer" and not self.auth_token_env_var:
            raise ValueError("auth_type='bearer' requires auth_token_env_var.")
        if self.auth_type == "oauth2_client_credentials" and not (
            self.oauth2_token_url and self.oauth2_client_id_env_var and self.oauth2_client_secret_env_var
        ):
            raise ValueError(
                "auth_type='oauth2_client_credentials' requires oauth2_token_url, "
                "oauth2_client_id_env_var, and oauth2_client_secret_env_var."
            )

        resource = HTTPResource(
            base_url=self.base_url,
            auth_header_value=dg.EnvVar(self.auth_header_env_var) if self.auth_header_env_var else "",
            auth_type=self.auth_type,
            auth_username=dg.EnvVar(self.auth_username_env_var) if self.auth_username_env_var else "",
            auth_password=dg.EnvVar(self.auth_password_env_var) if self.auth_password_env_var else "",
            auth_token=dg.EnvVar(self.auth_token_env_var) if self.auth_token_env_var else "",
            # Passed through as raw env var NAMES (not pre-resolved) -- resolved lazily
            # per call via _require_env, same as per_row_http_fetcher's oauth2 path.
            oauth2_token_url=self.oauth2_token_url or "",
            oauth2_client_id_env_var=self.oauth2_client_id_env_var or "",
            oauth2_client_secret_env_var=self.oauth2_client_secret_env_var or "",
            oauth2_scope=self.oauth2_scope or "",
            oauth2_audience=self.oauth2_audience or "",
            timeout_seconds=self.timeout_seconds,
            retries=self.retries,
            retry_on_status_codes=list(self.retry_on_status_codes or [429, 500, 502, 503, 504]),
            retry_backoff_seconds=self.retry_backoff_seconds,
        )
        return dg.Definitions(resources={self.resource_key: resource})
