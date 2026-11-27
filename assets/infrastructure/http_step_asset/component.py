"""HttpStepAssetComponent.

One synchronous HTTP call as a Dagster asset -- the Airflow `SimpleHttpOperator`
equivalent. A single request, a status-code (and optional response-field)
success check, materialize with response metadata.

Distinct from `integrations/http_external_asset` (a real trigger-a-job-then-
poll-a-separate-status-endpoint-until-terminal asset builder, for async
external systems with no dedicated vendor component) -- this component is
explicitly the simpler, synchronous sibling: no polling loop, one request in,
one result out. Also distinct from `assets/transforms/per_row_http_fetcher`
(fetches a URL per DataFrame row) and `assets/sinks/dataframe_to_http` (sinks
a DataFrame via HTTP) -- this is a single, DataFrame-free pipeline step.

Auth fields (`auth_type`/`*_env_var`) mirror `per_row_http_fetcher`'s
none/basic/bearer subset (assets/transforms/per_row_http_fetcher) for
registry-wide consistency -- inlined fresh here per this repo's no-shared-
helpers rule, not imported from that module. (per_row_http_fetcher also
offers `oauth2_client_credentials`; omitted here since a single synchronous
step has no recurring token-refresh loop to amortize the extra auth round
trip against -- point `auth_token_env_var` at a pre-fetched bearer token
instead.)
"""
from typing import Any, Dict, List, Literal, Optional, Union

import dagster as dg
from pydantic import Field, model_validator

# BEGIN field-shape normalization (inlined per component; keep copies identical — see tests/test_field_shapes.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def coerce_field_shapes(data, *, lists=(), csv=(), tags=()):
    """Accept the shapes people (and LLMs) naturally write for common boilerplate fields."""
    if not isinstance(data, dict):
        return data
    out = dict(data)
    for k in lists:  # List[str]: "a" / "a, b" -> ["a", "b"]; strip + drop empties
        v = out.get(k)
        if isinstance(v, str):
            v = v.split(",")
        if isinstance(v, (list, tuple)):
            out[k] = [x.strip() if isinstance(x, str) else x for x in v if not (isinstance(x, str) and not x.strip())]
    for k in csv:  # comma-separated str given as a list -> "a,b"
        v = out.get(k)
        if isinstance(v, (list, tuple, set)):
            out[k] = ",".join(str(x).strip() for x in v)
    for k in tags:  # Dict[str, str]: scalars -> str, ["k=v"] / ["k:v"] -> {k: v}
        v = out.get(k)
        if isinstance(v, (list, tuple)) and v and all(isinstance(x, str) and ("=" in x or ":" in x) for x in v):
            v = dict((p.strip() for p in (x.split("=", 1) if "=" in x else x.split(":", 1))) for x in v)
        if isinstance(v, dict):
            out[k] = {str(a).strip(): (str(b).lower() if isinstance(b, bool) else str(b)) for a, b in v.items() if b is not None}
    return out
# END field-shape normalization


def _require_env(name: str) -> str:
    """Read an env var via dagster.EnvVar, with an actionable message when it is unset."""
    from dagster import EnvVar

    value = EnvVar(name).get_value()
    if value is None:
        raise KeyError(
            f"Environment variable {name!r} is not set. Set it in the environment of the "
            "Dagster code location (or point the *_env_var field at one that is)."
        )
    return value


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


def _do_request(
    session,
    url: str,
    method: str,
    headers: Dict[str, str],
    body: Optional[str],
    json_body: Optional[Dict[str, Any]],
    timeout_seconds: int,
) -> Dict[str, Any]:
    """Issue the single HTTP request, return a plain dict. Never raises on a
    non-success status -- that's the caller's call (success_status_codes /
    response_json_path / response_json_success_value)."""
    request_kwargs: Dict[str, Any] = {"headers": headers, "timeout": timeout_seconds}
    if json_body is not None:
        request_kwargs["json"] = json_body
    elif body is not None:
        request_kwargs["data"] = body

    response = session.request(method, url, **request_kwargs)

    response_json: Any = None
    content_type = response.headers.get("Content-Type", "")
    if "json" in content_type.lower():
        try:
            response_json = response.json()
        except ValueError:
            response_json = None

    return {
        "status_code": response.status_code,
        "body": response.text,
        "json": response_json,
        "headers": dict(response.headers),
    }


class HttpStepAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """One synchronous HTTP call as a Dagster asset.

    Materializes on a successful call (status code in `success_status_codes`,
    default any 2xx -- AND, when both `response_json_path` and
    `response_json_success_value` are set, the extracted value matching that
    expected value). Raises `dg.Failure` with the response status/body
    surfaced in the exception otherwise.
    """

    asset_name: str = Field(description="Dagster asset key.")
    url: str = Field(description="URL to call.")
    method: Literal["GET", "POST", "PUT", "PATCH", "DELETE"] = Field(default="GET", description="HTTP method.")
    headers: Optional[Dict[str, str]] = Field(default=None, description="Extra request headers.")
    body: Optional[str] = Field(default=None, description="Raw request body (e.g. form-encoded). Mutually exclusive with json_body.")
    json_body: Optional[Dict[str, Any]] = Field(default=None, description="JSON request body, sent with Content-Type: application/json. Mutually exclusive with body.")

    auth_type: Literal["none", "basic", "bearer"] = Field(default="none", description="Auth scheme for the request.")
    auth_username_env_var: Optional[str] = Field(default=None, description="Required for auth_type='basic'.")
    auth_password_env_var: Optional[str] = Field(default=None, description="Required for auth_type='basic'.")
    auth_token_env_var: Optional[str] = Field(default=None, description="Required for auth_type='bearer'.")

    success_status_codes: Optional[List[int]] = Field(
        default=None,
        description="Status codes treated as success. Leave unset for Dagster's default (any 2xx).",
    )
    response_json_path: Optional[str] = Field(
        default=None,
        description="Dotted path into the parsed JSON response to check against response_json_success_value (e.g. 'data.status'), and to surface as the 'response_value' metadata entry regardless.",
    )
    response_json_success_value: Optional[str] = Field(
        default=None,
        description="When set alongside response_json_path, the call only succeeds if the extracted value, stringified, equals this (in addition to the status-code check).",
    )
    timeout_seconds: int = Field(default=30, ge=1, le=600, description="Request timeout in seconds.")

    group_name: Optional[str] = Field(default=None, description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Extra asset tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Defaults to ['python', 'http'].")
    deps: Optional[List[str]] = Field(default=None, description="Lineage-only upstream asset keys.")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=("owners", "kinds", "deps"), tags=("asset_tags",))

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='HTTP Step', editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        if self.body is not None and self.json_body is not None:
            raise ValueError("Set either body or json_body, not both.")
        if self.auth_type == "basic" and not (self.auth_username_env_var and self.auth_password_env_var):
            raise ValueError("auth_type='basic' requires auth_username_env_var + auth_password_env_var.")
        if self.auth_type == "bearer" and not self.auth_token_env_var:
            raise ValueError("auth_type='bearer' requires auth_token_env_var.")

        _self = self

        _inferred_kinds = list(self.kinds or ["python", "http"])
        _all_tags = dict(self.asset_tags or {})
        for _kind in _inferred_kinds:
            _all_tags[f"dagster/kind/{_kind}"] = ""

        @dg.asset(
            name=self.asset_name,
            group_name=self.group_name,
            description=self.description or f"HTTP {self.method} {self.url}.",
            owners=self.owners or [],
            tags=_all_tags,
            kinds=set(_inferred_kinds),
            deps=[dg.AssetKey.from_user_string(d) for d in (self.deps or [])],
        )
        def _http_step_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            self = _self
            import requests

            session = requests.Session()
            if self.headers:
                session.headers.update(self.headers)
            if self.auth_type == "basic":
                session.auth = (_require_env(self.auth_username_env_var), _require_env(self.auth_password_env_var))
            elif self.auth_type == "bearer":
                session.headers["Authorization"] = f"Bearer {_require_env(self.auth_token_env_var)}"

            result = _do_request(session, self.url, self.method, dict(session.headers), self.body, self.json_body, self.timeout_seconds)

            context.log.info(f"http_step_asset: {self.method} {self.url} -> {result['status_code']}")

            response_value = _extract_json_path(result["json"], self.response_json_path) if self.response_json_path else None

            status_ok = _status_is_success(result["status_code"], self.success_status_codes)
            value_ok = True
            if self.response_json_path and self.response_json_success_value is not None:
                value_ok = str(response_value) == self.response_json_success_value

            metadata: Dict[str, Any] = {
                "status_code": dg.MetadataValue.int(result["status_code"]),
                "url": dg.MetadataValue.url(self.url) if self.url.startswith(("http://", "https://")) else dg.MetadataValue.text(self.url),
            }
            if result["json"] is not None:
                metadata["response_json"] = dg.MetadataValue.json(result["json"])
            elif result["body"]:
                metadata["response_body"] = dg.MetadataValue.text(result["body"][:4000])
            if self.response_json_path:
                metadata["response_value"] = dg.MetadataValue.text(str(response_value))

            if not (status_ok and value_ok):
                raise dg.Failure(
                    f"http_step_asset: {self.method} {self.url} did not succeed "
                    f"(status_code={result['status_code']}"
                    + (f", response_value={response_value!r} != {self.response_json_success_value!r}" if not value_ok else "")
                    + f"): {result['body'][:500]}",
                    metadata=metadata,
                )

            return dg.MaterializeResult(metadata=metadata)

        return dg.Definitions(assets=[_http_step_asset])
