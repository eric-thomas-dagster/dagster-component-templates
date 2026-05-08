"""HttpExternalAssetComponent.

Dagster Component that wraps any HTTP-driven external job runner (Fivetran,
Airbyte, dbt Cloud, GitHub Actions, internal job APIs) as one or more Dagster
assets. Users configure trigger / status / log endpoints in YAML; the component
runs the trigger → poll → finalize loop, surfaces structured metadata, and
honors all standard Dagster per-asset attributes.

Self-contained — no shared helper modules, per the registry's distribution
model. The condition language, templating engine, HTTP client, and execution
loop are all inlined in this file.
"""
from __future__ import annotations

import datetime as _dt
import importlib
import json
import os
import re
import time
from typing import Any, Dict, List, Literal, Optional, Tuple, Union

import dagster as dg
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


# ───────────────────────────────────────────────────────────────────────────
# Condition language
# ───────────────────────────────────────────────────────────────────────────

# Operators that work on the value extracted by an extractor.
OP_FIELDS = ("equals", "in_", "matches", "exists", "truthy", "gt", "lt")
# YAML uses `in:` but Pydantic reserves it; we expose `in_` and accept `in`
# via field alias.

ConditionSource = Literal["status_response", "trigger_response", "logs", "headers"]


class _ConditionExtractor(dg.Model, dg.Resolvable):
    """A condition or pure value extractor.

    Mutually-exclusive shape: exactly one of {jsonpath, regex, header, literal,
    any_of, all_of, not_}. The leaf-extractor variants (jsonpath/regex/header/
    literal) may also carry an operator (equals/in/matches/exists/truthy/gt/lt).
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    source: ConditionSource = Field(
        default="status_response",
        description="Where to pull the value from.",
    )

    # Leaf extractors — pick one
    jsonpath: Optional[str] = None
    regex: Optional[str] = None
    group: Optional[int] = Field(default=None, description="Capture group for regex extractor.")
    header: Optional[str] = None
    literal: Optional[Any] = None

    # Operators
    equals: Optional[Any] = None
    in_: Optional[List[Any]] = Field(default=None, alias="in")
    matches: Optional[str] = None
    exists: Optional[bool] = None
    truthy: Optional[bool] = None
    gt: Optional[float] = None
    lt: Optional[float] = None

    # Boolean composition.
    # NOTE: typed as Any in the annotation to avoid Dagster's resolver
    # recursing infinitely on the self-reference; Pydantic still validates
    # nested entries because we coerce them to _ConditionExtractor in a
    # validator below.
    any_of: Optional[List[Any]] = None
    all_of: Optional[List[Any]] = None
    not_: Optional[Any] = Field(default=None, alias="not")

    @model_validator(mode="before")
    @classmethod
    def _coerce_nested_conditions(cls, values: Any) -> Any:
        """Recursively coerce dict entries inside any_of/all_of/not into _ConditionExtractor."""
        if not isinstance(values, dict):
            return values
        for key in ("any_of", "all_of"):
            v = values.get(key)
            if isinstance(v, list):
                values[key] = [
                    cls(**item) if isinstance(item, dict) else item for item in v
                ]
        for key in ("not_", "not"):
            v = values.get(key)
            if isinstance(v, dict):
                values[key] = cls(**v)
        return values

    @model_validator(mode="after")
    def _validate_exclusive(self) -> "_ConditionExtractor":
        leaf = sum(
            1
            for v in (self.jsonpath, self.regex, self.header, self.literal)
            if v is not None
        )
        boolean = sum(
            1 for v in (self.any_of, self.all_of, self.not_) if v is not None
        )
        # exactly one of: leaf extractor, boolean composition
        if leaf > 1:
            raise ValueError(
                "condition must specify exactly one extractor: "
                "jsonpath, regex, header, or literal — found multiple"
            )
        if boolean > 1:
            raise ValueError(
                "condition must specify exactly one boolean: any_of, all_of, "
                "or not — found multiple"
            )
        if leaf and boolean:
            raise ValueError(
                "condition cannot combine a leaf extractor with a boolean "
                "(any_of/all_of/not) — wrap the leaf in any_of/all_of with one "
                "element instead"
            )
        if leaf == 0 and boolean == 0:
            raise ValueError(
                "condition must specify exactly one of: jsonpath, regex, "
                "header, literal, any_of, all_of, not"
            )
        return self


_ConditionExtractor.model_rebuild()


# ───────────────────────────────────────────────────────────────────────────
# Condition evaluator
# ───────────────────────────────────────────────────────────────────────────

class _SourceData(BaseModel):
    """The set of sources a condition can pull from at evaluation time."""
    model_config = ConfigDict(arbitrary_types_allowed=True)
    status_response: Optional[Any] = None
    trigger_response: Optional[Any] = None
    logs: Optional[str] = None
    status_headers: Optional[Dict[str, str]] = None


_SourceData.model_rebuild()


def _jsonpath_extract(payload: Any, expr: str) -> Any:
    """Extract a JSONPath. Returns scalar for `.field` paths, list for `[*]` patterns."""
    try:
        from jsonpath_ng.ext import parse  # type: ignore
    except ImportError:
        raise ImportError(
            "jsonpath-ng required for jsonpath conditions. "
            "Install: pip install jsonpath-ng"
        )
    if payload is None:
        return None
    matches = [m.value for m in parse(expr).find(payload)]
    if not matches:
        return None
    if len(matches) == 1 and not expr.endswith("[*]") and "[*]" not in expr:
        return matches[0]
    return matches


def _extract_value(cond: _ConditionExtractor, sources: _SourceData) -> Any:
    """Pull the raw value referenced by a condition's extractor (no operator)."""
    if cond.literal is not None:
        return cond.literal

    if cond.source == "headers":
        body: Any = sources.status_headers or {}
    elif cond.source == "trigger_response":
        body = sources.trigger_response
    elif cond.source == "logs":
        body = sources.logs
    else:
        body = sources.status_response

    if cond.jsonpath:
        return _jsonpath_extract(body, cond.jsonpath)
    if cond.regex:
        if not isinstance(body, str):
            body = "" if body is None else (
                json.dumps(body) if not isinstance(body, (bytes, bytearray)) else body.decode("utf-8", errors="replace")
            )
        m = re.search(cond.regex, body)
        if not m:
            return None
        if cond.group is None:
            return m.group(0)
        try:
            return m.group(cond.group)
        except IndexError:
            return None
    if cond.header:
        if not isinstance(body, dict):
            return None
        # Headers are case-insensitive; do a case-fold lookup.
        for k, v in body.items():
            if k.lower() == cond.header.lower():
                return v
        return None
    return None


def _apply_operator(cond: _ConditionExtractor, value: Any) -> bool:
    """Apply the leaf operator (equals/in/matches/exists/truthy/gt/lt)."""
    if cond.exists is not None:
        return (value is not None) == cond.exists
    if cond.truthy is not None:
        return bool(value) == cond.truthy
    if cond.equals is not None:
        # Note: Pydantic deserializes `null` → None. So `equals: null` → None.
        return value == cond.equals
    if cond.in_ is not None:
        return value in cond.in_
    if cond.matches is not None:
        if value is None:
            return False
        return re.search(cond.matches, str(value)) is not None
    if cond.gt is not None:
        try:
            return float(value) > cond.gt
        except (TypeError, ValueError):
            return False
    if cond.lt is not None:
        try:
            return float(value) < cond.lt
        except (TypeError, ValueError):
            return False
    # No operator: fall back to truthy-evaluation (useful for `exists`-like checks).
    return value is not None and value is not False and value != "" and value != []


def evaluate_condition(cond: _ConditionExtractor, sources: _SourceData) -> bool:
    """Evaluate a condition (with composition) against the source data."""
    if cond.any_of:
        return any(evaluate_condition(c, sources) for c in cond.any_of)
    if cond.all_of:
        return all(evaluate_condition(c, sources) for c in cond.all_of)
    if cond.not_:
        return not evaluate_condition(cond.not_, sources)
    value = _extract_value(cond, sources)
    return _apply_operator(cond, value)


def extract_value(cond: _ConditionExtractor, sources: _SourceData) -> Any:
    """Return just the extracted value (for `metadata` / `run_id` / log patterns)."""
    return _extract_value(cond, sources)


# ───────────────────────────────────────────────────────────────────────────
# Templating
# ───────────────────────────────────────────────────────────────────────────

class HttpTriggerContext(BaseModel):
    """Passed to `from_python:` escape-hatch functions."""
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")
    asset_key: str
    partition_key: Optional[str] = None
    run_id: str
    deps: Dict[str, Dict[str, Any]] = Field(default_factory=dict)
    resources: Any = None
    http_client: Any = None
    logger: Any = None


def _build_template_context(
    asset_key: str,
    partition_key: Optional[str],
    run_id: str,
    deps_meta: Dict[str, Dict[str, Any]],
    secrets: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """Build the Jinja context dict."""
    return {
        "partition_key": partition_key,
        "run_id": run_id,
        "asset_key": asset_key,
        "now": lambda: _dt.datetime.now(_dt.timezone.utc).isoformat(),
        "env": dict(os.environ),
        "secrets": secrets or {},
        "deps": deps_meta,
    }


def _render_string(template_str: str, ctx: Dict[str, Any]) -> str:
    """Render a single string with Jinja."""
    try:
        from jinja2 import Environment, StrictUndefined
    except ImportError:
        raise ImportError("jinja2 required: pip install jinja2")
    env = Environment(undefined=StrictUndefined, autoescape=False)
    return env.from_string(template_str).render(**ctx)


def _render_value(value: Any, ctx: Dict[str, Any]) -> Any:
    """Recursively render Jinja in any string within a dict / list / scalar."""
    if isinstance(value, str):
        return _render_string(value, ctx)
    if isinstance(value, list):
        return [_render_value(v, ctx) for v in value]
    if isinstance(value, dict):
        # `from_python` escape hatch — resolve at call site, not here.
        if "from_python" in value and len(value) == 1:
            return value
        return {k: _render_value(v, ctx) for k, v in value.items()}
    return value


def _resolve_from_python(spec: Dict[str, Any], trigger_ctx: HttpTriggerContext) -> Any:
    """Resolve a `from_python: <module:function>` escape hatch."""
    path = spec["from_python"]
    if ":" not in path:
        raise ValueError(
            f"from_python must be 'module.path:function_name', got: {path!r}"
        )
    module_path, fn_name = path.rsplit(":", 1)
    try:
        mod = importlib.import_module(module_path)
    except ImportError as e:
        raise ImportError(f"could not import {module_path!r} for from_python: {e}")
    fn = getattr(mod, fn_name, None)
    if fn is None or not callable(fn):
        raise AttributeError(
            f"{module_path}.{fn_name} not found or not callable"
        )
    return fn(trigger_ctx)


def _render_body(
    body: Union[str, dict, list, None],
    ctx: Dict[str, Any],
    trigger_ctx: HttpTriggerContext,
) -> Tuple[Optional[bytes], Optional[Any], Optional[str]]:
    """
    Render the body field. Returns (body_bytes, body_json, content_type).

    - String → rendered as-is, sent verbatim, content-type left to caller.
    - dict / list → rendered (strings templated), then JSON-serialized.
    - None → no body.
    """
    if body is None:
        return None, None, None
    if isinstance(body, dict) and "from_python" in body and len(body) == 1:
        resolved = _resolve_from_python(body, trigger_ctx)
        if isinstance(resolved, str):
            return resolved.encode("utf-8"), None, None
        return None, resolved, "application/json"
    if isinstance(body, str):
        rendered = _render_string(body, ctx)
        # Try to detect JSON for content-type inference; otherwise let caller decide.
        s = rendered.strip()
        if (s.startswith("{") and s.endswith("}")) or (s.startswith("[") and s.endswith("]")):
            return rendered.encode("utf-8"), None, "application/json"
        return rendered.encode("utf-8"), None, None
    rendered = _render_value(body, ctx)
    return None, rendered, "application/json"


# ───────────────────────────────────────────────────────────────────────────
# Auth resources — protocol + 3 built-ins
# ───────────────────────────────────────────────────────────────────────────

class HttpAuthResource(dg.ConfigurableResource):
    """Base class for auth resources used by HttpExternalAssetComponent.

    Subclasses implement `apply_auth(headers: dict, params: dict) -> tuple` and
    return updated headers + params dictionaries. Use this base for custom
    auth (HMAC signing, OAuth refresh, etc.) without forking the component.
    """

    def apply_auth(self, headers: Dict[str, str], params: Dict[str, Any]) -> Tuple[Dict[str, str], Dict[str, Any]]:
        """Default no-op. Subclasses override."""
        return headers, params


class BearerTokenAuth(HttpAuthResource):
    """Authorization: Bearer <token>."""
    token: str = Field(description="Bearer token. Use Dagster EnvVar() for secrets.")

    def apply_auth(self, headers: Dict[str, str], params: Dict[str, Any]) -> Tuple[Dict[str, str], Dict[str, Any]]:
        return ({**headers, "Authorization": f"Bearer {self.token}"}, params)


class BasicAuth(HttpAuthResource):
    """HTTP Basic auth via Authorization header."""
    username: str = Field(description="Username.")
    password: str = Field(description="Password. Use Dagster EnvVar() for secrets.")

    def apply_auth(self, headers: Dict[str, str], params: Dict[str, Any]) -> Tuple[Dict[str, str], Dict[str, Any]]:
        import base64
        token = base64.b64encode(f"{self.username}:{self.password}".encode()).decode()
        return ({**headers, "Authorization": f"Basic {token}"}, params)


class HeaderAuth(HttpAuthResource):
    """Custom-header auth (e.g. `X-API-Key: ...`)."""
    header_name: str = Field(description="Header name, e.g. 'X-API-Key'.")
    header_value: str = Field(description="Header value. Use Dagster EnvVar() for secrets.")

    def apply_auth(self, headers: Dict[str, str], params: Dict[str, Any]) -> Tuple[Dict[str, str], Dict[str, Any]]:
        return ({**headers, self.header_name: self.header_value}, params)


# ───────────────────────────────────────────────────────────────────────────
# HTTP client wrapper
# ───────────────────────────────────────────────────────────────────────────

def _http_request(
    method: str,
    url: str,
    *,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, Any]] = None,
    body_bytes: Optional[bytes] = None,
    body_json: Optional[Any] = None,
    body_content_type: Optional[str] = None,
    timeout_seconds: float = 30.0,
    max_attempts: int = 3,
    backoff_initial: float = 1.0,
    backoff_factor: float = 2.0,
    logger: Any = None,
) -> Any:
    """Execute one HTTP request with retry on 5xx + connection errors.

    Returns the httpx.Response. Caller handles parsing the body.
    """
    try:
        import httpx
    except ImportError:
        raise ImportError("httpx required: pip install httpx")

    headers = dict(headers or {})
    if body_content_type and "Content-Type" not in headers and "content-type" not in {k.lower() for k in headers}:
        headers["Content-Type"] = body_content_type

    request_kwargs: Dict[str, Any] = {
        "method": method,
        "url": url,
        "headers": headers,
        "params": params,
        "timeout": timeout_seconds,
    }
    if body_bytes is not None:
        request_kwargs["content"] = body_bytes
    elif body_json is not None:
        request_kwargs["json"] = body_json

    attempt = 0
    delay = backoff_initial
    while True:
        attempt += 1
        try:
            with httpx.Client() as client:
                resp = client.request(**request_kwargs)
            # Retry on 5xx, respect Retry-After
            if resp.status_code >= 500 and attempt < max_attempts:
                ra = resp.headers.get("Retry-After")
                wait = float(ra) if ra and ra.replace(".", "").isdigit() else delay
                if logger:
                    logger.warning(f"{method} {url} → {resp.status_code}, retrying in {wait}s (attempt {attempt}/{max_attempts})")
                time.sleep(wait)
                delay *= backoff_factor
                continue
            return resp
        except httpx.RequestError as e:
            if attempt < max_attempts:
                if logger:
                    logger.warning(f"{method} {url} connection error: {e}, retrying in {delay}s")
                time.sleep(delay)
                delay *= backoff_factor
                continue
            raise


# ───────────────────────────────────────────────────────────────────────────
# Pydantic config models
# ───────────────────────────────────────────────────────────────────────────

class _RetryPolicySpec(dg.Model, dg.Resolvable):
    model_config = ConfigDict(extra="forbid")
    max_retries: int = Field(default=0)
    delay: Optional[float] = Field(default=None, description="Delay seconds between retries.")
    backoff: Optional[Literal["linear", "exponential"]] = Field(default=None)
    jitter: Optional[Literal["full", "plus_minus"]] = Field(default=None)


class _LogPattern(dg.Model, dg.Resolvable):
    model_config = ConfigDict(extra="forbid")
    name: str
    regex: str
    group: Optional[int] = None
    cast: Optional[Literal["int", "float", "str", "bool"]] = None
    mode: Literal["first_match", "count", "exists"] = "first_match"


class _TriggerSpec(dg.Model, dg.Resolvable):
    model_config = ConfigDict(extra="forbid")
    method: Literal["GET", "POST", "PUT", "PATCH", "DELETE"] = "POST"
    path: str = Field(description="Path appended to base_url. Supports Jinja templating.")
    path_params: Dict[str, Any] = Field(default_factory=dict)
    query_params: Dict[str, Any] = Field(default_factory=dict)
    headers: Dict[str, Any] = Field(default_factory=dict)
    body: Union[str, Dict[str, Any], List[Any], None] = None
    run_id: Optional[_ConditionExtractor] = Field(
        default=None,
        description="Extractor that pulls the external run-id out of the trigger response.",
    )


class _StatusSpec(dg.Model, dg.Resolvable):
    model_config = ConfigDict(extra="forbid")
    method: Literal["GET", "POST"] = "GET"
    path: str = Field(description="Status URL path. Supports Jinja templating; {run_id} is auto-substituted.")
    path_params: Dict[str, Any] = Field(default_factory=dict)
    query_params: Dict[str, Any] = Field(default_factory=dict)
    headers: Dict[str, Any] = Field(default_factory=dict)
    poll_interval_seconds: float = 15.0
    timeout_seconds: float = 3600.0
    is_terminal: _ConditionExtractor
    is_success: Optional[_ConditionExtractor] = None
    metadata: Dict[str, _ConditionExtractor] = Field(default_factory=dict)


class _LogsSpec(dg.Model, dg.Resolvable):
    model_config = ConfigDict(extra="forbid")
    method: Literal["GET"] = "GET"
    path: str
    path_params: Dict[str, Any] = Field(default_factory=dict)
    query_params: Dict[str, Any] = Field(default_factory=dict)
    headers: Dict[str, Any] = Field(default_factory=dict)
    next_cursor: Optional[_ConditionExtractor] = None
    max_total_bytes: int = Field(default=10 * 1024 * 1024, description="Cap total log fetch size.")
    patterns: List[_LogPattern] = Field(default_factory=list)
    failure_patterns: List[str] = Field(default_factory=list)


class _AssetSpec(dg.Model, dg.Resolvable):
    """Per-asset config block (one entry under `assets:`).

    Standard Dagster per-asset attributes are exposed at the top level; the
    HTTP-specific sub-blocks live under trigger / status / logs.
    """

    model_config = ConfigDict(extra="forbid")

    # Standard per-asset attributes — match Dagster's AssetSpec API.
    key: str = Field(description="Dagster asset key (e.g. 'fivetran/orders_sync').")
    description: Optional[str] = None
    group_name: Optional[str] = None
    kinds: Optional[List[str]] = None
    tags: Optional[Dict[str, str]] = None
    metadata: Optional[Dict[str, Any]] = None
    owners: Optional[List[str]] = None
    deps: Optional[List[str]] = None
    automation_condition: Optional[str] = Field(
        default=None,
        description="Dagster automation_condition factory call, e.g. \"on_cron('0 2 * * *')\".",
    )
    retry_policy: Optional[_RetryPolicySpec] = None
    code_version: Optional[str] = None

    # Partitions — canonical shape used across the registry (see FIELD_CONVENTIONS).
    partition_type: Optional[str] = None
    partition_start: Optional[str] = None
    partition_values: Optional[str] = None
    dynamic_partition_name: Optional[str] = None
    partition_dimensions: Optional[List[Dict[str, Any]]] = None

    # HTTP-specific
    trigger: _TriggerSpec
    status: _StatusSpec
    logs: Optional[_LogsSpec] = None


# ───────────────────────────────────────────────────────────────────────────
# Partition helper (canonical shape, copied from external_*_asset)
# ───────────────────────────────────────────────────────────────────────────

def _build_partitions_def(
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
    partition_dimensions,
):
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )

    if partition_dimensions and partition_type:
        raise ValueError(
            "Set either partition_type (flat-fields shape) or "
            "partition_dimensions (multi-axis shape), not both."
        )

    def _build_axis(spec):
        t = spec.get("type")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily":
            return DailyPartitionsDefinition(start_date=spec["start"])
        if t == "weekly":
            return WeeklyPartitionsDefinition(start_date=spec["start"])
        if t == "monthly":
            return MonthlyPartitionsDefinition(start_date=spec["start"])
        if t == "hourly":
            return HourlyPartitionsDefinition(start_date=spec["start"])
        if t == "static":
            vals = spec.get("values") or []
            if isinstance(vals, str):
                vals = [v.strip() for v in vals.split(",") if v.strip()]
            if not vals:
                raise ValueError("partition dimension type='static' requires non-empty 'values'")
            return StaticPartitionsDefinition(list(vals))
        if t == "dynamic":
            name = spec.get("dynamic_partition_name") or spec.get("name")
            if not name:
                raise ValueError("partition dimension type='dynamic' requires a name")
            return DynamicPartitionsDefinition(name=name)
        raise ValueError(f"unknown partition type: {t!r}")

    if partition_dimensions:
        if len(partition_dimensions) == 1:
            return _build_axis(partition_dimensions[0])
        axes = {d["name"]: _build_axis(d) for d in partition_dimensions}
        return MultiPartitionsDefinition(axes)

    if not partition_type:
        return None
    _values = [v.strip() for v in (partition_values or "").split(",") if v.strip()]
    if partition_type in ("daily", "weekly", "monthly", "hourly") and not partition_start:
        raise ValueError(
            f"partition_type={partition_type!r} requires partition_start (ISO date)."
        )
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "static":
        if not _values:
            raise ValueError("partition_type='static' requires partition_values.")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError(
                "partition_type='dynamic' requires dynamic_partition_name."
            )
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    if partition_type == "multi":
        if not _values:
            raise ValueError("partition_type='multi' requires partition_values.")
        if not partition_start:
            raise ValueError("partition_type='multi' requires partition_start.")
        return MultiPartitionsDefinition({
            "date": DailyPartitionsDefinition(start_date=partition_start),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")


# ───────────────────────────────────────────────────────────────────────────
# Helper: build retry policy from spec
# ───────────────────────────────────────────────────────────────────────────

def _build_retry_policy(spec: Optional[_RetryPolicySpec]):
    if spec is None or spec.max_retries <= 0:
        return None
    from dagster import Backoff, Jitter, RetryPolicy
    backoff = Backoff[spec.backoff.upper()] if spec.backoff else None
    jitter_map = {"full": Jitter.FULL, "plus_minus": Jitter.PLUS_MINUS}
    return RetryPolicy(
        max_retries=spec.max_retries,
        delay=spec.delay,
        backoff=backoff,
        jitter=jitter_map.get(spec.jitter) if spec.jitter else None,
    )


# ───────────────────────────────────────────────────────────────────────────
# Helper: load upstream materialization metadata for templating context
# ───────────────────────────────────────────────────────────────────────────

def _load_deps_metadata(context: dg.AssetExecutionContext, deps_keys: List[str]) -> Dict[str, Dict[str, Any]]:
    """For each dep, fetch the latest materialization metadata dict + run_id."""
    out: Dict[str, Dict[str, Any]] = {}
    for key in deps_keys:
        ak = dg.AssetKey.from_user_string(key)
        try:
            event = context.instance.get_latest_materialization_event(ak)
        except Exception:
            event = None
        if not event or not event.asset_materialization:
            out[key] = {"metadata": {}, "run_id": None}
            continue
        md = event.asset_materialization.metadata or {}
        # Unwrap MetadataValue objects to their underlying value where possible.
        unwrapped = {}
        for k, v in md.items():
            unwrapped[k] = getattr(v, "value", None) or getattr(v, "text", None) or v
        out[key] = {"metadata": unwrapped, "run_id": event.run_id}
    return out


# ───────────────────────────────────────────────────────────────────────────
# Trigger / poll / log execution loop
# ───────────────────────────────────────────────────────────────────────────

_RUN_TAG_PREFIX = "http_external/run_id"


def _execute_asset(
    asset_spec: _AssetSpec,
    base_url: str,
    auth: Optional[HttpAuthResource],
    context: dg.AssetExecutionContext,
) -> dg.MaterializeResult:
    """Run the trigger → poll → log loop for one asset materialization."""
    log = context.log
    asset_key_str = "/".join(context.asset_key.path)
    partition_key = context.partition_key if context.has_partition_key else None
    deps_meta = _load_deps_metadata(context, asset_spec.deps or [])
    template_ctx = _build_template_context(
        asset_key=asset_key_str,
        partition_key=partition_key,
        run_id=context.run_id or "",
        deps_meta=deps_meta,
    )
    trigger_ctx = HttpTriggerContext(
        asset_key=asset_key_str,
        partition_key=partition_key,
        run_id=context.run_id or "",
        deps=deps_meta,
        resources=getattr(context, "resources", None),
        logger=log,
    )

    # ── Idempotency: check for existing run-id stored as a Dagster run tag ──
    tag_key = f"{_RUN_TAG_PREFIX}/{asset_key_str}"
    prior_run_id: Optional[str] = None
    try:
        run = context.instance.get_run_by_id(context.run_id) if context.run_id else None
        if run and run.tags:
            prior_run_id = run.tags.get(tag_key)
    except Exception:
        prior_run_id = None

    started_at = time.monotonic()
    poll_count = 0
    trigger_response_data: Any = None

    if prior_run_id:
        log.info(f"resuming poll for prior external run_id={prior_run_id} (idempotent retry)")
        external_run_id = prior_run_id
    else:
        # ── Trigger ──
        trigger_path = _render_string(asset_spec.trigger.path, template_ctx)
        trigger_path_params = _render_value(asset_spec.trigger.path_params, template_ctx)
        # Substitute path params into the path itself if it has placeholders.
        for k, v in (trigger_path_params or {}).items():
            trigger_path = trigger_path.replace("{" + k + "}", str(v))
        trigger_url = base_url.rstrip("/") + (
            trigger_path if trigger_path.startswith("/") else "/" + trigger_path
        )
        trigger_query = _render_value(asset_spec.trigger.query_params, template_ctx)
        trigger_headers = _render_value(asset_spec.trigger.headers, template_ctx)
        body_bytes, body_json, body_ct = _render_body(asset_spec.trigger.body, template_ctx, trigger_ctx)
        if auth:
            trigger_headers, trigger_query = auth.apply_auth(trigger_headers or {}, trigger_query or {})

        log.info(f"trigger {asset_spec.trigger.method} {trigger_url}")
        resp = _http_request(
            method=asset_spec.trigger.method,
            url=trigger_url,
            headers=trigger_headers,
            params=trigger_query,
            body_bytes=body_bytes,
            body_json=body_json,
            body_content_type=body_ct,
            logger=log,
        )
        if resp.status_code >= 400:
            raise RuntimeError(
                f"trigger returned HTTP {resp.status_code}: {resp.text[:500]}"
            )
        try:
            trigger_response_data = resp.json()
        except Exception:
            trigger_response_data = resp.text

        # ── Extract run-id ──
        if asset_spec.trigger.run_id is None:
            raise ValueError(
                f"asset {asset_key_str!r} trigger.run_id extractor is required — "
                "the component needs to know the external run identifier to poll."
            )
        external_run_id = extract_value(
            asset_spec.trigger.run_id,
            _SourceData(trigger_response=trigger_response_data),
        )
        if external_run_id is None:
            raise RuntimeError(
                f"trigger.run_id extractor returned None on response: {str(trigger_response_data)[:500]}"
            )
        external_run_id = str(external_run_id)

        # Tag the run for retry idempotency.
        try:
            context.instance.add_run_tags(context.run_id, {tag_key: external_run_id})
        except Exception:
            pass

        log.info(f"external run started: {external_run_id}")

    # ── Poll status ──
    status_template_ctx = {**template_ctx, "run_id": external_run_id}
    status_resp_body: Any = None
    status_headers_dict: Dict[str, str] = {}
    is_success_result: Optional[bool] = None
    while True:
        if time.monotonic() - started_at > asset_spec.status.timeout_seconds:
            raise TimeoutError(
                f"external run {external_run_id} did not reach a terminal status "
                f"within {asset_spec.status.timeout_seconds}s. "
                f"Last response: {str(status_resp_body)[:500]}"
            )

        status_path = _render_string(asset_spec.status.path, status_template_ctx)
        # Substitute {run_id} and any other path_params placeholders.
        status_path_params = _render_value(asset_spec.status.path_params, status_template_ctx)
        all_path_params = {"run_id": external_run_id, **(status_path_params or {})}
        for k, v in all_path_params.items():
            status_path = status_path.replace("{" + k + "}", str(v))

        status_url = base_url.rstrip("/") + (
            status_path if status_path.startswith("/") else "/" + status_path
        )
        status_query = _render_value(asset_spec.status.query_params, status_template_ctx)
        status_headers_in = _render_value(asset_spec.status.headers, status_template_ctx) or {}
        if auth:
            status_headers_in, status_query = auth.apply_auth(status_headers_in, status_query or {})

        try:
            resp = _http_request(
                method=asset_spec.status.method,
                url=status_url,
                headers=status_headers_in,
                params=status_query,
                logger=log,
            )
            poll_count += 1
            status_resp_body = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else resp.text
            status_headers_dict = dict(resp.headers)
        except Exception as e:
            log.warning(f"status poll error (non-terminal): {e}")
            time.sleep(asset_spec.status.poll_interval_seconds)
            continue

        sources = _SourceData(
            status_response=status_resp_body,
            trigger_response=trigger_response_data,
            status_headers=status_headers_dict,
        )

        if not evaluate_condition(asset_spec.status.is_terminal, sources):
            time.sleep(asset_spec.status.poll_interval_seconds)
            continue

        # Terminal — compute success.
        if asset_spec.status.is_success is not None:
            is_success_result = evaluate_condition(asset_spec.status.is_success, sources)
        else:
            # Default heuristic — terminal AND status is not in {failed, cancelled, error}.
            v = None
            try:
                if isinstance(status_resp_body, dict):
                    v = status_resp_body.get("status") or status_resp_body.get("state")
            except Exception:
                v = None
            is_success_result = (
                v is None or str(v).lower() not in {"failed", "cancelled", "error", "errored"}
            )
            log.warning(
                "no is_success condition configured — using fallback heuristic. "
                "Add an explicit is_success for clearer behavior."
            )
        break

    # ── Logs ──
    log_pattern_results: Dict[str, Any] = {}
    if asset_spec.logs:
        full_logs = _fetch_logs(
            asset_spec.logs,
            base_url,
            auth,
            external_run_id,
            template_ctx,
            log,
        )
        # Failure-pattern override
        for fp in asset_spec.logs.failure_patterns:
            if re.search(fp, full_logs):
                if is_success_result is True:
                    log.warning(
                        f"failure pattern {fp!r} matched in logs — overriding success → failure"
                    )
                is_success_result = False
                break
        # Pattern extraction
        for pat in asset_spec.logs.patterns:
            log_pattern_results[pat.name] = _evaluate_log_pattern(pat, full_logs)

    if not is_success_result:
        raise RuntimeError(
            f"external run {external_run_id} terminal-state was not success. "
            f"Last status response: {str(status_resp_body)[:500]}"
        )

    # ── Build MaterializeResult ──
    duration = time.monotonic() - started_at
    metadata: Dict[str, Any] = {
        "external_run_id": external_run_id,
        "duration_seconds": dg.MetadataValue.float(duration),
        "poll_count": dg.MetadataValue.int(poll_count),
    }
    sources_for_metadata = _SourceData(
        status_response=status_resp_body,
        trigger_response=trigger_response_data,
        status_headers=status_headers_dict,
    )
    for name, ext in asset_spec.status.metadata.items():
        v = extract_value(ext, sources_for_metadata)
        metadata[name] = _to_metadata_value(name, v)
    for name, v in log_pattern_results.items():
        metadata[name] = _to_metadata_value(name, v)
    if asset_spec.metadata:
        for k, v in asset_spec.metadata.items():
            metadata.setdefault(k, _to_metadata_value(k, v))

    return dg.MaterializeResult(metadata=metadata)


def _to_metadata_value(name: str, v: Any):
    """Best-effort coercion of an extracted value to a Dagster MetadataValue."""
    if v is None:
        return dg.MetadataValue.text("")
    if isinstance(v, bool):
        return dg.MetadataValue.bool(v)
    if isinstance(v, int):
        return dg.MetadataValue.int(v)
    if isinstance(v, float):
        return dg.MetadataValue.float(v)
    if isinstance(v, str):
        if v.startswith(("http://", "https://")) or "url" in name.lower():
            return dg.MetadataValue.url(v)
        return dg.MetadataValue.text(v)
    try:
        return dg.MetadataValue.json(v)
    except Exception:
        return dg.MetadataValue.text(str(v))


def _evaluate_log_pattern(pat: _LogPattern, full_logs: str) -> Any:
    """Apply a log pattern according to its mode + cast."""
    if pat.mode == "exists":
        return bool(re.search(pat.regex, full_logs))
    if pat.mode == "count":
        return len(re.findall(pat.regex, full_logs))
    # first_match
    m = re.search(pat.regex, full_logs)
    if not m:
        return None
    val = m.group(pat.group) if pat.group is not None else m.group(0)
    if pat.cast == "int":
        try:
            return int(val)
        except (TypeError, ValueError):
            return None
    if pat.cast == "float":
        try:
            return float(val)
        except (TypeError, ValueError):
            return None
    if pat.cast == "bool":
        return str(val).lower() in {"true", "1", "yes"}
    return val


def _fetch_logs(
    logs_spec: _LogsSpec,
    base_url: str,
    auth: Optional[HttpAuthResource],
    external_run_id: str,
    template_ctx: Dict[str, Any],
    logger: Any,
) -> str:
    """Fetch log body, handling pagination via next_cursor and the byte cap."""
    accumulated = []
    total_bytes = 0
    cursor: Optional[str] = None
    ctx = {**template_ctx, "run_id": external_run_id}
    for _ in range(100):  # hard cap on pagination iterations
        path = _render_string(logs_spec.path, ctx).replace("{run_id}", external_run_id)
        path_params = _render_value(logs_spec.path_params, ctx)
        for k, v in (path_params or {}).items():
            path = path.replace("{" + k + "}", str(v))
        url = base_url.rstrip("/") + (path if path.startswith("/") else "/" + path)
        params = _render_value(logs_spec.query_params, ctx) or {}
        if cursor:
            params["cursor"] = cursor
        headers = _render_value(logs_spec.headers, ctx) or {}
        if auth:
            headers, params = auth.apply_auth(headers, params)
        resp = _http_request(method="GET", url=url, headers=headers, params=params, logger=logger)
        body = resp.text or ""
        accumulated.append(body)
        total_bytes += len(body)
        if total_bytes >= logs_spec.max_total_bytes:
            logger.warning(f"log fetch hit {logs_spec.max_total_bytes} byte cap; truncating")
            break
        if logs_spec.next_cursor is None:
            break
        cursor = extract_value(
            logs_spec.next_cursor,
            _SourceData(status_headers=dict(resp.headers), status_response=body),
        )
        if not cursor:
            break
    return "\n".join(accumulated)[: logs_spec.max_total_bytes]


# ───────────────────────────────────────────────────────────────────────────
# Component class
# ───────────────────────────────────────────────────────────────────────────

class HttpExternalAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Wrap any HTTP-driven external job runner as one or more Dagster assets.

    Same shape as community wrappers like fivetran_assets / airbyte_assets, but
    declarative — a single component config defines N assets, each with its own
    trigger / status / logs spec. Use this for ELT tools, internal job APIs,
    GitHub Actions, Jenkins, dbt Cloud, anything with a 'trigger / poll /
    fetch logs' shape.

    See README.md for the condition language, templating context, and design
    decisions.
    """

    base_url: str = Field(description="Base URL prepended to every endpoint path. Supports Jinja templating.")
    auth_resource_key: Optional[str] = Field(
        default=None,
        description=(
            "Resource key of an HttpAuthResource subclass (BearerTokenAuth, "
            "BasicAuth, HeaderAuth, or a custom subclass). Resource must be "
            "wired in your Definitions resources dict."
        ),
    )
    assets: List[_AssetSpec] = Field(
        description="One or more asset specs. Each emits a Dagster asset.",
    )

    @field_validator("assets")
    @classmethod
    def _at_least_one(cls, v: List[_AssetSpec]) -> List[_AssetSpec]:
        if not v:
            raise ValueError("HttpExternalAssetComponent.assets must contain at least one asset")
        return v

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        assets_defs = []
        for spec in self.assets:
            assets_defs.append(self._build_one_asset(spec))
        return dg.Definitions(assets=assets_defs)

    def _build_one_asset(self, spec: _AssetSpec):
        partitions_def = _build_partitions_def(
            spec.partition_type,
            spec.partition_start,
            spec.partition_values,
            spec.dynamic_partition_name,
            spec.partition_dimensions,
        )
        retry_policy = _build_retry_policy(spec.retry_policy)
        deps_keys = [dg.AssetKey.from_user_string(k) for k in (spec.deps or [])]

        # automation_condition is a string like "on_cron('0 2 * * *')"; eval'd
        # at component-load time. Keep it small and side-effect-free.
        automation_condition = None
        if spec.automation_condition:
            from dagster import AutomationCondition  # noqa: F401  (used in eval)
            try:
                automation_condition = eval(  # noqa: S307 — config-driven by the project owner
                    spec.automation_condition,
                    {"__builtins__": {}},
                    {"AutomationCondition": AutomationCondition, **_automation_condition_helpers()},
                )
            except Exception as e:
                raise ValueError(
                    f"asset {spec.key!r} automation_condition failed to evaluate: "
                    f"{spec.automation_condition!r} → {e}"
                )

        kinds_set = set(spec.kinds or ["http"])

        base_url_local = self.base_url
        auth_resource_key_local = self.auth_resource_key
        spec_local = spec

        @dg.asset(
            key=dg.AssetKey.from_user_string(spec.key),
            description=spec.description,
            group_name=spec.group_name,
            kinds=kinds_set,
            tags=spec.tags or None,
            metadata=spec.metadata or None,
            owners=spec.owners or None,
            deps=deps_keys or None,
            automation_condition=automation_condition,
            retry_policy=retry_policy,
            partitions_def=partitions_def,
            code_version=spec.code_version,
            required_resource_keys={auth_resource_key_local} if auth_resource_key_local else set(),
        )
        def _http_asset(context) -> dg.MaterializeResult:
            auth = (
                getattr(context.resources, auth_resource_key_local)
                if auth_resource_key_local
                else None
            )
            base_url_rendered = _render_string(
                base_url_local,
                _build_template_context(
                    asset_key="/".join(context.asset_key.path),
                    partition_key=context.partition_key if context.has_partition_key else None,
                    run_id=context.run_id or "",
                    deps_meta={},
                ),
            )
            return _execute_asset(spec_local, base_url_rendered, auth, context)

        return _http_asset


def _automation_condition_helpers() -> Dict[str, Any]:
    """Helpers exposed to the automation_condition eval context."""
    from dagster import AutomationCondition

    def on_cron(expr: str):
        return AutomationCondition.on_cron(expr)

    def eager():
        return AutomationCondition.eager()

    def any_downstream_conditions():
        return AutomationCondition.any_downstream_conditions()

    return {
        "on_cron": on_cron,
        "eager": eager,
        "any_downstream_conditions": any_downstream_conditions,
    }
