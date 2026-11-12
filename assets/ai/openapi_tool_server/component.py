"""OpenAPI Tool Server Component.

Turns an OpenAPI/Swagger spec's operations into agent-callable tools -- the
counterpart to `openapi_ingestion` (which pulls GET endpoints into a data
table on a schedule). This component exposes operations as callable actions,
gated by an `operation_ids` allow-list (the spec's own `operationId` field),
in one of two modes:

- `mode="assets"` (default): one Dagster asset PER selected operation. Each
  asset makes a real HTTP call (`requests`) to that operation's endpoint, with
  the same args-templating convention as `mcp_tool_call`
  (`{partition_key}` / `{partition_keys.<dim>}` / `{run_id}`), and the same
  partition / backfill_policy / retry_policy / freshness_policy wiring.
  Deterministic, no LLM, no MCP involved -- just a direct REST call per
  asset. Tagged `dagster-community/tool-source=openapi` (NOT the
  `dagster-community/mcp-server` tag `mcp_tool_call` uses, since no MCP
  server is involved in this mode).

- `mode="mcp_server"`: builds a real `fastmcp.FastMCP` server directly from
  the parsed OpenAPI spec (`FastMCP.from_openapi`), with the allow-list
  enforced via `route_map_fn` (operations not in `operation_ids` map to
  `MCPType.EXCLUDE`). A live server process isn't something `build_defs` can
  return (Dagster definitions are a static graph, not a running service), so
  this mode instead emits two INSPECTABLE things:
    1. A resource (`OpenApiMcpServerResource`, registered under
       `resource_key`) that LAZILY builds the FastMCP server on demand
       (`.build_server()`) -- for a user's own script / ASGI mount / sensor
       to actually host it.
    2. A small asset (`inspection_asset_name`) that materializes by building
       the server and listing its resulting tools (`FastMCP.list_tools()`,
       which only enumerates in-memory `OpenAPITool` objects built from the
       spec -- no network call, no live client needed), reporting tool count
       + names as metadata. This is the "something inspectable in Dagster"
       this mode needs: without it, picking `mode="mcp_server"` would make
       the component invisible in the asset graph and catalog UI.
    Tagged `dagster-community/mcp-server=<server_name>` (this mode DOES
    involve a real MCP server, so the existing tag applies) PLUS
    `dagster-community/tool-source=openapi` (to mark its OpenAPI origin).

fastmcp API verified by introspecting the installed `fastmcp==4.1.8`
package directly (not guessed):
    - `FastMCP.from_openapi(openapi_spec, client=None, name=..., route_maps=None,
      route_map_fn=None, mcp_component_fn=None, mcp_names=None, tags=None,
      validate_output=True, **settings)` -- `client` is optional; if omitted a
      default `httpx2.AsyncClient` is built from the spec's first `servers[].url`.
      A plain `httpx.AsyncClient` is ALSO accepted (fastmcp docstring: "Legacy
      httpx clients are temporarily accepted with a deprecation warning") --
      confirmed live against this spec/tool example, so `httpx` (ubiquitous)
      is used here in preference to the newer, less common `httpx2` package,
      with `httpx2` tried first only if already installed.
    - `RouteMap` / `MCPType` / `RouteMapFn` live at
      `fastmcp.server.providers.openapi` in 4.1.8 (NOT `fastmcp.server.openapi`,
      which doesn't exist in this version -- an earlier module layout
      referenced in some fastmcp docs/blog posts). `route_map_fn` is called as
      `(route: HTTPRoute, mcp_type: MCPType) -> MCPType | None` for EVERY
      route; returning `None` means "no override, keep the default mapping"
      (every route maps to `MCPType.TOOL` by default) -- so excluding a
      non-allow-listed operation requires explicitly returning
      `MCPType.EXCLUDE`, not `None`. `HTTPRoute.operation_id` is the field
      matched against the allow-list (confirmed via
      `dataclasses`/pydantic field introspection).
    - Verified end-to-end against a small real spec dict with a 3-operation
      allow-list of 2: `FastMCP.from_openapi(spec, route_map_fn=...)` then
      `asyncio.run(mcp.list_tools())` returned exactly the 2 allow-listed
      tool names, the 3rd (`createPet`) correctly excluded.
    NOT verified: fastmcp's older 2.x/3.x module layout (whether
    `RouteMap`/`MCPType` lived somewhere else) -- this component only
    supports what was actually introspected (4.1.8-shaped), and fails loudly
    with an actionable message if the import shape doesn't match, rather
    than guessing at a compat shim for versions never seen.
"""
import re
import sys
from typing import Any, Dict, List, Literal, Optional, Union

import dagster as dg
from dagster import (
    AssetExecutionContext,
    AssetKey,
    BackfillPolicy,
    Component,
    ComponentLoadContext,
    Definitions,
    MetadataValue,
    Model,
    Resolvable,
    asset,
)
from pydantic import ConfigDict, Field, model_validator


def _require_env(name: str) -> str:
    """Read an env var via dagster.EnvVar, with an actionable message when it is unset.
    Copied verbatim from assets/ingestion/openapi_ingestion/component.py (this repo's
    self-containment rule: inline, don't import)."""
    from dagster import EnvVar

    value = EnvVar(name).get_value()
    if value is None:
        raise KeyError(
            f"Environment variable {name!r} is not set. Set it in the environment of the "
            "Dagster code location (or point the *_env_var field at one that is)."
        )
    return value


def _fetch_spec(spec_url: Optional[str], spec_path: Optional[str]) -> dict:
    """Fetch an OpenAPI spec from a URL or local file. Returns the parsed dict.
    Copied verbatim from openapi_ingestion's own `_fetch_spec` (same spec_url/spec_path
    contract, same precedence when both are set: spec_url wins)."""
    from pathlib import Path

    if spec_url:
        import requests
        resp = requests.get(spec_url, timeout=30)
        resp.raise_for_status()
        content = resp.text
    elif spec_path:
        content = Path(spec_path).read_text()
    else:
        raise ValueError("Must set spec_url or spec_path")

    import json
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        import yaml
        return yaml.safe_load(content)


_HTTP_METHODS = ("get", "post", "put", "patch", "delete", "head", "options")


def _resolve_server_base_url(spec_url: Optional[str], server_url: str) -> str:
    """OpenAPI's servers[].url may be relative (e.g. '/api/v3', as the real Swagger
    Petstore v3 spec uses) -- per the OpenAPI 3.x spec, a relative server URL is relative
    to the location of the document that contains it. Resolving it against spec_url is
    what turns it into a real, schemed, absolute base URL (e.g.
    'https://petstore3.swagger.io/api/v3'); returning it unresolved left requests going
    out with no scheme at all. Only works when the spec was fetched via spec_url -- a
    relative servers[].url in a spec loaded from spec_path (no originating URL) can't be
    resolved and is treated as unusable here, falling through to api_base_url_env_var."""
    if server_url.startswith("http://") or server_url.startswith("https://"):
        return server_url
    if not spec_url:
        return ""
    from urllib.parse import urljoin
    return urljoin(spec_url, server_url)


def _extract_operations(spec: dict, operation_ids: Optional[List[str]]) -> List[Dict[str, Any]]:
    """Extract every operation (any HTTP method, not just GET -- openapi_ingestion's own
    `_extract_endpoints` only looks at GET since it's building a read-only data table;
    agent-callable tools need the full verb set), filtered through the operation_ids
    allow-list by name. `operation_ids=None` means "every operation in the spec" --
    same None-means-unfiltered convention as openapi_ingestion's include_tags/exclude_tags."""
    allow = set(operation_ids) if operation_ids else None
    ops: List[Dict[str, Any]] = []
    paths = spec.get("paths", {}) or {}

    for path, path_item in paths.items():
        if not isinstance(path_item, dict):
            continue
        shared_params = [p for p in (path_item.get("parameters") or []) if isinstance(p, dict)]
        for method in _HTTP_METHODS:
            op = path_item.get(method)
            if not op:
                continue
            op_id = op.get("operationId") or f"{method}_{path}".replace("/", "_").strip("_")
            if allow is not None and op_id not in allow:
                continue

            params = shared_params + [p for p in (op.get("parameters") or []) if isinstance(p, dict)]
            ops.append({
                "operation_id": op_id,
                "method": method.upper(),
                "path": path,
                # The server's base path (servers[].url) is resolved + applied exactly
                # once, by the caller when building the request URL -- NOT embedded here
                # too, which previously doubled it (e.g. '/api/v3/api/v3/pet/1').
                "full_path": path,
                "summary": op.get("summary", ""),
                "tags": op.get("tags", []) or [],
                # $ref parameters are not resolved -- a known, documented gap (see README),
                # not a silent one: an operation whose params are all $ref-indirected simply
                # treats every call arg as a body field instead of path/query/header.
                "parameters": [
                    {"name": p.get("name"), "in": p.get("in"), "required": bool(p.get("required"))}
                    for p in params if "$ref" not in p
                ],
                "has_request_body": bool(op.get("requestBody")),
            })
    return ops


def _safe_tag_value(value: str) -> str:
    """Dagster's strict tag-value validation only allows alpha-numeric/'_'/'-'/'.', <=63 chars
    -- a free-text server_name (e.g. 'OpenAPI Tool Server') would otherwise crash at
    build_defs time with DagsterInvalidDefinitionError."""
    v = re.sub(r"[^A-Za-z0-9_.\-]", "_", str(value))
    v = re.sub(r"_+", "_", v).strip("_")[:63]
    return v or "_"


def _safe_asset_name(operation_id: str) -> str:
    """Sanitize an operationId into a valid Dagster asset name."""
    name = re.sub(r"[^A-Za-z0-9_]", "_", operation_id)
    name = re.sub(r"_+", "_", name).strip("_")
    if not name:
        name = "operation"
    if name[0].isdigit():
        name = f"op_{name}"
    return name


def _substitute_args(args: Any, substitutions: Dict[str, Any]) -> Any:
    """Recursively substitute `{partition_key}` / `{partition_keys.X}` / `{run_id}` in string
    leaves. Identical in spirit to mcp_tool_call's own `_substitute_args` (copied, not
    imported, per this repo's self-containment rule)."""
    if isinstance(args, dict):
        return {k: _substitute_args(v, substitutions) for k, v in args.items()}
    if isinstance(args, list):
        return [_substitute_args(v, substitutions) for v in args]
    if isinstance(args, str):
        if "{" not in args:
            return args
        out = args
        out = out.replace("{run_id}", str(substitutions.get("run_id", "")))
        out = out.replace("{partition_key}", str(substitutions.get("partition_key", "")))
        for dim, val in (substitutions.get("partition_keys") or {}).items():
            out = out.replace("{partition_keys." + dim + "}", str(val))
        return out
    return args


def _build_partitions_def(
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
    partition_dimensions,
    cron_schedule=None,
    cron_fmt="%Y-%m-%d",
    end_offset=0,
    timezone=None,
):
    """Same shape as mcp_tool_call / other components -- strict combinations only.
    Copied verbatim (this repo's canonical implementation, per MANIFEST_CONVENTIONS.md's
    partitioning section: "copy it as-is when authoring a new partition-aware component")."""
    from dagster import (
        DailyPartitionsDefinition,
        WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition,
        HourlyPartitionsDefinition,
        StaticPartitionsDefinition,
        MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )

    if partition_dimensions and partition_type:
        raise ValueError(
            "Set either partition_type (flat-fields shape) or "
            "partition_dimensions (multi-axis shape), not both."
        )

    def _build_axis(spec):
        t = spec.get("type")
        _end_offset = spec.get("end_offset", 0)
        _timezone = spec.get("timezone")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily":
            return DailyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "weekly":
            return WeeklyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "monthly":
            return MonthlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "hourly":
            return HourlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "cron":
            from dagster import TimeWindowPartitionsDefinition
            if not spec.get("start"):
                raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
            if not spec.get("cron_schedule"):
                raise ValueError("partition dimension type='cron' requires 'cron_schedule'")
            return TimeWindowPartitionsDefinition(start=spec["start"], fmt=spec.get("fmt") or "%Y-%m-%d", cron_schedule=spec["cron_schedule"], end_offset=_end_offset, timezone=_timezone)
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
    if isinstance(partition_values, (list, tuple)):
        _values = [str(v).strip() for v in partition_values if str(v).strip()]
    else:
        _values = [v.strip() for v in (str(partition_values) if partition_values else "").split(",") if v.strip()]
    if partition_type in ("daily", "weekly", "monthly", "hourly", "cron") and not partition_start:
        raise ValueError(
            f"partition_type={partition_type!r} requires partition_start (ISO date, e.g. '2024-01-01')."
        )
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "cron":
        from dagster import TimeWindowPartitionsDefinition
        if not cron_schedule:
            raise ValueError(
                "partition_type='cron' requires cron_schedule, e.g. '0 0 * * 1-5' for weekdays only."
            )
        return TimeWindowPartitionsDefinition(start=partition_start, fmt=cron_fmt, cron_schedule=cron_schedule, end_offset=end_offset, timezone=timezone)
    if partition_type == "static":
        if not _values:
            raise ValueError("partition_type='static' requires partition_values (comma-separated).")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    if partition_type == "multi":
        if not _values:
            raise ValueError("partition_type='multi' requires partition_values (comma-separated).")
        if not partition_start:
            raise ValueError("partition_type='multi' requires partition_start (the date axis start).")
        return MultiPartitionsDefinition({
            "date": DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")


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


class _McpServerShapeResource(dg.ConfigurableResource):
    """Minimal MCPServerResource-shaped resource (name/type/command/url/env/headers/
    headers_env) -- inlined here rather than imported from resources/mcp_server_resource
    (confirmed not importable across components; see dagster_workflow_tools' and
    knowledge_search_server's own copies of this class -- structurally, not byte-for-byte,
    the same shape here, since each needs a different live-refresh mechanism).

    `live_env_passthrough` names env vars whose CURRENT value should be merged into `env`
    fresh every time `model_dump()` is actually called (run time -- when a consumer
    resolves this resource), not baked in at construction (component load) -- the same
    run-time-not-build-time principle `dagster_workflow_tools` applies to DAGSTER_HOME,
    generalized here to any number of named secrets (api_base_url_env_var/
    api_key_env_var's actual values)."""

    name: str
    type: Literal["http", "sse", "stdio"] = "stdio"
    command: Optional[List[str]] = None
    url: Optional[str] = None
    env: Optional[Dict[str, str]] = None
    headers: Optional[Dict[str, str]] = None
    headers_env: Optional[Dict[str, str]] = None
    live_env_passthrough: Optional[List[str]] = None

    def model_dump(self, *args, **kwargs):
        dumped = super().model_dump(*args, **kwargs)
        passthrough = dumped.pop("live_env_passthrough", None)
        if dumped.get("type") == "stdio" and passthrough:
            env = dict(dumped.get("env") or {})
            for var_name in passthrough:
                value = dg.EnvVar(var_name).get_value()
                if value:
                    env[var_name] = value
            dumped["env"] = env
        return dumped


def _this_module_name() -> str:
    """The real importable dotted path for the `python -m <this>` stdio server
    invocation. `__name__` alone is unreliable: it's literally `"__main__"` both for a
    bare `python component.py` invocation AND for `python -m dotted.path`. `__spec__.name`
    is the fix (same mechanism/rationale as dagster_workflow_tools'/
    knowledge_search_server's own `_this_module_name`, duplicated here -- not imported,
    per this repo's self-contained-component convention)."""
    if __spec__ is not None:
        return __spec__.name
    raise RuntimeError(
        "openapi_tool_server's stdio MCP server must be run as "
        "`python -m <dotted.path.to.this.module>` (e.g. `python -m "
        "dagster_community_components.assets.ai.openapi_tool_server.component`), not "
        "`python component.py` directly -- the spawned server's own command needs a real "
        "importable module name (available via __spec__.name). See README.md."
    )


class OpenApiMcpServerResource(dg.ConfigurableResource):
    """Holds everything needed to lazily build a real `fastmcp.FastMCP` server from a
    (previously fetched) OpenAPI spec, gated by the same `operation_ids` allow-list as the
    `mode="assets"` path. `build_server()` does the actual (optional) fastmcp import +
    construction -- nothing here imports fastmcp at module load, so a project that only uses
    `mode="assets"` never needs it installed."""

    spec: Dict[str, Any] = Field(description="Parsed OpenAPI spec dict.")
    spec_url: Optional[str] = Field(default=None, description="The URL the spec was fetched from, if any -- used to resolve a relative servers[].url into an absolute base URL.")
    server_name: str = Field(default="OpenAPI Tool Server", description="Name for the built FastMCP server.")
    operation_ids: Optional[List[str]] = Field(default=None, description="Allow-list of operationIds; None means every operation in the spec.")
    api_base_url_env_var: str = Field(default="API_BASE_URL", description="Env var with the API base URL, used only when the spec has no servers[] entry.")
    api_key_env_var: Optional[str] = Field(default=None, description="Env var holding the API key/token.")
    auth_header: str = Field(default="Authorization", description="HTTP header name for the credential.")
    auth_prefix: str = Field(default="Bearer", description="Prefix before the credential value in the header, e.g. 'Bearer'. Empty string for none.")
    validate_output: bool = Field(default=True, description="Passed through to FastMCP.from_openapi -- validate tool responses against the spec's output schema.")

    def build_server(self):
        """Build (not run) a FastMCP server from the held spec. Safe to call repeatedly --
        each call builds a fresh server; construction does no network I/O (the HTTP client
        is only used when a tool is actually CALLED, not when the server/tools are built or
        listed)."""
        try:
            from fastmcp import FastMCP
            from fastmcp.server.providers.openapi import MCPType
        except ImportError as e:
            raise ImportError(
                "fastmcp required for mode='mcp_server': pip install 'fastmcp>=2.0'"
            ) from e

        try:
            import httpx2 as _httpx  # fastmcp's modern client, if already installed
        except ImportError:
            import httpx as _httpx  # universally available; fastmcp accepts it (deprecation warning only) -- verified against fastmcp 4.1.8

        headers: Dict[str, str] = {}
        if self.api_key_env_var:
            key = _require_env(self.api_key_env_var)
            headers[self.auth_header] = f"{self.auth_prefix} {key}" if self.auth_prefix else key

        servers = self.spec.get("servers") or [{}]
        _raw_server_url = servers[0].get("url", "") if servers else ""
        base_url = _resolve_server_base_url(self.spec_url, _raw_server_url) if _raw_server_url else ""
        if not base_url:
            base_url = _require_env(self.api_base_url_env_var)

        client = _httpx.AsyncClient(base_url=base_url, headers=headers or None, timeout=30.0)

        allow = set(self.operation_ids) if self.operation_ids else None

        def _route_map_fn(route, mcp_type):
            # Returning None means "no override" and every route defaults to MCPType.TOOL
            # (fastmcp's own DEFAULT_ROUTE_MAPPINGS) -- so excluding a non-allow-listed
            # operation requires explicitly returning MCPType.EXCLUDE, verified against
            # fastmcp 4.1.8's provider.py (`if result is not None: route_type = result`).
            if allow is not None and route.operation_id not in allow:
                return MCPType.EXCLUDE
            return MCPType.TOOL

        return FastMCP.from_openapi(
            self.spec,
            client=client,
            name=self.server_name,
            route_map_fn=_route_map_fn,
            validate_output=self.validate_output,
        )


class OpenApiToolServerComponent(Component, Model, Resolvable):
    """Turn an OpenAPI/Swagger spec's operations into agent-callable tools.

    Example (default `mode="assets"` — one asset per allow-listed operation):
        ```yaml
        type: dagster_community_components.OpenApiToolServerComponent
        attributes:
          spec_url: "https://petstore3.swagger.io/api/v3/openapi.json"
          api_base_url_env_var: PETSTORE_BASE_URL
          api_key_env_var: PETSTORE_API_KEY
          operation_ids:
            - getPetById
            - findPetsByStatus
          operation_args:
            findPetsByStatus:
              status: available
        ```

    Example (`mode="mcp_server"` — a real FastMCP server + an inspection asset):
        ```yaml
        type: dagster_community_components.OpenApiToolServerComponent
        attributes:
          mode: mcp_server
          spec_url: "https://petstore3.swagger.io/api/v3/openapi.json"
          api_key_env_var: PETSTORE_API_KEY
          operation_ids:
            - getPetById
            - findPetsByStatus
          resource_key: petstore_mcp_server
        ```
    """

    model_config = ConfigDict(populate_by_name=True)

    mode: Literal["assets", "mcp_server"] = Field(
        default="assets",
        description=(
            "'assets' (default, simpler, more Dagster-native): one asset per allow-listed "
            "operation, each a direct HTTP call. 'mcp_server': build a real fastmcp.FastMCP "
            "server from the spec, exposed via a resource (resource_key) other code can host, "
            "plus a small inspection asset listing the server's resulting tools."
        ),
    )

    spec_url: Optional[str] = Field(default=None, description="URL to fetch the OpenAPI/Swagger spec from (JSON or YAML). Takes precedence over spec_path if both are set.")
    spec_path: Optional[str] = Field(default=None, description="Local file path to the OpenAPI/Swagger spec (JSON or YAML).")

    api_base_url_env_var: str = Field(default="API_BASE_URL", description="Env var with the API base URL. Only used as a fallback when the spec has no servers[] entry with a URL.")
    api_key_env_var: Optional[str] = Field(default=None, description="Env var holding the API key/token for Authorization.")
    auth_header: str = Field(default="Authorization", description="HTTP header name for the credential.")
    auth_prefix: str = Field(default="Bearer", description="Prefix before the credential value in the header, e.g. 'Bearer'. Set to empty string for none (raw key as the header value).")

    operation_ids: Optional[List[str]] = Field(
        default=None,
        description=(
            "Allow-list of operationId values (the OpenAPI spec's own `operationId` field per "
            "operation). Operations NOT in this list are NEVER exposed, in either mode. "
            "Leave unset to expose every operation in the spec."
        ),
    )

    # mode="assets" only
    asset_name_prefix: Optional[str] = Field(default=None, description="mode='assets' only. Prefix for emitted asset keys: '{asset_name_prefix}_{operation_id}' (sanitized). Useful to avoid collisions when two instances of this component target different specs.")
    operation_args: Optional[Dict[str, Dict[str, Any]]] = Field(
        default_factory=dict,
        description=(
            "mode='assets' only. Per-operationId call args, keyed by operationId. Each value "
            "is a flat dict of {param_name: value} -- matched against the operation's declared "
            "path/query/header parameters by name (path/query/header placement is inferred "
            "from the spec); any key that doesn't match a declared parameter is sent as a JSON "
            "request body field (for operations with a requestBody). String leaves get "
            "template substitution: {partition_key}, {partition_keys.<dim>}, {run_id}."
        ),
    )
    parse_as: str = Field(default="auto", description="mode='assets' only. How to parse each operation's response body: 'json' (raise if not JSON), 'text' (raw string), 'auto' (try JSON, fall back to text).")
    request_timeout_seconds: int = Field(default=30, description="mode='assets' only. Per-call HTTP timeout.")

    # mode="mcp_server" only
    server_name: str = Field(default="OpenAPI Tool Server", description="mode='mcp_server' only. Name for the built FastMCP server.")
    resource_key: str = Field(default="openapi_mcp_server", description="mode='mcp_server' only. Dagster resource key the built OpenApiMcpServerResource is registered under -- other code (a script, an ASGI mount, a sensor) reads it via context.resources.<resource_key>.build_server() to actually host the server.")
    inspection_asset_name: str = Field(default="openapi_mcp_tools", description="mode='mcp_server' only. Name of the small inspection asset that lists the built server's resulting tools.")
    validate_output: bool = Field(default=True, description="mode='mcp_server' only. Passed through to FastMCP.from_openapi -- validate tool responses against the spec's output schema.")
    mcp_server_resource_key: Optional[str] = Field(
        default=None,
        description=(
            "mode='mcp_server' only. If set, ALSO registers a stdio MCP-server-shaped resource "
            "(same name/type/command/env shape as MCPServerResource) under this key, built from "
            "this component's own settings -- python -m this module, with the "
            "OPENAPI_TOOL_SERVER_* env vars set from spec_url/spec_path/operation_ids/server_name/"
            "validate_output/auth_header/auth_prefix/api_base_url_env_var/api_key_env_var. The "
            "actual secret value(s) named by api_base_url_env_var/api_key_env_var are read fresh "
            "from the environment every time a consumer resolves this resource (not baked in at "
            "component load). Any mcp_servers-aware component (mcp_tool_call, mcp_tool_picker, the "
            "agent components, agentic_pipeline) can then reference it via server_resource_key "
            "instead of hand-writing an inline command+env entry."
        ),
    )

    group_name: Optional[str] = Field(default=None, description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description. mode='assets': applied to every emitted operation asset unless the operation has its own OpenAPI summary.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Extra asset tags, applied to every emitted asset.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds for the Dagster catalog. mode='assets' always adds 'api' and 'openapi' (even if you set your own kinds), the same way mcp_tool_call always adds 'mcp' -- so a UI can find every operation asset this component emits by kind alone.")

    partition_type: Optional[str] = Field(default=None, description="mode='assets' only. 'daily' | 'weekly' | 'monthly' | 'hourly' | 'cron' | 'static' | 'multi' | 'dynamic'. Applied to every emitted operation asset.")
    partition_cron_schedule: Optional[str] = Field(default=None, description="Required when partition_type='cron'. A cron expression restricting the date axis to a custom cadence, e.g. '0 0 * * 1-5' for weekdays only.")
    partition_cron_fmt: Optional[str] = Field(default="%Y-%m-%d", description="strftime format for partition keys produced by partition_type='cron'.")
    partition_end_offset: int = Field(default=0, description="Passed through to the underlying time-based partitions definition. 0 means a time-based partition set only ever holds COMPLETED windows.")
    partition_timezone: Optional[str] = Field(default=None, description="IANA timezone for the underlying time-based partitions definition (defaults to UTC when unset).")
    partition_start: Optional[str] = Field(default=None, description="ISO date for time-based partitions.")
    partition_values: Optional[Union[str, List[str]]] = Field(default=None, description="Comma-separated values for static/multi.")
    dynamic_partition_name: Optional[str] = Field(default=None, description="Name for DynamicPartitionsDefinition.")
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(default=None, description="Multi-axis partition spec: list of {name, type, start, values} dicts.")

    backfill_policy: Optional[Literal["single_run", "multi_run"]] = Field(
        default=None,
        description=(
            "mode='assets' only. 'single_run' materializes an entire backfill range in one run "
            "(one HTTP call per partition in the range, each with its own {partition_key} "
            "substitution). 'multi_run' launches one run per partition. Leave unset for "
            "Dagster's default."
        ),
    )
    backfill_policy_max_partitions_per_run: Optional[int] = Field(default=None, description="For backfill_policy='multi_run' only: maximum partitions materialized per run.")

    freshness_max_lag_minutes: Optional[int] = Field(default=None, description="FreshnessPolicy max lag in minutes.")
    freshness_cron: Optional[str] = Field(default=None, description="FreshnessPolicy cron schedule.")

    retry_policy_max_retries: Optional[int] = Field(default=None, description="RetryPolicy max retries.")
    retry_policy_delay_seconds: Optional[int] = Field(default=None, description="Seconds between retries.")
    retry_policy_backoff: str = Field(default="exponential", description="'linear' or 'exponential'.")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=("owners", "kinds", "operation_ids"), csv=("partition_values",), tags=("asset_tags",))

    @model_validator(mode="after")
    def _mcp_server_resource_key_requires_mcp_server_mode(self):
        if self.mcp_server_resource_key and self.mode != "mcp_server":
            raise ValueError("OpenApiToolServerComponent: mcp_server_resource_key requires mode='mcp_server'.")
        return self

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="OpenAPI Tool Server", editable=True)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        try:
            spec = _fetch_spec(self.spec_url, self.spec_path)
        except Exception as e:  # noqa: BLE001
            # An unreachable spec_url (or a missing/unparseable spec_path) must not crash the
            # whole code location -- build_defs runs eagerly on every code-server reload, and a
            # transient network failure or a spec URL that changes would otherwise take down
            # every OTHER component sharing this location too. Same fix pattern this repo
            # already applies to other eager-discovery components (e.g. terraform_cloud_asset,
            # coalesce_workspace): log a clear warning and return no assets/resources for this
            # instance until the spec is reachable again, instead of propagating the exception.
            if hasattr(context, "log"):
                context.log.warning(
                    f"openapi_tool_server: could not fetch OpenAPI spec (spec_url={self.spec_url!r}, "
                    f"spec_path={self.spec_path!r}): {e}. No assets/resources will be created for "
                    "this component until the spec is reachable and the code location is reloaded."
                )
            return Definitions()
        if self.mode == "mcp_server":
            return self._build_mcp_server_defs(spec)
        return self._build_asset_defs(spec)

    # ------------------------------------------------------------------
    # mode="mcp_server"
    # ------------------------------------------------------------------
    def _build_mcp_server_defs(self, spec: dict) -> Definitions:
        resource = OpenApiMcpServerResource(
            spec=spec,
            spec_url=self.spec_url,
            server_name=self.server_name,
            operation_ids=self.operation_ids,
            api_base_url_env_var=self.api_base_url_env_var,
            api_key_env_var=self.api_key_env_var,
            auth_header=self.auth_header,
            auth_prefix=self.auth_prefix,
            validate_output=self.validate_output,
        )

        resource_key = self.resource_key
        server_name = self.server_name
        operation_ids = self.operation_ids

        tags = dict(self.asset_tags or {})
        tags.setdefault("dagster-community/mcp-server", _safe_tag_value(server_name))
        tags.setdefault("dagster-community/tool-source", "openapi")

        @asset(
            key=AssetKey.from_user_string(self.inspection_asset_name),
            group_name=self.group_name,
            description=self.description or f"Lists the tools exposed by the {server_name!r} FastMCP server built from this OpenAPI spec.",
            owners=self.owners or [],
            tags=tags,
            required_resource_keys={resource_key},
        )
        def _mcp_server_inspection_asset(context: AssetExecutionContext):
            import asyncio

            res = getattr(context.resources, resource_key)
            mcp = res.build_server()
            tools = asyncio.run(mcp.list_tools())
            names = sorted(t.name for t in tools)

            return dg.MaterializeResult(metadata={
                "server_name": MetadataValue.text(server_name),
                "tool_count": MetadataValue.int(len(names)),
                "tool_names": MetadataValue.json(names),
                "operation_ids_allowlist": MetadataValue.json(operation_ids or []),
                "transport": MetadataValue.text("mcp"),
            })

        resources: Dict[str, Any] = {resource_key: resource}

        if self.mcp_server_resource_key:
            mcp_env: Dict[str, str] = {
                "OPENAPI_TOOL_SERVER_SERVER_NAME": server_name,
                "OPENAPI_TOOL_SERVER_API_BASE_URL_ENV_VAR": self.api_base_url_env_var,
                "OPENAPI_TOOL_SERVER_AUTH_HEADER": self.auth_header,
                "OPENAPI_TOOL_SERVER_AUTH_PREFIX": self.auth_prefix,
                "OPENAPI_TOOL_SERVER_VALIDATE_OUTPUT": "1" if self.validate_output else "0",
            }
            if self.spec_url:
                mcp_env["OPENAPI_TOOL_SERVER_SPEC_URL"] = self.spec_url
            if self.spec_path:
                mcp_env["OPENAPI_TOOL_SERVER_SPEC_PATH"] = self.spec_path
            if operation_ids:
                mcp_env["OPENAPI_TOOL_SERVER_OPERATION_IDS"] = ",".join(operation_ids)
            if self.api_key_env_var:
                mcp_env["OPENAPI_TOOL_SERVER_API_KEY_ENV_VAR"] = self.api_key_env_var

            # The actual secret value(s) these two fields NAME are read fresh from the
            # environment every time a consumer resolves this resource (model_dump()),
            # not baked in here at component load -- same run-time-not-build-time
            # principle dagster_workflow_tools' own mcp_server_resource_key applies to
            # DAGSTER_HOME.
            live_env_passthrough = [self.api_base_url_env_var]
            if self.api_key_env_var:
                live_env_passthrough.append(self.api_key_env_var)

            resources[self.mcp_server_resource_key] = _McpServerShapeResource(
                name=self.mcp_server_resource_key,
                type="stdio",
                command=[sys.executable, "-m", _this_module_name()],
                env=mcp_env,
                live_env_passthrough=live_env_passthrough,
            )

        return Definitions(assets=[_mcp_server_inspection_asset], resources=resources)

    # ------------------------------------------------------------------
    # mode="assets"
    # ------------------------------------------------------------------
    def _build_asset_defs(self, spec: dict) -> Definitions:
        operations = _extract_operations(spec, self.operation_ids)

        asset_name_prefix = self.asset_name_prefix
        operation_args = self.operation_args or {}
        parse_as = self.parse_as
        request_timeout_seconds = self.request_timeout_seconds
        api_base_url_env_var = self.api_base_url_env_var
        api_key_env_var = self.api_key_env_var
        auth_header = self.auth_header
        auth_prefix = self.auth_prefix
        group_name = self.group_name
        description = self.description
        owners = self.owners or []

        servers = spec.get("servers") or [{}]
        _raw_server_url = servers[0].get("url", "") if servers else ""
        base_url_from_spec = _resolve_server_base_url(self.spec_url, _raw_server_url) if _raw_server_url else ""

        partitions_def = _build_partitions_def(
            self.partition_type,
            self.partition_start,
            self.partition_values,
            self.dynamic_partition_name,
            self.partition_dimensions,
            cron_schedule=self.partition_cron_schedule,
            cron_fmt=self.partition_cron_fmt or "%Y-%m-%d",
            end_offset=self.partition_end_offset,
            timezone=self.partition_timezone,
        )

        _inferred_kinds = list(self.kinds or [])
        if "api" not in _inferred_kinds:
            _inferred_kinds.append("api")
        if "openapi" not in _inferred_kinds:
            _inferred_kinds.append("openapi")

        _freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from datetime import timedelta
            from dagster import FreshnessPolicy
            _lag = timedelta(minutes=int(self.freshness_max_lag_minutes))
            if self.freshness_cron:
                _freshness_policy = FreshnessPolicy.cron(deadline_cron=self.freshness_cron, lower_bound_delta=_lag)
            else:
                _freshness_policy = FreshnessPolicy.time_window(fail_window=_lag)

        _retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            _retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        _backfill_policy_obj = None
        if partitions_def is not None and self.backfill_policy == "single_run":
            _backfill_policy_obj = BackfillPolicy.single_run()
        elif partitions_def is not None and self.backfill_policy == "multi_run":
            _backfill_policy_obj = BackfillPolicy.multi_run(max_partitions_per_run=self.backfill_policy_max_partitions_per_run or 1)

        def _make_asset_fn(_op):
            # Factory, not a loop-variable closure -- see openapi_ingestion's own comment on
            # this exact bug (a closure over the loop var captures the LAST iteration's value
            # for every asset; a factory's own parameter is correctly bound per-call).
            def _asset_fn(context: AssetExecutionContext):
                import time
                import requests

                base_url = base_url_from_spec or _require_env(api_base_url_env_var)
                api_key = _require_env(api_key_env_var) if api_key_env_var else None

                is_range = (not context.has_partition_key) and context.has_partition_key_range
                keys: List[Any] = [None]
                if context.has_partition_key or is_range:
                    keys = list(context.partition_keys)

                def _call_for(pk):
                    substitutions = {"run_id": context.run_id}
                    if pk is not None:
                        if hasattr(pk, "keys_by_dimension"):
                            substitutions["partition_key"] = str(pk)
                            substitutions["partition_keys"] = dict(pk.keys_by_dimension)
                        else:
                            substitutions["partition_key"] = str(pk)
                            substitutions["partition_keys"] = {}
                    resolved_args = _substitute_args(dict(operation_args.get(_op["operation_id"], {})), substitutions)

                    path_params: Dict[str, Any] = {}
                    query_params: Dict[str, Any] = {}
                    header_params: Dict[str, str] = {}
                    body = dict(resolved_args)
                    param_loc = {p["name"]: p["in"] for p in _op["parameters"]}
                    for name, val in resolved_args.items():
                        loc = param_loc.get(name)
                        if loc == "path":
                            path_params[name] = val
                            body.pop(name, None)
                        elif loc == "query":
                            query_params[name] = val
                            body.pop(name, None)
                        elif loc == "header":
                            header_params[name] = str(val)
                            body.pop(name, None)

                    url_path = _op["full_path"]
                    for name, val in path_params.items():
                        url_path = url_path.replace("{" + name + "}", str(val))
                    url = url_path if url_path.startswith("http") else f"{base_url}{url_path}"

                    headers = dict(header_params)
                    if api_key:
                        headers[auth_header] = f"{auth_prefix} {api_key}" if auth_prefix else api_key

                    req_kwargs: Dict[str, Any] = {"headers": headers, "params": query_params, "timeout": request_timeout_seconds}
                    if _op["has_request_body"] and body:
                        req_kwargs["json"] = body

                    context.log.info(f"Calling {_op['method']} {url}")
                    _t0 = time.time()
                    resp = requests.request(_op["method"], url, **req_kwargs)
                    latency = round(time.time() - _t0, 3)
                    resp.raise_for_status()
                    raw = resp.text

                    if parse_as == "text":
                        value, kind = raw, "text"
                    else:
                        try:
                            value, kind = resp.json(), "json"
                        except ValueError:
                            if parse_as == "json":
                                raise RuntimeError(
                                    f"operation {_op['operation_id']!r} returned non-JSON body; raw[:500]={raw[:500]!r}"
                                )
                            value, kind = raw, "text"

                    return resolved_args, {
                        "value": value, "raw": raw, "raw_size": len(raw), "kind": kind,
                        "status_code": resp.status_code, "latency_seconds": latency, "url": url,
                    }

                calls = [(pk,) + _call_for(pk) for pk in keys]
                resolved_args_out = calls[0][1] if not is_range else {str(pk): r for pk, r, _res in calls}
                result = calls[0][2]

                md = {
                    "operation_id": MetadataValue.text(_op["operation_id"]),
                    "method": MetadataValue.text(_op["method"]),
                    "path": MetadataValue.text(_op["path"]),
                    "url": MetadataValue.text(result["url"]),
                    "status_code": MetadataValue.int(result["status_code"]),
                    "args": MetadataValue.json(resolved_args_out),
                    "transport": MetadataValue.text("http"),
                    "latency_seconds": MetadataValue.float(sum(c[2]["latency_seconds"] for c in calls)),
                    "response_bytes": MetadataValue.int(sum(c[2]["raw_size"] for c in calls)),
                    "parsed_as": MetadataValue.text(result["kind"]),
                }
                if is_range:
                    md["partition_keys"] = MetadataValue.json([str(pk) for pk in keys])

                if not is_range and result["kind"] == "json":
                    if isinstance(result["value"], list):
                        md["response_preview"] = MetadataValue.json(result["value"][:20])
                    elif isinstance(result["value"], dict):
                        md["response_preview"] = MetadataValue.json(dict(list(result["value"].items())[:20]))
                    else:
                        md["response_preview"] = MetadataValue.json(result["value"])
                else:
                    _raw = result["raw"] if not is_range else "\n".join(f"[{pk}] {c[2]['raw'][:500]}" for pk, c in zip(keys, calls))
                    md["response_preview"] = MetadataValue.json(_raw[:2000])

                context.add_output_metadata(md)

                if is_range:
                    return {str(pk): c[2]["value"] for pk, c in zip(keys, calls)}
                return result["value"]

            return _asset_fn

        assets = []
        for op in operations:
            _safe_name = _safe_asset_name(op["operation_id"])
            asset_name = f"{asset_name_prefix}_{_safe_name}" if asset_name_prefix else _safe_name

            tags = dict(self.asset_tags or {})
            for k in _inferred_kinds:
                tags[f"dagster/kind/{k}"] = ""
            tags.setdefault("dagster-community/tool-source", "openapi")
            tags.setdefault("dagster-community/openapi-operation", _safe_tag_value(op["operation_id"]))

            asset_def = asset(
                key=AssetKey.from_user_string(asset_name),
                partitions_def=partitions_def,
                backfill_policy=_backfill_policy_obj,
                group_name=group_name,
                description=description or op["summary"] or f"OpenAPI operation {op['operation_id']}: {op['method']} {op['path']}",
                owners=owners,
                tags=tags,
                freshness_policy=_freshness_policy,
                retry_policy=_retry_policy,
            )(_make_asset_fn(op))
            assets.append(asset_def)

        return Definitions(assets=assets)


# ── stdio MCP server entrypoint:  python -m <dotted.path.to.this.module>  ───────────────
# Used by mcp_server_resource_key (mode="mcp_server") -- NOT needed for context.resources.
# <resource_key>.build_server() hosting (an ASGI mount, a script) or dg.materialize()'s own
# inspection asset, both of which build the server in-process without spawning this file.
def main(argv: Optional[List[str]] = None) -> None:
    """Env: OPENAPI_TOOL_SERVER_SPEC_URL or OPENAPI_TOOL_SERVER_SPEC_PATH (one required),
    OPENAPI_TOOL_SERVER_SERVER_NAME (default 'OpenAPI Tool Server'),
    OPENAPI_TOOL_SERVER_OPERATION_IDS (csv, default: every operation),
    OPENAPI_TOOL_SERVER_API_BASE_URL_ENV_VAR (default 'API_BASE_URL'),
    OPENAPI_TOOL_SERVER_API_KEY_ENV_VAR, OPENAPI_TOOL_SERVER_AUTH_HEADER (default
    'Authorization'), OPENAPI_TOOL_SERVER_AUTH_PREFIX (default 'Bearer'),
    OPENAPI_TOOL_SERVER_VALIDATE_OUTPUT ('1'/'0', default '1')."""
    import logging
    import os
    import sys

    logging.basicConfig(level=logging.INFO, stream=sys.stderr, format="%(message)s")  # stdout is the MCP channel
    spec_url = os.environ.get("OPENAPI_TOOL_SERVER_SPEC_URL")
    spec_path = os.environ.get("OPENAPI_TOOL_SERVER_SPEC_PATH")
    if not spec_url and not spec_path:
        raise SystemExit("OPENAPI_TOOL_SERVER_SPEC_URL or OPENAPI_TOOL_SERVER_SPEC_PATH must be set")
    spec = _fetch_spec(spec_url, spec_path)

    operation_ids_env = os.environ.get("OPENAPI_TOOL_SERVER_OPERATION_IDS")
    operation_ids = [o.strip() for o in operation_ids_env.split(",") if o.strip()] if operation_ids_env else None

    resource = OpenApiMcpServerResource(
        spec=spec,
        spec_url=spec_url,
        server_name=os.environ.get("OPENAPI_TOOL_SERVER_SERVER_NAME") or "OpenAPI Tool Server",
        operation_ids=operation_ids,
        api_base_url_env_var=os.environ.get("OPENAPI_TOOL_SERVER_API_BASE_URL_ENV_VAR") or "API_BASE_URL",
        api_key_env_var=os.environ.get("OPENAPI_TOOL_SERVER_API_KEY_ENV_VAR"),
        auth_header=os.environ.get("OPENAPI_TOOL_SERVER_AUTH_HEADER") or "Authorization",
        auth_prefix=os.environ.get("OPENAPI_TOOL_SERVER_AUTH_PREFIX") or "Bearer",
        validate_output=os.environ.get("OPENAPI_TOOL_SERVER_VALIDATE_OUTPUT", "1") == "1",
    )
    resource.build_server().run(transport="stdio")


if __name__ == "__main__":
    main()
