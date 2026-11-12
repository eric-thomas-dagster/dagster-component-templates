# OpenAPI Tool Server

Turn an OpenAPI/Swagger spec's operations into **agent-callable tools**, gated by an `operationId` allow-list.

This is the counterpart to `openapi_ingestion`: that component pulls GET endpoints into a data table on a schedule (an ingestion component). This component exposes operations as callable actions an agent can invoke on demand — it doesn't ingest anything into a warehouse.

---

## Why this exists

An OpenAPI spec already fully describes a set of callable operations — paths, methods, parameters, request/response schemas. `openapi_ingestion` only reads GET endpoints into a table. If you want an LLM agent (or a deterministic pipeline) to actually *call* one of those operations — "get this pet by id", "create this order" — you need either real MCP tools built from the spec, or one real Dagster asset per operation it can call directly. This component builds either, from the same spec + allow-list.

---

## Modes

### `mode: assets` (default)

One Dagster asset **per allow-listed operation**. Each asset makes a direct HTTP call (`requests`) to that operation's endpoint — no MCP, no LLM, fully deterministic. Same args-templating / partition / backfill / retry / freshness wiring as `mcp_tool_call`.

Tagged `dagster-community/tool-source: openapi` + `dagster-community/openapi-operation: <operationId>` (not the `dagster-community/mcp-server` tag `mcp_tool_call` uses — there's no MCP server involved here).

```yaml
type: dagster_community_components.OpenApiToolServerComponent
attributes:
  spec_url: "https://petstore3.swagger.io/api/v3/openapi.json"
  api_key_env_var: PETSTORE_API_KEY
  operation_ids:
    - getPetById
    - findPetsByStatus
  operation_args:
    findPetsByStatus:
      status: available
```

`operation_args` is keyed by `operationId`. Each operation's args dict is matched against its declared parameters by name — the spec's own `in: path` / `in: query` / `in: header` tells this component where each arg goes; anything left over is sent as a JSON request body field (for operations with a `requestBody`).

### `mode: mcp_server`

Builds a real `fastmcp.FastMCP` server directly from the parsed spec (`FastMCP.from_openapi`), with the `operation_ids` allow-list enforced via `route_map_fn` — operations outside the allow-list map to `MCPType.EXCLUDE` and are never registered as tools on the server.

A live server process isn't something `build_defs` can return (Dagster's definitions are a static graph, not a running service), so this mode emits two things instead:

1. **A resource** (`resource_key`, default `openapi_mcp_server`) holding an `OpenApiMcpServerResource` that *lazily* builds the FastMCP server on demand via `.build_server()`. Your own code (a script, an ASGI mount, a sensor) reads it from `context.resources.<resource_key>` to actually host the server.
2. **A small inspection asset** (`inspection_asset_name`, default `openapi_mcp_tools`) that materializes by building the server and listing its resulting tools, reporting tool count + names as metadata. Without this the component would be invisible in the asset graph / catalog UI when `mode="mcp_server"` is picked — there'd be nothing to look at.

Tagged `dagster-community/mcp-server: <server_name>` (this mode genuinely builds an MCP server, so the existing convention applies) + `dagster-community/tool-source: openapi`.

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

Requires `fastmcp>=2.0` (lazily imported — only needed for this mode; `mode="assets"` never imports it).

### Referencing it by key (`mcp_server_resource_key`)

The resource above needs *your own code* to host it (an ASGI mount, a script). If you just
want an agent to call this spec's operations as tools, set `mcp_server_resource_key`
instead -- it ALSO registers a stdio MCP-server-shaped resource under that key, built
from this component's own `spec_url`/`spec_path`/`operation_ids`/`server_name`/
`api_base_url_env_var`/`api_key_env_var`/etc. settings (`python -m` this module, run as a
real subprocess). The actual secret value(s) named by `api_base_url_env_var`/
`api_key_env_var` are read fresh from the environment every time a consumer resolves this
resource (run time), not baked in at component load -- same principle
`dagster_workflow_tools`'s own `mcp_server_resource_key` applies to `DAGSTER_HOME`.

```yaml
type: dagster_community_components.OpenApiToolServerComponent
attributes:
  mode: mcp_server
  spec_url: "https://petstore3.swagger.io/api/v3/openapi.json"
  api_key_env_var: PETSTORE_API_KEY
  operation_ids: [getPetById, findPetsByStatus]
  mcp_server_resource_key: petstore_mcp
```

Then, in any `mcp_servers`-aware component:

```yaml
mcp_servers:
  - server_resource_key: petstore_mcp
```

Requires `mode='mcp_server'` -- setting `mcp_server_resource_key` with `mode='assets'`
raises a clear error at component load.

---

## Args templating (mode="assets")

Same convention as `mcp_tool_call`. String values inside `operation_args.<operationId>` get these substitutions at materialization time:

| Token | Value |
|---|---|
| `{run_id}` | Current Dagster run id |
| `{partition_key}` | Current partition (single-partition) |
| `{partition_keys.<dim>}` | Specific axis of a multi-dim partition |

Only string leaves are touched — numbers / bools pass through unchanged.

---

## Auth

Same convention as `openapi_ingestion`: `api_key_env_var` names an env var read at call time; `auth_header` (default `Authorization`) + `auth_prefix` (default `Bearer`) control how it's formatted into the request header. `api_base_url_env_var` is only consulted when the spec itself has no `servers[].url` entry — the spec's own server URL otherwise wins.

---

## Standard Dagster patterns supported (mode="assets")

| | |
|---|---|
| Partitions | `partition_type` + `partition_start` / `partition_values` / `dynamic_partition_name`, or `partition_dimensions` for multi-axis |
| Backfill | `backfill_policy` (`single_run` / `multi_run`) |
| Freshness | `freshness_max_lag_minutes` + `freshness_cron` |
| Retry | `retry_policy_max_retries` + `retry_policy_delay_seconds` + `retry_policy_backoff` |
| Kinds | `kinds` (`mode="assets"` always adds `api` and `openapi`, even on top of your own list — the same way `mcp_tool_call` always adds `mcp`) |
| Owners / Tags / Description / Group | `owners`, `asset_tags`, `description`, `group_name` |

`mode="mcp_server"` only emits the resource + the one inspection asset — partitioning / backfill / retry / freshness fields are ignored in that mode (there's only one asset, and it doesn't call the API itself).

---

## Output metadata (mode="assets")

Every operation asset emits this metadata on each materialization, the same discoverable-by-name convention as `mcp_tool_call`:

| Key | Meaning |
|---|---|
| `operation_id` | The OpenAPI `operationId` this asset was built from. |
| `method` | HTTP method (`GET`, `POST`, ...). |
| `path` | The raw OpenAPI path template, e.g. `/pets/{id}` (unsubstituted — see `url` for the resolved call). |
| `url` | The fully resolved URL actually called. |
| `status_code` | HTTP status code of the response. |
| `latency_seconds` | Wall-clock time of the HTTP call (summed across partition keys on a multi-partition run). |
| `response_bytes` | Size of the raw response body in bytes (summed across partition keys on a multi-partition run). |
| `transport` | Always `"http"` — present so a UI can tell an `openapi_tool_server` asset's call transport apart from an `mcp_tool_call` asset's `transport` value (`"stdio"`/`"sse"`/`"http"`). |
| `parsed_as` | `"json"` or `"text"`, per `parse_as` / actual response content. |
| `response_preview` | First ~20 list items / dict keys (or first 2000 chars of raw text) of the response. |
| `args` | The resolved request args actually sent (post-templating). |
| `partition_keys` | Only present on a partition-range run: the list of partition keys covered. |

---

## Known limitations

- **Required path/query params aren't validated.** If an operation declares a required `in: path` parameter and `operation_args` doesn't supply it, the call is made with the literal `{paramName}` segment still in the URL (which a real API will reject, loudly — not a silent wrong-data bug, just an unvalidated one).
- **`$ref`-indirected parameters aren't resolved.** An operation whose parameters are declared via `$ref` (rather than inline) has those entries skipped during extraction; any arg you pass for them is sent as a request body field instead of being placed in path/query/header.
- **`mode="assets"` fetches the spec at every `build_defs` call** (no state caching). `openapi_ingestion` uses `StateBackedComponent` to avoid re-fetching a large spec on every code-server reload; this component doesn't (yet) — a large remote spec means a slower reload. Worth adopting the same caching if this becomes a problem in practice.
- **A relative `servers[].url` only resolves when the spec was fetched via `spec_url`** (not `spec_path`). The real Swagger Petstore v3 spec is relative (`servers: [{url: "/api/v3"}]`, resolved against the host that served it) — this is now resolved correctly against `spec_url` in both modes; a spec loaded from `spec_path` with a relative `servers[].url` and no `spec_url` can't be resolved the same way and falls through to `api_base_url_env_var`.

---

## fastmcp API notes (mode="mcp_server")

Verified by introspecting the installed `fastmcp==4.1.8` package directly, and live-testing `FastMCP.from_openapi(...)` + `route_map_fn` against a real 3-operation spec with a 2-operation allow-list (confirmed exactly 2 tools registered, the 3rd excluded):

- `RouteMap` / `MCPType` / `RouteMapFn` live at `fastmcp.server.providers.openapi` in this version — not `fastmcp.server.openapi`, an older module path that doesn't exist in 4.1.8.
- `route_map_fn(route, mcp_type)` returning `None` means *"no override, keep the default"* (every route defaults to `MCPType.TOOL`) — excluding a non-allow-listed operation requires explicitly returning `MCPType.EXCLUDE`.
- `FastMCP.from_openapi`'s `client` accepts a plain `httpx.AsyncClient` (with a deprecation warning) in addition to the newer `httpx2.AsyncClient` — this component tries `httpx2` first, falling back to the far more common `httpx` package, so it doesn't add a dependency most projects don't already have.
- Not verified: fastmcp's older 2.x/3.x module layout. If your installed fastmcp doesn't match the 4.1.8-shaped import, this fails loudly with an actionable message rather than guessing at a compat shim for a version that was never actually tested against.

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Connection

| Field | Type | Default | Description |
|---|---|---|---|
| `api_key_env_var` | `str` | — | Env var holding the API key/token for Authorization. |
| `resource_key` | `str` | `"openapi_mcp_server"` | mode='mcp_server' only. Dagster resource key the built OpenApiMcpServerResource is registered under -- other code (a script, an ASGI mount, a sensor) reads it via context.resources.<resource_key>.build_server() to actually host the server. |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `group_name` | `str` | — | Dagster asset group name. |
| `description` | `str` | — | Asset description. mode='assets': applied to every emitted operation asset unless the operation has its own OpenAPI summary. |
| `owners` | `List[str]` | — | Asset owners. |
| `asset_tags` | `Union[Dict[str, Any], List[str]]` | — | Extra asset tags, applied to every emitted asset. |
| `kinds` | `List[str]` | — | Asset kinds for the Dagster catalog. mode='assets' always adds 'api' and 'openapi' (even if you set your own kinds), the same way mcp_tool_call always adds 'mcp' -- so a UI can find every operation asset this component emits by kind alone. |
| `backfill_policy` | `Literal['single_run', 'multi_run']` | — | mode='assets' only. 'single_run' materializes an entire backfill range in one run (one HTTP call per partition in the range, each with its own {partition_key} substitution). 'multi_run' launches one run per partition. Le… _(full docs in schema.json + component README)_ |

### Freshness

| Field | Type | Default | Description |
|---|---|---|---|
| `freshness_max_lag_minutes` | `int` | — | FreshnessPolicy max lag in minutes. |
| `freshness_cron` | `str` | — | FreshnessPolicy cron schedule. |

### Partitions

| Field | Type | Default | Description |
|---|---|---|---|
| `partition_type` | `str` | — | mode='assets' only. 'daily' \| 'weekly' \| 'monthly' \| 'hourly' \| 'cron' \| 'static' \| 'multi' \| 'dynamic'. Applied to every emitted operation asset. |
| `partition_cron_schedule` | `str` | — | Required when partition_type='cron'. A cron expression restricting the date axis to a custom cadence, e.g. '0 0 * * 1-5' for weekdays only. |
| `partition_cron_fmt` | `str` | `"%Y-%m-%d"` | strftime format for partition keys produced by partition_type='cron'. |
| `partition_end_offset` | `int` | `0` | Passed through to the underlying time-based partitions definition. 0 means a time-based partition set only ever holds COMPLETED windows. |
| `partition_timezone` | `str` | — | IANA timezone for the underlying time-based partitions definition (defaults to UTC when unset). |
| `partition_start` | `str` | — | ISO date for time-based partitions. |
| `partition_values` | `Union[str, List[str]]` | — | Comma-separated values for static/multi. |
| `partition_dimensions` | `List[Dict[str, Any]]` | — | Multi-axis partition spec: list of {name, type, start, values} dicts. |

### Retry policy

| Field | Type | Default | Description |
|---|---|---|---|
| `retry_policy_max_retries` | `int` | — | RetryPolicy max retries. |
| `retry_policy_delay_seconds` | `int` | — | Seconds between retries. |
| `retry_policy_backoff` | `str` | `"exponential"` | 'linear' or 'exponential'. |

### Source / target

| Field | Type | Default | Description |
|---|---|---|---|
| `mode` | `Literal['assets', 'mcp_server']` | `"assets"` | 'assets' (default, simpler, more Dagster-native): one asset per allow-listed operation, each a direct HTTP call. 'mcp_server': build a real fastmcp.FastMCP server from the spec, exposed via a resource (resource_key) othe… _(full docs in schema.json + component README)_ |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `spec_url` | `str` | — | URL to fetch the OpenAPI/Swagger spec from (JSON or YAML). Takes precedence over spec_path if both are set. |
| `spec_path` | `str` | — | Local file path to the OpenAPI/Swagger spec (JSON or YAML). |
| `api_base_url_env_var` | `str` | `"API_BASE_URL"` | Env var with the API base URL. Only used as a fallback when the spec has no servers[] entry with a URL. |
| `auth_header` | `str` | `"Authorization"` | HTTP header name for the credential. |
| `auth_prefix` | `str` | `"Bearer"` | Prefix before the credential value in the header, e.g. 'Bearer'. Set to empty string for none (raw key as the header value). |
| `operation_ids` | `List[str]` | — | Allow-list of operationId values (the OpenAPI spec's own `operationId` field per operation). Operations NOT in this list are NEVER exposed, in either mode. Leave unset to expose every operation in the spec. |
| `asset_name_prefix` | `str` | — | mode='assets' only. Prefix for emitted asset keys: '{asset_name_prefix}_{operation_id}' (sanitized). Useful to avoid collisions when two instances of this component target different specs. |
| `operation_args` | `Dict[str, Dict[str, Any]]` | `dict()` | mode='assets' only. Per-operationId call args, keyed by operationId. Each value is a flat dict of {param_name: value} -- matched against the operation's declared path/query/header parameters by name (path/query/header pl… _(full docs in schema.json + component README)_ |
| `parse_as` | `str` | `"auto"` | mode='assets' only. How to parse each operation's response body: 'json' (raise if not JSON), 'text' (raw string), 'auto' (try JSON, fall back to text). |
| `request_timeout_seconds` | `int` | `30` | mode='assets' only. Per-call HTTP timeout. |
| `server_name` | `str` | `"OpenAPI Tool Server"` | mode='mcp_server' only. Name for the built FastMCP server. |
| `inspection_asset_name` | `str` | `"openapi_mcp_tools"` | mode='mcp_server' only. Name of the small inspection asset that lists the built server's resulting tools. |
| `validate_output` | `bool` | `true` | mode='mcp_server' only. Passed through to FastMCP.from_openapi -- validate tool responses against the spec's output schema. |
| `mcp_server_resource_key` | `str` | — | mode='mcp_server' only. If set, ALSO registers a stdio MCP-server-shaped resource (same name/type/command/env shape as MCPServerResource) under this key, built from this component's own settings -- python -m this module… _(full docs in schema.json + component README)_ |
| `dynamic_partition_name` | `str` | — | Name for DynamicPartitionsDefinition. |
| `backfill_policy_max_partitions_per_run` | `int` | — | For backfill_policy='multi_run' only: maximum partitions materialized per run. |

[//]: # (FIELDS:END)

---

## Example

See `example.yaml`.
