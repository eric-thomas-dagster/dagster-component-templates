# OpenAPI Asset Component

Auto-discovers all GET endpoints from an OpenAPI spec (JSON or YAML, URL or local file), groups them by tag, and creates **one Dagster asset per tag group**. At execution time each asset fetches its endpoints and writes results to a destination database table via SQLAlchemy.

Built on `StateBackedComponent` so the spec is fetched **once** and cached — code-server reloads are instant even for large specs with hundreds of endpoints.

---

## What it does

1. **Prepare time** (`write_state_to_path`): fetches the OpenAPI spec and extracts all GET endpoints, grouped by their first tag. The result is serialised to a local JSON cache file.
2. **Load time** (`build_defs_from_state`): reads the cache and builds one `@dg.asset` per tag — no network calls, no parsing, instant startup.
3. **Execution time**: each asset iterates its endpoints, handles pagination automatically, flattens JSON responses with `pandas.json_normalize`, and writes to the destination table via SQLAlchemy's `DataFrame.to_sql`.

---

## Why StateBackedComponent matters

Without caching, every code-server reload (triggered by file saves, deployments, or `dagster dev` restarts) would fetch the OpenAPI spec over the network. For a large API with a multi-megabyte spec hosted behind a slow connection this can add seconds or tens of seconds to startup time, and can fail entirely if the spec endpoint is unavailable.

`StateBackedComponent` separates **spec discovery** from **asset definition loading**:

- Spec is fetched once, explicitly, via `dg utils refresh-defs-state` or on the first `dagster dev` start.
- Subsequent reloads read the local cache file — sub-millisecond, offline-capable.
- The cache can be committed to version control so CI/CD never needs to reach the API.

---

## Required packages

| Package | Minimum version | Purpose |
|---------|----------------|---------|
| `requests` | 2.31.0 | Fetch the spec and API responses |
| `pyyaml` | 6.0.0 | Parse YAML-format OpenAPI specs |
| `pandas` | 1.3.0 | Flatten nested JSON and write to SQL |
| `sqlalchemy` | 2.0.0 | Database connectivity |

Install with:

```bash
pip install requests>=2.31.0 pyyaml>=6.0.0 pandas>=1.3.0 sqlalchemy>=2.0.0
```

You will also need the appropriate SQLAlchemy dialect driver for your destination database (e.g. `psycopg2` for PostgreSQL, `pymysql` for MySQL, `pyodbc` for SQL Server).

---

## Configuration fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `spec_url` | `str \| null` | `null` | URL of the OpenAPI spec (JSON or YAML). Mutually exclusive with `spec_path`. |
| `spec_path` | `str \| null` | `null` | Path to a local OpenAPI spec file. Mutually exclusive with `spec_url`. |
| `api_base_url_env_var` | `str` | `API_BASE_URL` | Name of the env var holding the API base URL used at execution time. |
| `api_key_env_var` | `str \| null` | `null` | Name of the env var holding the API key. If omitted, no auth header is sent. |
| `auth_header` | `str` | `Authorization` | HTTP header name to use for the API key. |
| `auth_prefix` | `str` | `Bearer` | Prefix added before the key value (e.g. `Bearer <key>`). Set to `""` to send the key without a prefix. |
| `database_url_env_var` | `str` | `DATABASE_URL` | Name of the env var holding the SQLAlchemy connection URL for the destination database. |
| `schema_name` | `str \| null` | `null` | Database schema to write tables into. If omitted, uses the default schema. |
| `if_exists` | `str` | `replace` | Behaviour when a table already exists: `replace`, `append`, or `fail`. Passed directly to `DataFrame.to_sql`. |
| `include_tags` | `list[str] \| null` | `null` | Whitelist of OpenAPI tags to include. If `null`, all tags are included. |
| `exclude_tags` | `list[str] \| null` | `null` | Blacklist of OpenAPI tags to exclude. Applied after `include_tags`. |
| `group_name` | `str \| null` | `openapi` | Dagster asset group name shown in the UI. |
| `page_size` | `int` | `100` | Number of records to request per page when paginating. |
| `max_pages` | `int` | `100` | Maximum number of pages to fetch per endpoint, as a safety cap. |

---

## Example YAML

```yaml
type: dagster_component_templates.OpenAPIAssetComponent
attributes:
  spec_url: "https://api.mycompany.com/openapi.json"
  api_base_url_env_var: API_BASE_URL
  api_key_env_var: API_KEY
  database_url_env_var: DATABASE_URL
  schema_name: raw_api
  include_tags:
    - customers
    - orders
    - products
  page_size: 250
```

Local file spec with PostgreSQL destination and custom auth header:

```yaml
type: dagster_component_templates.OpenAPIAssetComponent
attributes:
  spec_path: "./specs/myapi.yaml"
  api_base_url_env_var: MYAPI_BASE_URL
  api_key_env_var: MYAPI_TOKEN
  auth_header: X-API-Token
  auth_prefix: ""
  database_url_env_var: POSTGRES_URL
  schema_name: landing
  if_exists: append
  exclude_tags:
    - internal
    - deprecated
```

---

## Authentication options

Three fields control how the API key is sent:

| Scenario | `api_key_env_var` | `auth_header` | `auth_prefix` | Resulting header |
|----------|------------------|---------------|---------------|-----------------|
| Bearer token (default) | `API_KEY` | `Authorization` | `Bearer` | `Authorization: Bearer <key>` |
| Raw token | `API_KEY` | `Authorization` | `""` | `Authorization: <key>` |
| Custom header | `API_KEY` | `X-API-Key` | `""` | `X-API-Key: <key>` |
| No auth | _(omit)_ | — | — | _(no header sent)_ |

---

## Pagination

The component auto-detects pagination style from the OpenAPI parameter names:

| Parameter names found | Strategy used |
|-----------------------|---------------|
| `page` or `page_number` | Page-based: increments `page` from 1, stops when a page returns fewer than `page_size` records or an empty response |
| `offset` | Offset-based: increments `offset` by `page_size` each request, stops on empty/short response |
| Neither | Single request: fetches the endpoint once with no pagination parameters |

Both strategies respect `max_pages` as a hard cap to prevent runaway loops.

For endpoints whose JSON response wraps the array inside an object key (e.g. `{"customers": [...], "total": 500}`), the component inspects the response schema and automatically unwraps the array.

---

## Refreshing the cached state

### First run / explicit refresh

```bash
# Dagster 1.9+
dg utils refresh-defs-state

# Or simply start the dev server — state is written on first load if no cache exists
dagster dev
```

### When to refresh

- After updating `spec_url` or `spec_path` to point at a new spec.
- After the upstream API adds or removes tags/endpoints and you want the asset graph to reflect those changes.
- You can automate this as a CI step before deploying to production.

---

## Fallback for older Dagster versions

If `StateBackedComponent` is not available in the installed Dagster version (pre-1.9), the component falls back to a standard `dg.Component` / `dg.Model` implementation. In fallback mode:

- The OpenAPI spec is **fetched on every code-server reload** (no caching).
- All the same configuration fields are supported.
- The `defs_state` field has no effect.

You can check which mode is active by looking for the `StateBackedComponent` import at the top of `component.py` — if `_HAS_STATE_BACKED` is `False` at runtime, the fallback is in use. Upgrade to Dagster 1.9+ to get the cached behaviour.
