# dlt REST API Source (Advanced)

> **Advanced / escape-hatch component.** If a dedicated hand-built component for your vendor already exists in this catalog (search `manifest.json` or the Designer UI), use that one instead -- it has typed fields and committed tests pinned to that vendor's real, verified API shape. Use `dlt_rest_api_source` for the long tail: a vendor with no dedicated component yet, an internal/private API, or a one-off integration not worth hand-building a whole component for.

Bring-your-own REST API source config. This component is a thin, config-driven passthrough to dlt's generic REST API source engine (`dlt.sources.rest_api.rest_api_source`) -- the exact same underlying engine every hand-built `*_ingestion` component in this repo (chargify_ingestion, reclaim_ingestion, hotjar_ingestion, and ~20 others) wraps with vendor-specific typed fields. This component skips the typed-fields layer and exposes the underlying config shape directly as resolvable YAML attributes:

```yaml
client:
  base_url: "https://api.example.com"
  auth: { ... }          # bearer / api_key / http_basic / oauth2_client_credentials
  headers: { ... }        # optional
  paginator: { ... }       # optional
resources:
  - name: customers
    endpoint:
      path: customers
      data_selector: "$"   # optional
      params: { ... }      # optional
      paginator: { ... }    # optional
```

That's the entire `rest_api_source` config contract -- `client` + `resources` -- so any REST API that fits it can be wired up with zero new Python code.

## Where to find config snippets

[dltHub's Context marketplace](https://dlthub.com/context) (`https://dlthub.com/context/source/<vendor>`) is a real, community-maintained library of pre-researched `rest_api_source` config snippets for specific vendors, generated from their public API docs. This session found it genuinely useful and accurate for several vendors when researching hand-built components -- but not all; always sanity-check a pasted snippet against the vendor's actual API docs (auth flow, pagination shape, required params) before trusting it in production, the same way every hand-built component in this repo cites its own primary-source verification. Paste the `client` / `resources` blocks it generates directly into this component's attributes.

## Secrets

This component does no secret-resolution of its own. Use this repo's existing component-YAML templating convention -- `{{ env.VAR_NAME }}` -- anywhere inside `client:` / `resources:` (the same mechanism already used by, e.g., `integrations/snowflake_workspace`'s nested `workspace:` block). It's resolved by dg's component loader before these values ever reach this component, so arbitrarily nested auth/header/param dicts can pull secrets from the environment with no special-casing required here:

```yaml
client:
  base_url: "https://api.example.com"
  auth:
    type: bearer
    token: "{{ env.MY_VENDOR_API_TOKEN }}"
```

## Supported auth shapes

Matches what the hand-built components in this catalog already use:

| `auth.type` | Shape |
|---|---|
| `bearer` | `{type: bearer, token: ...}` |
| `api_key` | `{type: api_key, name: ..., api_key: ..., location: header\|query}` |
| `http_basic` | `{type: http_basic, username: ..., password: ...}` |
| `oauth2_client_credentials` | `{type: oauth2_client_credentials, access_token_url: ..., client_id: ..., client_secret: ...}` |

## Dependent (parent/child) resources

Some real APIs need one request per row of a parent resource (e.g. "list surveys, then fetch each survey's responses"). dlt's `resolve` mechanism handles this -- see `hotjar_ingestion` in this repo for a complete worked example (its `survey_details` / `survey_responses` resources N+1 off of `surveys`):

```yaml
resources:
  - name: surveys
    endpoint:
      path: "sites/{{ env.SITE_ID }}/surveys"
      data_selector: results
  - name: survey_responses
    endpoint:
      path: "sites/{{ env.SITE_ID }}/surveys/{survey_id}/responses"
      data_selector: results
      params:
        survey_id:
          type: resolve
          resource: surveys
          field: id
```

## Validation

Since there's no vendor-specific API shape to validate against, this component validates the generic config shape itself at Definitions-build time (before it ever reaches dlt), with clear error messages:

- `client.base_url` is required.
- `resources` must be a non-empty list.
- Each resource entry requires `name` and `endpoint.path`.
- A malformed `endpoint` (not a mapping, or missing `path`) raises a clear `ValueError` naming the offending resource, instead of a raw pydantic/dlt traceback.

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination (snowflake, bigquery, postgres, filesystem, etc.). See `../DESTINATIONS.md`.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `client` | required | dlt `rest_api_source` `client` config (`base_url`, `auth`, optional `headers`/`paginator`) |
| `resources` | required | dlt `rest_api_source` `resources` list (non-empty; each needs `name` + `endpoint.path`) |

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `asset_name` | `str` | Name of the asset that will hold the data |
| `client` | `Dict[str, Any]` | dlt rest_api_source `client` config -- required keys/shape: `base_url` (required), `auth` (a dict; supports dlt's built-in auth types: {type: bearer, token: ...}, {type: api_key, name: ..., api_key: ..., location: header… _(full docs in schema.json + component README)_ |
| `resources` | `List[Dict[str, Any]]` | dlt rest_api_source `resources` list -- one dict per resource, each requiring `name` and `endpoint` (`endpoint.path` is required; optional `endpoint.data_selector`, `endpoint.params`, `endpoint.paginator`). Supports dlt'… _(full docs in schema.json + component README)_ |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `description` | `str` | — | Asset description |
| `group_name` | `str` | `"dlt_rest_api_source"` | Asset group for organization |
| `owners` | `List[str]` | — | Asset owners -- list of team names or email addresses, e.g. ['team:analytics', 'user@company.com'] |
| `asset_tags` | `Dict[str, str]` | — | Additional key-value tags to apply to the asset, e.g. {'domain': 'finance', 'tier': 'gold'} |
| `kinds` | `List[str]` | — | Asset kinds for the Dagster catalog. Auto-inferred from destination if not set. |
| `deps` | `List[str]` | — | Upstream asset keys this asset depends on (e.g. ['raw_orders', 'schema/asset']) |
| `column_lineage` | `Dict[str, List[str]]` | — | Column-level lineage: output column -> list of upstream columns it derives from. |

### Freshness

| Field | Type | Default | Description |
|---|---|---|---|
| `freshness_max_lag_minutes` | `int` | — | Maximum acceptable lag in minutes before the asset is considered stale. |
| `freshness_cron` | `str` | — | Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5'. |

### Partitions

| Field | Type | Default | Description |
|---|---|---|---|
| `partition_type` | `str` | — | Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', 'dynamic', or None for unpartitioned. |
| `partition_start` | `str` | — | Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types. |
| `partition_values` | `str` | — | Comma-separated values for static or multi partitioning, e.g. 'acme,globex,initech'. |
| `partition_dimensions` | `List[Dict[str, Any]]` | — | Multi-axis partition spec: list of {name, type, start, values, dynamic_partition_name} dicts. Overrides flat fields when set. |

### Retry policy

| Field | Type | Default | Description |
|---|---|---|---|
| `retry_policy_max_retries` | `int` | — | Max retries on asset failure. Defines a RetryPolicy. Useful for transient network failures, rate limits, etc. |
| `retry_policy_delay_seconds` | `int` | — | Seconds between retries (default 1). |
| `retry_policy_backoff` | `str` | `"exponential"` | Backoff strategy: 'linear' or 'exponential'. |

### Source / target

| Field | Type | Default | Description |
|---|---|---|---|
| `bucket_url` | `str` | — | Bucket/path URL for filesystem-shaped storage (e.g. 's3://my-bucket/path', 'gs://my-bucket/path', 'az://my-container/path', or 'file:///local/path'). Required when destination='filesystem' (the final write target). Also… _(full docs in schema.json + component README)_ |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `destination` | `str` | — | dlt destination identifier (e.g. 'snowflake', 'bigquery', 'postgres', 'redshift', 'filesystem', 'duckdb', 'databricks', 'athena', 'clickhouse', 'mssql', 'motherduck'). Leave empty for in-memory DuckDB -> DataFrame mode. |
| `dataset_name` | `str` | — | Target dataset/schema in the destination. Defaults to the asset name. |
| `persist_only` | `bool` | `false` | If True with destination set: emit a MaterializeResult and skip DataFrame return. If False: query the destination back into a DataFrame (only meaningful for SQL destinations -- non-SQL destinations always emit MaterializeResult). |
| `destination_credentials_url` | `str` | — | Inline connection string passed to dlt's destination factory. Useful when one Dagster project ingests into multiple accounts of the same destination type. If unset, dlt resolves credentials from env vars -- see ../DESTINATIONS.md. |
| `destination_credentials_env_var` | `str` | — | Alternative to destination_credentials_url: name of an env var holding the connection string. Resolved at run-time. |
| `athena_query_result_bucket` | `str` | — | Optional S3 path where Athena writes query results (e.g. 's3://my-bucket/results/'). Only used when destination='athena'. May be omitted to use Athena-managed query results instead. |
| `include_preview_metadata` | `bool` | `true` | Include sample data preview in metadata |
| `preview_rows` | `int` | `25` | Rows to include in the preview metadata when `include_preview_metadata` is True. For long DataFrames (>10x preview_rows), a random sample is used so the preview reflects the data distribution; otherwise head() is used. |
| `dynamic_partition_name` | `str` | — | Name for DynamicPartitionsDefinition (when partition_type='dynamic'), e.g. 'tenants'. |

[//]: # (FIELDS:END)

## Examples

### 1. Simple single-resource GET with HTTP Basic auth

Mirrors `chargify_ingestion`'s real resource shape in this repo:

```yaml
type: dagster_component_templates.DltRestApiSourceComponent
attributes:
  asset_name: my_vendor_ingestion
  client:
    base_url: "https://mysite.chargify.com"
    auth:
      type: http_basic
      username: "{{ env.MY_VENDOR_API_KEY }}"
      password: "x"
  resources:
    - name: customers
      endpoint:
        path: customers.json
        data_selector: "$"
```

### 2. Bearer auth + cursor pagination, persisted to Snowflake

```yaml
type: dagster_component_templates.DltRestApiSourceComponent
attributes:
  asset_name: my_other_vendor_ingestion
  client:
    base_url: "https://api.example.com/v1"
    auth:
      type: bearer
      token: "{{ env.MY_OTHER_VENDOR_TOKEN }}"
  resources:
    - name: items
      endpoint:
        path: items
        data_selector: "results"
        paginator:
          type: cursor
          cursor_param: cursor
          cursor_path: next_cursor
        params:
          limit: 100
  destination: snowflake
  destination_credentials_env_var: SNOWFLAKE_CONNECTION_STRING
```

### 3. Dependent (`resolve`) child resource

Mirrors `hotjar_ingestion`'s real `surveys` -> `survey_responses` chaining in this repo:

```yaml
type: dagster_component_templates.DltRestApiSourceComponent
attributes:
  asset_name: my_vendor_parent_child_ingestion
  client:
    base_url: "https://api.hotjar.io/v1"
    auth:
      type: bearer
      token: "{{ env.HOTJAR_ACCESS_TOKEN }}"
  resources:
    - name: surveys
      endpoint:
        path: "sites/{{ env.HOTJAR_SITE_ID }}/surveys"
        data_selector: results
        paginator:
          type: cursor
          cursor_param: cursor
          cursor_path: next_cursor
        params:
          limit: 100
    - name: survey_responses
      endpoint:
        path: "sites/{{ env.HOTJAR_SITE_ID }}/surveys/{survey_id}/responses"
        data_selector: results
        params:
          survey_id:
            type: resolve
            resource: surveys
            field: id
          limit: 100
```

See `example.yaml` for all three as a single file (examples 2 and 3 commented out, ready to uncomment).
