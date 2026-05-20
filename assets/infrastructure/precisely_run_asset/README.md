# Precisely Run Asset

Triggers a [Precisely Connect ETL](https://www.precisely.com/product/precisely-connect/connect) (formerly Syncsort DMX / DMExpress) job on demand and surfaces the result as a Dagster asset. Dagster owns the schedule and orchestration; Precisely executes the integration.

## What this does

When the asset is materialized:

1. POSTs to the Precisely Connect job-submit endpoint
2. Polls the documented Job Status endpoint until the job reaches a terminal status
3. Returns a `MaterializeResult` with run metadata on success, or raises on failure / timeout

## API verification status

| Endpoint | Verification | Override field |
|---|---|---|
| **Job Status** — `GET /projects/{jobRunId}/status` (plain-text response) | **Verified** against Precisely's [public REST API docs](https://help.precisely.com/r/Connect-ETL/pub/Latest/en-US/Connect-ETL-Rest-API-Reference/Job-Status) | (path is fixed) |
| **Submit** — `POST /projects/{job_id}/run` with JSON `{parameters: {...}}` | **Best-guess** RESTful shape — Precisely's submit endpoint isn't in the public REST docs | `submit_path_template` |

If your Connect ETL install uses a different submit path, override `submit_path_template`. The status-poll path is fixed because it's the documented one.

## Connection: env vars OR resource

Two equivalent ways to provide credentials:

**Option 1: env vars** (simpler for one-off use)

```yaml
host_env_var: PRECISELY_HOST
api_token_env_var: PRECISELY_API_TOKEN
```

**Option 2: shared `PreciselyResource`** (preferred when you have multiple Precisely components in the project)

```yaml
resource_key: precisely
```

…then wire `PreciselyResource(host=EnvVar("PRECISELY_HOST"), api_token=EnvVar("PRECISELY_API_TOKEN"))` in your project's resources.

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `asset_key` | `str` | Dagster asset key (slash-separated, e.g. `precisely/etl/load_orders`) |
| `job_id` | `str` | Precisely Connect job ID. Find in Precisely UI: **Jobs → Job Details → ID**. |

### Connection (pick env-var pair OR resource_key)

| Field | Type | Default | Description |
|---|---|---|---|
| `host_env_var` | `str` | — | Env var holding the Connect ETL host URL (e.g. `https://precisely.mycompany.com`) |
| `api_token_env_var` | `str` | — | Env var holding the Precisely API token (sent as `Authorization: Bearer …`) |
| `resource_key` | `str` | — | Key of a `PreciselyResource` defined elsewhere in your project |

### Execution

| Field | Type | Default | Description |
|---|---|---|---|
| `parameters` | `dict` | — | Job parameters posted in the submit request body |
| `poll_interval_seconds` | `float` | `10.0` | Seconds between status polls |
| `timeout_seconds` | `int` | `3600` | Max seconds to wait for terminal status |
| `submit_path_template` | `str` | `/projects/{job_id}/run` | Override if your install's submit path differs. `{job_id}` is replaced at submission. |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `group_name` | `str` | `"precisely"` | Asset group in the Dagster catalog |
| `description` | `str` | — | Asset description shown in the catalog |
| `owners` | `list[str]` | — | Owners — team names (`team:analytics`) or email addresses |
| `asset_tags` | `dict[str,str]` | — | Additional key-value tags applied to the asset |
| `kinds` | `list[str]` | auto: `precisely` | Asset kinds (e.g. `['precisely', 'mainframe']`) |
| `deps` | `list[str]` | — | Upstream asset keys (e.g. `['raw_orders', 'staging/customers']`) |
| `column_lineage` | `dict[str,list[str]]` | — | Column-level lineage: output column → list of upstream columns (e.g. `{revenue: [price, quantity]}`) |

### Freshness

| Field | Type | Default | Description |
|---|---|---|---|
| `freshness_max_lag_minutes` | `int` | — | Max acceptable lag before the asset is stale. Builds a `FreshnessPolicy` when set. |
| `freshness_cron` | `str` | — | Cron schedule for the freshness policy (e.g. `'0 9 * * 1-5'` for weekday 9am) |

### Partitions

| Field | Type | Default | Description |
|---|---|---|---|
| `partition_type` | `str` | — | `daily` / `weekly` / `monthly` / `hourly` / `static` / `multi` |
| `partition_start` | `str` | — | ISO start date for time-based partitions (e.g. `'2024-01-01'`) |
| `partition_date_column` | `str` | — | Column used to filter the upstream DataFrame to the current date partition |
| `partition_values` | `str` | — | Comma-separated values for static/multi partitioning (e.g. `'acme,globex,initech'`) |
| `partition_static_dim` | `str` | — | Dimension name for the static axis in multi-partitioning (e.g. `'customer'`) |
| `partition_static_column` | `str` | — | Column used to filter to the current static partition value |

### Retry policy

| Field | Type | Default | Description |
|---|---|---|---|
| `retry_policy_max_retries` | `int` | — | Max retries on failure. Defines a `RetryPolicy` when set. |
| `retry_policy_delay_seconds` | `int` | `1` | Seconds between retries |
| `retry_policy_backoff` | `str` | `"exponential"` | `linear` or `exponential` |

## Examples

### Minimal

```yaml
type: dagster_component_templates.PreciselyRunAssetComponent
attributes:
  asset_key: precisely/etl/load_customers
  job_id: "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
  host_env_var: PRECISELY_HOST
  api_token_env_var: PRECISELY_API_TOKEN
```

### With job parameters + retry policy + deps

```yaml
type: dagster_component_templates.PreciselyRunAssetComponent
attributes:
  asset_key: precisely/etl/load_orders
  job_id: "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
  host_env_var: PRECISELY_HOST
  api_token_env_var: PRECISELY_API_TOKEN
  parameters:
    inputPath: /data/inbound/orders
    outputSchema: analytics
  poll_interval_seconds: 15
  timeout_seconds: 7200
  retry_policy_max_retries: 3
  retry_policy_backoff: exponential
  deps:
    - raw_orders
    - staging/customers
```

### Daily-partitioned ETL with freshness policy

```yaml
type: dagster_component_templates.PreciselyRunAssetComponent
attributes:
  asset_key: precisely/etl/daily_orders
  job_id: "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
  host_env_var: PRECISELY_HOST
  api_token_env_var: PRECISELY_API_TOKEN
  partition_type: daily
  partition_start: "2024-01-01"
  partition_date_column: ORDER_DATE
  parameters:
    runDate: "{{ partition_key }}"
  freshness_max_lag_minutes: 90
  freshness_cron: "0 9 * * 1-5"        # weekday 9am, must be fresh-within-90m
  owners:
    - team:integration-eng
    - data-eng@company.com
```

### Custom submit endpoint

```yaml
type: dagster_component_templates.PreciselyRunAssetComponent
attributes:
  asset_key: precisely/etl/load_legacy
  job_id: "legacy-job-42"
  host_env_var: PRECISELY_HOST
  api_token_env_var: PRECISELY_API_TOKEN
  submit_path_template: "/api/v2/jobs/{job_id}/execute"   # validate vs your install
```

## Materialization metadata

On success, the asset records the following in the Dagster catalog:

| Key | Description |
|---|---|
| `job_id` | Precisely Connect job ID |
| `run_id` | Precisely run ID returned by the submit endpoint |
| `status` | Final terminal status (`COMPLETED` / `COMPLETED_WITH_WARNINGS`) |

## Failure semantics

The asset raises (failing the materialization) if Precisely returns any of these terminal statuses:

| Status | Meaning |
|---|---|
| `COMPLETED_WITH_ERRORS` | Job ran but produced errors |
| `CANCELLED` | Run was cancelled |
| `ERRORED` | Run failed |
| `LOST_CONTACT` | Connect ETL lost contact with the runtime |

A timeout (`timeout_seconds` elapsed without reaching a terminal status) also raises. Combine with `retry_policy_max_retries` for fault tolerance.

## Lineage

`deps` declares upstream Dagster asset dependencies, drawing edges in the asset graph without loading data at runtime:

```yaml
deps:
  - raw_orders                # simple asset key
  - staging/customers         # path-prefixed asset key
```

For column-level lineage in the catalog, set `column_lineage`:

```yaml
column_lineage:
  customer_revenue:
    - orders.total
    - orders.discount
    - customers.id
```

Dependencies can also be wired externally via `map_resolved_asset_specs()` in `definitions.py`.

## Requirements

```
requests>=2.28.0
```

## See also

- [`precisely_job_sensor`](../../sensors/precisely_job_sensor/) — fire a Dagster job when a Precisely run reaches terminal success
- [`precisely_validation.md`](https://github.com/eric-thomas-dagster/dagster-community-components-cli/blob/main/examples/precisely_validation.md) — what's verified against Precisely's public REST docs and what's best-guess
