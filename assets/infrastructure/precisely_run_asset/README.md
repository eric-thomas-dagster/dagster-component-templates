# Precisely Run Asset

Triggers a Precisely Connect data integration job on demand and surfaces the result as a Dagster asset. Dagster owns the schedule; Precisely executes the integration.

## What this does

When the asset is materialized:
1. Calls the Precisely Connect REST API to start the specified job
2. Polls run status until the job completes, fails, or times out
3. Returns a `MaterializeResult` with run metadata recorded in the Asset Catalog

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_key` | `str` | **required** | Dagster asset key (slash-separated, e.g. `precisely/integrations/load_orders`) |
| `job_id` | `str` | **required** | Precisely Connect job ID. Find in the Precisely UI: Jobs → Job Details → ID |
| `host_env_var` | `Optional[str]` | `None` | Env var holding the Precisely Connect host URL |
| `api_token_env_var` | `Optional[str]` | `None` | Env var holding the Precisely API token |
| `resource_key` | `Optional[str]` | `None` | Key of a `PreciselyResource` (alternative to env vars) |
| `parameters` | `Optional[dict]` | `None` | Job parameters passed at runtime |
| `poll_interval_seconds` | `float` | `10.0` | Seconds between status polls |
| `timeout_seconds` | `int` | `3600` | Max seconds to wait for job completion |
| `group_name` | `Optional[str]` | `"precisely"` | Dagster asset group name |
| `deps` | `Optional[list[str]]` | `None` | Upstream asset keys this asset depends on |

## Example

```yaml
type: dagster_component_templates.PreciselyRunAssetComponent
attributes:
  asset_key: precisely/integrations/load_orders
  job_id: job-abc-123
  host_env_var: PRECISELY_HOST
  api_token_env_var: PRECISELY_API_TOKEN
  deps:
    - raw_orders
```

## Materialization metadata

On success, the following metadata is recorded in the Asset Catalog:

| Key | Description |
|---|---|
| `job_id` | Precisely job ID |
| `run_id` | Precisely run ID |
| `status` | Final run status |
| `start_time` | Job start timestamp |
| `end_time` | Job end timestamp |

## Asset dependencies and lineage

The `deps` field declares upstream Dagster asset dependencies, drawing lineage edges in the asset graph without loading data at runtime:

```yaml
deps:
  - raw_orders              # simple asset key
  - raw/schema/orders       # asset key with path prefix
```

Dependencies can also be wired externally via `map_resolved_asset_specs()` in `definitions.py`.

## Requirements

```
requests>=2.28.0
```
