# Matillion Run Asset

Triggers a Matillion ETL job on demand and surfaces the result as a Dagster asset. Dagster owns the schedule; Matillion executes the transformation.

## What this does

When the asset is materialized:
1. Calls the Matillion REST API to start a job run in the specified project/version
2. Polls run status until the job completes, fails, or times out
3. Returns a `MaterializeResult` with run metadata recorded in the Asset Catalog

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_key` | `str` | **required** | Dagster asset key (slash-separated, e.g. `matillion/transforms/load_orders`) |
| `project` | `str` | **required** | Matillion project name |
| `job_name` | `str` | **required** | Matillion job name to run |
| `version` | `str` | `"default"` | Matillion project version name |
| `instance_url_env_var` | `Optional[str]` | `None` | Env var holding the Matillion instance URL |
| `username_env_var` | `Optional[str]` | `None` | Env var holding the Matillion username |
| `password_env_var` | `Optional[str]` | `None` | Env var holding the Matillion password |
| `resource_key` | `Optional[str]` | `None` | Key of a `MatillionResource` (alternative to env vars) |
| `variables` | `Optional[dict]` | `None` | Job variable overrides passed at runtime |
| `poll_interval_seconds` | `float` | `10.0` | Seconds between status polls |
| `timeout_seconds` | `int` | `3600` | Max seconds to wait for job completion |
| `group_name` | `Optional[str]` | `"matillion"` | Dagster asset group name |
| `deps` | `Optional[list[str]]` | `None` | Upstream asset keys this asset depends on |

## Example

```yaml
type: dagster_component_templates.MatillionRunAssetComponent
attributes:
  asset_key: matillion/transforms/load_orders
  project: MyProject
  job_name: Load Orders
  # version: default  # optional
  instance_url_env_var: MATILLION_INSTANCE_URL
  username_env_var: MATILLION_USERNAME
  password_env_var: MATILLION_PASSWORD
  deps:
    - raw_orders
```

## Materialization metadata

On success, the following metadata is recorded in the Asset Catalog:

| Key | Description |
|---|---|
| `run_id` | Matillion run ID |
| `project` | Matillion project name |
| `job_name` | Matillion job name |
| `status` | Final run status (e.g. `SUCCESS`) |
| `start_time` | Job start timestamp |
| `end_time` | Job end timestamp |
| `row_count` | Rows processed (if reported by Matillion) |

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
