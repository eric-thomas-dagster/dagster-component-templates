# Rivery Run Asset

Triggers a Rivery river (ELT pipeline) on demand and surfaces the result as a Dagster asset. Dagster owns the schedule; Rivery executes the ELT.

## What this does

When the asset is materialized:
1. Calls the Rivery REST API to start the specified river run
2. Polls run status until the river completes, fails, or times out
3. Returns a `MaterializeResult` with run metadata recorded in the Asset Catalog

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_key` | `str` | **required** | Dagster asset key (slash-separated, e.g. `rivery/rivers/load_orders`) |
| `river_id` | `str` | **required** | Rivery river ID to run |
| `api_token_env_var` | `Optional[str]` | `None` | Env var holding the Rivery API token |
| `resource_key` | `Optional[str]` | `None` | Key of a `RiveryResource` (alternative to env var) |
| `poll_interval_seconds` | `float` | `10.0` | Seconds between status polls |
| `timeout_seconds` | `int` | `3600` | Max seconds to wait for river completion |
| `group_name` | `Optional[str]` | `"rivery"` | Dagster asset group name |
| `deps` | `Optional[list[str]]` | `None` | Upstream asset keys this asset depends on |

## Example

```yaml
type: dagster_component_templates.RiveryRunAssetComponent
attributes:
  asset_key: rivery/rivers/load_orders
  river_id: abc123def456
  api_token_env_var: RIVERY_API_TOKEN
  deps:
    - raw_orders
```

## Materialization metadata

On success, the following metadata is recorded in the Asset Catalog:

| Key | Description |
|---|---|
| `river_id` | Rivery river ID |
| `run_id` | Rivery run ID |
| `status` | Final run status |
| `start_time` | Run start timestamp |
| `end_time` | Run end timestamp |

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
