# Ab Initio Run Asset

Submit an Ab Initio graph or job to the EME REST API on demand and surface it as a Dagster asset materialization.

Dagster owns the schedule. When the asset is materialized, Dagster triggers the Ab Initio graph, waits for completion, and records the result. Graph parameters (date ranges, input paths, environment flags) can be passed directly from the component configuration.

## How It Works

1. `POST /abinitio/eme-rest-api/v1/jobs/{job_path}/runs` — triggers the graph with optional parameters
2. Polls `GET /abinitio/eme-rest-api/v1/jobs/{job_path}/runs/{run_id}` until status is terminal
3. Returns `MaterializeResult` with run metadata on success, raises on failure/abort

## Configuration

| Field | Required | Description |
|-------|----------|-------------|
| `asset_key` | Yes | Dagster asset key (e.g. `abinitio/etl/load_orders`) |
| `job_path` | Yes | Ab Initio EME job/graph path (e.g. `/prod/etl/load_orders`) |
| `eme_url_env_var` | Env mode | Env var with EME base URL |
| `username_env_var` | Env mode | Env var with EME username |
| `password_env_var` | Env mode | Env var with EME password |
| `resource_key` | Resource mode | Key of an `AbInitioResource` |
| `parameters` | No | Key-value pairs passed to the graph (e.g. `AI_START_DATE`, `AI_INPUT_PATH`) |
| `poll_interval_seconds` | No | Poll frequency (default: 15s) |
| `timeout_seconds` | No | Max wait time (default: 3600s) |
| `group_name` | No | Dagster group name (default: `abinitio`) |

## Example

```yaml
type: dagster_component_templates.AbInitioRunAssetComponent
attributes:
  asset_key: abinitio/etl/load_orders
  job_path: /prod/etl/load_orders
  eme_url_env_var: ABINITIO_EME_URL
  username_env_var: ABINITIO_USERNAME
  password_env_var: ABINITIO_PASSWORD
  parameters:
    AI_START_DATE: "2024-01-01"
    AI_END_DATE: "2024-01-31"
    AI_INPUT_PATH: /data/inbound/orders
```

## AbInitioResource (recommended for multiple jobs)

Define the resource once and share it across all Ab Initio components:

```python
from dagster_component_templates.assets.abinitio_run_asset.component import AbInitioResource

defs = Definitions(
    resources={
        "abinitio": AbInitioResource(
            eme_url=EnvVar("ABINITIO_EME_URL"),
            username=EnvVar("ABINITIO_USERNAME"),
            password=EnvVar("ABINITIO_PASSWORD"),
        )
    }
)
```

Then reference it in any number of components:

```yaml
resource_key: abinitio
```

The same `AbInitioResource` class is available from both the sensor and run asset components.

## Ab Initio Graph Parameters

Ab Initio graphs frequently accept parameters that control execution behavior. Common examples:

| Parameter | Description |
|-----------|-------------|
| `AI_START_DATE` | Processing window start |
| `AI_END_DATE` | Processing window end |
| `AI_INPUT_PATH` | Source data directory |
| `AI_OUTPUT_PATH` | Target output directory |
| `AI_ENV` | Environment flag (dev/prod) |

## Dependencies

```
requests>=2.28.0
```
