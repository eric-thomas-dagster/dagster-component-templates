# Ab Initio Job Sensor

Triggers a Dagster job when an Ab Initio EME (Enterprise Meta Environment) graph or job run completes.

Polls the Ab Initio EME REST API for the latest run of a specified job path. When a new successful run is detected (`status: ok`), triggers the configured Dagster job.

## Configuration

Supports two auth modes — env vars (simpler) or a shared `AbInitioResource` (recommended for multiple sensors).

| Field | Required | Description |
|-------|----------|-------------|
| `sensor_name` | Yes | Unique Dagster sensor name |
| `job_path` | Yes | EME job/graph path (e.g. `/prod/etl/load_orders`) |
| `job_name` | Yes | Dagster job to trigger on completion |
| `eme_url_env_var` | Env mode | Env var with EME base URL |
| `username_env_var` | Env mode | Env var with EME username |
| `password_env_var` | Env mode | Env var with EME password |
| `resource_key` | Resource mode | Key of an `AbInitioResource` |
| `minimum_interval_seconds` | No | Poll frequency (default: 120) |

## Example

```yaml
type: dagster_component_templates.AbInitioJobSensorComponent
attributes:
  sensor_name: abinitio_etl_done
  eme_url_env_var: ABINITIO_EME_URL
  username_env_var: ABINITIO_USERNAME
  password_env_var: ABINITIO_PASSWORD
  job_path: /prod/etl/load_orders
  job_name: downstream_processing_job
```

## AbInitioResource

For multiple sensors sharing the same EME connection:

```python
from dagster_component_templates.sensors.abinitio_job_sensor.component import AbInitioResource

defs = Definitions(
    resources={"abinitio": AbInitioResource(
        eme_url=EnvVar("ABINITIO_EME_URL"),
        username=EnvVar("ABINITIO_USERNAME"),
        password=EnvVar("ABINITIO_PASSWORD"),
    )}
)
```

Then in YAML: `resource_key: abinitio`

## Dependencies

```
requests>=2.28.0
```
