# Coalesce Job Sensor

Polls the Coalesce Scheduler API and triggers a Dagster job when a specified Coalesce job run completes successfully. Uses a cursor to avoid re-triggering on already-processed runs.

## Required packages

```
requests>=2.28.0
```

## Fields

| Field | Required | Default | Description |
|---|---|---|---|
| `sensor_name` | Yes | — | Unique sensor name |
| `environment_id` | Yes | — | Coalesce environment ID |
| `job_id` | Yes | — | Coalesce job ID to monitor |
| `api_token_env_var` | Yes | — | Env var containing the Coalesce API token |
| `job_name` | Yes | — | Dagster job to trigger when the Coalesce run succeeds |
| `minimum_interval_seconds` | No | `60` | Seconds between polls |
| `default_status` | No | `running` | Sensor default status: `running` or `stopped` |
| `resource_key` | No | `null` | Optional Dagster resource key |

## Example YAML

```yaml
type: dagster_component_templates.CoalesceJobSensorComponent
attributes:
  sensor_name: coalesce_transform_done
  environment_id: "prod-environment-abc123"
  job_id: "daily-transform-job-xyz"
  api_token_env_var: COALESCE_API_TOKEN
  job_name: downstream_processing_job
  minimum_interval_seconds: 60
  default_status: running
```
