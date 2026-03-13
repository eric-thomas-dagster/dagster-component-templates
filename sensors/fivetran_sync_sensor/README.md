# Fivetran Sync Sensor

Polls Fivetran Sync and triggers a Dagster job when a sync or job completes successfully.

## What this does

On each tick, the sensor:
1. Calls the Fivetran Sync API to check the latest job or sync status
2. Compares against its stored cursor to detect new completions
3. Fires a `RunRequest` for the configured `job_name` on success
4. Updates the cursor so the same completion never triggers twice

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `sensor_name` | `str` | `**required**` | Unique sensor name |
| `connector_id` | `str` | `**required**` | Fivetran connector ID |
| `api_key_env_var` | `str` | `**required**` | Env var containing the Fivetran API key |
| `api_secret_env_var` | `str` | `**required**` | Env var containing the Fivetran API secret |
| `job_name` | `str` | `**required**` | Job to trigger when sync completes |
| `minimum_interval_seconds` | `int` | `60` | Seconds between polls |
| `default_status` | `str` | `"running"` | running or stopped |
| `resource_key` | `Optional[str]` | `None` | Optional Dagster resource key. |

## Example

```yaml
type: dagster_component_templates.FivetranSyncSensorComponent
attributes:
  sensor_name: my_sensor_name
  connector_id: my_connector_id
  api_key_env_var: API_KEY_ENV_VAR
  api_secret_env_var: API_SECRET_ENV_VAR
  job_name: my_job_name
  # minimum_interval_seconds: 60  # optional
  # default_status: "running"  # optional
  # resource_key: None  # optional
```

## Cursor and deduplication

The sensor uses a cursor to track which completions have already been processed. Each `RunRequest` is issued with a unique `run_key`, preventing duplicate pipeline runs if the sensor ticks multiple times while a job is still running.

## Requirements

```
requests>=2.28.0
```
