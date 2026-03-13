# Airbyte Sync Sensor

Polls Airbyte Sync and triggers a Dagster job when a sync or job completes successfully.

## What this does

On each tick, the sensor:
1. Calls the Airbyte Sync API to check the latest job or sync status
2. Compares against its stored cursor to detect new completions
3. Fires a `RunRequest` for the configured `job_name` on success
4. Updates the cursor so the same completion never triggers twice

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `sensor_name` | `str` | `**required**` | Unique sensor name |
| `connection_id` | `str` | `**required**` | Airbyte connection UUID |
| `airbyte_url` | `str` | `"http://localhost:8000"` | Airbyte server URL |
| `username_env_var` | `Optional[str]` | `None` |  |
| `password_env_var` | `Optional[str]` | `None` |  |
| `job_name` | `str` | `**required**` | Job to trigger when sync completes |
| `minimum_interval_seconds` | `int` | `60` | Seconds between polls |
| `default_status` | `str` | `"running"` | running or stopped |
| `resource_key` | `Optional[str]` | `None` | Optional Dagster resource key. |

## Example

```yaml
type: dagster_component_templates.AirbyteSyncSensorComponent
attributes:
  sensor_name: my_sensor_name
  connection_id: my_connection_id
  # airbyte_url: "http://localhost:8000"  # optional
  # username_env_var: None  # optional
  # password_env_var: None  # optional
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
