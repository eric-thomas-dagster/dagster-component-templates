# Rivery Job Sensor

Polls Rivery Job and triggers a Dagster job when a sync or job completes successfully.

## What this does

On each tick, the sensor:
1. Calls the Rivery Job API to check the latest job or sync status
2. Compares against its stored cursor to detect new completions
3. Fires a `RunRequest` for the configured `job_name` on success
4. Updates the cursor so the same completion never triggers twice

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `sensor_name` | `str` | `**required**` | Unique sensor name |
| `river_id` | `str` | `**required**` |  |
| `job_name` | `str` | `**required**` | Dagster job to trigger when river run completes |
| `api_token_env_var` | `Optional[str]` | `None` | Env var with Rivery API token |
| `region` | `str` | `"us2"` |  |
| `resource_key` | `Optional[str]` | `None` | Key of a RiveryResource |
| `minimum_interval_seconds` | `int` | `60` | Seconds between polls |
| `default_status` | `str` | `"running"` | running or stopped |

## Example

```yaml
type: dagster_component_templates.RiveryJobSensorComponent
attributes:
  sensor_name: my_sensor_name
  river_id: my_river_id
  job_name: my_job_name
  # api_token_env_var: None  # optional
  # region: "us2"  # optional
  # resource_key: None  # optional
  # minimum_interval_seconds: 60  # optional
  # default_status: "running"  # optional
```

## Cursor and deduplication

The sensor uses a cursor to track which completions have already been processed. Each `RunRequest` is issued with a unique `run_key`, preventing duplicate pipeline runs if the sensor ticks multiple times while a job is still running.

## Requirements

```
requests>=2.28.0
```
