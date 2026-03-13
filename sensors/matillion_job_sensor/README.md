# Matillion Job Sensor

Polls Matillion Job and triggers a Dagster job when a sync or job completes successfully.

## What this does

On each tick, the sensor:
1. Calls the Matillion Job API to check the latest job or sync status
2. Compares against its stored cursor to detect new completions
3. Fires a `RunRequest` for the configured `job_name` on success
4. Updates the cursor so the same completion never triggers twice

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `sensor_name` | `str` | `**required**` | Unique sensor name |
| `project` | `str` | `**required**` | Matillion project name |
| `version` | `str` | `"default"` | Matillion project version name |
| `job_name` | `str` | `**required**` | Matillion job name to monitor |
| `job_name_dagster` | `str` | `**required**` | Dagster job to trigger when Matillion run completes |
| `instance_url_env_var` | `Optional[str]` | `None` | Env var with Matillion instance URL |
| `username_env_var` | `Optional[str]` | `None` | Env var with Matillion username |
| `password_env_var` | `Optional[str]` | `None` | Env var with Matillion password |
| `resource_key` | `Optional[str]` | `None` | Key of a MatillionResource |
| `minimum_interval_seconds` | `int` | `60` | Seconds between polls |
| `default_status` | `str` | `"running"` | running or stopped |

## Example

```yaml
type: dagster_component_templates.MatillionJobSensorComponent
attributes:
  sensor_name: my_sensor_name
  project: my_project
  # version: "default"  # optional
  job_name: my_job_name
  job_name_dagster: my_job_name_dagster
  # instance_url_env_var: None  # optional
  # username_env_var: None  # optional
  # password_env_var: None  # optional
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
