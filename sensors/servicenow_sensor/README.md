# Servicenow Sensor

Polls Servicenow and triggers a Dagster job when a sync or job completes successfully.

## What this does

On each tick, the sensor:
1. Calls the Servicenow API to check the latest job or sync status
2. Compares against its stored cursor to detect new completions
3. Fires a `RunRequest` for the configured `job_name` on success
4. Updates the cursor so the same completion never triggers twice

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `sensor_name` | `str` | `**required**` | Unique sensor name |
| `instance_env_var` | `str` | `**required**` |  |
| `username_env_var` | `str` | `**required**` | Env var with ServiceNow username |
| `password_env_var` | `str` | `**required**` | Env var with ServiceNow password |
| `table` | `str` | `**required**` |  |
| `sysparm_query` | `str` | `**required**` | ServiceNow encoded query string to filter records |
| `job_name` | `str` | `**required**` | Job to trigger when matching records are found |
| `sysparm_fields` | `str` | `"sys_id` | Comma-separated fields to return |
| `minimum_interval_seconds` | `int` | `120` | Seconds between polls |
| `default_status` | `str` | `"running"` | running or stopped |
| `resource_key` | `Optional[str]` | `None` | Optional Dagster resource key. |

## Example

```yaml
type: dagster_component_templates.ServiceNowSensorComponent
attributes:
  sensor_name: my_sensor_name
  instance_env_var: INSTANCE_ENV_VAR
  username_env_var: USERNAME_ENV_VAR
  password_env_var: PASSWORD_ENV_VAR
  table: my_table
  sysparm_query: my_sysparm_query
  job_name: my_job_name
  # sysparm_fields: "sys_id  # optional
  # minimum_interval_seconds: 120  # optional
  # default_status: "running"  # optional
  # resource_key: None  # optional
```

## Cursor and deduplication

The sensor uses a cursor to track which completions have already been processed. Each `RunRequest` is issued with a unique `run_key`, preventing duplicate pipeline runs if the sensor ticks multiple times while a job is still running.

## Requirements

```
requests>=2.28.0
```
