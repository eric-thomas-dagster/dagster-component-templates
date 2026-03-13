# Sftp Monitor

Polls Sftp and triggers a Dagster job when a sync or job completes successfully.

## What this does

On each tick, the sensor:
1. Calls the Sftp API to check the latest job or sync status
2. Compares against its stored cursor to detect new completions
3. Fires a `RunRequest` for the configured `job_name` on success
4. Updates the cursor so the same completion never triggers twice

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `sensor_name` | `str` | `**required**` | Unique sensor name |
| `host` | `str` | `**required**` | SFTP host |
| `port` | `int` | `22` | SFTP port |
| `username_env_var` | `str` | `**required**` | Env var with SFTP username |
| `password_env_var` | `Optional[str]` | `None` | Env var with SFTP password |
| `private_key_env_var` | `Optional[str]` | `None` | Env var with path to SSH private key file |
| `remote_path` | `str` | `**required**` | Remote directory path to monitor |
| `file_pattern` | `str` | `"*"` | Glob-style filename filter (e.g.  |
| `job_name` | `str` | `**required**` | Job to trigger when new files are detected |
| `minimum_interval_seconds` | `int` | `300` | Seconds between polls |
| `default_status` | `str` | `"running"` | running or stopped |
| `resource_key` | `Optional[str]` | `None` | Optional Dagster resource key. |

## Example

```yaml
type: dagster_component_templates.SftpMonitorSensorComponent
attributes:
  sensor_name: my_sensor_name
  host: my-service.internal.company.com
  # port: 22  # optional
  username_env_var: USERNAME_ENV_VAR
  # password_env_var: None  # optional
  # private_key_env_var: None  # optional
  remote_path: ./path/to/file
  # file_pattern: "*"  # optional
  job_name: my_job_name
  # minimum_interval_seconds: 300  # optional
  # default_status: "running"  # optional
  # resource_key: None  # optional
```

## Cursor and deduplication

The sensor uses a cursor to track which completions have already been processed. Each `RunRequest` is issued with a unique `run_key`, preventing duplicate pipeline runs if the sensor ticks multiple times while a job is still running.

## Requirements

```
paramiko>=3.0.0
```
