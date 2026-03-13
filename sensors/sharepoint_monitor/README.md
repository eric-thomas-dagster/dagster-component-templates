# Sharepoint Monitor

Polls Sharepoint and triggers a Dagster job when a sync or job completes successfully.

## What this does

On each tick, the sensor:
1. Calls the Sharepoint API to check the latest job or sync status
2. Compares against its stored cursor to detect new completions
3. Fires a `RunRequest` for the configured `job_name` on success
4. Updates the cursor so the same completion never triggers twice

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `sensor_name` | `str` | `**required**` | Unique sensor name |
| `site_url` | `str` | `**required**` | SharePoint site URL |
| `library_name` | `str` | `**required**` | Document library name (e.g.  |
| `tenant_id_env_var` | `str` | `**required**` | Env var with Azure tenant ID |
| `client_id_env_var` | `str` | `**required**` | Env var with Azure app client ID |
| `client_secret_env_var` | `str` | `**required**` | Env var with Azure app client secret |
| `folder_path` | `str` | `""` | Optional subfolder path within the library |
| `file_extension_filter` | `str` | `""` | Filter by extension e.g.  |
| `job_name` | `str` | `**required**` | Job to trigger when new/modified files are detected |
| `minimum_interval_seconds` | `int` | `300` | Seconds between polls |
| `default_status` | `str` | `"running"` | running or stopped |
| `resource_key` | `Optional[str]` | `None` | Optional Dagster resource key. |

## Example

```yaml
type: dagster_component_templates.SharePointMonitorSensorComponent
attributes:
  sensor_name: my_sensor_name
  site_url: my_site_url
  library_name: my_library_name
  tenant_id_env_var: TENANT_ID_ENV_VAR
  client_id_env_var: CLIENT_ID_ENV_VAR
  client_secret_env_var: CLIENT_SECRET_ENV_VAR
  # folder_path: ""  # optional
  # file_extension_filter: ""  # optional
  job_name: my_job_name
  # minimum_interval_seconds: 300  # optional
  # default_status: "running"  # optional
  # resource_key: None  # optional
```

## Cursor and deduplication

The sensor uses a cursor to track which completions have already been processed. Each `RunRequest` is issued with a unique `run_key`, preventing duplicate pipeline runs if the sensor ticks multiple times while a job is still running.

## Requirements

```
requests>=2.28.0
```
