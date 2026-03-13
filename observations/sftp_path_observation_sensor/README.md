# Sftp Path Observation Sensor

Periodically connects to Sftp Path and records an `AssetObservation` on the target external asset — surfacing health metrics in the Dagster UI without triggering a pipeline run.

## What this does

On each sensor tick, this component:
1. Connects to Sftp Path using the provided credentials
2. Queries the path for current health metrics
3. Records an `AssetObservation` on the target asset with the collected data

Observations appear in the **Asset Activity** timeline, giving you a health history viewable in the Dagster UI.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `sensor_name` | `str` | `**required**` | Unique sensor name |
| `asset_key` | `str` | `**required**` | Asset key of the ExternalSftpPathAsset to observe |
| `host` | `str` | `**required**` | SFTP host |
| `port` | `int` | `22` | SFTP port |
| `remote_path` | `str` | `**required**` | Remote directory path |
| `username_env_var` | `str` | `**required**` | Env var with SFTP username |
| `password_env_var` | `Optional[str]` | `None` | Env var with SFTP password |
| `private_key_env_var` | `Optional[str]` | `None` | Env var with path to SSH private key |
| `check_interval_seconds` | `int` | `300` | Seconds between health checks |
| `resource_key` | `Optional[str]` | `None` | Optional Dagster resource key. |

## Example

```yaml
type: dagster_component_templates.SftpPathObservationSensorComponent
attributes:
  sensor_name: my_sensor_name
  asset_key: my_service/asset
  host: my-service.internal.company.com
  # port: 22  # optional
  remote_path: ./path/to/file
  username_env_var: USERNAME_ENV_VAR
  # password_env_var: None  # optional
  # private_key_env_var: None  # optional
  # check_interval_seconds: 300  # optional
  # resource_key: None  # optional
```

## Pair with the external asset

This sensor observes an external asset declared with `ExternalSftpPathAsset`. Declare the external asset first:

```yaml
# 1. Declare the external asset
type: dagster_component_templates.ExternalSftpPathAsset
attributes:
  asset_key: external/sftp_path

---

# 2. Observe it
type: dagster_component_templates.SftpPathObservationSensorComponent
attributes:
  sensor_name: sftp_path_observation_sensor_sensor
  asset_key: external/sftp_path
```

## Requirements

```
paramiko>=3.0.0
```
