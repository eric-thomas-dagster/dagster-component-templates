# Adls Observation Sensor

Periodically connects to Adls and records an `AssetObservation` on the target external asset — surfacing health metrics in the Dagster UI without triggering a pipeline run.

## What this does

On each sensor tick, this component:
1. Connects to Adls using the provided credentials
2. Queries the resource for current health metrics
3. Records an `AssetObservation` on the target asset with the collected data

Observations appear in the **Asset Activity** timeline, giving you a health history viewable in the Dagster UI.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `sensor_name` | `str` | `**required**` | Unique sensor name |
| `asset_key` | `str` | `**required**` | Asset key of the ExternalAdlsAsset to observe |
| `account_name` | `str` | `**required**` | Azure storage account name |
| `container_name` | `str` | `**required**` | ADLS container / filesystem name |
| `path_prefix` | `str` | `""` | Path prefix within the container |
| `connection_string_env_var` | `Optional[str]` | `None` |  |
| `check_interval_seconds` | `int` | `300` | Seconds between health checks |
| `resource_key` | `Optional[str]` | `None` | Optional Dagster resource key. |

## Example

```yaml
type: dagster_component_templates.AdlsObservationSensorComponent
attributes:
  sensor_name: my_sensor_name
  asset_key: my_service/asset
  account_name: my_account_name
  container_name: my_container_name
  # path_prefix: ""  # optional
  # connection_string_env_var: None  # optional
  # check_interval_seconds: 300  # optional
  # resource_key: None  # optional
```

## Pair with the external asset

This sensor observes an external asset declared with `ExternalAdlsAsset`. Declare the external asset first:

```yaml
# 1. Declare the external asset
type: dagster_component_templates.ExternalAdlsAsset
attributes:
  asset_key: external/adls

---

# 2. Observe it
type: dagster_component_templates.AdlsObservationSensorComponent
attributes:
  sensor_name: adls_observation_sensor_sensor
  asset_key: external/adls
```

## Requirements

```
azure-storage-file-datalake>=12.0.0
azure-identity>=1.10.0
```
