# Sql Observation Sensor

Periodically connects to Sql and records an `AssetObservation` on the target external asset — surfacing health metrics in the Dagster UI without triggering a pipeline run.

## What this does

On each sensor tick, this component:
1. Connects to Sql using the provided credentials
2. Queries the resource for current health metrics
3. Records an `AssetObservation` on the target asset with the collected data

Observations appear in the **Asset Activity** timeline, giving you a health history viewable in the Dagster UI.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `sensor_name` | `str` | `**required**` | Unique sensor name |
| `asset_key` | `str` | `**required**` | Asset key of the ExternalSqlAsset to observe |
| `table_name` | `str` | `**required**` | Table name to observe |
| `connection_string_env_var` | `str` | `**required**` | Env var with SQLAlchemy connection string |
| `watermark_column` | `Optional[str]` | `None` |  |
| `check_interval_seconds` | `int` | `300` | Seconds between health checks |
| `resource_key` | `Optional[str]` | `None` | Optional Dagster resource key. |

## Example

```yaml
type: dagster_component_templates.SqlObservationSensorComponent
attributes:
  sensor_name: my_sensor_name
  asset_key: my_service/asset
  table_name: my_table_name
  connection_string_env_var: CONNECTION_STRING_ENV_VAR
  # watermark_column: None  # optional
  # check_interval_seconds: 300  # optional
  # resource_key: None  # optional
```

## Pair with the external asset

This sensor observes an external asset declared with `ExternalSqlAsset`. Declare the external asset first:

```yaml
# 1. Declare the external asset
type: dagster_component_templates.ExternalSqlAsset
attributes:
  asset_key: external/sql

---

# 2. Observe it
type: dagster_component_templates.SqlObservationSensorComponent
attributes:
  sensor_name: sql_observation_sensor_sensor
  asset_key: external/sql
```

## Requirements

```
sqlalchemy>=2.0.0
```
