# Snowflake Table Observation Sensor

Periodically connects to Snowflake Table and records an `AssetObservation` on the target external asset — surfacing health metrics in the Dagster UI without triggering a pipeline run.

## What this does

On each sensor tick, this component:
1. Connects to Snowflake Table using the provided credentials
2. Queries the table for current health metrics
3. Records an `AssetObservation` on the target asset with the collected data

Observations appear in the **Asset Activity** timeline, giving you a health history viewable in the Dagster UI.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `sensor_name` | `str` | `**required**` | Unique sensor name |
| `asset_key` | `str` | `**required**` | Asset key of the ExternalSnowflakeTableAsset to observe |
| `account` | `str` | `**required**` | Snowflake account identifier |
| `database` | `str` | `**required**` | Snowflake database |
| `schema_name` | `str` | `**required**` | Snowflake schema |
| `table_name` | `str` | `**required**` | Snowflake table |
| `username_env_var` | `str` | `**required**` | Env var with Snowflake username |
| `password_env_var` | `Optional[str]` | `None` | Env var with password |
| `warehouse` | `Optional[str]` | `None` | Snowflake warehouse to use |
| `check_interval_seconds` | `int` | `300` | Seconds between health checks |
| `resource_key` | `Optional[str]` | `None` | Optional Dagster resource key. |

## Example

```yaml
type: dagster_component_templates.SnowflakeTableObservationSensorComponent
attributes:
  sensor_name: my_sensor_name
  asset_key: my_service/asset
  account: my_account
  database: my_database
  schema_name: my_schema_name
  table_name: my_table_name
  username_env_var: USERNAME_ENV_VAR
  # password_env_var: None  # optional
  # warehouse: None  # optional
  # check_interval_seconds: 300  # optional
  # resource_key: None  # optional
```

## Pair with the external asset

This sensor observes an external asset declared with `ExternalSnowflakeTableAsset`. Declare the external asset first:

```yaml
# 1. Declare the external asset
type: dagster_component_templates.ExternalSnowflakeTableAsset
attributes:
  asset_key: external/snowflake_table

---

# 2. Observe it
type: dagster_component_templates.SnowflakeTableObservationSensorComponent
attributes:
  sensor_name: snowflake_table_observation_sensor_sensor
  asset_key: external/snowflake_table
```

## Requirements

```
snowflake-connector-python>=3.0.0
```
