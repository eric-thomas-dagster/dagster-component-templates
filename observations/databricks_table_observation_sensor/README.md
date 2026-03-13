# Databricks Table Observation Sensor

Periodically connects to Databricks Table and records an `AssetObservation` on the target external asset — surfacing health metrics in the Dagster UI without triggering a pipeline run.

## What this does

On each sensor tick, this component:
1. Connects to Databricks Table using the provided credentials
2. Queries the table for current health metrics
3. Records an `AssetObservation` on the target asset with the collected data

Observations appear in the **Asset Activity** timeline, giving you a health history viewable in the Dagster UI.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `sensor_name` | `str` | `**required**` | Unique sensor name |
| `asset_key` | `str` | `**required**` | Asset key of the ExternalDatabricksTableAsset to observe |
| `workspace_url` | `str` | `**required**` | Databricks workspace URL |
| `catalog` | `Optional[str]` | `None` | Unity Catalog name |
| `schema_name` | `str` | `**required**` | Schema/database name |
| `table_name` | `str` | `**required**` | Table name |
| `token_env_var` | `str` | `**required**` | Env var with Databricks personal access token |
| `http_path` | `str` | `**required**` |  |
| `check_interval_seconds` | `int` | `300` | Seconds between health checks |
| `resource_key` | `Optional[str]` | `None` | Optional Dagster resource key. |

## Example

```yaml
type: dagster_component_templates.DatabricksTableObservationSensorComponent
attributes:
  sensor_name: my_sensor_name
  asset_key: my_service/asset
  workspace_url: my_workspace_url
  # catalog: None  # optional
  schema_name: my_schema_name
  table_name: my_table_name
  token_env_var: TOKEN_ENV_VAR
  http_path: ./path/to/file
  # check_interval_seconds: 300  # optional
  # resource_key: None  # optional
```

## Pair with the external asset

This sensor observes an external asset declared with `ExternalDatabricksTableAsset`. Declare the external asset first:

```yaml
# 1. Declare the external asset
type: dagster_component_templates.ExternalDatabricksTableAsset
attributes:
  asset_key: external/databricks_table

---

# 2. Observe it
type: dagster_component_templates.DatabricksTableObservationSensorComponent
attributes:
  sensor_name: databricks_table_observation_sensor_sensor
  asset_key: external/databricks_table
```

## Requirements

```
databricks-sql-connector>=3.0.0
```
