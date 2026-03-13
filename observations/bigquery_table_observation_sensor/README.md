# Bigquery Table Observation Sensor

Periodically connects to Bigquery Table and records an `AssetObservation` on the target external asset — surfacing health metrics in the Dagster UI without triggering a pipeline run.

## What this does

On each sensor tick, this component:
1. Connects to Bigquery Table using the provided credentials
2. Queries the table for current health metrics
3. Records an `AssetObservation` on the target asset with the collected data

Observations appear in the **Asset Activity** timeline, giving you a health history viewable in the Dagster UI.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `sensor_name` | `str` | `**required**` | Unique sensor name |
| `asset_key` | `str` | `**required**` | Asset key of the ExternalBigQueryTableAsset to observe |
| `project_id` | `str` | `**required**` | GCP project ID |
| `dataset_id` | `str` | `**required**` | BigQuery dataset ID |
| `table_id` | `str` | `**required**` | BigQuery table ID |
| `check_interval_seconds` | `int` | `300` | Seconds between health checks |
| `resource_key` | `Optional[str]` | `None` | Optional Dagster resource key. |

## Example

```yaml
type: dagster_component_templates.BigQueryTableObservationSensorComponent
attributes:
  sensor_name: my_sensor_name
  asset_key: my_service/asset
  project_id: my_project_id
  dataset_id: my_dataset_id
  table_id: my_table_id
  # check_interval_seconds: 300  # optional
  # resource_key: None  # optional
```

## Pair with the external asset

This sensor observes an external asset declared with `ExternalBigQueryTableAsset`. Declare the external asset first:

```yaml
# 1. Declare the external asset
type: dagster_component_templates.ExternalBigQueryTableAsset
attributes:
  asset_key: external/bigquery_table

---

# 2. Observe it
type: dagster_component_templates.BigQueryTableObservationSensorComponent
attributes:
  sensor_name: bigquery_table_observation_sensor_sensor
  asset_key: external/bigquery_table
```

## Requirements

```
google-cloud-bigquery>=3.0.0
```
