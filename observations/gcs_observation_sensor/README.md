# Gcs Observation Sensor

Periodically connects to Gcs and records an `AssetObservation` on the target external asset — surfacing health metrics in the Dagster UI without triggering a pipeline run.

## What this does

On each sensor tick, this component:
1. Connects to Gcs using the provided credentials
2. Queries the resource for current health metrics
3. Records an `AssetObservation` on the target asset with the collected data

Observations appear in the **Asset Activity** timeline, giving you a health history viewable in the Dagster UI.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `sensor_name` | `str` | `**required**` | Unique sensor name |
| `asset_key` | `str` | `**required**` | Asset key of the ExternalGcsAsset to observe |
| `bucket_name` | `str` | `**required**` | GCS bucket name |
| `prefix` | `str` | `""` | Object prefix |
| `project` | `Optional[str]` | `None` | GCP project ID |
| `check_interval_seconds` | `int` | `300` | Seconds between health checks |
| `resource_key` | `Optional[str]` | `None` | Optional Dagster resource key. |

## Example

```yaml
type: dagster_component_templates.GcsObservationSensorComponent
attributes:
  sensor_name: my_sensor_name
  asset_key: my_service/asset
  bucket_name: my_bucket_name
  # prefix: ""  # optional
  # project: None  # optional
  # check_interval_seconds: 300  # optional
  # resource_key: None  # optional
```

## Pair with the external asset

This sensor observes an external asset declared with `ExternalGcsAsset`. Declare the external asset first:

```yaml
# 1. Declare the external asset
type: dagster_component_templates.ExternalGcsAsset
attributes:
  asset_key: external/gcs

---

# 2. Observe it
type: dagster_component_templates.GcsObservationSensorComponent
attributes:
  sensor_name: gcs_observation_sensor_sensor
  asset_key: external/gcs
```

## Requirements

```
google-cloud-storage>=2.0.0
```
