# Sqs Observation Sensor

Periodically connects to Sqs and records an `AssetObservation` on the target external asset — surfacing health metrics in the Dagster UI without triggering a pipeline run.

## What this does

On each sensor tick, this component:
1. Connects to Sqs using the provided credentials
2. Queries the queue for current health metrics
3. Records an `AssetObservation` on the target asset with the collected data

Observations appear in the **Asset Activity** timeline, giving you a health history viewable in the Dagster UI.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `sensor_name` | `str` | `**required**` | Unique sensor name |
| `asset_key` | `str` | `**required**` | Asset key of the ExternalSqsAsset to observe |
| `queue_url` | `str` | `**required**` | Full SQS queue URL |
| `region_name` | `Optional[str]` | `None` | AWS region |
| `check_interval_seconds` | `int` | `60` | Seconds between health checks |
| `resource_key` | `Optional[str]` | `None` | Optional Dagster resource key. |

## Example

```yaml
type: dagster_component_templates.SqsObservationSensorComponent
attributes:
  sensor_name: my_sensor_name
  asset_key: my_service/asset
  queue_url: my_queue_url
  # region_name: None  # optional
  # check_interval_seconds: 60  # optional
  # resource_key: None  # optional
```

## Pair with the external asset

This sensor observes an external asset declared with `ExternalSqsAsset`. Declare the external asset first:

```yaml
# 1. Declare the external asset
type: dagster_component_templates.ExternalSqsAsset
attributes:
  asset_key: external/sqs

---

# 2. Observe it
type: dagster_component_templates.SqsObservationSensorComponent
attributes:
  sensor_name: sqs_observation_sensor_sensor
  asset_key: external/sqs
```

## Requirements

```
boto3>=1.20.0
```
