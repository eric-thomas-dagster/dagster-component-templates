# Pubsub Observation Sensor

Periodically connects to Pubsub and records an `AssetObservation` on the target external asset — surfacing health metrics in the Dagster UI without triggering a pipeline run.

## What this does

On each sensor tick, this component:
1. Connects to Pubsub using the provided credentials
2. Queries the topic for current health metrics
3. Records an `AssetObservation` on the target asset with the collected data

Observations appear in the **Asset Activity** timeline, giving you a health history viewable in the Dagster UI.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `sensor_name` | `str` | `**required**` | Unique sensor name |
| `asset_key` | `str` | `**required**` | Asset key of the ExternalPubsubAsset to observe |
| `project_id` | `str` | `**required**` | GCP project ID |
| `topic_id` | `str` | `**required**` | Pub/Sub topic ID |
| `subscription_id` | `Optional[str]` | `None` | Subscription ID for lag metrics |
| `check_interval_seconds` | `int` | `300` | Seconds between health checks |
| `resource_key` | `Optional[str]` | `None` | Optional Dagster resource key. |

## Example

```yaml
type: dagster_component_templates.PubsubObservationSensorComponent
attributes:
  sensor_name: my_sensor_name
  asset_key: my_service/asset
  project_id: my_project_id
  topic_id: my_topic_id
  # subscription_id: None  # optional
  # check_interval_seconds: 300  # optional
  # resource_key: None  # optional
```

## Pair with the external asset

This sensor observes an external asset declared with `ExternalPubsubAsset`. Declare the external asset first:

```yaml
# 1. Declare the external asset
type: dagster_component_templates.ExternalPubsubAsset
attributes:
  asset_key: external/pubsub

---

# 2. Observe it
type: dagster_component_templates.PubsubObservationSensorComponent
attributes:
  sensor_name: pubsub_observation_sensor_sensor
  asset_key: external/pubsub
```

## Requirements

```
google-cloud-pubsub>=2.0.0
```
