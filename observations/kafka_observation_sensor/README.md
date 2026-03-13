# Kafka Observation Sensor

Periodically connects to Kafka and records an `AssetObservation` on the target external asset — surfacing health metrics in the Dagster UI without triggering a pipeline run.

## What this does

On each sensor tick, this component:
1. Connects to Kafka using the provided credentials
2. Queries the topic for current health metrics
3. Records an `AssetObservation` on the target asset with the collected data

Observations appear in the **Asset Activity** timeline, giving you a health history viewable in the Dagster UI.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `sensor_name` | `str` | `**required**` | Unique sensor name |
| `asset_key` | `str` | `**required**` | Asset key of the ExternalKafkaAsset to observe |
| `bootstrap_servers` | `str` | `**required**` | Comma-separated Kafka broker addresses |
| `topic` | `str` | `**required**` | Kafka topic name |
| `check_interval_seconds` | `int` | `300` | Seconds between health checks |
| `resource_key` | `Optional[str]` | `None` | Optional Dagster resource key. |

## Example

```yaml
type: dagster_component_templates.KafkaObservationSensorComponent
attributes:
  sensor_name: my_sensor_name
  asset_key: my_service/asset
  bootstrap_servers: my_bootstrap_servers
  topic: my_topic
  # check_interval_seconds: 300  # optional
  # resource_key: None  # optional
```

## Pair with the external asset

This sensor observes an external asset declared with `ExternalKafkaAsset`. Declare the external asset first:

```yaml
# 1. Declare the external asset
type: dagster_component_templates.ExternalKafkaAsset
attributes:
  asset_key: external/kafka

---

# 2. Observe it
type: dagster_component_templates.KafkaObservationSensorComponent
attributes:
  sensor_name: kafka_observation_sensor_sensor
  asset_key: external/kafka
```

## Requirements

```
kafka-python>=2.0.0
```
