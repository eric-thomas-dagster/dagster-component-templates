# Mqtt Observation Sensor

Periodically connects to Mqtt and records an `AssetObservation` on the target external asset — surfacing health metrics in the Dagster UI without triggering a pipeline run.

## What this does

On each sensor tick, this component:
1. Connects to Mqtt using the provided credentials
2. Queries the resource for current health metrics
3. Records an `AssetObservation` on the target asset with the collected data

Observations appear in the **Asset Activity** timeline, giving you a health history viewable in the Dagster UI.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `sensor_name` | `str` | `**required**` | Unique sensor name |
| `asset_key` | `str` | `**required**` | Asset key of the ExternalMqttAsset to observe |
| `broker_host` | `str` | `**required**` | MQTT broker hostname |
| `topic` | `str` | `**required**` | MQTT topic to subscribe to |
| `broker_port` | `int` | `1883` | MQTT broker port |
| `connect_timeout_seconds` | `float` | `5.0` | Seconds to wait for connection |
| `check_interval_seconds` | `int` | `300` | Seconds between health checks |
| `resource_key` | `Optional[str]` | `None` | Optional Dagster resource key. |

## Example

```yaml
type: dagster_component_templates.MqttObservationSensorComponent
attributes:
  sensor_name: my_sensor_name
  asset_key: my_service/asset
  broker_host: my-service.internal.company.com
  topic: my_topic
  # broker_port: 1883  # optional
  # connect_timeout_seconds: 5.0  # optional
  # check_interval_seconds: 300  # optional
  # resource_key: None  # optional
```

## Pair with the external asset

This sensor observes an external asset declared with `ExternalMqttAsset`. Declare the external asset first:

```yaml
# 1. Declare the external asset
type: dagster_component_templates.ExternalMqttAsset
attributes:
  asset_key: external/mqtt

---

# 2. Observe it
type: dagster_component_templates.MqttObservationSensorComponent
attributes:
  sensor_name: mqtt_observation_sensor_sensor
  asset_key: external/mqtt
```

## Requirements

```
paho-mqtt>=1.6.1
```
