# External Mqtt Asset

Declares a Mqtt resource as an external asset in the Dagster asset graph — making it visible for lineage without materializing it from Dagster.

## What this does

External assets represent data sources managed outside of Dagster. Adding one to your project:
- Makes the Mqtt resource visible as a node in the **Dagster asset graph**
- Enables downstream assets to declare `deps` on it, drawing lineage edges
- Records connection metadata in the **Asset Catalog**

This component does not read from or write to Mqtt. It is a **lineage declaration only**.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_key` | `str` | `**required**` | Dagster asset key |
| `broker_host` | `str` | `**required**` | MQTT broker hostname |
| `topic` | `str` | `**required**` |  |
| `broker_port` | `int` | `1883` | MQTT broker port |
| `group_name` | `Optional[str]` | `None` | Dagster asset group name |
| `description` | `Optional[str]` | `None` | Human-readable description |

## Example

```yaml
type: dagster_component_templates.ExternalMqttAsset
attributes:
  asset_key: my_service/asset
  broker_host: my-service.internal.company.com
  topic: my_topic
  # broker_port: 1883  # optional
  # group_name: None  # optional
  # description: None  # optional
```

## Pair with the observation sensor

Use the companion observation sensor to periodically health-check the resource and record metrics as `AssetObservation` events:

```yaml
# 1. Declare the external asset (lineage node)
type: dagster_component_templates.ExternalMqttAsset
attributes:
  asset_key: external/mqtt
  broker_host: my-mqtt.internal
  topic: TOPIC

---

# 2. Observe it on a schedule
type: dagster_component_templates.MqttObservationSensorComponent
attributes:
  sensor_name: mqtt_asset_observer
  asset_key: external/mqtt
  broker_host: my-mqtt.internal
  topic: TOPIC
```

## Requirements

No additional packages required — this component only creates a `dg.AssetSpec`.
