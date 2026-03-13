# External Kafka Asset

Declares a Kafka topic as an external asset in the Dagster asset graph — making it visible for lineage without materializing it from Dagster.

## What this does

External assets represent data sources managed outside of Dagster. Adding one to your project:
- Makes the Kafka topic visible as a node in the **Dagster asset graph**
- Enables downstream assets to declare `deps` on it, drawing lineage edges
- Records connection metadata in the **Asset Catalog**

This component does not read from or write to Kafka. It is a **lineage declaration only**.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_key` | `str` | `**required**` | Dagster asset key |
| `bootstrap_servers` | `str` | `**required**` | Comma-separated Kafka broker addresses |
| `topic` | `str` | `**required**` | Kafka topic name |
| `group_name` | `Optional[str]` | `None` | Dagster asset group name |
| `description` | `Optional[str]` | `None` | Human-readable description |

## Example

```yaml
type: dagster_component_templates.ExternalKafkaAsset
attributes:
  asset_key: my_service/asset
  bootstrap_servers: my_bootstrap_servers
  topic: my_topic
  # group_name: None  # optional
  # description: None  # optional
```

## Pair with the observation sensor

Use the companion observation sensor to periodically health-check the topic and record metrics as `AssetObservation` events:

```yaml
# 1. Declare the external asset (lineage node)
type: dagster_component_templates.ExternalKafkaAsset
attributes:
  asset_key: external/kafka
  bootstrap_servers: BOOTSTRAP_SERVERS
  topic: TOPIC

---

# 2. Observe it on a schedule
type: dagster_component_templates.KafkaObservationSensorComponent
attributes:
  sensor_name: kafka_asset_observer
  asset_key: external/kafka
  asset_key: external/kafka
  bootstrap_servers: BOOTSTRAP_SERVERS
  topic: TOPIC
```

## Requirements

No additional packages required — this component only creates a `dg.AssetSpec`.
