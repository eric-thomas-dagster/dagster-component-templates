# External Pulsar Asset

Declares a Pulsar resource as an external asset in the Dagster asset graph — making it visible for lineage without materializing it from Dagster.

## What this does

External assets represent data sources managed outside of Dagster. Adding one to your project:
- Makes the Pulsar resource visible as a node in the **Dagster asset graph**
- Enables downstream assets to declare `deps` on it, drawing lineage edges
- Records connection metadata in the **Asset Catalog**

This component does not read from or write to Pulsar. It is a **lineage declaration only**.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_key` | `str` | `**required**` | Dagster asset key |
| `service_url` | `str` | `**required**` | Pulsar service URL |
| `topic` | `str` | `**required**` | Pulsar topic name |
| `group_name` | `Optional[str]` | `None` | Dagster asset group name |
| `description` | `Optional[str]` | `None` | Human-readable description |

## Example

```yaml
type: dagster_component_templates.ExternalPulsarAsset
attributes:
  asset_key: my_service/asset
  service_url: my_service_url
  topic: my_topic
  # group_name: None  # optional
  # description: None  # optional
```

## Pair with the observation sensor

Use the companion observation sensor to periodically health-check the resource and record metrics as `AssetObservation` events:

```yaml
# 1. Declare the external asset (lineage node)
type: dagster_component_templates.ExternalPulsarAsset
attributes:
  asset_key: external/pulsar
  service_url: SERVICE_URL
  topic: TOPIC

---

# 2. Observe it on a schedule
type: dagster_component_templates.PulsarObservationSensorComponent
attributes:
  sensor_name: pulsar_asset_observer
  asset_key: external/pulsar
  service_url: SERVICE_URL
  topic: TOPIC
```

## Requirements

No additional packages required — this component only creates a `dg.AssetSpec`.
