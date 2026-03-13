# External Nats Asset

Declares a Nats resource as an external asset in the Dagster asset graph — making it visible for lineage without materializing it from Dagster.

## What this does

External assets represent data sources managed outside of Dagster. Adding one to your project:
- Makes the Nats resource visible as a node in the **Dagster asset graph**
- Enables downstream assets to declare `deps` on it, drawing lineage edges
- Records connection metadata in the **Asset Catalog**

This component does not read from or write to Nats. It is a **lineage declaration only**.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_key` | `str` | `**required**` | Dagster asset key |
| `servers` | `str` | `**required**` | Comma-separated NATS server URLs |
| `stream_name` | `str` | `**required**` | JetStream stream name |
| `group_name` | `Optional[str]` | `None` | Dagster asset group name |
| `description` | `Optional[str]` | `None` | Human-readable description |

## Example

```yaml
type: dagster_component_templates.ExternalNatsAsset
attributes:
  asset_key: my_service/asset
  servers: my_servers
  stream_name: my_stream_name
  # group_name: None  # optional
  # description: None  # optional
```

## Pair with the observation sensor

Use the companion observation sensor to periodically health-check the resource and record metrics as `AssetObservation` events:

```yaml
# 1. Declare the external asset (lineage node)
type: dagster_component_templates.ExternalNatsAsset
attributes:
  asset_key: external/nats
  servers: SERVERS
  stream_name: my_stream

---

# 2. Observe it on a schedule
type: dagster_component_templates.NatsObservationSensorComponent
attributes:
  sensor_name: nats_asset_observer
  asset_key: external/nats
  asset_key: external/nats
  servers: SERVERS
  stream_name: my_stream
```

## Requirements

No additional packages required — this component only creates a `dg.AssetSpec`.
