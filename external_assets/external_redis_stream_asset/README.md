# External Redis Stream Asset

Declares a Redis Stream stream as an external asset in the Dagster asset graph — making it visible for lineage without materializing it from Dagster.

## What this does

External assets represent data sources managed outside of Dagster. Adding one to your project:
- Makes the Redis Stream stream visible as a node in the **Dagster asset graph**
- Enables downstream assets to declare `deps` on it, drawing lineage edges
- Records connection metadata in the **Asset Catalog**

This component does not read from or write to Redis Stream. It is a **lineage declaration only**.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_key` | `str` | `**required**` | Dagster asset key |
| `stream_name` | `str` | `**required**` | Redis stream name |
| `host` | `str` | `"localhost"` | Redis host |
| `port` | `int` | `6379` | Redis port |
| `group_name` | `Optional[str]` | `None` | Dagster asset group name |
| `description` | `Optional[str]` | `None` | Human-readable description |

## Example

```yaml
type: dagster_component_templates.ExternalRedisStreamAsset
attributes:
  asset_key: my_service/asset
  stream_name: my_stream_name
  # host: "localhost"  # optional
  # port: 6379  # optional
  # group_name: None  # optional
  # description: None  # optional
```

## Pair with the observation sensor

Use the companion observation sensor to periodically health-check the stream and record metrics as `AssetObservation` events:

```yaml
# 1. Declare the external asset (lineage node)
type: dagster_component_templates.ExternalRedisStreamAsset
attributes:
  asset_key: external/redis_stream
  stream_name: my_stream

---

# 2. Observe it on a schedule
type: dagster_component_templates.RedisStreamObservationSensorComponent
attributes:
  sensor_name: redis_stream_asset_observer
  asset_key: external/redis_stream
  stream_name: my_stream
```

## Requirements

No additional packages required — this component only creates a `dg.AssetSpec`.
