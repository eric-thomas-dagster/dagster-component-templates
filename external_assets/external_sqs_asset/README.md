# External Sqs Asset

Declares a Sqs queue as an external asset in the Dagster asset graph — making it visible for lineage without materializing it from Dagster.

## What this does

External assets represent data sources managed outside of Dagster. Adding one to your project:
- Makes the Sqs queue visible as a node in the **Dagster asset graph**
- Enables downstream assets to declare `deps` on it, drawing lineage edges
- Records connection metadata in the **Asset Catalog**

This component does not read from or write to Sqs. It is a **lineage declaration only**.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_key` | `str` | `**required**` | Dagster asset key |
| `queue_url` | `str` | `**required**` | Full SQS queue URL |
| `region_name` | `Optional[str]` | `None` | AWS region |
| `group_name` | `Optional[str]` | `None` | Dagster asset group name |
| `description` | `Optional[str]` | `None` | Human-readable description |

## Example

```yaml
type: dagster_component_templates.ExternalSqsAsset
attributes:
  asset_key: my_service/asset
  queue_url: my_queue_url
  # region_name: None  # optional
  # group_name: None  # optional
  # description: None  # optional
```

## Pair with the observation sensor

Use the companion observation sensor to periodically health-check the queue and record metrics as `AssetObservation` events:

```yaml
# 1. Declare the external asset (lineage node)
type: dagster_component_templates.ExternalSqsAsset
attributes:
  asset_key: external/sqs
  queue_url: QUEUE_URL

---

# 2. Observe it on a schedule
type: dagster_component_templates.SqsObservationSensorComponent
attributes:
  sensor_name: sqs_asset_observer
  asset_key: external/sqs
  asset_key: external/sqs
  queue_url: QUEUE_URL
```

## Requirements

No additional packages required — this component only creates a `dg.AssetSpec`.
