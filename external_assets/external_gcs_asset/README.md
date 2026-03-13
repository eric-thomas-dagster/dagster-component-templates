# External Gcs Asset

Declares a Gcs storage container as an external asset in the Dagster asset graph — making it visible for lineage without materializing it from Dagster.

## What this does

External assets represent data sources managed outside of Dagster. Adding one to your project:
- Makes the Gcs storage container visible as a node in the **Dagster asset graph**
- Enables downstream assets to declare `deps` on it, drawing lineage edges
- Records connection metadata in the **Asset Catalog**

This component does not read from or write to Gcs. It is a **lineage declaration only**.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_key` | `str` | `**required**` | Dagster asset key |
| `bucket_name` | `str` | `**required**` | GCS bucket name |
| `prefix` | `str` | `""` | Object prefix |
| `project` | `Optional[str]` | `None` | GCP project ID |
| `group_name` | `Optional[str]` | `None` | Dagster asset group name |
| `description` | `Optional[str]` | `None` | Human-readable description |

## Example

```yaml
type: dagster_component_templates.ExternalGcsAsset
attributes:
  asset_key: my_service/asset
  bucket_name: my_bucket_name
  # prefix: ""  # optional
  # project: None  # optional
  # group_name: None  # optional
  # description: None  # optional
```

## Pair with the observation sensor

Use the companion observation sensor to periodically health-check the storage container and record metrics as `AssetObservation` events:

```yaml
# 1. Declare the external asset (lineage node)
type: dagster_component_templates.ExternalGcsAsset
attributes:
  asset_key: external/gcs
  bucket_name: my_bucket

---

# 2. Observe it on a schedule
type: dagster_component_templates.GcsObservationSensorComponent
attributes:
  sensor_name: gcs_asset_observer
  asset_key: external/gcs
  asset_key: external/gcs
  bucket_name: my_bucket
```

## Requirements

No additional packages required — this component only creates a `dg.AssetSpec`.
