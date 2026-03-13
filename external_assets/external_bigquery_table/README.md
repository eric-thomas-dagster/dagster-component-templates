# External Bigquery Table

Declares a Bigquery Table table as an external asset in the Dagster asset graph — making it visible for lineage without materializing it from Dagster.

## What this does

External assets represent data sources managed outside of Dagster. Adding one to your project:
- Makes the Bigquery Table table visible as a node in the **Dagster asset graph**
- Enables downstream assets to declare `deps` on it, drawing lineage edges
- Records connection metadata in the **Asset Catalog**

This component does not read from or write to Bigquery Table. It is a **lineage declaration only**.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_key` | `str` | `**required**` | Dagster asset key |
| `project_id` | `str` | `**required**` | GCP project ID |
| `dataset_id` | `str` | `**required**` | BigQuery dataset ID |
| `table_id` | `str` | `**required**` | BigQuery table ID |
| `group_name` | `Optional[str]` | `None` | Dagster asset group name |
| `description` | `Optional[str]` | `None` | Human-readable description |

## Example

```yaml
type: dagster_component_templates.ExternalBigQueryTableAsset
attributes:
  asset_key: my_service/asset
  project_id: my_project_id
  dataset_id: my_dataset_id
  table_id: my_table_id
  # group_name: None  # optional
  # description: None  # optional
```

## Pair with the observation sensor

Use the companion observation sensor to periodically health-check the table and record metrics as `AssetObservation` events:

```yaml
# 1. Declare the external asset (lineage node)
type: dagster_component_templates.ExternalBigQueryTableAsset
attributes:
  asset_key: external/bigquery
  project_id: PROJECT_ID
  dataset_id: DATASET_ID

---

# 2. Observe it on a schedule
type: dagster_component_templates.BigQueryTableObservationSensorComponentObservationSensorComponent
attributes:
  sensor_name: bigquery_table_observer
  asset_key: external/bigquery
  project_id: PROJECT_ID
  dataset_id: DATASET_ID
```

## Requirements

No additional packages required — this component only creates a `dg.AssetSpec`.
