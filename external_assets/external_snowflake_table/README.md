# External Snowflake Table

Declares a Snowflake Table table as an external asset in the Dagster asset graph — making it visible for lineage without materializing it from Dagster.

## What this does

External assets represent data sources managed outside of Dagster. Adding one to your project:
- Makes the Snowflake Table table visible as a node in the **Dagster asset graph**
- Enables downstream assets to declare `deps` on it, drawing lineage edges
- Records connection metadata in the **Asset Catalog**

This component does not read from or write to Snowflake Table. It is a **lineage declaration only**.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_key` | `str` | `**required**` | Dagster asset key |
| `account` | `str` | `**required**` |  |
| `database` | `str` | `**required**` | Snowflake database name |
| `schema_name` | `str` | `**required**` | Snowflake schema name |
| `table_name` | `str` | `**required**` | Snowflake table name |
| `group_name` | `Optional[str]` | `None` | Dagster asset group name |
| `description` | `Optional[str]` | `None` | Human-readable description |

## Example

```yaml
type: dagster_component_templates.ExternalSnowflakeTableAsset
attributes:
  asset_key: my_service/asset
  account: my_account
  database: my_database
  schema_name: my_schema_name
  table_name: my_table_name
  # group_name: None  # optional
  # description: None  # optional
```

## Pair with the observation sensor

Use the companion observation sensor to periodically health-check the table and record metrics as `AssetObservation` events:

```yaml
# 1. Declare the external asset (lineage node)
type: dagster_component_templates.ExternalSnowflakeTableAsset
attributes:
  asset_key: external/snowflake
  account: ACCOUNT
  database: DATABASE

---

# 2. Observe it on a schedule
type: dagster_component_templates.SnowflakeTableObservationSensorComponentObservationSensorComponent
attributes:
  sensor_name: snowflake_table_observer
  asset_key: external/snowflake
  asset_key: external/snowflake
  account: ACCOUNT
  database: DATABASE
```

## Requirements

No additional packages required — this component only creates a `dg.AssetSpec`.
