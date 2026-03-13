# External Sql Asset

Declares a Sql resource as an external asset in the Dagster asset graph — making it visible for lineage without materializing it from Dagster.

## What this does

External assets represent data sources managed outside of Dagster. Adding one to your project:
- Makes the Sql resource visible as a node in the **Dagster asset graph**
- Enables downstream assets to declare `deps` on it, drawing lineage edges
- Records connection metadata in the **Asset Catalog**

This component does not read from or write to Sql. It is a **lineage declaration only**.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_key` | `str` | `**required**` | Dagster asset key |
| `table_name` | `str` | `**required**` |  |
| `connection_string_env_var` | `str` | `**required**` | Env var containing the SQLAlchemy connection string |
| `group_name` | `Optional[str]` | `None` | Dagster asset group name |
| `description` | `Optional[str]` | `None` | Human-readable description |

## Example

```yaml
type: dagster_component_templates.ExternalSqlAsset
attributes:
  asset_key: my_service/asset
  table_name: my_table_name
  connection_string_env_var: CONNECTION_STRING_ENV_VAR
  # group_name: None  # optional
  # description: None  # optional
```

## Pair with the observation sensor

Use the companion observation sensor to periodically health-check the resource and record metrics as `AssetObservation` events:

```yaml
# 1. Declare the external asset (lineage node)
type: dagster_component_templates.ExternalSqlAsset
attributes:
  asset_key: external/sql
  table_name: my_table
  connection_string_env_var: CONNECTION_STRING_ENV_VAR

---

# 2. Observe it on a schedule
type: dagster_component_templates.SqlObservationSensorComponent
attributes:
  sensor_name: sql_asset_observer
  asset_key: external/sql
  asset_key: external/sql
  table_name: my_table
  connection_string_env_var: CONNECTION_STRING_ENV_VAR
```

## Requirements

No additional packages required — this component only creates a `dg.AssetSpec`.
