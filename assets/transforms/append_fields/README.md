# AppendFields

Append columns from a small "source" DataFrame to every row of a larger main DataFrame using a broadcast (cross) join. Only the first row of the source is used, making this ideal for attaching global configuration, metadata, or reference values to all rows.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Output Dagster asset name |
| `upstream_asset_key` | `str` | required | Large/main DataFrame asset key |
| `source_asset_key` | `str` | required | Small DataFrame whose columns to append |
| `fields` | `Optional[List[str]]` | `null` | Specific fields from source to append (null = all columns) |
| `group_name` | `Optional[str]` | `null` | Dagster asset group name |

## Behavior

- Only the first row of the source DataFrame is used. If the source has multiple rows, all but the first are ignored.
- If `fields` is provided, only those columns from the source are appended.
- A temporary `_merge_key` column is added to both DataFrames to execute the cross join and is dropped from the output.
- The resulting DataFrame contains all original columns from the main DataFrame plus the selected source columns.

## YAML Example

```yaml
type: dagster_component_templates.AppendFields
attributes:
  asset_name: orders_with_config
  upstream_asset_key: orders
  source_asset_key: global_config
  fields:
    - currency
    - tax_rate
    - fiscal_year
  group_name: transforms
```

### Append all fields from source

```yaml
type: dagster_component_templates.AppendFields
attributes:
  asset_name: enriched_events
  upstream_asset_key: events
  source_asset_key: run_metadata
  fields: null
  group_name: transforms
```

## IO Manager Note

This component uses Dagster's IO manager to pass DataFrames between assets. No IO manager configuration is required for local development — Dagster's default FilesystemIOManager handles serialization automatically.
