# MakeGroup

For each unique combination of `key_columns`, assigns a sequential integer group ID and writes it as a new column. Equivalent to Alteryx's Make Group tool — useful for joining at a higher granularity or making compact identifiers.

## Example

```yaml
type: dagster_component_templates.MakeGroupComponent
attributes:
  asset_name: orders_with_session_id
  upstream_asset_key: orders_raw
  key_columns: [customer_id, order_date]
  output_column: session_id
  group_name: prep
```


## Requirements

```
pandas
```
