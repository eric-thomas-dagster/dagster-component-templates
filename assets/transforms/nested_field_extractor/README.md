# NestedFieldExtractorComponent

Pull out fields from nested dict columns using dot-path notation. Simpler than JSONPath — just dot-separated keys like `address.city`.

## Fields

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `asset_name` | string | yes | — | Output Dagster asset name |
| `upstream_asset_key` | string | yes | — | Upstream asset key providing a DataFrame |
| `group_name` | string | no | null | Dagster asset group name |
| `extractions` | object | yes | — | Mapping of new_column_name to dot-path |
| `source_column` | string | no | null | Column containing dicts. If omitted, each row is treated as the dict. |
| `missing_value` | string | no | null | Value to use when path is not found |
| `drop_source` | boolean | no | false | Drop the source column after extraction |

## Example

```yaml
component_type: dagster_component_templates.NestedFieldExtractorComponent
asset_name: extracted_address_fields
upstream_asset_key: raw_customers
extractions:
  city: "address.city"
  zip: "address.zip"
source_column: customer_data
missing_value: null
drop_source: false
```
