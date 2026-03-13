# ArrayExploderComponent

Expand an array-valued column so each element becomes its own row. Wraps `pandas.DataFrame.explode()`.

## Fields

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `asset_name` | string | yes | — | Output Dagster asset name |
| `upstream_asset_key` | string | yes | — | Upstream asset key providing a DataFrame |
| `group_name` | string | no | null | Dagster asset group name |
| `column` | string | yes | — | Column containing arrays to explode |
| `ignore_index` | boolean | no | true | Reset index after explode |
| `drop_nulls` | boolean | no | false | Drop rows where the exploded value is null/NaN |

## Example

```yaml
component_type: dagster_component_templates.ArrayExploderComponent
asset_name: exploded_tags
upstream_asset_key: raw_documents
column: tags
ignore_index: true
drop_nulls: false
```
