# JsonFlattenComponent

Recursively flatten nested dict columns into dot-notation columns (e.g. `address.city`, `meta.tags.0`). Uses `pandas.json_normalize` internally.

## Fields

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `asset_name` | string | yes | — | Output Dagster asset name |
| `upstream_asset_key` | string | yes | — | Upstream asset key providing a DataFrame |
| `group_name` | string | no | null | Dagster asset group name |
| `column` | string | no | null | Column containing dicts to flatten. If omitted, all object-dtype columns are flattened. |
| `separator` | string | no | `"."` | Separator for nested key names |
| `max_depth` | integer | no | null | Maximum recursion depth (null = unlimited) |
| `drop_original` | boolean | no | true | Drop the source column after flattening |

## Example

```yaml
component_type: dagster_component_templates.JsonFlattenComponent
asset_name: flattened_users
upstream_asset_key: raw_users
column: address
separator: "."
max_depth: 3
drop_original: true
```
