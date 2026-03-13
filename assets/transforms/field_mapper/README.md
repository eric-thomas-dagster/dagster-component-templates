# FieldMapperComponent

Rename or remap column names according to a mapping dictionary. More powerful than a simple rename — supports dropping unmapped columns and specifying passthrough columns.

## Fields

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `asset_name` | string | yes | — | Output Dagster asset name |
| `upstream_asset_key` | string | yes | — | Upstream asset key providing a DataFrame |
| `group_name` | string | no | null | Dagster asset group name |
| `mapping` | object | yes | — | Mapping of old_column_name to new_column_name |
| `drop_unmapped` | boolean | no | false | If true, only keep columns in the mapping (plus passthrough) |
| `passthrough` | array of strings | no | null | Columns to keep as-is even when `drop_unmapped=true` |

## Example

```yaml
component_type: dagster_component_templates.FieldMapperComponent
asset_name: normalized_events
upstream_asset_key: raw_mongo_events
mapping:
  _id: id
  createdAt: created_at
  userId: user_id
drop_unmapped: true
passthrough: [event_type, payload]
```
