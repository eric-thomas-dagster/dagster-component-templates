# JsonPathExtractorComponent

Extract values from dict/JSON columns using JSONPath expressions. Uses the `jsonpath-ng` library.

## Fields

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `asset_name` | string | yes | — | Output Dagster asset name |
| `upstream_asset_key` | string | yes | — | Upstream asset key providing a DataFrame |
| `group_name` | string | no | null | Dagster asset group name |
| `extractions` | object | yes | — | Mapping of new_column_name to JSONPath expression |
| `source_column` | string | no | null | Column to apply JSONPath to. If omitted, applies to each row as dict. |
| `drop_source` | boolean | no | false | Drop the source column after extraction |

## Example

```yaml
component_type: dagster_component_templates.JsonPathExtractorComponent
asset_name: extracted_user_fields
upstream_asset_key: raw_events
extractions:
  user_id: "$.user.id"
  user_email: "$.user.email"
  tags: "$.meta.tags[*]"
source_column: payload
drop_source: false
```
