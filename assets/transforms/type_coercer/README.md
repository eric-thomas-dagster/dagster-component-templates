# TypeCoercerComponent

Cast DataFrame columns to specified types. Handles mixed types common in document database output. Supports `int`, `float`, `str`, `bool`, `datetime`, `date`, and `json` target types.

## Fields

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `asset_name` | string | yes | — | Output Dagster asset name |
| `upstream_asset_key` | string | yes | — | Upstream asset key providing a DataFrame |
| `group_name` | string | no | null | Dagster asset group name |
| `type_map` | object | yes | — | Mapping of column_name to target type |
| `errors` | string (enum) | no | `"coerce"` | Behavior on failure: `raise`, `coerce`, `ignore` |
| `datetime_format` | string | no | null | strptime format string for datetime parsing |

## Example

```yaml
component_type: dagster_component_templates.TypeCoercerComponent
asset_name: typed_records
upstream_asset_key: raw_records
type_map:
  age: int
  score: float
  active: bool
  created_at: datetime
errors: coerce
datetime_format: "%Y-%m-%dT%H:%M:%S"
```
