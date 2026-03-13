# SchemaValidatorComponent

Validate each row against a JSON Schema. Rows that fail validation are either dropped, tagged with error messages, or raise an exception. Uses the `jsonschema` library.

## Fields

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `asset_name` | string | yes | — | Output Dagster asset name |
| `upstream_asset_key` | string | yes | — | Upstream asset key providing a DataFrame |
| `group_name` | string | no | null | Dagster asset group name |
| `json_schema` | object | yes | — | The JSON Schema to validate against |
| `on_invalid` | string (enum) | no | `"tag"` | Action on invalid rows: `drop`, `tag`, `raise` |
| `tag_column` | string | no | `"_validation_errors"` | Column added when `on_invalid="tag"` |
| `subset_columns` | array of strings | no | null | Only validate these columns |

## Example

```yaml
component_type: dagster_component_templates.SchemaValidatorComponent
asset_name: validated_orders
upstream_asset_key: raw_orders
json_schema:
  type: object
  required: [order_id, amount]
  properties:
    amount:
      type: number
      minimum: 0
on_invalid: tag
tag_column: _validation_errors
```
