# DocumentMergerComponent

Merge two document DataFrames on a key column. Wraps `pandas.merge()` but designed for document-style data with nested columns. Accepts two upstream assets (`left` and `right`).

## Fields

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `asset_name` | string | yes | — | Output Dagster asset name |
| `left_asset_key` | string | yes | — | Asset key for the left DataFrame |
| `right_asset_key` | string | yes | — | Asset key for the right DataFrame |
| `group_name` | string | no | null | Dagster asset group name |
| `on` | string or array | yes | — | Join key column(s) |
| `how` | string (enum) | no | `"left"` | Join type: `inner`, `left`, `right`, `outer` |
| `suffixes` | array of 2 strings | no | `["_left", "_right"]` | Suffixes for overlapping column names |
| `flatten_result` | boolean | no | false | Auto-flatten nested dict columns after merge |

## Example

```yaml
component_type: dagster_component_templates.DocumentMergerComponent
asset_name: merged_orders_customers
left_asset_key: cleaned_orders
right_asset_key: cleaned_customers
on: customer_id
how: left
suffixes: ["_order", "_customer"]
flatten_result: true
```
