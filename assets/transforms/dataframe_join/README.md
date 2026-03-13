# DataframeJoin

Join two upstream DataFrame assets using pandas merge. Supports all standard join types (inner, left, right, outer, cross), joining on shared column names or on different column names via `left_on`/`right_on`, and custom suffixes for overlapping column names.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Output Dagster asset name |
| `left_asset_key` | `str` | required | Left DataFrame asset key |
| `right_asset_key` | `str` | required | Right DataFrame asset key |
| `how` | `str` | `"inner"` | Join type: `inner`, `left`, `right`, `outer`, or `cross` |
| `on` | `Optional[List[str]]` | `null` | Column(s) to join on (same name in both DataFrames) |
| `left_on` | `Optional[List[str]]` | `null` | Left join columns (use when column names differ) |
| `right_on` | `Optional[List[str]]` | `null` | Right join columns (use when column names differ) |
| `suffixes` | `List[str]` | `["_x", "_y"]` | Suffixes appended to overlapping column names |
| `group_name` | `Optional[str]` | `null` | Dagster asset group name |

## Behavior

- Use `on` when the join key column has the same name in both DataFrames.
- Use `left_on` and `right_on` together when join key columns have different names.
- For a cross join, set `how: cross` and leave `on`, `left_on`, `right_on` as null.
- Overlapping non-key columns are disambiguated using `suffixes`.

## YAML Example

```yaml
type: dagster_component_templates.DataframeJoin
attributes:
  asset_name: orders_with_customers
  left_asset_key: orders
  right_asset_key: customers
  how: left
  on:
    - customer_id
  left_on: null
  right_on: null
  suffixes:
    - _order
    - _customer
  group_name: transforms
```

## IO Manager Note

This component uses Dagster's IO manager to pass DataFrames between assets. No IO manager configuration is required for local development — Dagster's default FilesystemIOManager handles serialization automatically.
