# ArrangeComponent

Reorder, move, and rename columns in a DataFrame. Supports explicit full
ordering, moving columns to the front or back, and renaming — all in one step.

## Use Cases

- Promote key identifier columns to the front for readability
- Move audit/metadata columns (created_at, updated_at) to the end
- Standardize column order before writing to a target schema
- Rename and reorder in a single pipeline step

## Fields

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `asset_name` | `str` | Yes | — | Output Dagster asset name |
| `upstream_asset_key` | `str` | Yes | — | Upstream asset key providing a DataFrame |
| `column_order` | `List[str]` | No | `None` | Explicit full column order; unlisted columns appended at end |
| `move_to_front` | `List[str]` | No | `None` | Move these columns to the front |
| `move_to_back` | `List[str]` | No | `None` | Move these columns to the end |
| `rename` | `dict` | No | `None` | Mapping of `{old_name: new_name}` |
| `group_name` | `str` | No | `None` | Dagster asset group name |

## Operation Order

1. **Rename** columns (so `column_order`/`move_to_front`/`move_to_back` use the new names).
2. **Reorder** using `column_order`, or `move_to_front`/`move_to_back`.

Note: `column_order` takes precedence over `move_to_front`/`move_to_back` if both are set.

## Example YAML

```yaml
type: dagster_component_templates.ArrangeComponent
attributes:
  asset_name: orders_arranged
  upstream_asset_key: raw_orders
  move_to_front:
    - order_id
    - customer_id
    - order_date
  move_to_back:
    - internal_flag
    - created_at
    - updated_at
  rename:
    order_date: date
    customer_id: cust_id
  group_name: order_processing
```

### Full Column Order Example

```yaml
type: dagster_component_templates.ArrangeComponent
attributes:
  asset_name: products_ordered
  upstream_asset_key: raw_products
  column_order:
    - sku
    - name
    - category
    - price
  group_name: catalog
```

## Inputs and Outputs

| | Type | Description |
|-|------|-------------|
| Input | `pd.DataFrame` | The upstream DataFrame |
| Output | `pd.DataFrame` | DataFrame with columns reordered and/or renamed |

## IO Manager Note

This component returns a plain `pd.DataFrame`. Ensure your Dagster environment
has an IO manager configured that can handle DataFrames.

## Notes

- Columns in `column_order` not present in the DataFrame are silently skipped.
- All original columns are preserved (use `SelectColumnsComponent` to drop columns).
- Rename is applied before reordering, so use the new names in ordering fields.
