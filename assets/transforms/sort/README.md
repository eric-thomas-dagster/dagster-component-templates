# SortComponent

Sort a DataFrame by one or more columns. Supports ascending/descending order
per column, NaN placement control, and optional index reset.

## Use Cases

- Sort events by timestamp before windowed aggregations
- Rank products by revenue descending for top-N analysis
- Prepare data for merge/join operations that require sorted input
- Sort by multiple columns with mixed directions

## Fields

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `asset_name` | `str` | Yes | — | Output Dagster asset name |
| `upstream_asset_key` | `str` | Yes | — | Upstream asset key providing a DataFrame |
| `by` | `List[str]` | Yes | — | Columns to sort by, in priority order |
| `ascending` | `bool` | No | `True` | Sort ascending (True) or descending (False) for all columns |
| `ascending_per_column` | `List[bool]` | No | `None` | Per-column ascending flags; overrides `ascending` when set |
| `na_position` | `str` | No | `"last"` | Where to place NaN values: `"first"` or `"last"` |
| `reset_index` | `bool` | No | `True` | Reset the integer index after sorting |
| `group_name` | `str` | No | `None` | Dagster asset group name |

## Mixed Sort Directions

Use `ascending_per_column` to sort different columns in different directions.
The list must match the length of `by`:

```yaml
by:
  - department
  - salary
ascending_per_column:
  - true   # department: A → Z
  - false  # salary: high → low
```

## Example YAML

```yaml
type: dagster_component_templates.SortComponent
attributes:
  asset_name: orders_sorted
  upstream_asset_key: raw_orders
  by:
    - order_date
    - total_amount
  ascending: false
  na_position: last
  reset_index: true
  group_name: order_processing
```

## Inputs and Outputs

| | Type | Description |
|-|------|-------------|
| Input | `pd.DataFrame` | The upstream DataFrame to sort |
| Output | `pd.DataFrame` | Sorted DataFrame |

## IO Manager Note

This component returns a plain `pd.DataFrame`. Ensure your Dagster environment
has an IO manager configured that can handle DataFrames.

## Notes

- `ascending_per_column` takes precedence over `ascending` when both are set.
- NaN handling depends on `na_position`; pandas places NaNs at the end by default.
- Sort stability is guaranteed (pandas uses mergesort internally for stable sorts).
