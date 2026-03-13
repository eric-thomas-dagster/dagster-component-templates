# cross_tab

## Purpose

Performs a pivot table / cross-tabulation on an upstream DataFrame. Unique values in the `pivot_column` are rotated into new column headers, while `value_column` values are aggregated using the specified function. This is equivalent to Excel's PivotTable or the Alteryx Cross-Tab tool.

## Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `asset_name` | `str` | required | Output Dagster asset name |
| `upstream_asset_key` | `str` | required | Upstream asset key providing the input DataFrame |
| `index_column` | `str` | required | Column to use for row labels |
| `pivot_column` | `str` | required | Column whose unique values become new column headers |
| `value_column` | `str` | required | Values to aggregate in the pivot table |
| `agg_func` | `str` | `"sum"` | Aggregation function (sum, mean, count, min, max, etc.) |
| `fill_value` | `float` | `0` | Value to fill NaN cells after pivoting |
| `group_name` | `str` | `None` | Dagster asset group name |

## Example YAML

```yaml
type: dagster_component_templates.CrossTabComponent
attributes:
  asset_name: sales_by_region_and_quarter
  upstream_asset_key: sales_data
  index_column: region
  pivot_column: quarter
  value_column: revenue
  agg_func: sum
  fill_value: 0
  group_name: transforms
```

Given input data with columns `region`, `quarter`, `revenue`, the output will have one row per unique `region` and one column per unique `quarter` value (e.g., `Q1`, `Q2`, `Q3`, `Q4`).

## Notes

### Column MultiIndex Flattening

After pivoting, Pandas sometimes produces a MultiIndex column structure. This component flattens the column names to simple strings automatically.

### fill_value

Combinations of index/pivot values that have no data will be filled with `fill_value` (default `0`). Set to `null` if you prefer `NaN` for missing combinations.

### Aggregation Functions

Any function accepted by `pd.pivot_table`'s `aggfunc` parameter is valid, including `sum`, `mean`, `count`, `min`, `max`, `std`, `first`, `last`.

### IO Manager

This component uses Dagster's IO manager to pass DataFrames between assets. No IO manager configuration is required for local development — Dagster's default FilesystemIOManager handles serialization automatically.
