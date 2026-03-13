# running_total

## Purpose

Computes a running (cumulative) total on a numeric column. Supports optional grouping to reset the running total per group (e.g., cumulative revenue per region), optional pre-sorting, and three aggregation modes: `sum`, `count`, and `mean`. The result is appended as a new column to the upstream DataFrame.

## Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `asset_name` | `str` | required | Output Dagster asset name |
| `upstream_asset_key` | `str` | required | Upstream asset key providing the input DataFrame |
| `value_column` | `str` | required | Column to accumulate |
| `output_column` | `str` | `None` | Name for the cumulative column. Defaults to `running_{value_column}` |
| `group_by` | `List[str]` | `None` | Reset running total per group |
| `sort_by` | `str` | `None` | Sort by this column before computing the running total |
| `sort_ascending` | `bool` | `True` | Sort direction when `sort_by` is set |
| `agg_function` | `str` | `"sum"` | Cumulative function: `sum`, `count`, or `mean` |
| `group_name` | `str` | `None` | Dagster asset group name |

## Example YAML

```yaml
type: dagster_component_templates.RunningTotalComponent
attributes:
  asset_name: cumulative_revenue_by_region
  upstream_asset_key: daily_sales
  value_column: revenue
  output_column: cumulative_revenue
  group_by:
    - region
  sort_by: sale_date
  sort_ascending: true
  agg_function: sum
  group_name: transforms
```

## Notes

### Sorting

It is strongly recommended to set `sort_by` when computing a running total over time-series data. Without sorting, the order of rows in the DataFrame determines the running total, which may be non-deterministic.

### Grouping

When `group_by` is set, the running total resets at the start of each group. Groups are determined by the unique combinations of values in the `group_by` columns.

### agg_function Options

- `sum`: Cumulative sum (`cumsum`)
- `count`: Row count within the group (1-indexed)
- `mean`: Expanding mean up to each row

### IO Manager

This component uses Dagster's IO manager to pass DataFrames between assets. No IO manager configuration is required for local development — Dagster's default FilesystemIOManager handles serialization automatically.
