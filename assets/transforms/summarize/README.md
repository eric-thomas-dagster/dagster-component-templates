# summarize

## Purpose

Groups a DataFrame by one or more columns and applies aggregation functions to produce a summary DataFrame. This is the DataFrame equivalent of a SQL `GROUP BY` statement or the Alteryx Summarize tool. The component receives an upstream DataFrame via Dagster's `ins` mechanism and returns the aggregated result.

## Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `asset_name` | `str` | required | Output Dagster asset name |
| `upstream_asset_key` | `str` | required | Upstream asset key providing the input DataFrame |
| `group_by` | `List[str]` | required | Columns to group by |
| `aggregations` | `dict` | required | Mapping of output column name to aggregation function |
| `group_name` | `str` | `None` | Dagster asset group name |

## Aggregation Functions

Supported values: `sum`, `mean`, `count`, `min`, `max`, `std`, `first`, `last`, `nunique`.

The `aggregations` dict maps output column names to function names:

```yaml
aggregations:
  total_revenue: sum     # sums the 'total_revenue' column
  order_count: count     # counts non-null values
  avg_price: mean        # computes mean of 'avg_price'
```

## Example YAML

```yaml
type: dagster_component_templates.SummarizeComponent
attributes:
  asset_name: revenue_by_region
  upstream_asset_key: sales_data
  group_by:
    - region
    - product_category
  aggregations:
    revenue: sum
    order_count: count
    avg_order_value: mean
    min_order: min
    max_order: max
  group_name: transforms
```

## Notes

### Column Names in aggregations

The keys in `aggregations` should match column names in the upstream DataFrame. The aggregation function is applied to each named column.

### IO Manager

This component uses Dagster's IO manager to pass DataFrames between assets. No IO manager configuration is required for local development — Dagster's default FilesystemIOManager handles serialization automatically.

### Result Shape

The output DataFrame contains the `group_by` columns plus one column per aggregation entry. The index is reset so that group columns are regular columns in the output.
