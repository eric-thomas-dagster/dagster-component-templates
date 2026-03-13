# weighted_average

## Purpose

Computes a weighted average of a numeric column using a weight column. When `group_by` is specified, one weighted average is computed per group and the result is a summary DataFrame. Without grouping, a single scalar weighted average is returned as a one-row DataFrame.

A weighted average is computed as: `sum(value * weight) / sum(weight)`.

## Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `asset_name` | `str` | required | Output Dagster asset name |
| `upstream_asset_key` | `str` | required | Upstream asset key providing the input DataFrame |
| `value_column` | `str` | required | Column containing values to average |
| `weight_column` | `str` | required | Column containing weights |
| `group_by` | `List[str]` | `None` | Group columns; one weighted average is computed per group |
| `output_column` | `str` | `"weighted_avg"` | Name for the output weighted average column |
| `group_name` | `str` | `None` | Dagster asset group name |

## Example YAML

```yaml
type: dagster_component_templates.WeightedAverageComponent
attributes:
  asset_name: weighted_avg_price_by_category
  upstream_asset_key: order_line_items
  value_column: unit_price
  weight_column: quantity
  group_by:
    - product_category
    - region
  output_column: weighted_avg_price
  group_name: transforms
```

## Notes

### Grouped vs Ungrouped

- **Grouped**: When `group_by` is set, the output has one row per unique group combination, with columns for each group key and the `output_column`. This is equivalent to computing a volume-weighted average price (VWAP) per segment.
- **Ungrouped**: Returns a single-row DataFrame with just the `output_column`. Useful for computing an overall weighted average across the full dataset.

### Formula

The weighted average is calculated as:

```
weighted_avg = sum(value_column * weight_column) / sum(weight_column)
```

### Zero-Weight Groups

If all weights in a group sum to zero, the result will be `NaN` for that group due to division by zero. Ensure weight columns contain positive values.

### IO Manager

This component uses Dagster's IO manager to pass DataFrames between assets. No IO manager configuration is required for local development — Dagster's default FilesystemIOManager handles serialization automatically.
