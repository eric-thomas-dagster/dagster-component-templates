# transpose

## Purpose

Transposes a DataFrame so that rows become columns and columns become rows. Optionally, a column can be designated as the index before transposing — its values then become the new column headers in the transposed result. This is equivalent to transposing a matrix or using Ctrl+Shift+V (Paste Transpose) in a spreadsheet.

## Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `asset_name` | `str` | required | Output Dagster asset name |
| `upstream_asset_key` | `str` | required | Upstream asset key providing the input DataFrame |
| `index_column` | `str` | `None` | Column to use as index before transposing; its values become new column headers |
| `reset_column_name` | `str` | `"field"` | Name for the new column containing original column names after transpose |
| `group_name` | `str` | `None` | Dagster asset group name |

## Example YAML

```yaml
type: dagster_component_templates.TransposeComponent
attributes:
  asset_name: metrics_transposed
  upstream_asset_key: monthly_metrics
  index_column: metric_name
  reset_column_name: field
  group_name: transforms
```

## Notes

### index_column Behavior

When `index_column` is set, the component calls `df.set_index(index_column).T` before resetting the index. This means the values in `index_column` (e.g., `revenue`, `units_sold`) become the new column headers in the transposed result.

When `index_column` is not set, a plain `df.T` is performed and the original column names become row values in the `reset_column_name` column.

### reset_column_name

After transposing, the former column names appear as a regular column in the result. The `reset_column_name` field controls what this column is called (default: `"field"`).

### Data Types

Transposing a mixed-type DataFrame will cause Pandas to upcast all columns to `object` dtype. If downstream processing requires specific types, add a type-casting component after transposing.

### IO Manager

This component uses Dagster's IO manager to pass DataFrames between assets. No IO manager configuration is required for local development — Dagster's default FilesystemIOManager handles serialization automatically.
