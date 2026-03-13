# Imputation Component

Fill missing values in a DataFrame using a variety of strategies including statistical measures, constant values, directional fill, and interpolation.

This component uses Dagster's IO manager to pass DataFrames between assets. No IO manager configuration is required for local development — Dagster's default FilesystemIOManager handles serialization automatically.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Output Dagster asset name |
| `upstream_asset_key` | `str` | required | Upstream asset key providing a DataFrame |
| `strategy` | `str` | `"mean"` | Imputation strategy (see below) |
| `fill_value` | `Optional[str]` | `None` | Constant value for `strategy="constant"` |
| `columns` | `Optional[List[str]]` | `None` | Columns to impute (None = auto-select based on strategy) |
| `limit` | `Optional[int]` | `None` | Max consecutive fills for forward/backward fill and interpolate |
| `group_name` | `Optional[str]` | `None` | Dagster asset group name |

## Strategies

| Strategy | Description | Auto-selects |
|---|---|---|
| `mean` | Fill with column mean | Numeric columns |
| `median` | Fill with column median | Numeric columns |
| `mode` | Fill with most frequent value | All columns |
| `constant` | Fill with `fill_value` | All columns |
| `forward_fill` | Propagate last valid value forward | All columns |
| `backward_fill` | Propagate next valid value backward | All columns |
| `interpolate` | Linear interpolation | All columns |

## Example YAML

```yaml
type: dagster_component_templates.ImputationComponent
attributes:
  asset_name: imputed_sensor_readings
  upstream_asset_key: raw_sensor_readings
  strategy: forward_fill
  columns:
    - temperature
    - humidity
    - pressure
  limit: 3
  group_name: data_quality
```

## Column Auto-Selection

When `columns` is not specified:
- `mean` and `median` target all numeric (`number`-dtype) columns.
- All other strategies target every column in the DataFrame.

## Limit Parameter

For `forward_fill`, `backward_fill`, and `interpolate`, the `limit` field caps the maximum number of consecutive NaN values that will be filled. Gaps larger than `limit` are only partially filled.

## Notes

- The component logs the number of nulls filled per column for observability.
- Columns listed in `columns` that are missing from the DataFrame are skipped with a warning.
- The `mode` strategy uses the most frequent value; if no mode exists the column is left unchanged.
