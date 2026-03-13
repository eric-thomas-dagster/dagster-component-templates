# Multi-Row Formula Component

Apply formulas that reference neighboring rows — lag, lead, rolling windows, differences, cumulative sums, and percent changes. Essential for time-series feature engineering and trend analysis.

This component uses Dagster's IO manager to pass DataFrames between assets. No IO manager configuration is required for local development — Dagster's default FilesystemIOManager handles serialization automatically.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Output Dagster asset name |
| `upstream_asset_key` | `str` | required | Upstream asset key providing a DataFrame |
| `operations` | `List[dict]` | required | List of operation dicts (see Operations below) |
| `sort_by` | `Optional[str]` | `None` | Sort by this column before applying operations |
| `group_by` | `Optional[List[str]]` | `None` | Apply operations within these groups |
| `group_name` | `Optional[str]` | `None` | Dagster asset group name |

## Operation Dict Fields

Each entry in `operations` is a dict with the following keys:

| Key | Type | Default | Description |
|---|---|---|---|
| `output_column` | `str` | required | Name of the new column |
| `column` | `str` | required | Source column to operate on |
| `operation` | `str` | required | One of the supported operations (see below) |
| `periods` | `int` | `1` | Periods for `lag`, `lead`, `diff`, `pct_change` |
| `window` | `int` | `3` | Window size for rolling operations |
| `min_periods` | `int` | `1` | Minimum periods for rolling operations |

## Supported Operations

| Operation | Description |
|---|---|
| `lag` | Shift values forward by `periods` |
| `lead` | Shift values backward by `periods` |
| `rolling_mean` | Rolling mean over `window` rows |
| `rolling_sum` | Rolling sum over `window` rows |
| `rolling_min` | Rolling minimum over `window` rows |
| `rolling_max` | Rolling maximum over `window` rows |
| `rolling_std` | Rolling standard deviation over `window` rows |
| `diff` | Difference from `periods` rows prior |
| `pct_change` | Percent change from `periods` rows prior |
| `cumsum` | Cumulative sum |
| `cummax` | Cumulative maximum |
| `cummin` | Cumulative minimum |

## Example YAML

```yaml
type: dagster_component_templates.MultiRowFormulaComponent
attributes:
  asset_name: sales_with_trends
  upstream_asset_key: daily_sales
  sort_by: date
  group_by:
    - region
  operations:
    - output_column: revenue_lag_1
      column: revenue
      operation: lag
      periods: 1
    - output_column: revenue_rolling_7d
      column: revenue
      operation: rolling_mean
      window: 7
      min_periods: 1
    - output_column: revenue_pct_change
      column: revenue
      operation: pct_change
      periods: 1
  group_name: time_series
```

## Notes

- When `group_by` is set, all operations are applied within each group independently.
- `sort_by` is applied once before all operations; the sort order affects lag/lead/rolling results.
- Operations are applied sequentially in list order.
