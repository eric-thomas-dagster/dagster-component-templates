# ts_filler

Fill gaps in a time series DataFrame by resampling to a regular frequency and applying a fill strategy. Supports group-wise filling (e.g., per product or device) and multiple fill methods including forward fill, backward fill, interpolation, zero-fill, and mean-fill.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Output Dagster asset name |
| `upstream_asset_key` | `str` | required | Upstream asset key providing a time series DataFrame |
| `date_column` | `str` | required | Column containing datetime values |
| `frequency` | `str` | `"D"` | Pandas offset alias: `"D"`, `"W"`, `"ME"`, `"h"`, `"min"` |
| `fill_method` | `str` | `"forward_fill"` | `"forward_fill"`, `"backward_fill"`, `"interpolate"`, `"zero"`, `"mean"` |
| `value_columns` | `Optional[List[str]]` | `null` | Columns to fill (null = all numeric columns) |
| `group_by` | `Optional[List[str]]` | `null` | Fill within groups (e.g. `["product_id"]`) |
| `group_name` | `Optional[str]` | `null` | Dagster asset group name |

## Frequency Aliases

Common pandas offset aliases for `frequency`:
- `"D"` — daily
- `"W"` — weekly (Sunday)
- `"ME"` — month-end
- `"MS"` — month-start
- `"h"` — hourly
- `"min"` — minute

## Example YAML

```yaml
component_type: ts_filler
description: Fill gaps in daily sensor readings using forward fill, grouped by device_id.

asset_name: sensor_readings_filled
upstream_asset_key: raw_sensor_readings
date_column: reading_date
frequency: D
fill_method: forward_fill
value_columns:
  - temperature
  - humidity
group_by:
  - device_id
group_name: time_series
```

## Metadata Logged

- `original_rows` — Row count before resampling
- `output_rows` — Row count after resampling
- `rows_added` — Number of gap rows inserted
- `frequency` — Resampling frequency used
- `fill_method` — Fill strategy applied

## IO Manager Note

This component uses Dagster's IO manager to pass DataFrames between assets. No IO manager configuration is required for local development — Dagster's default FilesystemIOManager handles serialization automatically.

## Requirements

- `dagster`
- `pandas`
