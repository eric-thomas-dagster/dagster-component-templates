# DatetimeParser

Parse, convert, or extract components from datetime columns in a DataFrame. Supports automatic detection of date formats, timezone conversion, and extraction of calendar components (year, month, day, hour, weekday) into separate columns.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Output Dagster asset name |
| `upstream_asset_key` | `str` | required | Upstream asset key providing a DataFrame |
| `date_column` | `str` | required | Column containing date/datetime values |
| `input_format` | `Optional[str]` | `null` | strptime format string (None = auto-detect) |
| `output_format` | `Optional[str]` | `null` | strftime format for string output (None = keep as datetime) |
| `output_column` | `Optional[str]` | `null` | Output column name (defaults to overwriting date_column) |
| `timezone` | `Optional[str]` | `null` | Convert to this timezone (e.g. "UTC", "America/New_York") |
| `extract_components` | `bool` | `false` | If True, extract year/month/day/hour/weekday as separate columns |
| `group_name` | `Optional[str]` | `null` | Dagster asset group name |

## YAML Example

```yaml
type: dagster_component_templates.DatetimeParser
attributes:
  asset_name: parsed_order_dates
  upstream_asset_key: raw_orders
  date_column: order_date
  input_format: "%Y-%m-%d"
  output_format: null
  output_column: order_date_parsed
  timezone: America/New_York
  extract_components: true
  group_name: transforms
```

## Behavior

- If `input_format` is `null`, pandas will attempt to auto-detect the date format.
- If `output_column` is `null`, the parsed value overwrites `date_column` in place.
- If `timezone` is set and the column is timezone-naive, it is first localized to UTC before converting. If already timezone-aware, it is converted directly.
- If `output_format` is set, the result is cast to a string using the given strftime format.
- If `extract_components` is `true`, the following columns are added:
  - `{date_column}_year`
  - `{date_column}_month`
  - `{date_column}_day`
  - `{date_column}_hour`
  - `{date_column}_day_of_week`

## IO Manager Note

This component uses Dagster's IO manager to pass DataFrames between assets. No IO manager configuration is required for local development — Dagster's default FilesystemIOManager handles serialization automatically.
