# AutoFieldComponent

Automatically optimize DataFrame column dtypes to reduce memory usage.
Downcasts integers and floats, converts low-cardinality strings to `category`
dtype, and optionally attempts to parse string columns as datetime.

## Use Cases

- Reduce memory footprint of large DataFrames before passing to downstream steps
- Improve pandas performance by converting string columns to category dtype
- Automatically parse date strings loaded as objects
- Prepare DataFrames for efficient Parquet storage

## Fields

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `asset_name` | `str` | Yes | — | Output Dagster asset name |
| `upstream_asset_key` | `str` | Yes | — | Upstream asset key providing a DataFrame |
| `columns` | `List[str]` | No | `None` | Columns to optimize. `None` = all columns. |
| `downcast_integers` | `bool` | No | `True` | Reduce int64 to smallest fitting integer type |
| `downcast_floats` | `bool` | No | `True` | Reduce float64 to float32 where possible |
| `convert_strings_to_category` | `bool` | No | `True` | Convert low-cardinality strings to category |
| `category_threshold` | `float` | No | `0.5` | Unique ratio below which strings become category |
| `parse_dates` | `bool` | No | `False` | Attempt to parse high-cardinality strings as datetime |
| `group_name` | `str` | No | `None` | Dagster asset group name |

## How It Works

For each target column:

1. **Integer columns** — downcasted to `int8`, `int16`, or `int32` as values permit.
2. **Float columns** — downcasted to `float32`.
3. **Object (string) columns** — if `unique / total < category_threshold`, converted to `category`. Otherwise, if `parse_dates=True`, an attempt is made to parse as datetime.

## Example YAML

```yaml
type: dagster_component_templates.AutoFieldComponent
attributes:
  asset_name: events_optimized
  upstream_asset_key: raw_events
  downcast_integers: true
  downcast_floats: true
  convert_strings_to_category: true
  category_threshold: 0.3
  parse_dates: true
  group_name: optimized_data
```

## Inputs and Outputs

| | Type | Description |
|-|------|-------------|
| Input | `pd.DataFrame` | The upstream DataFrame to optimize |
| Output | `pd.DataFrame` | DataFrame with optimized column dtypes |

## Logging

After each run, logs: `"Memory: X.XX MB -> Y.YY MB (saved Z.Z%)"`.

## IO Manager Note

This component returns a plain `pd.DataFrame`. Ensure your Dagster environment
has an IO manager configured that can handle DataFrames.

## Notes

- Downcasting integers changes the dtype but not the values (within the range of the smaller type).
- `category` dtype is ideal for columns with fewer than ~50% unique values.
- Date parsing failures are silently ignored; the column is left as-is.
