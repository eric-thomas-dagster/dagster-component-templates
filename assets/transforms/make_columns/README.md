# MakeColumnsComponent

Reshape a single column of values into a wide multi-column format. Takes all
values from a specified column and distributes them across `n_columns` output
columns, reading left-to-right and top-to-bottom.

## Use Cases

- Convert a long list of values into a printable grid layout
- Restructure feature vectors from long to wide format
- Create column-per-slot representations from a flat value list
- Generate wide tables from key-value pair DataFrames

## Fields

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `asset_name` | `str` | Yes | — | Output Dagster asset name |
| `upstream_asset_key` | `str` | Yes | — | Upstream asset key providing a DataFrame |
| `n_columns` | `int` | No | `2` | Number of output columns to wrap data into |
| `value_column` | `str` | Yes | — | Column whose values to reshape |
| `key_column` | `str` | No | `None` | Column to use as row labels in the output |
| `fill_value` | `str` | No | `None` | Fill value for the last row if values don't divide evenly |
| `group_name` | `str` | No | `None` | Dagster asset group name |

## How It Works

Given 7 values and `n_columns=3`:

```
Input values: [A, B, C, D, E, F, G]
Padded to 9:  [A, B, C, D, E, F, G, None, None]

Output:
  col_1  col_2  col_3
  A      D      G
  B      E      None
  C      F      None
```

Values are distributed in column-first order (interleaved across columns).

## Example YAML

```yaml
type: dagster_component_templates.MakeColumnsComponent
attributes:
  asset_name: features_wide
  upstream_asset_key: feature_list
  n_columns: 4
  value_column: feature_value
  key_column: feature_name
  fill_value: "N/A"
  group_name: wide_format
```

### Two-Column Grid Example

```yaml
type: dagster_component_templates.MakeColumnsComponent
attributes:
  asset_name: tags_wide
  upstream_asset_key: tag_list
  n_columns: 2
  value_column: tag
  fill_value: ""
  group_name: display
```

## Inputs and Outputs

| | Type | Description |
|-|------|-------------|
| Input | `pd.DataFrame` | The upstream DataFrame containing the value column |
| Output | `pd.DataFrame` | Wide-format DataFrame with `col_1`, `col_2`, ..., `col_N` |

## IO Manager Note

This component returns a plain `pd.DataFrame`. Ensure your Dagster environment
has an IO manager configured that can handle DataFrames.

## Notes

- Output columns are named `col_1`, `col_2`, ..., `col_N`.
- The total number of output rows is `ceil(len(values) / n_columns)`.
- `fill_value` pads the last row; set to `None` to leave cells as `NaN`.
- When `key_column` is set, the output DataFrame index is set from that column's values.
