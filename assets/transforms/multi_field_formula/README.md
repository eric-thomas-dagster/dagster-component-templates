# MultiFieldFormulaComponent

Apply a single formula expression to multiple columns at once. Use `{col}` in
the expression as a placeholder that gets replaced with each column name in turn.
Supports creating new columns with a prefix or suffix instead of overwriting.

## Use Cases

- Log-transform multiple numeric features for ML preprocessing
- Apply string operations (`.str.upper()`, `.str.strip()`) to several columns at once
- Normalize or scale multiple columns with the same formula
- Create derived feature columns with a consistent naming convention

## Fields

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `asset_name` | `str` | Yes | — | Output Dagster asset name |
| `upstream_asset_key` | `str` | Yes | — | Upstream asset key providing a DataFrame |
| `expression` | `str` | Yes | — | Formula using `{col}` as a column placeholder |
| `columns` | `List[str]` | Yes | — | Columns to apply the expression to |
| `output_prefix` | `str` | No | `None` | Prefix for output column names (creates new columns) |
| `output_suffix` | `str` | No | `None` | Suffix for output column names (creates new columns) |
| `group_name` | `str` | No | `None` | Dagster asset group name |

## Expression Syntax

The `expression` is evaluated with Python's `eval()`. Available variables:

- `upstream` / `df` — the full DataFrame
- `pd` — pandas module
- `np` — numpy module
- `{col}` — replaced with `upstream['column_name']` at eval time

### Examples

| Expression | Effect |
|-----------|--------|
| `np.log1p(upstream['{col}'])` | Natural log + 1 transform |
| `upstream['{col}'].str.upper()` | Uppercase string column |
| `(upstream['{col}'] - upstream['{col}'].mean()) / upstream['{col}'].std()` | Z-score normalize |
| `upstream['{col}'].fillna(0)` | Fill NaN with zero |

## Example YAML

```yaml
type: dagster_component_templates.MultiFieldFormulaComponent
attributes:
  asset_name: sales_log_transformed
  upstream_asset_key: raw_sales
  expression: "np.log1p(upstream['{col}'])"
  columns:
    - revenue
    - quantity
    - discount
  output_prefix: log_
  group_name: feature_engineering
```

## Inputs and Outputs

| | Type | Description |
|-|------|-------------|
| Input | `pd.DataFrame` | The upstream DataFrame |
| Output | `pd.DataFrame` | DataFrame with transformed or new columns |

## IO Manager Note

This component returns a plain `pd.DataFrame`. Ensure your Dagster environment
has an IO manager configured that can handle DataFrames.

## Notes

- Columns not found in the DataFrame generate a warning and are skipped.
- Expression failures per column are logged as warnings and do not halt the pipeline.
- Both `output_prefix` and `output_suffix` can be set simultaneously.
- If neither prefix nor suffix is set, the original columns are overwritten.
