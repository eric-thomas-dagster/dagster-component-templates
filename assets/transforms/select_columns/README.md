# SelectColumnsComponent

Select, drop, and rename columns in a DataFrame in a single step. Operations
are applied in order: drop first, then select/reorder, then rename.

## Use Cases

- Remove PII or sensitive columns before sharing data
- Standardize column names to match a target schema
- Reduce wide DataFrames to only the columns needed downstream
- Reorder columns for readability or downstream compatibility

## Fields

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `asset_name` | `str` | Yes | — | Output Dagster asset name |
| `upstream_asset_key` | `str` | Yes | — | Upstream asset key providing a DataFrame |
| `columns` | `List[str]` | No | `None` | Columns to keep, in this order. `None` = keep all. |
| `drop_columns` | `List[str]` | No | `None` | Columns to remove from the DataFrame |
| `rename` | `dict` | No | `None` | Mapping of `{old_name: new_name}` to rename columns |
| `reorder` | `bool` | No | `True` | If `columns` is set, reorder to match that order |
| `group_name` | `str` | No | `None` | Dagster asset group name |

## Operation Order

1. **Drop** columns listed in `drop_columns` (missing columns are silently ignored)
2. **Select/reorder** to `columns` list (missing columns are warned and skipped)
3. **Rename** columns using the `rename` mapping

## Example YAML

```yaml
type: dagster_component_templates.SelectColumnsComponent
attributes:
  asset_name: customers_slim
  upstream_asset_key: raw_customers
  columns:
    - customer_id
    - email
    - full_name
    - signup_date
  drop_columns:
    - internal_notes
    - legacy_id
  rename:
    full_name: name
    signup_date: created_at
  reorder: true
  group_name: customer_data
```

## Inputs and Outputs

| | Type | Description |
|-|------|-------------|
| Input | `pd.DataFrame` | The upstream DataFrame to reshape |
| Output | `pd.DataFrame` | DataFrame with selected, dropped, and renamed columns |

## IO Manager Note

This component returns a plain `pd.DataFrame`. Ensure your Dagster environment
has an IO manager configured that can handle DataFrames.

## Notes

- Columns listed in `drop_columns` that don't exist are silently ignored.
- Columns listed in `columns` that don't exist after dropping generate a warning.
- `rename` is applied after `columns` selection, so use new names in downstream references.
- Setting `reorder=False` with `columns` will keep all columns but not reorder them.
