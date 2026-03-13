# TextToColumns

Split a text column into multiple columns or rows using a delimiter. Supports optional whitespace stripping, a limit on the number of splits, and custom output column naming. Can also explode split values into separate rows.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Output Dagster asset name |
| `upstream_asset_key` | `str` | required | Upstream asset key providing a DataFrame |
| `column` | `str` | required | Column to split |
| `delimiter` | `str` | `","` | Delimiter string |
| `max_splits` | `Optional[int]` | `null` | Max number of splits (null = unlimited) |
| `output_columns` | `Optional[List[str]]` | `null` | Names for resulting columns (auto-generated if null) |
| `expand_to_rows` | `bool` | `false` | If true, split into rows instead of columns |
| `strip_whitespace` | `bool` | `true` | Strip whitespace from each part after splitting |
| `group_name` | `Optional[str]` | `null` | Dagster asset group name |

## Behavior

- When `expand_to_rows` is `false`: the original column is removed and replaced with new columns named by `output_columns` or auto-generated as `{column}_0`, `{column}_1`, etc.
- When `expand_to_rows` is `true`: the column is split into a list and exploded so each value occupies its own row. The row index is reset.
- `max_splits` limits how many times the delimiter is applied (useful when only the first N parts are needed).

## YAML Example

```yaml
type: dagster_component_templates.TextToColumns
attributes:
  asset_name: split_address_fields
  upstream_asset_key: raw_addresses
  column: full_address
  delimiter: ","
  max_splits: null
  output_columns:
    - street
    - city
    - state
    - zip
  expand_to_rows: false
  strip_whitespace: true
  group_name: transforms
```

## IO Manager Note

This component uses Dagster's IO manager to pass DataFrames between assets. No IO manager configuration is required for local development — Dagster's default FilesystemIOManager handles serialization automatically.
