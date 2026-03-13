# Data Cleansing Component

Fix common data quality issues in string columns — nulls, whitespace, case normalization, and punctuation removal. Operates on all string columns or a configurable subset.

This component uses Dagster's IO manager to pass DataFrames between assets. No IO manager configuration is required for local development — Dagster's default FilesystemIOManager handles serialization automatically.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Output Dagster asset name |
| `upstream_asset_key` | `str` | required | Upstream asset key providing a DataFrame |
| `null_handling` | `str` | `"keep"` | How to handle nulls: `"keep"`, `"drop"` (drop rows with any null), or `"fill"` (fill with `null_fill_value`) |
| `null_fill_value` | `Optional[str]` | `None` | Value to fill nulls with when `null_handling="fill"` |
| `trim_whitespace` | `bool` | `True` | Strip leading/trailing whitespace from string columns |
| `normalize_case` | `Optional[str]` | `None` | Case normalization: `None`, `"upper"`, `"lower"`, or `"title"` |
| `remove_punctuation` | `bool` | `False` | Remove punctuation characters from string columns |
| `columns` | `Optional[List[str]]` | `None` | Apply transformations only to these columns (None = all string columns) |
| `group_name` | `Optional[str]` | `None` | Dagster asset group name |

## Example YAML

```yaml
type: dagster_component_templates.DataCleansingComponent
attributes:
  asset_name: cleansed_customers
  upstream_asset_key: raw_customers
  null_handling: fill
  null_fill_value: "unknown"
  trim_whitespace: true
  normalize_case: lower
  remove_punctuation: false
  columns:
    - first_name
    - last_name
    - email
    - city
  group_name: data_quality
```

## Null Handling Modes

- **`keep`** — Leave null values unchanged (default).
- **`drop`** — Drop any row that contains at least one null value.
- **`fill`** — Replace all null values with `null_fill_value`.

## Case Normalization

When `normalize_case` is set, the specified transformation is applied to every targeted string column after whitespace trimming:

- `"upper"` — Convert to ALL CAPS.
- `"lower"` — Convert to all lowercase.
- `"title"` — Capitalize the first letter of each word.

## Punctuation Removal

When `remove_punctuation: true`, all characters in Python's `string.punctuation` set are removed from each targeted string column. This includes: `!"#$%&'()*+,-./:;<=>?@[\]^_`{|}~`.

## Column Targeting

By default, all pandas `object`-dtype columns are targeted. Use `columns` to restrict processing to a named subset. Columns listed in `columns` that are not present in the DataFrame are skipped with a warning.

## Notes

- Null handling is applied before string transformations, so `fillna` values are also subject to trimming and case normalization.
- The component operates on a copy of the upstream DataFrame — the original asset is never mutated.
