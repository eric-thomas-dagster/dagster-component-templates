# DynamicRenameComponent

Pattern-based column renaming. Drop-in for Alteryx's **Dynamic Rename** tool.

## Modes (`mode:` field)

| Mode | What it does | Required extra fields |
|---|---|---|
| `first_row` | Promotes the first data row to column names (and drops it from output). Use when reading a CSV/Excel whose true header is row 2+ | — |
| `add_prefix` | Prefix every (selected) column name with `prefix` | `prefix:` |
| `add_suffix` | Suffix every (selected) column name with `suffix` | `suffix:` |
| `replace` | Regex substitution on column names | `pattern:` (regex), `replacement:` (defaults to "") |
| `mapping` | Explicit `{old: new}` rename map | `mapping:` |
| `mapping_from_column` | Read rename pairs from a secondary lookup asset's columns | `mapping_asset_key:`, `mapping_key_column:`, `mapping_value_column:` |

## Use Cases

- Promote first data row to header (Alteryx default "FirstRow" mode)
- Bulk prefix/suffix every column for namespace separation (`raw_x`, `raw_y`...)
- Strip a known prefix from auto-generated column names (regex)
- Apply a rename map sourced from a separate dimension table

## Example — first row as header

```yaml
type: dagster_community_components.DynamicRenameComponent
attributes:
  asset_name: properly_headed_table
  upstream_asset_key: csv_no_header
  mode: first_row
```

## Example — strip a prefix via regex

```yaml
type: dagster_community_components.DynamicRenameComponent
attributes:
  asset_name: trimmed_cols
  upstream_asset_key: raw_data
  mode: replace
  pattern: "^Right_"
  replacement: ""
```

## Example — rename via lookup table

```yaml
type: dagster_community_components.DynamicRenameComponent
attributes:
  asset_name: bi_friendly_cols
  upstream_asset_key: raw_data
  mode: mapping_from_column
  mapping_asset_key: column_dictionary
  mapping_key_column: source_name
  mapping_value_column: display_name
```

## Inputs and Outputs

| | Type | Description |
|-|------|-------------|
| Input (upstream) | `pd.DataFrame` | The table to rename columns on |
| Input (mapping, optional) | `pd.DataFrame` | Only required for `mode=mapping_from_column` |
| Output | `pd.DataFrame` | Same data, renamed columns (and one fewer row in `first_row` mode) |

## Notes

- `columns: [a, b, c]` (optional) restricts the rename to the listed
  columns. Default is to operate on all columns.
- For `first_row` mode: empty / NaN values in the first row leave the
  original column name unchanged.
- For `mapping` and `mapping_from_column`: pairs whose OLD name isn't
  present in the upstream are silently skipped.
