# Unique Dedup Component

Remove duplicate records from a DataFrame with configurable control over which columns define uniqueness, which copy to keep, and how to return the results.

This component uses Dagster's IO manager to pass DataFrames between assets. No IO manager configuration is required for local development — Dagster's default FilesystemIOManager handles serialization automatically.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Output Dagster asset name |
| `upstream_asset_key` | `str` | required | Upstream asset key providing a DataFrame |
| `subset` | `Optional[List[str]]` | `None` | Columns to consider for duplication (None = all columns) |
| `keep` | `str` | `"first"` | Which copy to retain: `"first"`, `"last"`, or `"none"` (drop all) |
| `output_mode` | `str` | `"unique"` | Output mode (see below) |
| `flag_column` | `str` | `"is_duplicate"` | Column name for the duplicate flag when `output_mode="all"` |
| `group_name` | `Optional[str]` | `None` | Dagster asset group name |

## Output Modes

| Mode | Description |
|---|---|
| `unique` | Return only non-duplicate rows (standard deduplication) |
| `duplicates` | Return only the duplicate rows (useful for auditing) |
| `all` | Return all rows with a boolean column (`flag_column`) marking duplicates |

## Keep Values

| Value | Description |
|---|---|
| `"first"` | Keep the first occurrence of each duplicate group |
| `"last"` | Keep the last occurrence of each duplicate group |
| `"none"` | Drop all occurrences (no copy is retained) |

## Example YAML

```yaml
type: dagster_component_templates.UniqueDedupComponent
attributes:
  asset_name: unique_transactions
  upstream_asset_key: raw_transactions
  subset:
    - transaction_id
    - customer_id
  keep: first
  output_mode: unique
  flag_column: is_duplicate
  group_name: data_quality
```

## Auditing Duplicates

Use `output_mode: duplicates` to route duplicate rows to a separate asset for review:

```yaml
output_mode: duplicates
keep: false  # return both copies of each duplicate pair
```

## Notes

- When `keep="none"`, all rows that appear more than once (across the `subset` columns) are dropped — including the original.
- `output_mode="all"` preserves row count and is useful when the downstream system needs a flag rather than filtering.
- The component logs the row count before and after deduplication for observability.
