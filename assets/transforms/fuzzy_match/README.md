# FuzzyMatch

Find and optionally deduplicate rows with similar string values using fuzzy matching powered by Python's `difflib.SequenceMatcher`. Supports three operating modes: deduplication, similarity scoring, and group clustering.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Output Dagster asset name |
| `upstream_asset_key` | `str` | required | Upstream asset key providing a DataFrame |
| `column` | `str` | required | Column to fuzzy match on |
| `threshold` | `float` | `0.8` | Similarity threshold between 0 and 1 |
| `mode` | `str` | `"deduplicate"` | Operation mode: `deduplicate`, `score`, or `group` |
| `output_column` | `str` | `"fuzzy_group"` | Column name for group IDs (mode `group` only) |
| `keep` | `str` | `"first"` | For mode `deduplicate`: which occurrence to keep — `first` or `last` |
| `group_name` | `Optional[str]` | `null` | Dagster asset group name |

## Modes

- **deduplicate**: Removes rows whose column value is similar (above `threshold`) to a previously seen value. Controlled by `keep` to determine which occurrence to retain.
- **score**: Adds a `fuzzy_score` column with each row's highest similarity score against any other row.
- **group**: Adds a group ID column (named by `output_column`) assigning the same integer ID to rows whose values are mutually similar above `threshold`.

## YAML Example

```yaml
type: dagster_component_templates.FuzzyMatch
attributes:
  asset_name: deduplicated_companies
  upstream_asset_key: raw_company_names
  column: company_name
  threshold: 0.85
  mode: deduplicate
  output_column: fuzzy_group
  keep: first
  group_name: transforms
```

## Performance Note

This component uses an O(n²) comparison approach via `difflib.SequenceMatcher`. For very large DataFrames (tens of thousands of rows or more), consider pre-filtering or using a dedicated fuzzy matching library such as `rapidfuzz`.

## IO Manager Note

This component uses Dagster's IO manager to pass DataFrames between assets. No IO manager configuration is required for local development — Dagster's default FilesystemIOManager handles serialization automatically.
