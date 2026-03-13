# FilterComponent

Filter rows from a DataFrame using a pandas query expression. Supports both
inclusive filtering (keep matching rows) and exclusive filtering (keep rows
that do NOT match).

## Use Cases

- Remove inactive or deleted records before downstream processing
- Segment customers by age, geography, or status
- Remove outliers using numeric range conditions
- Invert a filter to isolate the rejected set for audit or debugging

## Fields

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `asset_name` | `str` | Yes | — | Output Dagster asset name |
| `upstream_asset_key` | `str` | Yes | — | Upstream asset key providing a DataFrame |
| `condition` | `str` | Yes | — | Pandas query expression e.g. `"age > 18 and country == 'US'"` |
| `negate` | `bool` | No | `False` | If True, keep rows that do NOT match the condition |
| `group_name` | `str` | No | `None` | Dagster asset group name |

## Condition Syntax

The `condition` field uses [pandas `.query()` syntax](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.query.html):

- Comparisons: `age > 18`, `status == 'active'`, `score != 0`
- Boolean operators: `and`, `or`, `not`
- Column names with spaces: use backticks `` `column name` > 5 ``
- String membership: `country in ['US', 'CA', 'MX']`

## Example YAML

```yaml
type: dagster_component_templates.FilterComponent
attributes:
  asset_name: active_adult_customers
  upstream_asset_key: raw_customers
  condition: "age > 18 and status == 'active' and country == 'US'"
  negate: false
  group_name: customer_segments
```

### Negate Example

```yaml
type: dagster_component_templates.FilterComponent
attributes:
  asset_name: rejected_customers
  upstream_asset_key: raw_customers
  condition: "age > 18 and status == 'active'"
  negate: true
  group_name: audit
```

## Inputs and Outputs

| | Type | Description |
|-|------|-------------|
| Input | `pd.DataFrame` | The upstream DataFrame to filter |
| Output | `pd.DataFrame` | Filtered DataFrame with reset integer index |

## Logging

The component logs: `"Kept N/M rows (X%)"` after each run.

## IO Manager Note

This component returns a plain `pd.DataFrame`. Ensure your Dagster environment
has an IO manager configured that can handle DataFrames (e.g.,
`FilesystemIOManager` with pickle, or a Parquet/database IO manager).

## Notes

- The index is always reset (`reset_index(drop=True)`) on the output.
- Columns referenced in `condition` must exist in the upstream DataFrame.
- Use `negate=True` to produce the complement of a filter without duplicating logic.
