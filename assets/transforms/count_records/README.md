# CountRecordsComponent

Count rows in a DataFrame, either as a single total or grouped by one or more
columns. Optionally attaches null-count metadata to the Dagster asset.

## Use Cases

- Generate row-count summaries for data quality monitoring
- Count events or transactions by category for reporting dashboards
- Audit null counts across all columns and surface them in the Dagster UI
- Create aggregated count tables for downstream joins or alerts

## Fields

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `asset_name` | `str` | Yes | — | Output Dagster asset name |
| `upstream_asset_key` | `str` | Yes | — | Upstream asset key providing a DataFrame |
| `group_by` | `List[str]` | No | `None` | Group columns; `None` returns a single-row total |
| `count_column` | `str` | No | `"count"` | Name of the count column in the output |
| `include_null_counts` | `bool` | No | `False` | Attach per-column null counts as asset metadata |
| `group_name` | `str` | No | `None` | Dagster asset group name |

## Output Shape

| Scenario | Output |
|----------|--------|
| `group_by=None` | Single-row DataFrame: `[{"count": N}]` |
| `group_by=["col1", "col2"]` | One row per group with the group columns + count column |

## Example YAML

```yaml
type: dagster_component_templates.CountRecordsComponent
attributes:
  asset_name: orders_by_status
  upstream_asset_key: cleaned_orders
  group_by:
    - status
    - region
  count_column: order_count
  include_null_counts: true
  group_name: reporting
```

### Simple Count Example

```yaml
type: dagster_component_templates.CountRecordsComponent
attributes:
  asset_name: total_row_count
  upstream_asset_key: raw_events
  group_name: monitoring
```

## Inputs and Outputs

| | Type | Description |
|-|------|-------------|
| Input | `pd.DataFrame` | The upstream DataFrame |
| Output | `pd.DataFrame` | Count summary DataFrame |

## Metadata

When `include_null_counts=True`, null counts per column are attached as JSON
metadata on the Dagster asset and are visible in the Dagster UI asset detail view.

## IO Manager Note

This component returns a plain `pd.DataFrame`. Ensure your Dagster environment
has an IO manager configured that can handle DataFrames.

## Notes

- When `group_by` is set, groups with zero rows are not included (standard pandas `.groupby()` behavior).
- Null counts are computed on the original upstream DataFrame, not the output count table.
