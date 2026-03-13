# Rank Component

Rank records by a column value, with optional group-level ranking and support for percentile normalization. Adds a rank column to a DataFrame using pandas `rank()`.

This component uses Dagster's IO manager to pass DataFrames between assets. No IO manager configuration is required for local development — Dagster's default FilesystemIOManager handles serialization automatically.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Output Dagster asset name |
| `upstream_asset_key` | `str` | required | Upstream asset key providing a DataFrame |
| `column` | `str` | required | Column to rank by |
| `method` | `str` | `"average"` | Tie-breaking method (see below) |
| `ascending` | `bool` | `True` | Rank in ascending order (`True`) or descending order (`False`) |
| `group_by` | `Optional[List[str]]` | `None` | Rank within these groups independently |
| `output_column` | `Optional[str]` | `None` | Name for rank column (defaults to `{column}_rank`) |
| `normalize` | `bool` | `False` | Output percentile rank (0–1) instead of integer rank |
| `group_name` | `Optional[str]` | `None` | Dagster asset group name |

## Tie-Breaking Methods

| Method | Description |
|---|---|
| `average` | Average rank of tied values |
| `min` | Lowest rank in the tied group |
| `max` | Highest rank in the tied group |
| `first` | Rank in order of first appearance |
| `dense` | Like `min` but no gaps between ranks |

## Example YAML

```yaml
type: dagster_component_templates.RankComponent
attributes:
  asset_name: products_ranked_by_revenue
  upstream_asset_key: product_sales
  column: total_revenue
  method: dense
  ascending: false
  group_by:
    - category
  output_column: revenue_rank
  normalize: false
  group_name: analytics
```

## Group-Level Ranking

When `group_by` is specified, ranks are computed independently within each group using `groupby(...).rank()`. Rank 1 in each group corresponds to the highest (or lowest, depending on `ascending`) value within that group.

## Percentile Rank

When `normalize: true`, the output is a float in the range `[0, 1]` representing the fraction of values in the column (or group) that the current value is greater than or equal to.

## Notes

- The rank column is always added to the DataFrame; the source column is preserved.
- For descending rank (e.g. rank 1 = highest revenue), set `ascending: false`.
- The default output column name is `{column}_rank` unless `output_column` is specified.
