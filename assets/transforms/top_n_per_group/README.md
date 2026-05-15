# TopNPerGroupComponent

Keep the top N rows per group, sorted by a value column. Equivalent to SQL's `ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ... DESC) <= N`, or pandas's `df.sort_values().groupby().head(N)`.

## Use cases

- Top 3 products per category by revenue
- 5 most recent orders per customer
- 10 fastest queries per database
- Bottom-N variants: 3 slowest queries, 5 lowest-scoring leads

## Fields

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `asset_name` | `str` | Yes | — | Output asset name |
| `upstream_asset_key` | `str` | Yes | — | Upstream DataFrame asset |
| `group_by` | `List[str]` | Yes | — | Group columns (`["category"]`, `["customer_id", "region"]`) |
| `sort_by` | `str` | Yes | — | Column used to rank within each group |
| `n` | `int` | No | `3` | How many rows to keep per group |
| `ascending` | `bool` | No | `false` | `false` = top N. `true` = bottom N |
| `rank_column` | `str` | No | — | If set, adds a 1..N rank column with this name |
| `sort_method` | `str` | No | `"first"` | Tie-break rule: `first` / `dense` / `min` / `max` / `average` |

## Examples

Top 3 products per category by revenue:

```yaml
type: dagster_component_templates.TopNPerGroupComponent
attributes:
  asset_name: top_products
  upstream_asset_key: product_sales
  group_by: [category]
  sort_by: revenue
  n: 3
  rank_column: rank
```

5 most recent orders per customer:

```yaml
type: dagster_component_templates.TopNPerGroupComponent
attributes:
  asset_name: recent_orders_per_customer
  upstream_asset_key: orders
  group_by: [customer_id]
  sort_by: order_date
  n: 5
```

Bottom 10 (slowest) queries per host:

```yaml
type: dagster_component_templates.TopNPerGroupComponent
attributes:
  asset_name: slowest_queries
  upstream_asset_key: query_log
  group_by: [host]
  sort_by: duration_ms
  n: 10
  ascending: true
```

## When to use this vs alternatives

- `rank` + `filter` — works but verbose: rank adds a column, then you filter on it. `top_n_per_group` is the single-step primitive for this common case.
- `window_calculation` — when you need ROW_NUMBER side-by-side with other window functions (lag/lead/sum).

## See also

- `rank`, `window_calculation`, `summarize`, `sort`, `filter`
