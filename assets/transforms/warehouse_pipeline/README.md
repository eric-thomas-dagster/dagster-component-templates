# Warehouse Pipeline (YAML → single CTE-CTAS)

Multi-step warehouse-native pipeline compiled to ONE SQL statement using WITH-clauses (CTE chain) and run as a single CTAS. The whole sequence is planned together by the warehouse engine — predicate pushdown across steps, projection pruning, join reordering — and writes the final result to the output table.

Think of it as a YAML-defined view-building or stored-procedure-shape — analogous to `polars_pipeline` for in-Python work, but the optimizer is the warehouse's instead of polars's.

## When to use vs the per-step family

| Use this when | Use the per-step `warehouse_*` family when |
|---|---|
| ONE asset for the whole chain is the right lineage granularity | You want one Dagster asset per transform step |
| Intermediates don't need to be inspectable | You want intermediate tables you can query / debug |
| You want the warehouse optimizer to plan the whole chain together | The intermediates are reused by other assets |

## YAML

```yaml
type: dagster_component_templates.WarehousePipelineComponent
attributes:
  asset_name: top_5_categories_by_revenue
  database_url: snowflake://...
  dialect: snowflake
  source:
    upstream_table: raw.orders
  operations:
    - op: filter
      predicate: "status = 'paid'"
    - op: group_by
      group_by: [category]
      aggregations:
        revenue:     {col: amount,   agg: sum}
        order_count: {col: order_id, agg: count}
    - op: top_n
      sort_by: revenue
      n: 5
      ascending: false
  output_table: analytics.top_5_categories_by_revenue
  mode: replace
  deps: [raw_orders_in_warehouse]
```

## Supported ops

| op | Notes |
|---|---|
| `filter` | `predicate: <SQL>` |
| `select` | `columns: [a, b]` |
| `drop` | `columns: [a, b]` — needs `SELECT * EXCEPT()` (DuckDB / BigQuery / Snowflake / Databricks) |
| `with_columns` | `expressions: {out_col: <SQL expr>}` |
| `group_by` | `group_by: [cols]`, `aggregations: {out: {col, agg}}` |
| `sort` | `by: [cols]`, `descending: bool/list` |
| `limit` | `n: int` |
| `top_n` | Global top N: `sort_by`, `n`, `ascending` |
| `top_n_per_group` | Top N per partition: `group_by`, `sort_by`, `n`, `ascending` |
| `dedup` | `subset` + optional `order_by` + `descending` |
| `distinct` | `SELECT DISTINCT *` |
| `union` | `other: <table_name>`, `distinct: bool`, optional `select_cols` |
| `join` | `right: <table>`, `how:`, `on_columns:` or `left_on`/`right_on`, optional `select_cols` |

`agg` values: `sum / mean / avg / min / max / count / nunique / median / stddev / variance`.

## Generated SQL

For the example above:

```sql
CREATE OR REPLACE TABLE analytics.top_5_categories_by_revenue AS
WITH "_step_1" AS (
    SELECT * FROM "raw"."orders" WHERE status = 'paid'
  ),
  "_step_2" AS (
    SELECT "category", SUM("amount") AS "revenue", COUNT("order_id") AS "order_count"
    FROM "_step_1" GROUP BY "category"
  ),
  "_step_3" AS (
    SELECT * FROM "_step_2" ORDER BY "revenue" DESC LIMIT 5
  )
SELECT * FROM "_step_3"
```

## Limitations

- `drop` and `top_n_per_group` (which uses `SELECT * EXCEPT()` internally) require dialects that support that syntax: DuckDB / BigQuery / Snowflake / Databricks. On others, use explicit `select` projections instead.
- `rename` is intentionally not supported — use `select` with `AS` aliases instead (e.g. `columns: ['order_id AS id', 'total']`).
- `source:` currently supports only `upstream_table: <name>`. Inline SQL / CTE-from-SQL coming.

## Sibling components

- Per-step lineage in the warehouse: `warehouse_filter`, `warehouse_summarize`, `warehouse_top_n_per_group`, `warehouse_dedup`, `warehouse_join`, `warehouse_union`, `warehouse_formula`, `warehouse_multi_field_formula`, `warehouse_multi_row_formula`
- Single-asset chains in other engines: `polars_pipeline`, `pyspark_pipeline`, `snowpark_pipeline`
