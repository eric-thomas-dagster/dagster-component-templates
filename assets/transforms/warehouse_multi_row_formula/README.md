# Warehouse Multi-Row Formula (Alteryx equivalent)

Row-relative formulas — running totals, LAG/LEAD, ranks, FIRST/LAST values — compiled to SQL window functions and executed via CTAS. The Alteryx "Multi-Row Formula" tool equivalent, in-warehouse.

All expressions share the same `OVER (PARTITION BY ... ORDER BY ...)` clause set at the top level. Use this when N declared columns all want the SAME window.

## YAML

```yaml
type: dagster_component_templates.WarehouseMultiRowFormulaComponent
attributes:
  asset_name: orders_with_running
  database_url: snowflake://...
  dialect: snowflake
  upstream_table: raw.orders
  output_table: analytics.orders_with_running
  partition_by: [customer_id]
  order_by: [order_date]
  order_descending: [false]
  expressions:
    running_total:   {kind: running_total, col: amount}
    running_avg:     {kind: running_avg,   col: amount}
    prev_amount:     {kind: lag, col: amount, offset: 1, default: 0}
    order_rank:      {kind: row_number}
    pct_within_customer:
      kind: expression
      expression: "100.0 * amount / SUM(amount)"
      col: amount
  mode: replace
  deps: [raw_orders_in_warehouse]
```

## Supported window-function `kind:` values

| Kind | Required `col:` | SQL function | Notes |
|---|---|---|---|
| `running_total` / `running_sum` | yes | `SUM(col)` | Cumulative sum over the window |
| `running_avg` / `running_mean` | yes | `AVG(col)` | Cumulative mean |
| `lag` | yes | `LAG(col, offset, default)` | Previous-row value. `offset:` and `default:` optional |
| `lead` | yes | `LEAD(col, offset, default)` | Next-row value |
| `row_number` | no | `ROW_NUMBER()` | 1-based rank |
| `rank` | no | `RANK()` | Tied rows share rank, leaves gaps |
| `dense_rank` | no | `DENSE_RANK()` | Tied rows share rank, no gaps |
| `percent_rank` | no | `PERCENT_RANK()` | Percentile rank |
| `first_value` | yes | `FIRST_VALUE(col)` | First row in window |
| `last_value` | yes | `LAST_VALUE(col)` | Last row in window |
| `expression` | optional | raw | Inline SQL — `{col}` placeholder substituted if both `col:` and `{col}` present |

## Generated SQL

For the example above:

```sql
CREATE OR REPLACE TABLE analytics.orders_with_running AS
SELECT *,
  (SUM(amount) OVER (PARTITION BY customer_id ORDER BY order_date ASC)) AS running_total,
  (AVG(amount) OVER (PARTITION BY customer_id ORDER BY order_date ASC)) AS running_avg,
  (LAG(amount, 1, 0) OVER (PARTITION BY customer_id ORDER BY order_date ASC)) AS prev_amount,
  (ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date ASC)) AS order_rank,
  (100.0 * amount / SUM(amount) OVER (PARTITION BY customer_id ORDER BY order_date ASC)) AS pct_within_customer
FROM raw.orders
```

## Sibling components

- `warehouse_formula` — single-column inline expressions (window functions also work here if you write `OVER (...)` yourself)
- `warehouse_multi_field_formula` — same formula template applied to many columns
- `warehouse_top_n_per_group` — ROW_NUMBER pushdown with a dedicated DSL
- `warehouse_dedup` — ROW_NUMBER() = 1 with a dedicated DSL
