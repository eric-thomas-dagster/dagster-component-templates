# Warehouse Join (pushdown)

Join two tables in the warehouse via CTAS. The warehouse engine plans + executes the join; the result lands as a new table. No data through Python.

For N-way joins, chain multiple `warehouse_join` assets — each output_table becomes the next left_table.

## YAML

```yaml
type: dagster_component_templates.WarehouseJoinComponent
attributes:
  asset_name: orders_with_customers
  database_url: snowflake://...
  dialect: snowflake
  left_table: raw.orders
  right_table: raw.customers
  output_table: analytics.orders_with_customers
  how: left                    # inner | left | right | outer/full | cross
  on_columns: [customer_id]    # OR use left_on + right_on. NB: not `on:` — YAML 1.1 parses bare `on` as boolean
  select_cols:                 # optional projection — use _l. / _r. to disambiguate
    - _l.order_id
    - _l.amount
    - _l.status
    - _r.customer_name
    - _r.region
  mode: replace
  deps: [raw_orders_in_warehouse, raw_customers_in_warehouse]
```

## Aliases

Left table is aliased as `_l`; right table as `_r`. Use these in `select_cols:` to disambiguate columns with the same name on both sides.

## When names differ

```yaml
left_on:  [customer_id]
right_on: [id]               # right.id maps to left.customer_id
```

## Generated SQL

```sql
CREATE OR REPLACE TABLE analytics.orders_with_customers AS
SELECT _l.order_id, _l.amount, _l.status, _r.customer_name, _r.region
FROM "raw"."orders" AS _l
LEFT JOIN "raw"."customers" AS _r
ON _l."customer_id" = _r."customer_id"
```
