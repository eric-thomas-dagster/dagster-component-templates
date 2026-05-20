# Warehouse Filter (pushdown)

Predicate pushdown via CTAS. The warehouse engine evaluates the WHERE clause and writes only matching rows to the output table — no data through Python.

Sibling of [`filter`](../filter) (Python) and [`warehouse_summarize`](../warehouse_summarize) (CTAS group-by). Composes with the rest of the `warehouse_*` family by chaining `output_table` → `upstream_table` across steps.

## YAML

```yaml
type: dagster_component_templates.WarehouseFilterComponent
attributes:
  asset_name: paid_orders
  database_url: snowflake://user:pass@account/db/schema?warehouse=COMPUTE_WH
  dialect: snowflake
  upstream_table: raw.orders
  output_table: analytics.paid_orders
  predicate: "status = 'paid' AND amount > 100"
  mode: replace
  deps: [raw_orders_in_warehouse]
```

## Dialect handling

`CREATE OR REPLACE TABLE` on dialects that support it (DuckDB, Snowflake, BigQuery, Databricks); `DROP IF EXISTS` + `CREATE TABLE AS` on the others (Postgres, Redshift, MSSQL, MySQL). `mode: create_if_not_exists` skips the drop.
