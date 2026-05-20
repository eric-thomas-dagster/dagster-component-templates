# Warehouse Union (pushdown)

Stack N tables vertically via CTAS UNION ALL (or UNION DISTINCT). The warehouse engine concatenates; no data through Python.

Sibling of [`dataframe_union`](../dataframe_union) (pandas/polars).

## YAML

```yaml
type: dagster_component_templates.WarehouseUnionComponent
attributes:
  asset_name: orders_all_regions
  database_url: snowflake://...
  dialect: snowflake
  upstream_tables:
    - raw.orders_us
    - raw.orders_eu
    - raw.orders_apac
  output_table: analytics.orders_all_regions
  distinct: false           # false = UNION ALL (keep dupes); true = UNION (drop dupes)
  select_cols:              # optional — project a common subset across input schemas
    - order_id
    - customer_id
    - amount
    - region
  mode: replace
  deps: [orders_us_in_warehouse, orders_eu_in_warehouse, orders_apac_in_warehouse]
```

## Schema compatibility

All `upstream_tables` must agree on column count + types (same as SQL UNION). When schemas partially overlap, set `select_cols:` to a common projection so each branch produces the same shape.

## Generated SQL

```sql
CREATE OR REPLACE TABLE analytics.orders_all_regions AS
  SELECT order_id, customer_id, amount, region FROM "raw"."orders_us"
  UNION ALL
  SELECT order_id, customer_id, amount, region FROM "raw"."orders_eu"
  UNION ALL
  SELECT order_id, customer_id, amount, region FROM "raw"."orders_apac"
```
