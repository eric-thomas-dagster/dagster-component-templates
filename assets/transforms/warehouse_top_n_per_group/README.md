# Warehouse Top-N Per Group (pushdown)

Top-N-per-group via `ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)` rendered into a CTAS. The warehouse engine ranks + prunes — no data through Python.

Sibling of [`top_n_per_group`](../top_n_per_group). Same semantics, executed in the warehouse instead of pandas/polars.

## YAML

```yaml
type: dagster_component_templates.WarehouseTopNPerGroupComponent
attributes:
  asset_name: top_3_products_per_category
  database_url: snowflake://...
  dialect: snowflake
  upstream_table: raw.products
  output_table: analytics.top_3_products_per_category
  group_by: [category]
  sort_by: revenue
  n: 3
  ascending: false      # default — top N (descending)
  rank_column: rank     # optional output rank column
  mode: replace
  deps: [raw_products_in_warehouse]
```

## Generated SQL

```sql
CREATE OR REPLACE TABLE analytics.top_3_products_per_category AS
SELECT * FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY "category" ORDER BY "revenue" DESC) AS "rank"
  FROM "raw"."products"
) AS _t
WHERE "rank" <= 3
```
