# Warehouse Dedup (pushdown)

Deduplicate rows in the warehouse via CTAS. No data through Python.

Two strategies:
- `subset:` set → `ROW_NUMBER() OVER (PARTITION BY subset ORDER BY order_by) = 1`. Keeps one row per `subset` key; the tie-breaker is explicit (`order_by` + `descending`). Use to dedup by primary-key-ish columns while picking the "best" duplicate (e.g., the most recent).
- `subset:` unset → `SELECT DISTINCT *`. Drops fully-duplicate rows.

## YAML

```yaml
type: dagster_component_templates.WarehouseDedupComponent
attributes:
  asset_name: customers_unique
  database_url: snowflake://...
  dialect: snowflake
  upstream_table: raw.customers
  output_table: analytics.customers_unique
  subset: [customer_id]
  order_by: [updated_at]
  descending: true        # keep the latest row per customer_id
  mode: replace
  deps: [raw_customers_in_warehouse]
```

## Generated SQL (subset variant)

```sql
CREATE OR REPLACE TABLE analytics.customers_unique AS
SELECT * FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY "customer_id" ORDER BY "updated_at" DESC) AS "_dedup_rn"
  FROM "raw"."customers"
) AS _t
WHERE "_dedup_rn" = 1
```
