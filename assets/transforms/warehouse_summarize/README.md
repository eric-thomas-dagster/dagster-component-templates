# Warehouse Summarize (pushdown)

Group + aggregate inside the warehouse via a `CREATE TABLE AS SELECT` (CTAS). No data is ever read into Python — the warehouse engine (Snowflake / BigQuery / DuckDB / Postgres / Redshift / Databricks / etc.) does the work.

This is the warehouse-native counterpart to the in-Python `summarize` component. Use it when the upstream table is large and you want to avoid the round-trip through pandas/polars.

## When to use

- Upstream table is millions+ rows
- Warehouse engine is your performant compute (Snowflake / BigQuery / DuckDB / etc.)
- Downstream consumers will also read from the same warehouse
- You want lineage in Dagster but materialization in the warehouse

For smaller frames or chains that include Python transforms, use the regular `summarize` (now with optional polars backend).

## YAML

```yaml
type: dagster_component_templates.WarehouseSummarizeComponent
attributes:
  asset_name: revenue_by_region
  database_url: snowflake://user:pass@account/db/schema?warehouse=COMPUTE_WH
  dialect: snowflake
  upstream_table: raw.orders
  output_table: analytics.revenue_by_region
  group_by: [region, product_category]
  aggregations:
    revenue:     {col: total, agg: sum}
    order_count: {col: order_id, agg: count}
    avg_order:   {col: total, agg: mean}
  mode: replace
  deps: [raw_orders_in_warehouse]
```

## Required fields

- `asset_name` — Dagster asset name (the output table this asset produces)
- `database_url` OR `database_url_env_var` — SQLAlchemy URL for the warehouse
- `dialect` — one of `duckdb`, `postgres`, `snowflake`, `bigquery`, `redshift`, `databricks`, `mssql`, `mysql`
- `upstream_table` — source table name (e.g. `raw.orders`)
- `output_table` — destination table name (e.g. `analytics.revenue_by_region`)
- `group_by` — list of columns
- `aggregations` — same shape as `summarize`: `{out_col: agg_func}` or `{out_col: {col: <src>, agg: <func>}}`

## Supported aggregations

`sum` / `mean` (or `avg`) / `min` / `max` / `count` / `nunique` (or `n_unique`).

Dialect-specific aggregates (`median`, `std`, percentile functions) aren't included in the first cut — they translate differently across warehouses and need a dialect-aware mapping.

## How CTAS translates per dialect

- **CREATE OR REPLACE TABLE** — duckdb, snowflake, bigquery, databricks
- **DROP IF EXISTS + CREATE TABLE AS** — postgres, redshift, mssql, mysql (since they lack `CREATE OR REPLACE`)

`mode: create_if_not_exists` skips the drop and only creates if missing.

## Composition

Compose multiple `warehouse_*` pushdown components by chaining `output_table` → `upstream_table`. Use `deps:` to make the Dagster lineage match the SQL lineage:

```yaml
# Step 1: warehouse_summarize
output_table: analytics.revenue_by_region
asset_name: revenue_by_region

# Step 2: warehouse_top_n_per_group reads from the same table
upstream_table: analytics.revenue_by_region
deps: [revenue_by_region]
```

## What this component returns

`MaterializeResult` with metadata:
- `dagster/row_count` — `SELECT COUNT(*)` on the output table
- `warehouse/output_table` — the table name
- `warehouse/dialect` — the dialect
- `warehouse/sql` — the actual CTAS executed (for reproducibility)
- `preview` (optional, set `include_preview_metadata: true`) — `SELECT … LIMIT N` from the output

Note: this asset doesn't return a DataFrame. Downstream `warehouse_*` components compose by table name; downstream Python components need a sibling reader (e.g. `sql_to_database_asset` or a custom asset) to pull the data into pandas/polars.
