# Database Constraints Migration

Apply primary keys, foreign keys, NOT NULL, and DEFAULTs to target tables that already exist — the data-first complement to `database_tables_migration`.

## When to use this vs. `database_tables_migration`

**Data-first workflow (this component):**

1. `database_replication` with `mode: full_refresh` — Sling creates target tables from type inference and loads data
2. **`database_constraints_migration`** — reads constraints from source, applies them to the now-existing target tables via `ALTER TABLE`
3. `database_views_migration` — recreate views

Simpler setup; data goes in regardless of constraint violations; failures during constraint application surface in this step's status DataFrame.

**DDL-first workflow (`database_tables_migration`):**

1. `database_tables_migration` — creates target tables with full DDL (types + PKs + FKs + NOT NULL + defaults)
2. `database_replication` with `mode: append` — streams data into pre-shaped tables
3. `database_views_migration` — recreate views

More disciplined; constraint violations fail at insert time; no separate constraints step needed.

Pick the one that matches your tolerance for re-running. Both produce status DataFrames you can pipe to CSV.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Dagster asset name |
| `source_connection_env_var` | `str` | required | Env var with source SQLAlchemy URL |
| `target_connection_env_var` | `str` | required | Env var with target SQLAlchemy URL |
| `source_type` / `target_type` | `str` | required | Source / target dialect |
| `schemas` | `list[str]` | all non-system | Source schemas |
| `table_names` | `list[str]` | all | Whitelist |
| `skip_table_names` | `list[str]` | none | Blacklist |
| `target_schema` | `str` | source schema | Override target schema |
| `table_replacements` | `dict[str, str]` | `{}` | FK reference rewrites |
| `constraint_types` | `list[str]` | all four | Subset of `primary_key`, `foreign_key`, `not_null`, `default` |
| `fail_on_any_error` | `bool` | `false` | Asset fails if any constraint fails. Default: report and succeed. |

## Status DataFrame shape

```
| schema_name | table_name | target_table  | constraint_type | constraint_name | columns       | status  | error_message              | ddl                         |
|-------------|------------|---------------|-----------------|-----------------|---------------|---------|----------------------------|-----------------------------|
| app         | customers  | raw.customers | primary_key     | customers_pkey  | customer_id   | success |                            | ALTER TABLE … ADD PRIMARY…  |
| app         | orders     | raw.orders    | primary_key     | orders_pkey     | order_id      | success |                            | ALTER TABLE … ADD PRIMARY…  |
| app         | orders     | raw.orders    | foreign_key     | orders_cust_fk  | customer_id   | failed  | violates fk: orphan rows   | ALTER TABLE … ADD FOREIGN…  |
| app         | orders     | raw.orders    | not_null        | status          | status        | success |                            | ALTER TABLE … SET NOT NULL  |
```

The "failed" row is informative — it tells you exactly which orphan rows are blocking the FK so you can clean them up and re-run.

## Example

```yaml
type: dagster_component_templates.DatabaseConstraintsMigrationComponent
attributes:
  asset_name: warehouse_constraints_applied
  source_connection_env_var: SOURCE_DB_URL
  target_connection_env_var: TARGET_DB_URL
  source_type: postgres
  target_type: duckdb
  schemas: [app]
  target_schema: raw
  table_replacements:
    app.orders: raw.orders
    app.customers: raw.customers
  deps: [customers_in_warehouse, orders_in_warehouse]
```

Pipe the report to CSV:

```yaml
type: dagster_component_templates.DataframeToCsvComponent
attributes:
  asset_name: constraints_migration_report
  upstream_asset_key: warehouse_constraints_applied
  file_path: /tmp/constraints_migration_report.csv
```

## Requirements

```
dagster
pandas>=1.5.0
sqlalchemy>=2.0.0
tabulate>=0.10.0
```

## Notes

- **FK application order is constraint-by-constraint.** If table A has an FK to table B and B's PK hasn't been applied yet, A's FK might fail on dialects that require referenced columns to be `PRIMARY KEY` or `UNIQUE`. Workaround: run twice — the second pass picks up FKs that failed because of PK ordering.
- **`NOT NULL` is dialect-aware.** Most targets support `ALTER COLUMN ... SET NOT NULL`. MSSQL doesn't — its status row shows `not supported standalone` with the workaround (re-declare the column with its type).
- **DEFAULTs are filtered.** `nextval('seq')` and dialect-specific defaults are skipped (you'd need separate sequence migration). `now()` / `SYSDATE` / `GETDATE()` normalize to `CURRENT_TIMESTAMP`.
- **DuckDB only supports CHECK + UNIQUE at CREATE TABLE time** (no `ALTER TABLE ADD CHECK/UNIQUE`). If your target is DuckDB and you want CHECK/UNIQUE, use `database_tables_migration` (DDL-first) instead — applying them inline. The constraints_migration component will log clear failures for these on DuckDB.

## Logs

Every constraint application emits a log line — success at `INFO`, failure at `WARNING`. Examples:

```
INFO    Applied: primary_key orders_pkey on raw.orders
INFO    Applied: foreign_key orders_customer_fk on raw.orders
INFO    Applied: not_null on raw.customers.name
WARNING Failed: check orders_amount_positive on raw.orders: NotSupportedError: …
```

Plus a summary at the end: `Constraints migration: 8/10 succeeded, 2 failed`. Combine with the status DataFrame for the full picture.
