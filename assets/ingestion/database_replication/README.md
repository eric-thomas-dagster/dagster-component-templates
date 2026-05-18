# Database Replication

Move a table between two SQL databases without landing the rows in Python memory. Wraps [Sling](https://slingdata.io) under the hood via the official `dagster-sling` integration — you supply a small opinionated surface, the component builds the Sling replication spec and feeds it to `@sling_assets`.

## When to use this

| Scenario | Use |
|---|---|
| DataFrame already in memory, want to write to Snowflake | `dataframe_to_snowflake_bulk` |
| Source table fits in memory, transformations along the way | `sql_to_database_asset` |
| **Source table too big to fit in pandas / any-size SQL→SQL move** | **this component** |

Common cases:

- Oracle / Db2 → Snowflake warehouse migration
- Postgres operational DB → Redshift / BigQuery / Snowflake analytics warehouse
- MySQL → Databricks
- Snowflake → Snowflake (cross-account replication)

## Supported types

`postgres`, `mysql`, `mssql`, `oracle`, `db2`, `snowflake`, `bigquery`, `redshift`, `databricks`, `duckdb`, `clickhouse`, `mariadb`, `sqlite`

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Output Dagster asset name |
| `source_connection_env_var` | `str` | required | Env var with full Sling source URL |
| `target_connection_env_var` | `str` | required | Env var with full Sling target URL |
| `source_type` | `str` | required | Source database type (see list above) |
| `target_type` | `str` | required | Target database type |
| `source_table` | `str` | required | `'table'` or `'schema.table'` |
| `target_table` | `str` | required | `'table'` or `'schema.table'` |
| `mode` | `str` | `full_refresh` | `full_refresh` / `incremental` / `snapshot` / `truncate` |
| `incremental_column` | `str` | `None` | Watermark column (required for `mode: incremental`) |
| `primary_key` | `list[str]` | `None` | Primary key for upserts in incremental mode |
| `select_columns` | `list[str]` | `None` | Subset of columns (default: all) |
| `where_clause` | `str` | `None` | Optional WHERE filter applied to source |

## Connection URL formats

```bash
# Postgres
export PG_URL='postgres://user:pass@host:5432/db'

# MySQL
export MYSQL_URL='mysql://user:pass@host:3306/db'

# MSSQL
export MSSQL_URL='sqlserver://user:pass@host:1433/db'

# Oracle
export ORACLE_URL='oracle://user:pass@host:1521/?service_name=ORCL'

# Db2
export DB2_URL='db2://user:pass@host:50000/SAMPLE'

# Snowflake
export SNOWFLAKE_URL='snowflake://user:pass@account.snowflakecomputing.com/DB/SCHEMA?warehouse=COMPUTE_WH&role=TRANSFORMER'

# BigQuery (service account JSON path)
export BIGQUERY_URL='bigquery://project-id/dataset?keyfile=/path/to/sa.json'

# Redshift
export REDSHIFT_URL='redshift://user:pass@cluster.redshift.amazonaws.com:5439/db'

# Databricks
export DATABRICKS_URL='databricks://token:dapi...@host/?http_path=/sql/...'
```

## Example: Oracle → Snowflake full refresh

```yaml
type: dagster_component_templates.DatabaseReplicationComponent
attributes:
  asset_name: employees_replicated
  source_connection_env_var: ORACLE_URL
  source_type: oracle
  source_table: HR.EMPLOYEES
  target_connection_env_var: SNOWFLAKE_URL
  target_type: snowflake
  target_table: RAW.EMPLOYEES
  mode: full_refresh
```

## Example: Postgres → Snowflake incremental upsert

```yaml
type: dagster_component_templates.DatabaseReplicationComponent
attributes:
  asset_name: orders_replicated
  source_connection_env_var: PG_URL
  source_type: postgres
  source_table: public.orders
  target_connection_env_var: SNOWFLAKE_URL
  target_type: snowflake
  target_table: RAW.ORDERS
  mode: incremental
  incremental_column: updated_at
  primary_key: [order_id]
```

## Example: Db2 → Snowflake with column subset + filter

```yaml
type: dagster_component_templates.DatabaseReplicationComponent
attributes:
  asset_name: legacy_customers
  source_connection_env_var: DB2_URL
  source_type: db2
  source_table: SAMPLE.CUSTOMER
  target_connection_env_var: SNOWFLAKE_URL
  target_type: snowflake
  target_table: RAW.LEGACY_CUSTOMERS
  mode: full_refresh
  select_columns: [CUSTOMER_ID, NAME, EMAIL, COUNTRY, CREATED_AT]
  where_clause: "COUNTRY IN ('US', 'CA', 'UK')"
```

## Companion components

This is the **dedicated/opinionated** SQL replication component. Related:

- [`sling_replication_asset`](https://dagster-component-ui.vercel.app/c/sling_replication_asset) — power-user variant. Bring your own Sling `replication.yaml` with full Sling surface (multi-stream, hooks, custom config).
- [`database_tables_migration`](https://dagster-component-ui.vercel.app/c/database_tables_migration) — recreate target tables with full DDL (PKs, FKs, NOT NULLs) before data lands. Pairs with `mode: truncate` on this component.
- [`database_constraints_migration`](https://dagster-component-ui.vercel.app/c/database_constraints_migration) — data-first companion: apply PKs/FKs/NOT NULLs after Sling has loaded data with `mode: full_refresh`.
- [`database_views_migration`](https://dagster-component-ui.vercel.app/c/database_views_migration) — recreate source views on target.
- [`database_schema_inventory`](https://dagster-component-ui.vercel.app/c/database_schema_inventory) — discover what's in source before the migration.

For the full migration playbook (inventory → DDL → data → views), see the [warehouse_migration walkthrough](https://raw.githubusercontent.com/eric-thomas-dagster/dagster-community-components-cli/main/examples/warehouse_migration.md).

## Requirements

```
dagster
dagster-sling>=0.24.0
```

Sling itself is bundled as a binary inside the `dagster-sling` package — no separate install needed.

## Notes

- **No data lands in Python memory.** Sling streams rows directly source→target as parallel batches.
- **Modes** map to Sling's native modes: `full_refresh` → `full-refresh`, `incremental` → `incremental`, etc.
- **Connection drivers**: For Oracle/Db2 you may need additional system libraries — see Sling's documentation for the relevant native driver requirements.
- **Lineage**: The asset is generated by `@sling_assets` so it shows up in the Dagster UI with native Sling metadata (rows replicated, bytes, runtime).
