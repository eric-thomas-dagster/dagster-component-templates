# Database Tables Migration

DDL-first warehouse migration. Reads source-side `CREATE TABLE` shape from `INFORMATION_SCHEMA` (columns + types + primary keys + foreign keys + NOT NULL + defaults), builds a portable target-dialect `CREATE TABLE` statement, executes it. **No data is moved** — this component only sets up the target schema.

## Two workflows

**Workflow A — DDL-first (the disciplined migration):**

1. `database_tables_migration` ← recreates tables with full constraints
2. `database_replication` with `mode: append` ← streams data into pre-shaped tables
3. `database_views_migration` ← rebuilds views

Data lands into tables that already enforce PKs / NOT NULLs / FKs. Bad rows fail at insert time and surface in Sling's error stream. Migration audit is one `CREATE TABLE` per table.

**Workflow B — data-first (Sling-driven, simpler):**

1. `database_replication` with `mode: full_refresh` ← creates target tables from type inference + loads data
2. `database_constraints_migration` ← applies PKs / FKs / NOT NULLs after data is loaded
3. `database_views_migration` ← rebuilds views

Easier to set up; data goes in regardless of constraint violations; constraint failures surface in step 2.

This component is workflow A. Use it standalone to preview what the target schema *will* look like, or wire `database_replication` (`mode: append`) behind it.

## What gets translated

| Source | Target | Notes |
|---|---|---|
| Columns + types | Portable mapping per target dialect | Oracle `VARCHAR2` → `VARCHAR`, `NUMBER(p,s)` → `DECIMAL(p,s)`, MSSQL `NVARCHAR` → `VARCHAR`, etc. |
| Primary keys | Inline `PRIMARY KEY (...)` | Same column order as source |
| Foreign keys | Inline `FOREIGN KEY (...) REFERENCES ...` | FK targets get `table_replacements` applied |
| NOT NULL | Inline `NOT NULL` | |
| DEFAULT values | Inline `DEFAULT ...` | `now()` / `SYSDATE` / `GETDATE()` normalized to `CURRENT_TIMESTAMP`; Postgres `nextval(...)` skipped (sequences moved separately) |
| UNIQUE constraints | Inline `UNIQUE (...)` | Non-PK uniqueness, same column order as source |
| CHECK constraints | Inline `CHECK (...)` | Built-in rewrites: Postgres `= ANY (ARRAY[...])` → `IN (...)`, `::type` casts stripped. Dialect-specific functions via `function_replacements`. CHECKs that fail to apply land in the status DataFrame for hand-rewrite as overrides. |

Not translated (yet): comments, column-level encoding hints, expression-based indexes.

## Target enforcement matrix

Not every warehouse enforces every constraint. The component runs pre-flight warnings so the migration team knows what they're getting:

| Target | PK | FK | NOT NULL | DEFAULT | CHECK | UNIQUE |
|---|---|---|---|---|---|---|
| DuckDB / Postgres / MySQL / Oracle / Db2 / MSSQL | ✓ enforced | ✓ enforced | ✓ enforced | ✓ | ✓ enforced | ✓ enforced |
| **Snowflake** | informational | informational | ✓ enforced | ✓ | informational | informational |
| **BigQuery** | informational | informational | ✓ | ✓ | **unsupported** | informational |
| **Redshift** | informational | informational | ✓ enforced | ✓ | informational | informational |

- **enforced**: bad rows blocked at INSERT
- **informational**: constraint accepted into the catalog, visible to BI tools / optimizer, but bad rows can still land
- **unsupported**: target rejects the DDL — failure shows up in the status DataFrame. Skip via `include_check_constraints: false` for BigQuery.

The pre-flight log surfaces a `⚠` for each non-enforced constraint at the start of the run so it's not a surprise.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Dagster asset name |
| `source_connection_env_var` | `str` | required | Env var with source SQLAlchemy URL |
| `target_connection_env_var` | `str` | required | Env var with target SQLAlchemy URL |
| `source_type` / `target_type` | `str` | required | postgres / mysql / mssql / oracle / db2 / snowflake / redshift / duckdb |
| `schemas` | `list[str]` | all non-system | Source schemas to scan |
| `table_names` | `list[str]` | all | Whitelist (qualified names) |
| `skip_table_names` | `list[str]` | none | Blacklist |
| `target_schema` | `str` | source schema | Override target schema (e.g. `raw`) |
| `table_replacements` | `dict[str, str]` | `{}` | FK reference rewrites (old qualified name → new) |
| `drop_if_exists` | `bool` | `false` | DROP TABLE before CREATE (useful for re-runs) |
| `include_primary_keys` | `bool` | `true` | Include PK in CREATE TABLE |
| `include_foreign_keys` | `bool` | `true` | Include FK in CREATE TABLE |
| `fail_on_any_error` | `bool` | `false` | Asset run fails if any table fails. Default: succeed and report failures in status DataFrame |

## Status DataFrame shape

```
| schema_name | table_name | target_table | status  | error_message | n_columns | has_primary_key | n_foreign_keys | ddl_preview     |
|-------------|------------|--------------|---------|---------------|-----------|-----------------|----------------|-----------------|
| app         | customers  | raw.customers| success |               | 6         | true            | 0              | CREATE TABLE …  |
| app         | orders     | raw.orders   | success |               | 7         | true            | 1              | CREATE TABLE …  |
| app         | broken_t   | raw.broken_t | failed  | syntax near…  | 3         | false           | 0              | …               |
```

Pipe to `dataframe_to_csv` for the migration completion artifact:

```yaml
type: dagster_component_templates.DataframeToCsvComponent
attributes:
  asset_name: ddl_migration_report
  upstream_asset_key: warehouse_ddl_ready
  file_path: /tmp/ddl_migration_report.csv
```

## Example: Postgres → DuckDB DDL-first

```yaml
type: dagster_component_templates.DatabaseTablesMigrationComponent
attributes:
  asset_name: warehouse_ddl_ready
  source_connection_env_var: SOURCE_DB_URL
  target_connection_env_var: TARGET_DB_URL
  source_type: postgres
  target_type: duckdb
  schemas: [app]
  target_schema: raw
  table_replacements:
    app.customers: raw.customers
    app.orders: raw.orders
  drop_if_exists: true
```

## Wired with `database_replication` (full DDL-first flow)

```yaml
# 1. Create the target tables with PKs + FKs + NOT NULL
type: dagster_component_templates.DatabaseTablesMigrationComponent
attributes:
  asset_name: warehouse_ddl_ready
  # …
```

```yaml
# 2. Stream data into pre-shaped tables — note: mode: append (NOT full_refresh,
#    which would DROP and recreate without your DDL)
type: dagster_component_templates.DatabaseReplicationComponent
attributes:
  asset_name: orders_in_warehouse
  source_table: app.orders
  target_table: raw.orders
  mode: append
  deps: [warehouse_ddl_ready]
  # …
```

## Notes

- **Run order matters.** If table A has an FK to table B, create B first. Currently the component creates tables in `INFORMATION_SCHEMA` order — usually alphabetical, which often works but not always. For complex FK graphs, re-run the asset (failed FK tables will succeed on the second pass).
- **Type fidelity vs. portability.** The translation map covers common cases. Exotic types (Oracle `INTERVAL DAY TO SECOND`, MSSQL `GEOGRAPHY`, Postgres `tsvector`) fall through verbatim and may fail on target — those land in the status DataFrame for hand-rewriting.
- **Sequences are skipped.** Postgres `nextval('seq')` defaults aren't translated (the sequence doesn't exist on target). Use `IDENTITY` columns on the target side, or recreate sequences separately and update the DEFAULT post-migration.

## Companion

For the **data-first** workflow, use `database_constraints_migration` instead (applies PKs/FKs to tables Sling already created from inference).

## Requirements

```
dagster
pandas>=1.5.0
sqlalchemy>=2.0.0
tabulate>=0.10.0
```

Plus dialect drivers — see [`database_replication`](https://dagster-component-ui.vercel.app/c/database_replication) for the matrix.
