# Database Schema Inventory

List every object in a source SQL database — tables, views, procedures, functions, sequences, triggers, scheduled jobs — into a single DataFrame asset. The companion to `database_replication` for warehouse-migration projects.

## Why this component exists

`database_replication` moves the **data** (tables) from a legacy DB to the new warehouse. But a real migration also has to deal with:

- PL/SQL procedures / functions / packages (Oracle, Db2)
- T-SQL stored procedures (MSSQL)
- Database-resident scheduled jobs (Oracle DBMS_SCHEDULER, SQL Server Agent)
- Views (often hundreds of business-logic SQL views)
- Triggers
- Sequences

None of these auto-translate to Snowflake / BigQuery / Redshift / Databricks. The migration team has to inventory them and rewrite each one. **This component produces the inventory** — a DataFrame with one row per object that becomes the migration checklist.

## Supported source dialects

| Dialect | Tables | Views | Procs | Functions | Sequences | Triggers | Jobs / Tasks |
|---|:-:|:-:|:-:|:-:|:-:|:-:|:-:|
| `postgres` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | — |
| `mysql` | ✓ | ✓ | ✓ | ✓ | — | ✓ | — |
| `mssql` | ✓ | ✓ | ✓ | ✓ | — | ✓ | — |
| `oracle` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ (DBMS_SCHEDULER) |
| `db2` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | — |
| `snowflake` | ✓ | ✓ | ✓ | ✓ | ✓ | — | — |
| `redshift` | ✓ | ✓ | — | ✓ | — | — | — |

For Oracle the inventory also captures `package` objects.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Output Dagster asset name |
| `connection_env_var` | `str` | required | Env var with SQLAlchemy URL |
| `database_type` | `str` | required | postgres / mysql / mssql / oracle / db2 / snowflake / redshift |
| `schemas` | `list[str]` | `None` | Restrict to specific schemas (default: all non-system) |
| `object_types` | `list[str]` | `None` | Restrict to specific types (default: every type the dialect supports) |

## Output shape

DataFrame with columns:

| Column | Type | Notes |
|---|---|---|
| `object_type` | `str` | `table`, `view`, `procedure`, `function`, `package`, `sequence`, `trigger`, `job` |
| `schema_name` | `str` | |
| `object_name` | `str` | |
| `definition` | `str` (nullable) | SQL/PLSQL body where available |
| `row_count` | `int` (nullable) | For tables only; uses dialect's stats catalog (NUM_ROWS / CARD / ROW_COUNT) — may be stale |

## Example: Oracle inventory

```yaml
type: dagster_component_templates.DatabaseSchemaInventoryComponent
attributes:
  asset_name: oracle_migration_inventory
  connection_env_var: ORACLE_DB_URL
  database_type: oracle
  group_name: migration_planning
```

```bash
export ORACLE_DB_URL='oracle+oracledb://system:DagsterDemo1@localhost:1521/?service_name=FREEPDB1'
dg launch --assets oracle_migration_inventory
```

The asset's metadata shows a markdown summary by `object_type` and a preview of the first 50 rows — useful for quick sanity-checking in the Dagster UI before downstream consumption.

## Common pattern: pipe to CSV

The DataFrame is just a normal asset, so pipe it straight to `dataframe_to_csv` for a migration-plan artifact:

```yaml
# defs/inventory_csv/defs.yaml
type: dagster_component_templates.DataframeToCsvComponent
attributes:
  asset_name: oracle_migration_plan_csv
  upstream_asset_key: oracle_migration_inventory
  output_path: /artifacts/oracle_migration_plan.csv
```

## Common pattern: filter to "what's left after replication"

To list everything `database_replication` can't migrate automatically (i.e. everything that's NOT a table), use `filter`:

```yaml
type: dagster_component_templates.FilterComponent
attributes:
  asset_name: oracle_manual_migration_work
  upstream_asset_key: oracle_migration_inventory
  query: "object_type != 'table'"
```

This is the rebuild-by-hand checklist for the migration team.

## Demo notes

- **Row counts use stats catalogs.** Oracle `all_tables.num_rows`, Db2 `syscat.tables.card`, Snowflake `INFORMATION_SCHEMA.TABLES.ROW_COUNT`. These come from the DB's optimizer statistics, NOT from a live `COUNT(*)`. They can be stale — run `ANALYZE` (or equivalent) for accurate numbers, or leave `row_count` blank if your migration team can pull it on the target side later.
- **System schemas are excluded.** Each dialect's well-known system schemas are filtered out automatically.
- **`definition` is best-effort.** Some dialects don't expose proc/function body in `INFORMATION_SCHEMA` — those columns will be NULL. For DDL extraction, use the source DB's native dump tool (`pg_dump`, `expdp`, `db2look`, etc.).

## Requirements

```
dagster
pandas>=1.5.0
sqlalchemy>=2.0.0
```

Plus a dialect driver for the source database:

| Dialect | Driver |
|---|---|
| `postgres` | `psycopg2-binary>=2.9.0` |
| `mysql` | `pymysql>=1.0.0` |
| `mssql` | `pyodbc>=4.0.0` |
| `oracle` | `oracledb>=2.0.0` |
| `db2` | `ibm-db>=3.2.0` + `ibm-db-sa>=0.4.0` |
| `snowflake` | `snowflake-sqlalchemy>=1.5.0` |
| `redshift` | `redshift-connector>=2.0.0` + `sqlalchemy-redshift>=0.8.0` |
