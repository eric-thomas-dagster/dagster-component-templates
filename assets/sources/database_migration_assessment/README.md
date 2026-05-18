# Database Migration Assessment

Pre-flight dry-run for a warehouse migration. Reads source DDL for every table + view, attempts the target migration inside a transaction that **rolls back every change**, and returns a single consolidated DataFrame:

```
| object_type | schema_name | name              | target_name      | status            | complexity | dialect_markers          | reason                  | proposed_target_ddl |
|-------------|-------------|-------------------|------------------|-------------------|------------|--------------------------|-------------------------|---------------------|
| table       | HR          | EMPLOYEES         | RAW.EMPLOYEES    | auto_convertible  | simple     |                          |                         | CREATE TABLE …       |
| table       | HR          | EMPLOYEE_ARCHIVE  | RAW.EMP_ARCHIVE  | needs_review      | medium     | NVL,DECODE               |                         | CREATE TABLE …       |
| table       | HR          | XML_DOCUMENTS     | RAW.XML_DOCS     | will_fail         | complex    | XMLTYPE                  | unsupported column type |                       |
| view        | HR          | V_DEPT_HIERARCHY  | RAW.V_DEPT_…     | will_fail         | complex    | CONNECT BY, START WITH   | syntax error near…      | CREATE OR REPLACE …  |
```

**Use this BEFORE the real migration** to scope the project, estimate effort, and surface every issue the team needs to address. Inspired by AWS SCT's Assessment Report and Microsoft SSMA's test mode.

## Status classification

| Status | Meaning |
|---|---|
| `auto_convertible` | Dry-run succeeded, no dialect-specific markers detected. Will migrate cleanly. |
| `needs_review` | Dry-run succeeded BUT source DDL uses dialect-specific markers (`NVL`, `SYSDATE`, `DECODE`, etc.) the team should sanity-check on target. Auto-applied function_replacements may have handled it. |
| `will_fail` | Dry-run failed. Reason column has the specific error. Either provide `table_ddl_overrides` / `view_ddl_overrides`, rewrite the source DDL, or skip the object via `exclude_patterns`. |

## Complexity heuristic

| Complexity | Criteria | Rough effort |
|---|---|---|
| `simple` | Auto-convertible, no markers, no substitutions needed | ~0 mins (just migrate) |
| `medium` | Auto-convertible but substitutions were applied OR target gives informational-only enforcement | ~30 mins per (review + verify) |
| `complex` | Would fail OR has dialect-specific markers that need manual review | ~2 hrs per (rewrite + verify) |

The metadata exposes `estimated_manual_effort` as a rough person-hours number for the `complex` + `medium` rows. Use it as a discussion-starter for project scoping, not a hard estimate.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Dagster asset name |
| `source_connection_env_var` | `str` | required | Env var with source SQLAlchemy URL |
| `target_connection_env_var` | `str` | required | Env var with target SQLAlchemy URL (used for dry-run only; nothing committed) |
| `source_type` / `target_type` | `str` | required | postgres / mysql / mssql / oracle / db2 / snowflake / redshift / duckdb |
| `schemas` | `list[str]` | all non-system | Source schemas to assess |
| `include_patterns` | `list[str]` | all | fnmatch globs against `schema.name` |
| `exclude_patterns` | `list[str]` | none | fnmatch globs to drop |
| `target_schema` | `str` | source schema | Target schema for the proposed names |
| `table_replacements` | `dict[str, str]` | `{}` | FK / view-body rewrites (use the SAME map you'll use in the real migration) |
| `function_replacements` | `dict[str, str]` | `{}` | Function-name substitutions for CHECK / view bodies |
| `assess_tables` | `bool` | `true` | Include CREATE TABLE assessment |
| `assess_views` | `bool` | `true` | Include CREATE VIEW assessment |

## Asset metadata

The assessment surfaces these in the materialization metadata (visible in Dagster UI):

- `auto_convertible`, `needs_review`, `will_fail` — counts
- `auto_convertible_pct` — float percentage
- `complexity_simple`, `complexity_medium`, `complexity_complex` — counts
- `estimated_manual_effort` — rough person-hours string (e.g. "~16h" or "~2.5 person-days")
- `report` — markdown table of all rows (visible inline in the UI)

## Example: Oracle → Snowflake assessment

```yaml
type: dagster_component_templates.DatabaseMigrationAssessmentComponent
attributes:
  asset_name: oracle_migration_assessment
  source_connection_env_var: ORACLE_DB_URL
  target_connection_env_var: SNOWFLAKE_DB_URL
  source_type: oracle
  target_type: snowflake
  schemas: [HR, FINANCE]
  target_schema: RAW
  table_replacements:
    HR.EMPLOYEES: RAW.EMPLOYEES
    FINANCE.ORDERS: RAW.ORDERS
  function_replacements:
    NVL: COALESCE
    SYSDATE: CURRENT_TIMESTAMP
  group_name: migration_planning
```

Pipe the report to CSV for the team:

```yaml
type: dagster_component_templates.DataframeToCsvComponent
attributes:
  asset_name: migration_assessment_csv
  upstream_asset_key: oracle_migration_assessment
  file_path: /artifacts/oracle_to_snowflake_assessment.csv
```

## How dry-run works

For each table / view, the component:

1. Reads source DDL via `INFORMATION_SCHEMA` / `ALL_*` / `SYSCAT.*`
2. Builds a portable target-dialect statement (`CREATE TABLE` / `CREATE OR REPLACE VIEW`) using the same translation logic as `database_tables_migration` and `database_views_migration`
3. Opens a target connection, **begins a transaction**, executes the statement, **rolls back** the transaction → no state change on target

**Limitations:**

- **Oracle / MSSQL auto-commit DDL.** Some dialects (notably Oracle) auto-commit DDL inside transactions. For these targets, the rollback won't fully undo a successful CREATE — the table exists until you DROP it. Document and re-run with a clean target if you go this route. Postgres / DuckDB / Snowflake / BigQuery handle transactional DDL correctly.
- **FK targets must already exist for `auto_convertible` status.** Dry-run validates each table independently against the current target state. If table A has an FK to table B and B hasn't been migrated yet, A's dry-run will mark as `will_fail`. This is honest behavior — in the real run, the migration component creates tables in order. Treat `will_fail` rows with FK errors as "expected during dry-run" and trust the real run.

## Companion components

After the assessment, the real migration uses the same source/target/replacements config:

- [`database_tables_migration`](https://dagster-component-ui.vercel.app/c/database_tables_migration) — DDL-first execution
- [`database_views_migration`](https://dagster-component-ui.vercel.app/c/database_views_migration) — view recreation
- [`database_replication`](https://dagster-component-ui.vercel.app/c/database_replication) — data movement
- [`database_constraints_migration`](https://dagster-component-ui.vercel.app/c/database_constraints_migration) — data-first constraint application

The migration components also accept `dry_run: true` if you want to re-validate just one step before committing.

## Requirements

```
dagster
pandas>=1.5.0
sqlalchemy>=2.0.0
tabulate>=0.10.0
```

Plus dialect drivers for source + target.
