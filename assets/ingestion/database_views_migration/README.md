# Database Views Migration

Migrate every view in a source schema to a target in one asset. Emits a per-view status DataFrame (`schema_name`, `view_name`, `target_view`, `status`, `error_message`) so the team gets a concrete pass/fail report — exactly what's needed for the migration completion artifact.

For a single composable view migration, use [`database_view_migration`](https://dagster-component-ui.vercel.app/c/database_view_migration) instead.

## How it works

1. Lists every view in the source DB's `schemas` (excluding system schemas)
2. Applies any `view_names` whitelist and `skip_view_names` blacklist
3. For each remaining view:
   - Reads its DDL from source (`INFORMATION_SCHEMA.VIEWS` / `ALL_VIEWS` / `SYSCAT.VIEWS`)
   - Applies `table_replacements` (e.g. `app.orders` → `raw.orders`) and `function_replacements` (e.g. `NVL` → `COALESCE`)
   - Executes `CREATE OR REPLACE VIEW <target> AS <substituted_body>` on target
   - Records `success` / `failed` + error message
4. Returns a DataFrame of all view outcomes (the asset's value)

Pipe the asset to `dataframe_to_csv` for a migration completion report artifact.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Dagster asset name |
| `source_connection_env_var` | `str` | required | Env var with source SQLAlchemy URL |
| `target_connection_env_var` | `str` | required | Env var with target SQLAlchemy URL |
| `source_type` / `target_type` | `str` | required | postgres / mysql / mssql / oracle / db2 / snowflake / redshift / duckdb |
| `schemas` | `list[str]` | all non-system | Source schemas to scan |
| `view_names` | `list[str]` | all | Whitelist (qualified names) |
| `skip_view_names` | `list[str]` | none | Blacklist of known-broken views |
| `target_schema` | `str` | source schema | Override target schema (e.g. `raw` to land everything in `raw.<view>`) |
| `table_replacements` | `dict[str, str]` | `{}` | Substitutions applied to every view body |
| `function_replacements` | `dict[str, str]` | `{}` | Function-name substitutions (case-insensitive whole-word) |
| `fail_on_any_error` | `bool` | `false` | If true, the asset fails the run when any view fails. Default: report failures in the status DataFrame and succeed. |

## Status DataFrame shape

```
| schema_name | view_name        | target_view              | status  | error_message                | ddl_chars |
|-------------|------------------|--------------------------|---------|------------------------------|-----------|
| app         | v_orders_summary | raw.v_orders_summary     | success |                              | 142       |
| app         | v_oracle_specific| raw.v_oracle_specific    | failed  | syntax error near 'CONNECT'  | 0         |
| app         | v_customer_360   | raw.v_customer_360       | success |                              | 487       |
```

## Output metadata

- `views_succeeded` — count of successful migrations
- `views_failed` — count of failures
- `report` — markdown table of every view's outcome (visible in the Dagster UI)

## Example: migrate all views from Postgres `app` schema to DuckDB `raw`

```yaml
type: dagster_component_templates.DatabaseViewsMigrationComponent
attributes:
  asset_name: all_views_migrated
  source_connection_env_var: SOURCE_DB_URL
  target_connection_env_var: TARGET_DB_URL
  source_type: postgres
  target_type: duckdb
  schemas: [app]
  target_schema: raw
  table_replacements:
    app.orders: raw.orders
    app.customers: raw.customers
  deps: [orders_in_warehouse, customers_in_warehouse]
```

Then write the report to CSV:

```yaml
type: dagster_component_templates.DataframeToCsvComponent
attributes:
  asset_name: views_migration_report
  upstream_asset_key: all_views_migrated
  file_path: /tmp/views_migration_report.csv
```

The CSV is exactly the per-view checklist the migration team submits at the end.

## Requirements

```
dagster
pandas>=1.5.0
sqlalchemy>=2.0.0
tabulate>=0.10.0
```

Plus dialect drivers for source + target — see [`database_replication`](https://dagster-component-ui.vercel.app/c/database_replication) for the matrix.

## Notes

- **Dependencies between views aren't auto-resolved.** If view A references view B, this component might attempt A before B and A fails. Strategies: (1) re-run — the second run picks up B that now exists; (2) split into per-view `database_view_migration` assets with explicit `deps:`.
- **Dialect mismatch is the expected failure mode.** Oracle `CONNECT BY`, MSSQL `CROSS APPLY`, `(+)` outer joins — the failures end up in the status CSV with clear error messages. Rewrite those by hand and re-run.
