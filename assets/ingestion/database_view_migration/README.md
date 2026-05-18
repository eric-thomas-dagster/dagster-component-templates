# Database View Migration

Replicate a single SQL view from a source database to a target. Reads the view DDL from the source's `INFORMATION_SCHEMA` / `ALL_VIEWS` / `SYSCAT.VIEWS`, applies table-reference + function-name substitutions (so `app.orders` → `raw.orders`, `NVL` → `COALESCE`), then executes `CREATE OR REPLACE VIEW` on the target.

## When this works — and when it doesn't

Works cleanly for plain SELECT-aggregate-join views. The 80% case.

Fails loudly (which is the *right* failure mode) on dialect-specific syntax that requires manual rewrite:

- **Oracle:** `(+)` outer joins, `CONNECT BY`, `ROWNUM`, `DUAL` references
- **SQL Server:** `TOP N`, `CROSS APPLY`, `OUTER APPLY`
- **Db2:** legacy syntax that's not ANSI-SQL

When the `CREATE VIEW` errors at migration time, the migration team's checklist gets exactly the view names that need hand-rewriting — concrete instead of a wishful estimate. (For bulk migration with status reporting, use `database_views_migration` instead.)

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Dagster asset name for the migrated view |
| `source_connection_env_var` | `str` | required | Env var with source SQLAlchemy URL |
| `target_connection_env_var` | `str` | required | Env var with target SQLAlchemy URL |
| `source_type` | `str` | required | postgres / mysql / mssql / oracle / db2 / snowflake / redshift / duckdb |
| `target_type` | `str` | required | (same options) |
| `source_view` | `str` | required | `schema.name` or just `name` |
| `target_view` | `str` | required | `schema.name` or just `name` |
| `table_replacements` | `dict[str, str]` | `None` | Substitutions in the DDL (e.g. `{'app.orders': 'raw.orders'}`) |
| `function_replacements` | `dict[str, str]` | `None` | Function-name substitutions (case-insensitive whole-word) |
| `deps` | `list[str]` | `None` | Typically the asset keys of the replicated tables this view depends on |

## Example: Oracle → Snowflake view migration

```yaml
type: dagster_component_templates.DatabaseViewMigrationComponent
attributes:
  asset_name: v_quarterly_revenue_in_warehouse
  source_connection_env_var: ORACLE_URL
  target_connection_env_var: SNOWFLAKE_URL
  source_type: oracle
  target_type: snowflake
  source_view: FINANCE.V_QUARTERLY_REVENUE
  target_view: ANALYTICS.V_QUARTERLY_REVENUE

  # Map source tables to their migrated target locations
  table_replacements:
    FINANCE.ORDERS: RAW.ORDERS
    FINANCE.LINE_ITEMS: RAW.LINE_ITEMS

  # Dialect translations the team has identified
  function_replacements:
    NVL: COALESCE
    SYSDATE: CURRENT_TIMESTAMP
    DECODE: IFF       # very rough — review!

  deps: [orders_in_warehouse, line_items_in_warehouse]
  group_name: migration_views
```

## Output metadata

The asset surfaces source/target DDL side-by-side in its metadata panel — useful for reviewing exactly what was substituted before the team signs off on a view.

## Companion

For migrating **all** views in a schema at once with a pass/fail status report, use `database_views_migration` — same engine, one-shot input.

## Requirements

```
dagster
sqlalchemy>=2.0.0
```

Plus the dialect drivers for both source and target — see [`database_replication`](https://dagster-component-ui.vercel.app/c/database_replication) for the driver matrix.
