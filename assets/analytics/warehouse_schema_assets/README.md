# Warehouse Schema Assets

Auto-discovers tables (and optionally views) in one or more database schemas and creates one **external** Dagster `AssetSpec` per table. Column names and types are fetched during introspection and surfaced in the Dagster UI as a full column schema, with no extra work required.

---

## What it does

1. At prepare time (`write_state_to_path`), the component connects to your database, queries `information_schema` via SQLAlchemy, and writes a JSON snapshot of every table (plus its columns) to disk.
2. On every subsequent code-server reload, `build_defs_from_state` reads that snapshot and constructs `AssetSpec` objects — **no database connection required**.
3. Each asset carries metadata: `warehouse/schema`, `warehouse/table`, `warehouse/column_count`, and a full `TableSchema` so the Dagster UI renders column names and types inline.

---

## Why StateBackedComponent?

Warehouse introspection is expensive. A large Snowflake or BigQuery instance may have thousands of tables across dozens of schemas. Querying `information_schema` on every code-server reload would make the Dagster UI painfully slow and consume unnecessary database credits.

`StateBackedComponent` separates the expensive work (introspection) from the fast work (building defs):

| Operation | When it runs | DB connection? |
|---|---|---|
| `write_state_to_path` | Once, on `dg utils refresh-defs-state` or `dagster dev` startup | Yes |
| `build_defs_from_state` | Every code-server reload | No — reads local JSON cache |

The cached snapshot is a plain JSON file checked into your repo (or stored in a configurable location). Refreshing is explicit and intentional.

---

## Lineage story

Because every discovered table becomes a named Dagster asset, other components can declare dependencies on them directly:

```yaml
# sql_to_database_asset component
type: dagster_component_templates.SqlToDatabaseAsset
attributes:
  deps:
    - raw/orders
    - raw/customers
```

The asset key `raw/orders` corresponds to the table `orders` in schema `raw`, discovered by this component. Dagster renders the full upstream/downstream lineage graph automatically.

---

## Required packages

Add to your project's dependencies:

```
sqlalchemy>=2.0.0
```

You also need a **database-specific driver**. Install the one that matches your warehouse:

| Database | Driver package |
|---|---|
| PostgreSQL | `psycopg2-binary` |
| Snowflake | `snowflake-sqlalchemy` |
| BigQuery | `sqlalchemy-bigquery` |
| Redshift | `sqlalchemy-redshift` + `psycopg2-binary` |
| DuckDB | `duckdb-engine` |
| MySQL / MariaDB | `mysqlclient` or `PyMySQL` |
| Microsoft SQL Server | `pyodbc` |
| SQLite | built-in (no extra package needed) |

---

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `database_url_env_var` | `str` | required | Name of the environment variable containing the SQLAlchemy connection URL (e.g. `postgresql+psycopg2://user:pass@host/db`) |
| `schema_names` | `list[str]` | `["public"]` | List of database schemas to introspect |
| `exclude_tables` | `list[str] \| null` | `null` | Tables to skip by exact name (applied across all schemas) |
| `table_pattern` | `str \| null` | `null` | Glob pattern to filter tables (e.g. `stg_*` keeps only staging tables) |
| `include_views` | `bool` | `false` | Whether to include views in addition to base tables |
| `include_columns` | `bool` | `true` | Whether to fetch and cache column names/types |
| `key_prefix` | `str \| null` | `null` | Optional prefix prepended to every asset key (e.g. `"warehouse"` → `warehouse/raw/orders`) |
| `group_by_schema` | `bool` | `true` | When `true`, each schema becomes its own asset group in the Dagster UI |
| `group_name` | `str \| null` | `null` | Override group name for all assets; takes precedence over `group_by_schema` |
| `tag_with_schema` | `bool` | `false` | When `true`, adds a tag `{schema_name}: ""` to every asset in that schema |

---

## Asset key structure

Keys are built as a path of strings:

```
[key_prefix /] schema / table_name
```

Examples:

| `key_prefix` | `schema` | table | Resulting asset key |
|---|---|---|---|
| _(none)_ | `raw` | `orders` | `raw/orders` |
| `warehouse` | `raw` | `orders` | `warehouse/raw/orders` |
| _(none)_ | `public` | `users` | `public/users` |

---

## Example YAML configurations

### PostgreSQL

```yaml
type: dagster_component_templates.WarehouseSchemaAssetsComponent
attributes:
  database_url_env_var: POSTGRES_URL
  # POSTGRES_URL=postgresql+psycopg2://user:password@localhost:5432/mydb
  schema_names:
    - raw
    - staging
    - marts
  include_views: true
  include_columns: true
  group_by_schema: true
  tag_with_schema: false
```

### Snowflake

```yaml
type: dagster_component_templates.WarehouseSchemaAssetsComponent
attributes:
  database_url_env_var: SNOWFLAKE_URL
  # SNOWFLAKE_URL=snowflake://user:password@account/database?warehouse=COMPUTE_WH&role=TRANSFORMER
  schema_names:
    - RAW
    - STAGING
    - MARTS
  include_views: false
  include_columns: true
  key_prefix: snowflake
  group_by_schema: true
  tag_with_schema: true
```

### BigQuery

```yaml
type: dagster_component_templates.WarehouseSchemaAssetsComponent
attributes:
  database_url_env_var: BIGQUERY_URL
  # BIGQUERY_URL=bigquery://my-gcp-project/my_dataset
  schema_names:
    - raw
    - analytics
  include_views: true
  include_columns: true
  exclude_tables:
    - _tmp_migration_scratch
  group_by_schema: true
```

---

## Filtering tables

**By schema** — only introspect the schemas you care about:

```yaml
schema_names:
  - raw
  - staging
```

**Exclude specific tables** — skip known system or scratch tables:

```yaml
exclude_tables:
  - _fivetran_audit
  - _tmp_migration
  - sysdiagrams
```

**Glob pattern** — keep only tables matching a pattern:

```yaml
table_pattern: "stg_*"   # only tables whose names start with stg_
```

Patterns follow Python's `fnmatch` syntax: `*` matches any sequence of characters, `?` matches a single character.

**Include views** — off by default, opt in per component:

```yaml
include_views: true
```

---

## Refreshing the schema cache

The cached JSON snapshot must be regenerated whenever your warehouse schema changes (new tables added, columns changed, etc.).

**During development** — `dagster dev` automatically calls `write_state_to_path` on first startup if no cache exists.

**Explicit refresh** — run the Dagster utility command:

```bash
dg utils refresh-defs-state
```

This re-introspects the database and overwrites the cached JSON. Commit the updated snapshot to version control so your teammates and CI/CD pipeline get the refreshed schema without needing database access.

**In CI/CD** — add a refresh step before deploying:

```bash
export WAREHOUSE_URL="postgresql+psycopg2://..."
dg utils refresh-defs-state
git add .dagster/defs_state/
git commit -m "chore: refresh warehouse schema cache"
```

---

## Fallback behaviour

If `StateBackedComponent` is not available in your version of Dagster, the component automatically falls back to a simpler implementation that queries the database on every code-server reload. This is functionally equivalent but slower for large warehouses. Upgrade to a recent Dagster release to get the caching behaviour.
