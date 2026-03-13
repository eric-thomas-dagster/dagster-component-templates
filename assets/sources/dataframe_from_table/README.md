# dataframe_from_table

## Purpose

Reads a database table and outputs a Pandas DataFrame asset. This component acts as a bridge from DB-centric ingestion assets (e.g., those that write raw data to a relational database) into the DataFrame pipeline, where downstream transform components can process the data using the IO manager pattern.

## Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `asset_name` | `str` | required | Output Dagster asset name |
| `table_name` | `str` | required | Database table to read |
| `database_url_env_var` | `str` | `"DATABASE_URL"` | Environment variable containing the DB connection URL |
| `schema` | `str` | `None` | Database schema (e.g., `public`) |
| `columns` | `List[str]` | `None` | Columns to select; `None` selects all columns |
| `where_clause` | `str` | `None` | SQL WHERE filter (omit the `WHERE` keyword) |
| `deps` | `List[str]` | `None` | Upstream asset keys to declare as dependencies for lineage |
| `group_name` | `str` | `None` | Dagster asset group name |

## Example YAML

```yaml
type: dagster_component_templates.DataframeFromTableComponent
attributes:
  asset_name: customers_dataframe
  table_name: customers
  database_url_env_var: DATABASE_URL
  schema: public
  columns:
    - id
    - name
    - email
    - created_at
  where_clause: "created_at >= '2024-01-01'"
  deps:
    - raw_customers_ingested
  group_name: ingestion
```

## Notes

### Environment Variable

The `database_url_env_var` field specifies the name of an environment variable that holds the SQLAlchemy-compatible connection URL (e.g., `postgresql://user:pass@host/db`). This avoids hardcoding credentials in YAML.

### Column Filtering and WHERE Clauses

When `where_clause` is set, the component falls back to a `pd.read_sql` query rather than `pd.read_sql_table`. The WHERE clause is injected directly into the SQL — ensure it is safe for your environment.

### Dependency Lineage

Use `deps` to declare upstream Dagster assets whose completion should precede this read (e.g., an ingestion asset that populates the table). This wires up lineage in the Dagster asset graph without requiring DataFrame-level data passing.

### IO Manager

This component returns a `pd.DataFrame` and is compatible with any downstream transform component that uses `AssetIn` to receive the DataFrame. Dagster's default `FilesystemIOManager` handles serialization automatically for local development. No IO manager configuration is required for local development — Dagster's default FilesystemIOManager handles serialization automatically.

### Requirements

Install `sqlalchemy` along with the appropriate database driver (e.g., `psycopg2` for PostgreSQL, `pymysql` for MySQL).
