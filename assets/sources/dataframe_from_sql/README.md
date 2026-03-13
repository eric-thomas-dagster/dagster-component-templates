# dataframe_from_sql

## Purpose

Executes an arbitrary SQL query against a relational database and returns the result set as a Pandas DataFrame asset. This is more flexible than `dataframe_from_table` — it supports joins, aggregations, CTEs, and any other SQL construct supported by your database engine.

## Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `asset_name` | `str` | required | Output Dagster asset name |
| `query` | `str` | required | SQL query to execute |
| `database_url_env_var` | `str` | `"DATABASE_URL"` | Environment variable containing the DB connection URL |
| `parse_dates` | `List[str]` | `None` | Columns to parse as dates |
| `deps` | `List[str]` | `None` | Upstream asset keys for lineage |
| `group_name` | `str` | `None` | Dagster asset group name |

## Example YAML

```yaml
type: dagster_component_templates.DataframeFromSqlComponent
attributes:
  asset_name: monthly_revenue
  query: |
    SELECT
      DATE_TRUNC('month', order_date) AS month,
      region,
      SUM(amount) AS total_revenue
    FROM orders
    WHERE status = 'completed'
    GROUP BY 1, 2
    ORDER BY 1, 2
  database_url_env_var: DATABASE_URL
  parse_dates:
    - month
  deps:
    - orders_ingested
  group_name: reporting
```

## Notes

### SQL Query

The `query` field accepts any SQL string supported by your database. YAML block scalars (`|`) are recommended for multi-line queries. The query is passed directly to `pd.read_sql`, which uses SQLAlchemy under the hood.

### Environment Variable

The `database_url_env_var` field specifies the name of an environment variable holding a SQLAlchemy-compatible connection URL (e.g., `postgresql://user:pass@host/db`).

### Dependency Lineage

Use `deps` to declare upstream Dagster assets that should complete before this query runs. This is useful when the query reads from a table that a preceding ingestion asset populates.

### IO Manager

This component uses Dagster's IO manager to pass DataFrames between assets. No IO manager configuration is required for local development — Dagster's default FilesystemIOManager handles serialization automatically.

### Performance Note

For large result sets, consider using `LIMIT` in your query during development or using `nrows` with a CSV component instead. SQLAlchemy streams results into memory as a DataFrame.
