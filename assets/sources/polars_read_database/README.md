# Polars Read Database (SQL pushdown → polars)

Execute a SQL query against a database and get a polars DataFrame back. The database engine does the filtering / aggregation / joining (true compute pushdown), and polars receives the result set directly via connectorx — no pandas intermediate.

## Use when

- Source is a SQL database/warehouse
- You want the engine to do the work (WHERE clauses, aggregations, joins all run on the DB side)
- Output should land in polars for the next step

## YAML

```yaml
type: dagster_component_templates.PolarsReadDatabaseComponent
attributes:
  asset_name: paid_orders_q1
  connection_uri: postgresql://user:pass@host:5432/orders
  query: |
    SELECT order_id, customer_id, amount, status, created_at
    FROM raw.orders
    WHERE status = 'paid' AND created_at >= '2026-01-01'
  output_type: polars
```

## Supported databases

Via `connectorx` (default engine): PostgreSQL, MySQL, MariaDB, SQLite, MS SQL Server, Oracle, BigQuery, Redshift, ClickHouse, Trino. Use the standard SQLAlchemy-style URI.

Via `adbc` (set `engine: adbc`): PostgreSQL, SQLite, Snowflake, and others via Arrow Database Connectivity. Set when you want Arrow-native column transfer with no pandas/Python row-by-row conversion.

## Pushdown story

**The query IS the pushdown.** Polars's `read_database_uri` executes the SQL as-is — it doesn't synthesize WHERE clauses for you. Write the predicate, join, or aggregation INTO the query and the database engine plans + optimizes execution.

If you want polars-side optimization on top of the database read, follow this with a `polars_pipeline` step in the same Dagster asset (not as a separate downstream asset — the asset boundary breaks the lazy chain).

## Output

`output_type: polars` (default) for downstream polars chains; `output_type: pandas` to convert at the boundary.

## Connection

Either set `connection_uri:` literally in the YAML, or set `connection_uri_env_var:` to point at an env var holding the URI. The env-var form is preferred for production demos where the connection string contains credentials.
