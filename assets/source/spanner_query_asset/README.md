# Spanner Query Asset

Run a SQL query against a [Cloud Spanner](https://cloud.google.com/spanner) database and return rows as a pandas DataFrame.

```yaml
type: dagster_component_templates.SpannerQueryAssetComponent
attributes:
  asset_name: active_orders
  instance_id: my-instance
  database_id: app-prod
  sql: |
    SELECT order_id, customer_id, total_cents
    FROM Orders WHERE status = @status
  params: { status: ACTIVE }
  param_types: { status: STRING }
```

## When to pick Spanner vs. peers

| You need | Use |
|---|---|
| **Transactional row reads at globally-distributed scale** | Spanner |
| Analytical scans / star-schema joins | BigQuery (`bigquery_query_asset`) |
| Document/JSON CRUD | Firestore (`firestore_reader_asset`) |
| Wide-column NoSQL at petabyte scale | Bigtable |

## Parameters

Named params are referenced as `@name` in SQL. When using non-string types (timestamps, integers, dates), provide `param_types` — Spanner is stricter than BigQuery about inference.

## Auth

Service account needs `roles/spanner.databaseReader` on the database (or `roles/spanner.databaseUser` for read+write).

## Note

This runs a **single-use read transaction** (a Spanner snapshot). For consistent multi-statement reads or read-write, build a custom asset that uses a session pool — this component is intended for single-query reads.
