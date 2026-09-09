# RunHistoryToWarehouseJobComponent

Op-shaped job that **MERGE**s Dagster run history (id, status, timings,
job, tags) into a warehouse table. Complements the event-log exports —
those are event-level; this is run-level (one row per run).

Perfect for the "we want Dagster reliability in Looker/Tableau"
question — run history is often enough to build most SLA and success
rate dashboards without needing the full event log.

## Supported warehouses

Toggle via the `warehouse` field:
- `snowflake` — via `snowflake-connector-python` (MERGE via temp table)
- `bigquery` — via `google-cloud-bigquery` `insert_rows_json` (append-only; dedup downstream)
- `postgres` — via `psycopg2` (`INSERT ... ON CONFLICT DO UPDATE`)

## Target table DDL

```sql
CREATE TABLE dagster_run_history (
  run_id STRING PRIMARY KEY,
  job_name STRING,
  status STRING,
  create_timestamp DOUBLE,
  start_time DOUBLE,
  end_time DOUBLE,
  duration_seconds DOUBLE,
  root_run_id STRING,
  parent_run_id STRING,
  mode STRING,
  tags_json STRING
);
```

(Adapt types per warehouse — `NUMBER` on Snowflake, `FLOAT64` on BQ, etc.)

## YAML example

```yaml
type: dagster_component_templates.RunHistoryToWarehouseJobComponent
attributes:
  job_name: dagster_run_history_to_snowflake
  schedule: "*/15 * * * *"
  default_status: RUNNING
  warehouse: snowflake
  snowflake_account: abc12345.us-east-1
  snowflake_user: DAGSTER_INGEST
  snowflake_password_env: SNOWFLAKE_PASSWORD
  snowflake_warehouse: LOAD_WH
  snowflake_database: ANALYTICS
  snowflake_schema: PLATFORM_OBSERVABILITY
  table: DAGSTER_RUN_HISTORY
```

## Behavior

- Cursor: last `updated_after` timestamp swept, stored as a run tag.
- Snowflake / Postgres do proper MERGE-by-run_id upserts.
- BigQuery does append-only inserts (BQ MERGE via streaming buffer is awkward). Downstream: dedup via a view on `ROW_NUMBER() OVER (PARTITION BY run_id ORDER BY end_time DESC)`.
