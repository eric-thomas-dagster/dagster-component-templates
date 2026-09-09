# EventLogToBigQueryJobComponent

Op-shaped job that streams new Dagster event log entries into a
BigQuery table via `insert_rows_json`. Ideal for BI on Dagster
reliability (Looker/Tableau on top of BigQuery).

## Target table DDL

Create the table once, manually (or via your usual DDL flow):

```sql
CREATE TABLE `<project>.<dataset>.dagster_events` (
  storage_id INT64,
  timestamp FLOAT64,
  timestamp_ts TIMESTAMP,
  run_id STRING,
  job_name STRING,
  event_type STRING,
  step_key STRING,
  asset_key STRING,
  message STRING
);
```

Consider partitioning by `DATE(timestamp_ts)` for larger deployments.

## YAML example

```yaml
type: dagster_component_templates.EventLogToBigQueryJobComponent
attributes:
  job_name: dagster_events_to_bigquery
  schedule: "*/15 * * * *"
  default_status: RUNNING
  gcp_project: acme-analytics
  dataset: dagster_observability
  table: dagster_events
```

## Behavior

- Cursor via run tag (last `storage_id`); first run pulls `initial_lookback_hours` of history.
- Streams via BigQuery `insert_rows_json` (streaming buffer). Errors from BQ fail the run.

## Auth

- Uses `google-cloud-bigquery`'s standard Application Default Credentials chain.
- In Kubernetes / GKE: use Workload Identity.
- Locally: `gcloud auth application-default login`.
