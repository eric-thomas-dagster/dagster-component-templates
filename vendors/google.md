# Google Cloud (GCP)

The GCP community surface is **~32 components** spanning BigQuery (read + write + ML + Vector Search + freshness checks), GCS, Pub/Sub, Vertex AI, and audit-log ingestion. BigQuery has the deepest coverage by component count + the most `live`-validated paths.

## Official Dagster integration (`dagster-gcp`)

For BigQuery resource + IO manager + Dataproc + GCS resource, **prefer the official `dagster-gcp` package**. It's the supported path with first-class auth via `google-auth`.

The community components below complement that:
- BigQuery-specific Dagster-asset wrappers (CTAS, dry-run check, freshness check, ML train + predict, vector-search asset, GCS load/export)
- GCS monitor + cleanup + observation sensors
- Pub/Sub trigger + monitor
- Vertex AI text-embeddings asset

## Components — by sub-area

### BigQuery — execution + writes

| Component | Category | What it does | Validation |
|---|---|---|---|
| [`bigquery_resource`](https://dagster-component-ui.vercel.app/c/bigquery_resource) | resource | BigQuery client (`google-cloud-bigquery`) | `code` |
| [`bigquery_io_manager`](https://dagster-component-ui.vercel.app/c/bigquery_io_manager) | io_manager | Pandas ↔ BigQuery via the storage-write API | `code` |
| [`bigquery_query_asset`](https://dagster-component-ui.vercel.app/c/bigquery_query_asset) | source | Run a SQL query → Pandas | `live` |
| [`bigquery_create_table_from_query_asset`](https://dagster-component-ui.vercel.app/c/bigquery_create_table_from_query_asset) | transformation | CTAS from a templated SQL string | `live` |
| [`bigquery_load_from_gcs_asset`](https://dagster-component-ui.vercel.app/c/bigquery_load_from_gcs_asset) | ingestion | `LOAD DATA FROM URIS('gs://...')` | `live` |
| [`bigquery_export_to_gcs_asset`](https://dagster-component-ui.vercel.app/c/bigquery_export_to_gcs_asset) | sink | `EXPORT DATA OPTIONS(...)` | `live` |
| [`dataframe_to_bigquery`](https://dagster-component-ui.vercel.app/c/dataframe_to_bigquery) | sink | Pandas → BigQuery via `client.load_table_from_dataframe` | `live` |
| [`external_bigquery_table`](https://dagster-component-ui.vercel.app/c/external_bigquery_table) | external | Declare-only BigQuery table | `live` |

### BigQuery — checks + observation

| Component | Category | Validation |
|---|---|---|
| [`bigquery_dry_run_check`](https://dagster-component-ui.vercel.app/c/bigquery_dry_run_check) | asset check (cost-aware: dry-runs a query without executing) | `live` |
| [`bigquery_table_freshness_check`](https://dagster-component-ui.vercel.app/c/bigquery_table_freshness_check) | asset check | `live` |
| [`bigquery_table_observation_sensor`](https://dagster-component-ui.vercel.app/c/bigquery_table_observation_sensor) | observation | `code` |

### BigQuery — ML + Vector Search

| Component | Category | Validation |
|---|---|---|
| [`bigquery_ml_train_asset`](https://dagster-component-ui.vercel.app/c/bigquery_ml_train_asset) | ai (`CREATE MODEL ...`) | `live` |
| [`bigquery_ml_predict_asset`](https://dagster-component-ui.vercel.app/c/bigquery_ml_predict_asset) | ai (`ML.PREDICT`) | `live` |
| [`bigquery_vector_search_asset`](https://dagster-component-ui.vercel.app/c/bigquery_vector_search_asset) | source (BigQuery vector search) | `live` |

### GCS

| Component | Category | Validation |
|---|---|---|
| [`gcs_resource`](https://dagster-component-ui.vercel.app/c/gcs_resource) | resource | `code` |
| [`gcs_parquet_io_manager`](https://dagster-component-ui.vercel.app/c/gcs_parquet_io_manager) | io_manager | `code` |
| [`gcs_monitor`](https://dagster-component-ui.vercel.app/c/gcs_monitor) | sensor (watch prefix for new objects) | `live` |
| [`gcs_observation_sensor`](https://dagster-component-ui.vercel.app/c/gcs_observation_sensor) | observation | `code` |
| [`gcs_cleanup_job`](https://dagster-component-ui.vercel.app/c/gcs_cleanup_job) | jobs | `code` |
| [`gcs_to_database_asset`](https://dagster-component-ui.vercel.app/c/gcs_to_database_asset) | ingestion | `live` |
| [`external_gcs_asset`](https://dagster-component-ui.vercel.app/c/external_gcs_asset) | external | `live` |

### Pub/Sub

| Component | Category | Validation |
|---|---|---|
| [`google_pubsub`](https://dagster-component-ui.vercel.app/c/google_pubsub) | integration | `code` |
| [`pubsub_monitor`](https://dagster-component-ui.vercel.app/c/pubsub_monitor) | sensor | `code` |
| [`pubsub_observation_sensor`](https://dagster-component-ui.vercel.app/c/pubsub_observation_sensor) | observation | `code` |
| [`pubsub_publish_asset`](https://dagster-component-ui.vercel.app/c/pubsub_publish_asset) | sink | `live` |
| [`pubsub_to_database_asset`](https://dagster-component-ui.vercel.app/c/pubsub_to_database_asset) | ingestion | `code` |
| [`external_pubsub_asset`](https://dagster-component-ui.vercel.app/c/external_pubsub_asset) | external | `live` |

### Vertex AI

| Component | Category | Validation |
|---|---|---|
| [`google_vertex_ai`](https://dagster-component-ui.vercel.app/c/google_vertex_ai) | integration | `code` |
| [`vertex_ai_text_embeddings_asset`](https://dagster-component-ui.vercel.app/c/vertex_ai_text_embeddings_asset) | ai | `live` |

### Cloud Deployment Manager

| Component | Category | Validation |
|---|---|---|
| [`gcp_deployment_manager_asset`](https://dagster-component-ui.vercel.app/c/gcp_deployment_manager_asset) | infrastructure | `live` |

### Audit ingestion (governance / SIEM)

| Component | Category | Validation |
|---|---|---|
| [`gcp_audit_log_ingestion`](https://dagster-component-ui.vercel.app/c/gcp_audit_log_ingestion) | ingestion | `code` |

## Walkthroughs

| Topic | Walkthrough |
|---|---|
| BigQuery query + freshness + dry-run + ML | Covered as patterns in `examples/external_assets.md` + `examples/data_quality_checks.md` |
| External-asset declarations (BigQuery, GCS, Pub/Sub) | [`examples/external_assets.md`](https://github.com/eric-thomas-dagster/dagster-community-components-cli/blob/main/examples/external_assets.md) |

## Connection / auth — quick reference

All GCP components use the `google-auth` Application Default Credentials chain:

1. `GOOGLE_APPLICATION_CREDENTIALS` env var pointing at a JSON key
2. `gcloud auth application-default login` (dev laptops)
3. Workload Identity (GKE), service account on GCE / Cloud Run, etc.

Components typically take a `project_id:` (required) and optionally `location:` (defaults to `US` for BigQuery; varies for other services).

## Gotchas

- **`dataframe_to_bigquery` typed-column path** — uses `client.load_table_from_dataframe` which pyarrow-converts pandas dtypes. Datetime / int / float / bool / string all round-trip correctly to BQ TIMESTAMP / INT64 / FLOAT64 / BOOL / STRING. No `use_logical_type` flag needed (unlike Snowflake's write_pandas).
- **BigQuery slot quota** — long-running queries from `bigquery_query_asset` can hit the per-project slot ceiling. Use `bigquery_dry_run_check` to estimate cost before running.
- **GCS monitor + dynamic partitions** — `gcs_monitor` emits one dynamic partition per new object key. Combined with `bigquery_load_from_gcs_asset`, you get a one-asset-run-per-file ingestion pattern.
- **Vertex AI region-pinning** — Vertex models are region-scoped. If your Dagster process is in `us-central1` and the model is in `europe-west4`, calls error with `404 NotFound`. Set `location:` explicitly.

## Roadmap

- **Cloud Functions trigger asset** — pair with Pub/Sub for event-driven processing.
- **Cloud Composer (managed Airflow) integration** — useful for migrations to Dagster (read DAGs, register them as external assets).
- **Vertex AI training pipeline trigger** — currently `google_vertex_ai` exists; a dedicated training-job-as-asset component is cleaner for orchestration.

## See also

- Official `dagster-gcp` integration — preferred for the core resource + IO-manager + Dataproc paths
- [BigQuery REST docs](https://cloud.google.com/bigquery/docs/reference/rest)
- [`examples/external_assets.md`](https://github.com/eric-thomas-dagster/dagster-community-components-cli/blob/main/examples/external_assets.md)
