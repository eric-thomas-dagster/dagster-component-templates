# BigQuery → GCS Export

Native BQ `EXTRACT` / `EXPORT DATA`. Data never round-trips through the Dagster executor — BQ pushes directly to GCS, so it's fast at any scale.

Two source modes:
- **`source_table_id`** — uses BQ `EXTRACT` job: free for the BQ side, native sharding for >1 GB tables.
- **`source_query`** — uses `EXPORT DATA OPTIONS(...) AS <query>`: charged like a normal query, more flexible (joins, projections, filters).

```yaml
type: dagster_component_templates.BigQueryExportToGcsAssetComponent
attributes:
  asset_name: orders_export
  credentials_path: ${GOOGLE_APPLICATION_CREDENTIALS}
  source_table_id: my-project.analytics.orders_clean
  destination_uri: "gs://my-bucket/exports/orders_*.parquet"
  format: parquet
  compression: snappy
```

Required SA roles: `roles/bigquery.dataViewer` (source) + `roles/bigquery.jobUser` (project) + `roles/storage.objectCreator` (destination bucket).

For DataFrame-mediated alternatives (slower but vendor-agnostic), see `dataframe_to_gcs` / `dataframe_to_s3` / `dataframe_to_adls`.
