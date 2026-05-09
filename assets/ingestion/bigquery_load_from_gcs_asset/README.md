# BigQuery Load from GCS

Native BQ load-job API. Data never round-trips through the Dagster executor — BQ pulls directly from GCS, so it's fast at any scale.

Supports parquet, CSV, JSONL, AVRO, ORC. Optional explicit schema, partitioning, and clustering on the destination.

```yaml
type: dagster_component_templates.BigQueryLoadFromGcsAssetComponent
attributes:
  asset_name: orders_landing
  credentials_path: ${GOOGLE_APPLICATION_CREDENTIALS}
  source_uris:
    - "gs://my-bucket/landing/orders/*.parquet"
  destination_table_id: my-project.raw.orders_landing
  format: parquet
  write_disposition: WRITE_TRUNCATE
  partition_field: order_date
  partition_type: DAY
  cluster_fields: [customer_id]
```

Provide an explicit `table_schema: [{name, type, mode?}, ...]` and `autodetect: false` for production-strength loads where you want to lock down types and column ordering.

Required SA roles: `roles/bigquery.dataEditor` (destination dataset) + `roles/bigquery.jobUser` (project) + `roles/storage.objectViewer` (source bucket).

For DataFrame-mediated alternatives (slower but vendor-agnostic), see `gcs_to_database_asset` / `s3_to_database_asset` / `adls_to_database_asset`.
