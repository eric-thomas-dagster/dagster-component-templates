# Bigtable Writer Asset

Write rows of an upstream DataFrame to a Cloud Bigtable table — one DataFrame row → one Bigtable row.

```yaml
type: dagster_component_templates.BigtableWriterAssetComponent
attributes:
  asset_name: device_state_written
  upstream_asset_key: device_state
  instance_id: prod-bigtable
  table_id: device_telemetry
  row_key_column: device_id
  column_family: meta
  column_map:
    temperature: { family: metrics, qualifier: temp_c }
    last_seen:   { family: meta,    qualifier: ts }
  json_columns: [labels]
```

## Column placement

- **Default**: every non-row-key column → `<column_family>:<column_name>`.
- **Per-column override** via `column_map`: pick a different family or qualifier per DataFrame column.
- **JSON values**: list columns in `json_columns` to `json.dumps()` before encoding (handles dict/list cells).

## Performance

- Writes happen in batches of `batch_size` rows (default 500).
- Bigtable is single-region; for cross-region replication configure it at the instance level.

## Auth

Service account needs `roles/bigtable.user` on the instance.

## Important

- **Family must exist**: column families are part of the table schema. Create them via `cbt createfamily` or the console before writing.
- **Cells are bytes**: every value is utf-8-encoded. Numeric columns stored this way won't sort/compare lexicographically — for numeric range scans, pad with zeros or store as `big-endian int64` (custom code).

## Sister components

- `bigtable_reader_asset` — counterpart for reads
- `firestore_writer_asset` — document NoSQL alternative
- `dataframe_to_bigquery` — analytical alternative
