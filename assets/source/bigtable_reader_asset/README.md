# Bigtable Reader Asset

Read rows from a [Cloud Bigtable](https://cloud.google.com/bigtable) table into a pandas DataFrame.

```yaml
type: dagster_component_templates.BigtableReaderAssetComponent
attributes:
  asset_name: device_telemetry_recent
  instance_id: prod-bigtable
  table_id: device_telemetry
  row_key_prefix: "device#"
  column_families: [meta, metrics]
  limit: 1000
```

## When to pick Bigtable vs. peers

| You need | Use |
|---|---|
| **Wide-column NoSQL at petabyte scale**, sub-10ms reads | Bigtable |
| Globally-distributed transactional RDBMS | Spanner |
| Document/JSON CRUD | Firestore |
| Analytical scans | BigQuery |

## Output shape

- One row per Bigtable row
- `_row_key` column with the decoded row key
- One column per `<family>:<qualifier>` cell (only the latest cell version)
- `decode_values_as: utf-8` (default), `bytes`, or `json`

## Scan modes

- **Prefix**: `row_key_prefix: "device#"` — all rows starting with that prefix
- **Range**: `start_key: "device#100"` + `end_key: "device#200"` — explicit range
- **Full scan**: neither set — reads the entire table (be careful)

## Auth

Service account needs `roles/bigtable.reader` on the instance.

## Sister components

- `bigtable_writer_asset` — counterpart for writes
- `firestore_reader_asset` — document NoSQL alternative
- `spanner_query_asset` — strongly-consistent RDBMS alternative
