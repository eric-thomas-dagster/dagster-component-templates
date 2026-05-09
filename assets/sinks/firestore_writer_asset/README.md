# Firestore Writer

Write DataFrame rows to a Firestore collection — one document per row. Useful as a real-time-readable sink behind ML scoring tables, customer-360 denormalized views, dynamic config tables that mobile / web clients should be able to read directly.

```yaml
type: dagster_component_templates.FirestoreWriterAssetComponent
attributes:
  asset_name: customer_360_to_firestore
  upstream_asset_key: customer_360
  credentials_path: ${GOOGLE_APPLICATION_CREDENTIALS}
  collection: customer_360
  id_column: customer_id
  write_mode: merge          # set | merge | create
  batch_size: 500
```

## Write modes

- **`merge`** (default): deep-merge into existing doc, preserves untouched fields. Best for incremental updates.
- **`set`**: full overwrite. Best for "snapshot" tables that fully rebuild.
- **`create`**: fail if doc already exists. Best for ledger / append-only patterns.

## Required SA roles

`roles/datastore.user` on the project. Firestore API enabled.

## Sister components

- `firestore_reader_asset` — read it back.
- `bigquery_export_to_gcs_asset` + `bigquery_load_from_gcs_asset` — warehouse-to-warehouse alternative.
