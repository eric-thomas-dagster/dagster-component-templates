# Firestore Reader

Read documents from a Firestore collection (or collection group) into a pandas DataFrame.

```yaml
type: dagster_component_templates.FirestoreReaderAssetComponent
attributes:
  asset_name: active_orders
  credentials_path: ${GOOGLE_APPLICATION_CREDENTIALS}
  collection: orders
  where:
    - { field: status, op: "==", value: active }
    - { field: amount, op: ">=", value: 100 }
  order_by: ["-created_at"]
  limit: 1000
```

## Filter ops

`==`, `!=`, `<`, `<=`, `>`, `>=`, `in`, `not-in`, `array-contains`, `array-contains-any`.

## Sub-collections + collection groups

- **Sub-collection**: `collection: tenants/acme/orders` reads only that one path.
- **Collection group**: `collection: orders` + `collection_group: true` matches every collection named `orders` anywhere in the database (e.g. across tenants).

## Required SA roles

`roles/datastore.viewer` on the project. Firestore API enabled.

## Sister components

- `firestore_writer_asset` — write rows back.
- `bigquery_query_asset` / `database_query` — SQL alternatives.
- `mongodb_to_database_asset` — same shape, MongoDB.
