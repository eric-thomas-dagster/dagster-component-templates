# CouchDB Writer Component

Write a DataFrame to an Apache CouchDB database as a Dagster sink asset.

## Overview

The `CouchdbWriterComponent` accepts an upstream DataFrame asset and writes each row as a CouchDB document. Supports upsert (fetch existing revision and PUT to update) and insert (POST to create new) modes. An optional `id_column` lets you control document IDs.

## Use Cases

- **Document sync**: Write processed data back to CouchDB
- **Offline-first apps**: Populate CouchDB for mobile/offline sync
- **IoT data storage**: Write device readings to CouchDB
- **Content management**: Store processed content in CouchDB

## Configuration

### Upsert with custom document ID

```yaml
type: dagster_component_templates.CouchdbWriterComponent
attributes:
  asset_name: write_products_to_couchdb
  upstream_asset_key: enriched_products
  database: products
  if_exists: upsert
  id_column: product_id
  group_name: sinks
```

### Insert new documents (auto-generated IDs)

```yaml
type: dagster_component_templates.CouchdbWriterComponent
attributes:
  asset_name: insert_events_to_couchdb
  upstream_asset_key: new_events
  url_env_var: COUCHDB_URL
  database: events
  if_exists: insert
```

## Notes

- In upsert mode, the component fetches the existing `_rev` for each document before updating to avoid conflicts
- In insert mode without `id_column`, CouchDB auto-generates document IDs via POST
- For large datasets, consider batching writes using CouchDB's `_bulk_docs` API (not yet supported in this component)

## Environment Variables

| Variable | Description |
|----------|-------------|
| `COUCHDB_URL` | CouchDB URL with credentials (e.g. `http://admin:password@localhost:5984`) |

## Dependencies

- `pandas>=1.5.0`
- `requests>=2.28.0`
