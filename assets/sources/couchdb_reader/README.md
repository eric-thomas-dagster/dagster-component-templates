# CouchDB Reader Component

Query documents from an Apache CouchDB database using the Mango query API and return results as a Dagster asset DataFrame.

## Overview

The `CouchdbReaderComponent` uses the CouchDB `_find` (Mango) endpoint to query documents from a CouchDB database, returning results as a pandas DataFrame. Supports selector-based filtering and field projection.

## Use Cases

- **Document retrieval**: Extract CouchDB documents for analytics
- **Offline-first apps**: Sync CouchDB data into analytics pipelines
- **IoT backends**: Read device data stored in CouchDB
- **Content management**: Extract CMS content stored in CouchDB

## Configuration

### Read all documents

```yaml
type: dagster_component_templates.CouchdbReaderComponent
attributes:
  asset_name: all_products
  database: products
```

### Filtered query with field projection

```yaml
type: dagster_component_templates.CouchdbReaderComponent
attributes:
  asset_name: active_orders
  url_env_var: COUCHDB_URL
  database: orders
  selector:
    status: active
    type: order
  fields:
    - _id
    - customer_id
    - total
    - created_at
  limit: 1000
  group_name: sources
```

## Output Schema

| Column | Description |
|--------|-------------|
| `_id` | CouchDB document ID |
| `_rev` | CouchDB document revision |
| `[document fields]` | All document fields (or projected subset) |

## Environment Variables

| Variable | Description |
|----------|-------------|
| `COUCHDB_URL` | CouchDB URL with credentials (e.g. `http://admin:password@localhost:5984`) |

## Dependencies

- `pandas>=1.5.0`
- `requests>=2.28.0`
