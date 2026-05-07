# MongoDB Ingestion Component


Ingest [MongoDB](https://www.mongodb.com) collections using [dlt](https://dlthub.com)'s verified `mongodb` source.


## Overview

dlt handles API authentication, pagination, rate limiting, and incremental loading. This component wraps it as a Dagster asset that returns a pandas DataFrame by default, or persists directly to a destination if you set one.


## Source-specific fields

| Field | Type | Required | Description |
|---|---|---|---|
| `asset_name` | `str` | yes | Name of the output asset |
| `connection_url` | `str` | yes | MongoDB connection URL (e.g. `mongodb://user:pass@host:27017`) |
| `database` | `str` | yes | Database name |
| `collection_names` | `List[str]` | yes | Collections to extract |

## Standard fields

`description`, `group_name`, `owners`, `asset_tags`, `kinds`, `freshness_max_lag_minutes`, `freshness_cron`, `include_preview_metadata`, `deps` — same convention as every other component in this library.


## Destination

By default this asset runs an in-memory DuckDB pipeline and returns a pandas DataFrame for downstream Dagster transformations. To persist directly to a warehouse or object store, set `destination`:


```yaml
type: dagster_component_templates.MongoDBIngestionComponent
attributes:
  asset_name: mongodb_data
  # ... source config ...
  destination: postgres          # or bigquery, postgres, filesystem, ...
  dataset_name: mongodb_raw
  persist_only: true
```

Credentials come from env vars (`DESTINATION__<NAME>__CREDENTIALS__*`) or, for projects with multiple destination accounts, from inline `destination_credentials_url` / `destination_credentials_env_var`.


**For full destination configuration — env-var conventions, multi-account setups, every supported dlt destination — see [`../DESTINATIONS.md`](../DESTINATIONS.md).**


## Example — DataFrame mode (default)

```yaml
type: dagster_component_templates.MongoDBIngestionComponent
attributes:
  asset_name: mongo_data
  connection_url: "{{ env('MONGODB_URL') }}"
  database: my_database
  collection_names: [users, orders, products]
  group_name: mongodb
```

The asset emits a pandas DataFrame combining all selected resources, with a `_resource_type` column tagging each row.


## Example — persist to Postgres

```yaml
type: dagster_component_templates.MongoDBIngestionComponent
attributes:
  asset_name: mongo_data
  connection_url: "{{ env('MONGODB_URL') }}"
  database: my_database
  collection_names: [users, orders]
  destination: postgres
  dataset_name: mongo_raw
  persist_only: true
```

Set `DESTINATION__POSTGRES__CREDENTIALS__*` env vars before running. The asset emits a `MaterializeResult` with destination metadata.


## Notes

- **Schema flattening**: nested BSON documents are flattened into child tables — see [dlt's MongoDB schema docs](https://dlthub.com/docs/dlt-ecosystem/verified-sources/mongodb).
- **Incremental loading**: configure incremental cursors via dlt's source-level config.
- **Non-SQL destinations**: setting `destination=filesystem` (or any vector store / lake format) requires `persist_only=true`. The component logs a warning and returns a `MaterializeResult` if you forget.

## Asset dependencies

```yaml
deps:
  - some_upstream_asset
  - schema/scoped_asset
```

Dependencies declared here draw lineage edges in the Dagster graph without loading data at runtime.


## Learn more

- [dlt MongoDB source](https://dlthub.com/docs/dlt-ecosystem/verified-sources/mongodb)
- [dlt destinations overview](https://dlthub.com/docs/dlt-ecosystem/destinations)
- [`../DESTINATIONS.md`](../DESTINATIONS.md) — configuration reference for this library


## Azure Cosmos DB MongoDB API

This component works against **Azure Cosmos DB MongoDB API** out of the
box — Cosmos exposes a wire-compatible MongoDB endpoint, so the existing
`pymongo` client works without any Azure-specific changes.

```yaml
attributes:
  connection_string_env_var: COSMOS_MONGO_CONNECTION_STRING
  database: my-cosmos-db
  collection: orders
```

Get the connection string from the Cosmos DB portal:
**Settings → Connection strings → PRIMARY CONNECTION STRING**.

