# MongoDBIOManager

ConfigurableIOManager that writes pandas DataFrames to MongoDB. Each row becomes a document; the asset key becomes the collection name. On replace mode, drops the collection before insert.

## Example

```yaml
type: dagster_component_templates.MongoDBIOManagerComponent
attributes:
  resource_key: io_manager
  connection_uri_env_var: <fill in>
  database: <fill in>
  if_exists: <fill in>
```

## Requirements

```
pandas
pymongo
```


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

