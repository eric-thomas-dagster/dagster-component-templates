# MongoDB Resource

Register a MongoDB resource for flexible document database access via pymongo

A Dagster resource component that provides a MongoDB `ConfigurableResource` backed by `pymongo`.

## Installation

```
pip install pymongo
```

## Configuration

```yaml
type: mongodb_resource.component.MongoDBResourceComponent
attributes:
  resource_key: mongodb_resource
  connection_string_env_var: MONGODB_CONNECTION_STRING
  database: my_database
  tls: true
```

## Auth

Set the environment variable named in `connection_string_env_var` to your full MongoDB URI, including credentials (e.g. `mongodb+srv://user:pass@cluster.mongodb.net`). TLS is enabled by default.


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

