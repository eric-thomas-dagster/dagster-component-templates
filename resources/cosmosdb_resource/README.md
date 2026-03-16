# CosmosDBResourceComponent

A Dagster component that provides an Azure Cosmos DB `ConfigurableResource` backed by the `azure-cosmos` SDK.

## Installation

```
pip install azure-cosmos
```

## Configuration

```yaml
type: cosmosdb_resource.component.CosmosDBResourceComponent
attributes:
  resource_key: cosmosdb_resource
  endpoint: "https://myaccount.documents.azure.com:443/"
  key_env_var: COSMOS_KEY
  database_name: my_database
```

## Auth

Set the environment variable named in `key_env_var` to your Cosmos DB account primary or secondary key. The resource exposes `get_client()` for a raw `CosmosClient` and `get_database()` for a `DatabaseProxy` scoped to `database_name`.
