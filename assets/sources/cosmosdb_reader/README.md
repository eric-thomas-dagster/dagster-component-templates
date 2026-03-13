# CosmosDB Reader Component

Query items from an Azure Cosmos DB container and return them as a Dagster asset DataFrame.

## Overview

The `CosmosdbReaderComponent` connects to Azure Cosmos DB and executes SQL-like queries against a container, returning results as a pandas DataFrame. Supports cross-partition queries and configurable page sizes.

## Use Cases

- **Azure workloads**: Extract Cosmos DB data into analytics pipelines
- **Multi-model data**: Read JSON documents with Cosmos DB SQL API
- **Global distribution**: Access data from globally distributed Cosmos DB containers
- **ETL pipelines**: Migrate Cosmos DB data to a data warehouse

## Configuration

### Select all items

```yaml
type: dagster_component_templates.CosmosdbReaderComponent
attributes:
  asset_name: cosmos_products
  database: ecommerce
  container: products
```

### Filtered query

```yaml
type: dagster_component_templates.CosmosdbReaderComponent
attributes:
  asset_name: active_subscriptions
  database: billing
  container: subscriptions
  query: "SELECT c.id, c.userId, c.plan FROM c WHERE c.status = 'active'"
  group_name: sources
```

## Output Schema

Returns all Cosmos DB item fields as DataFrame columns. System properties (`_rid`, `_ts`, etc.) are included unless excluded in the query.

## Environment Variables

| Variable | Description |
|----------|-------------|
| `COSMOS_ENDPOINT` | Cosmos DB account endpoint URL |
| `COSMOS_KEY` | Cosmos DB primary or secondary key |

## Dependencies

- `pandas>=1.5.0`
- `azure-cosmos>=4.0.0`
