# CosmosDB Writer Component

Write a DataFrame to an Azure Cosmos DB container as a Dagster sink asset.

## Overview

The `CosmosdbWriterComponent` accepts an upstream DataFrame asset and writes each row as a Cosmos DB item. Supports upsert (create or replace existing) and insert (create only) modes.

## Use Cases

- **Azure workloads**: Write processed data back to Cosmos DB
- **Global distribution**: Store results in a globally distributed Cosmos DB container
- **Multi-model storage**: Write JSON documents to Cosmos DB SQL API
- **ETL sink**: Final destination in Azure-based data pipelines

## Configuration

### Upsert mode (create or replace)

```yaml
type: dagster_component_templates.CosmosdbWriterComponent
attributes:
  asset_name: write_products_to_cosmos
  upstream_asset_key: enriched_products
  database: catalog
  container: products
  if_exists: upsert
  group_name: sinks
```

### Insert mode (create only)

```yaml
type: dagster_component_templates.CosmosdbWriterComponent
attributes:
  asset_name: insert_events_to_cosmos
  upstream_asset_key: new_events
  database: analytics
  container: events
  if_exists: insert
```

## Notes

- Items must have an `id` field that matches the container's partition key scheme
- Cosmos DB requires each item to have a unique `id` within its partition

## Environment Variables

| Variable | Description |
|----------|-------------|
| `COSMOS_ENDPOINT` | Cosmos DB account endpoint URL |
| `COSMOS_KEY` | Cosmos DB primary or secondary key |

## Dependencies

- `pandas>=1.5.0`
- `azure-cosmos>=4.0.0`
