# Neo4j Writer Component

Write a DataFrame to Neo4j as graph nodes as a Dagster sink asset.

## Overview

The `Neo4jWriterComponent` accepts an upstream DataFrame asset and writes each row as a Neo4j node with a specified label. Uses UNWIND for efficient batch processing. Supports MERGE (create or update) and CREATE modes.

## Use Cases

- **Knowledge graph population**: Load entity data into Neo4j as nodes
- **Recommendation engine data**: Write user and product nodes to Neo4j
- **Fraud detection**: Load transaction entities into a graph
- **Social network analysis**: Write person nodes from processed data

## Configuration

### Merge nodes (create or update)

```yaml
type: dagster_component_templates.Neo4jWriterComponent
attributes:
  asset_name: write_products_to_neo4j
  upstream_asset_key: enriched_products
  node_label: Product
  id_column: product_id
  merge: true
  group_name: sinks
```

### Create new nodes

```yaml
type: dagster_component_templates.Neo4jWriterComponent
attributes:
  asset_name: write_events_to_neo4j
  upstream_asset_key: new_events
  uri_env_var: NEO4J_URI
  username_env_var: NEO4J_USERNAME
  password_env_var: NEO4J_PASSWORD
  node_label: Event
  id_column: event_id
  merge: false
  database: neo4j
```

## Notes

- Uses `UNWIND` + `MERGE`/`CREATE` for efficient batch writes
- All DataFrame columns become node properties
- The `id_column` field is used as the MERGE identity key

## Environment Variables

| Variable | Description |
|----------|-------------|
| `NEO4J_URI` | Neo4j connection URI |
| `NEO4J_USERNAME` | Neo4j username |
| `NEO4J_PASSWORD` | Neo4j password |

## Dependencies

- `pandas>=1.5.0`
- `neo4j>=5.0.0`
