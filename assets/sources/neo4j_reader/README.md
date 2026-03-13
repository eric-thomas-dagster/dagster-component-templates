# Neo4j Reader Component

Execute a Cypher query against a Neo4j graph database and return results as a Dagster asset DataFrame.

## Overview

The `Neo4jReaderComponent` connects to a Neo4j graph database and executes a Cypher query, returning results as a pandas DataFrame. Supports both local and cloud Neo4j instances (including AuraDB).

## Use Cases

- **Knowledge graphs**: Extract entity relationships for ML pipelines
- **Recommendation engines**: Read collaborative filtering data
- **Fraud detection**: Extract transaction networks for analysis
- **Social networks**: Query follower/following relationships

## Configuration

### Basic node query

```yaml
type: dagster_component_templates.Neo4jReaderComponent
attributes:
  asset_name: neo4j_users
  query: "MATCH (n:User) RETURN n.id AS id, n.name AS name, n.email AS email LIMIT 1000"
```

### Relationship query

```yaml
type: dagster_component_templates.Neo4jReaderComponent
attributes:
  asset_name: user_purchases
  uri_env_var: NEO4J_URI
  username_env_var: NEO4J_USERNAME
  password_env_var: NEO4J_PASSWORD
  query: "MATCH (u:User)-[r:PURCHASED]->(p:Product) RETURN u.id AS user_id, p.id AS product_id, r.timestamp AS purchased_at"
  database: neo4j
  group_name: sources
```

### AuraDB cloud instance

```yaml
type: dagster_component_templates.Neo4jReaderComponent
attributes:
  asset_name: aura_graph_data
  uri_env_var: NEO4J_AURA_URI
  username_env_var: NEO4J_AURA_USERNAME
  password_env_var: NEO4J_AURA_PASSWORD
  query: "MATCH (n) RETURN labels(n) AS labels, count(n) AS count"
```

## Output Schema

Returns Cypher result columns as DataFrame columns. Column names are determined by the `RETURN` clause aliases.

## Environment Variables

| Variable | Description |
|----------|-------------|
| `NEO4J_URI` | Neo4j connection URI (e.g. `bolt://localhost:7687` or `neo4j+s://xxx.databases.neo4j.io`) |
| `NEO4J_USERNAME` | Neo4j username |
| `NEO4J_PASSWORD` | Neo4j password |

## Dependencies

- `pandas>=1.5.0`
- `neo4j>=5.0.0`
