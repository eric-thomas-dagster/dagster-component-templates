# Neo4j Resource

Register a Neo4j resource for graph database queries via the Bolt protocol

A Dagster resource component that provides a Neo4j `ConfigurableResource` backed by the official `neo4j` Python driver.

## Installation

```
pip install neo4j
```

## Configuration

```yaml
type: neo4j_resource.component.Neo4jResourceComponent
attributes:
  resource_key: neo4j_resource
  uri: "bolt://localhost:7687"
  username: neo4j
  password_env_var: NEO4J_PASSWORD
  database: neo4j
```

## Auth

Set the environment variable named in `password_env_var` to your Neo4j password. The resource exposes `get_driver()` which returns a `neo4j.Driver` instance using Bolt auth.
