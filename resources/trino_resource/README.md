# Trino Resource

Register a Trino distributed SQL query engine resource for use by other components.

## Installation

```
pip install trino
```

## Configuration

```yaml
type: dagster_component_templates.TrinoResourceComponent
attributes:
  resource_key: trino_resource
  host: localhost
  port: 8080
  user: dagster
  catalog: iceberg
  schema_name: default
```

## Authentication

For authenticated clusters, set the password environment variable:

```bash
export TRINO_PASSWORD=your-password
```

## Usage in other components

```yaml
attributes:
  resource_key: trino_resource
```
