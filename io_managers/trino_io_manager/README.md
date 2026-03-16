# Trino IO Manager

Register a Trino IO manager that reads and writes Pandas DataFrames via Trino SQL.

## Installation

```
pip install trino pandas
```

## Configuration

```yaml
type: dagster_component_templates.TrinoIOManagerComponent
attributes:
  resource_key: io_manager
  host: localhost
  port: 8080
  user: dagster
  catalog: iceberg
  schema_name: default
```

## Authentication

```bash
export TRINO_PASSWORD=your-password  # only if auth is required
```
