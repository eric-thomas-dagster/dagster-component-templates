# MotherDuck IO Manager

Register a DuckDB IO manager connected to MotherDuck (serverless DuckDB cloud) for asset storage.

## Installation

```
pip install dagster-duckdb-pandas duckdb
```

## Configuration

```yaml
type: dagster_component_templates.MotherDuckIOManagerComponent
attributes:
  resource_key: io_manager
  motherduck_token_env_var: MOTHERDUCK_TOKEN
  database: my_db
  schema_name: main
```

## Authentication

```bash
export MOTHERDUCK_TOKEN=your-motherduck-token
```

Get a token at https://app.motherduck.com/settings/tokens
