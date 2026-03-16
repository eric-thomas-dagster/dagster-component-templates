# DuckDB Resource

Register a DuckDB resource for fast in-process analytical queries

A Dagster resource component that provides a DuckDB `ConfigurableResource` backed by `duckdb`.

## Installation

```
pip install duckdb
```

## Configuration

```yaml
type: duckdb_resource.component.DuckDBResourceComponent
attributes:
  resource_key: duckdb_resource
  database: ":memory:"
  read_only: false
```

## Auth

DuckDB has no external credentials. Set `database` to a file path for persistent storage, or leave it as `":memory:"` for an ephemeral in-process database. The resource exposes `get_connection()` which returns a `duckdb.DuckDBPyConnection`.
