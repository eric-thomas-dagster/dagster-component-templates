# DuckDBPySparkIOManager

PySpark variant of duckdb_io_manager. Wraps the official `dagster-duckdb-pyspark` package — DataFrames flow as Spark DataFrames between assets, persisted to local DuckDB between runs.

Wraps the official `dagster-duckdb-pyspark` package.

## Example

```yaml
type: dagster_component_templates.DuckDBPySparkIOManagerComponent
attributes:
  resource_key: <fill in>
  database: <fill in>
  schema_name: <fill in>
```

## Requirements

```
dagster
dagster-duckdb-pyspark
pyspark
```
