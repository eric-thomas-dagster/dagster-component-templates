# MSSQLIOManager

ConfigurableIOManager that writes pandas DataFrames to SQL Server via SQLAlchemy + pyodbc. Asset key becomes the table name; partitioned assets land in one table per partition.

## Example

```yaml
type: dagster_component_templates.MSSQLIOManagerComponent
attributes:
  resource_key: io_manager
  connection_url_env_var: <fill in>
  schema_name: <fill in>
  if_exists: <fill in>
```

## Requirements

```
pandas
sqlalchemy
pyodbc
```
