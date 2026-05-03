# MySQLIOManager

ConfigurableIOManager that writes pandas DataFrames to MySQL via SQLAlchemy. Each asset gets its own table named from the asset key. Supports partitioned assets (one table per partition) and customer schema selection.

## Example

```yaml
type: dagster_component_templates.MySQLIOManagerComponent
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
pymysql
```
