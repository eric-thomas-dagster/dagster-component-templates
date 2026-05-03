# MongoDBIOManager

ConfigurableIOManager that writes pandas DataFrames to MongoDB. Each row becomes a document; the asset key becomes the collection name. On replace mode, drops the collection before insert.

## Example

```yaml
type: dagster_component_templates.MongoDBIOManagerComponent
attributes:
  resource_key: io_manager
  connection_uri_env_var: <fill in>
  database: <fill in>
  if_exists: <fill in>
```

## Requirements

```
pandas
pymongo
```
