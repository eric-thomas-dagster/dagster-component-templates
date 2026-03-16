# MongoDBResourceComponent

A Dagster component that provides a MongoDB `ConfigurableResource` backed by `pymongo`.

## Installation

```
pip install pymongo
```

## Configuration

```yaml
type: mongodb_resource.component.MongoDBResourceComponent
attributes:
  resource_key: mongodb_resource
  connection_string_env_var: MONGODB_CONNECTION_STRING
  database: my_database
  tls: true
```

## Auth

Set the environment variable named in `connection_string_env_var` to your full MongoDB URI, including credentials (e.g. `mongodb+srv://user:pass@cluster.mongodb.net`). TLS is enabled by default.
