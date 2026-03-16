# MySQL Resource

Register a MySQL resource for relational database access

A Dagster resource component that provides a MySQL `ConfigurableResource` backed by `mysql-connector-python`.

## Installation

```
pip install mysql-connector-python
```

## Configuration

```yaml
type: mysql_resource.component.MySQLResourceComponent
attributes:
  resource_key: mysql_resource
  host: my-mysql-host.example.com
  port: 3306
  database: my_database
  username: my_user
  password_env_var: MYSQL_PASSWORD
  ssl_disabled: false
```

## Auth

Set the environment variable named in `password_env_var` to your MySQL password. SSL is enabled by default; set `ssl_disabled: true` only for trusted local connections.
