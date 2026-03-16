# Apache Cassandra Resource

Register an Apache Cassandra resource for high-throughput distributed database access

A Dagster resource component that provides a Cassandra `ConfigurableResource` backed by `cassandra-driver`.

## Installation

```
pip install cassandra-driver
```

## Configuration

```yaml
type: cassandra_resource.component.CassandraResourceComponent
attributes:
  resource_key: cassandra_resource
  hosts: "host1,host2"
  port: 9042
  keyspace: my_keyspace
  username: cassandra
  password_env_var: CASSANDRA_PASSWORD
```

## Auth

Set `password_env_var` to the name of an environment variable containing the Cassandra password. If `username` is omitted, no authentication provider is configured.
