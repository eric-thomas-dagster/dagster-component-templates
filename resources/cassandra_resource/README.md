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


## Azure Cosmos DB Cassandra API + Azure Managed Instance for Cassandra

This component works against:

- **Azure Cosmos DB Cassandra API** — wire-compatible CQL endpoint;
  use `<account>.cassandra.cosmos.azure.com:10350` and TLS.
- **Azure Managed Instance for Apache Cassandra** — fully-managed
  multi-node Cassandra; use the seed node hosts from the cluster.

```yaml
attributes:
  contact_points: ["myaccount.cassandra.cosmos.azure.com"]
  port: 10350
  ssl: true
  keyspace: orders
  username_env_var: COSMOS_CASSANDRA_USERNAME
  password_env_var: COSMOS_CASSANDRA_PASSWORD
```

