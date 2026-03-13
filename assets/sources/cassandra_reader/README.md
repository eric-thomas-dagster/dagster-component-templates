# Cassandra Reader Component

Execute a CQL query against an Apache Cassandra keyspace and return results as a Dagster asset DataFrame.

## Overview

The `CassandraReaderComponent` connects to an Apache Cassandra cluster and executes a CQL (Cassandra Query Language) query, returning results as a pandas DataFrame. Supports authentication and multi-node cluster configurations.

## Use Cases

- **Time-series data**: Read IoT sensor readings or event streams
- **Wide-column analytics**: Extract large Cassandra tables for analysis
- **Cassandra to warehouse**: Migrate Cassandra data to a data warehouse
- **Audit logs**: Pull audit records stored in Cassandra

## Configuration

### Basic query

```yaml
type: dagster_component_templates.CassandraReaderComponent
attributes:
  asset_name: cassandra_sessions
  hosts:
    - localhost
  keyspace: app_data
  query: "SELECT * FROM sessions LIMIT 5000"
```

### Multi-node cluster with authentication

```yaml
type: dagster_component_templates.CassandraReaderComponent
attributes:
  asset_name: cassandra_events
  hosts:
    - cassandra1.example.com
    - cassandra2.example.com
    - cassandra3.example.com
  port: 9042
  keyspace: analytics
  query: "SELECT user_id, event_type, created_at FROM events WHERE date = '2024-01-01' ALLOW FILTERING"
  username_env_var: CASSANDRA_USERNAME
  password_env_var: CASSANDRA_PASSWORD
  group_name: sources
```

## Output Schema

Returns all CQL result columns as DataFrame columns. Column names match the Cassandra table column names.

## Environment Variables

| Variable | Description |
|----------|-------------|
| `CASSANDRA_USERNAME` | Cassandra username (if authentication enabled) |
| `CASSANDRA_PASSWORD` | Cassandra password (if authentication enabled) |

## Dependencies

- `pandas>=1.5.0`
- `cassandra-driver>=3.25.0`
