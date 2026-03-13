# Cassandra Writer Component

Write a DataFrame to an Apache Cassandra table as a Dagster sink asset.

## Overview

The `CassandraWriterComponent` accepts an upstream DataFrame asset and writes each row to a Cassandra table using prepared INSERT CQL statements executed in batches of 100 for efficiency. DataFrame column names must match Cassandra table column names.

## Use Cases

- **Time-series writes**: Write IoT sensor data or event streams to Cassandra
- **Wide-column storage**: Store analytics results in Cassandra tables
- **High-throughput writes**: Leverage Cassandra's write performance for large datasets
- **Distributed storage**: Write data to Cassandra clusters

## Configuration

### Basic write

```yaml
type: dagster_component_templates.CassandraWriterComponent
attributes:
  asset_name: write_metrics_to_cassandra
  upstream_asset_key: computed_metrics
  hosts:
    - localhost
  keyspace: metrics
  table: daily_metrics
```

### Multi-node cluster with authentication

```yaml
type: dagster_component_templates.CassandraWriterComponent
attributes:
  asset_name: write_events_to_cassandra
  upstream_asset_key: processed_events
  hosts:
    - cassandra1.example.com
    - cassandra2.example.com
  keyspace: analytics
  table: events
  username_env_var: CASSANDRA_USERNAME
  password_env_var: CASSANDRA_PASSWORD
  group_name: sinks
```

## Notes

- DataFrame column names must match the Cassandra table column names exactly
- Cassandra INSERT is inherently an upsert operation (it replaces existing rows with the same primary key)
- Writes are batched in groups of 100 for performance

## Environment Variables

| Variable | Description |
|----------|-------------|
| `CASSANDRA_USERNAME` | Cassandra username (if authentication enabled) |
| `CASSANDRA_PASSWORD` | Cassandra password (if authentication enabled) |

## Dependencies

- `pandas>=1.5.0`
- `cassandra-driver>=3.25.0`
