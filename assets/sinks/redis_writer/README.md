# Redis Writer Component

Write a DataFrame to Redis as a Dagster sink asset.

## Overview

The `RedisWriterComponent` accepts an upstream DataFrame asset and writes each row to Redis using a specified key column. Supports three write modes: hash (HSET all fields), string (SET serialized JSON), and list (RPUSH JSON value). Optional TTL/expiration support.

## Use Cases

- **Feature store**: Write ML features as Redis hashes for low-latency serving
- **Session cache**: Cache computed session data in Redis
- **Rate limiting counters**: Write count data to Redis
- **Real-time lookups**: Populate Redis with lookup tables from batch pipelines

## Configuration

### Write as hashes (recommended for structured data)

```yaml
type: dagster_component_templates.RedisWriterComponent
attributes:
  asset_name: write_user_features_to_redis
  upstream_asset_key: computed_user_features
  key_column: user_id
  write_mode: hash
  expire_seconds: 86400
  group_name: sinks
```

### Write as JSON strings

```yaml
type: dagster_component_templates.RedisWriterComponent
attributes:
  asset_name: write_product_cache
  upstream_asset_key: enriched_products
  key_column: product_id
  write_mode: string
  expire_seconds: 3600
```

### Append to lists

```yaml
type: dagster_component_templates.RedisWriterComponent
attributes:
  asset_name: write_events_to_redis_list
  upstream_asset_key: new_events
  key_column: user_id
  write_mode: list
```

## Write Modes

| Mode | Redis Command | Description |
|------|--------------|-------------|
| `hash` | `HSET` | All row fields as hash fields |
| `string` | `SET` | Entire row as JSON string |
| `list` | `RPUSH` | Non-key fields as JSON appended to list |

## Environment Variables

| Variable | Description |
|----------|-------------|
| `REDIS_HOST` | Redis host address |
| `REDIS_PASSWORD` | Redis password (if AUTH enabled) |

## Dependencies

- `pandas>=1.5.0`
- `redis>=4.0.0`
