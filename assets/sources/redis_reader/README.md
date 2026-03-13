# Redis Reader Component

Read keys from a Redis instance by glob pattern and return their values as a Dagster asset DataFrame.

## Overview

The `RedisReaderComponent` connects to Redis, scans for keys matching a pattern, and reads their values. Supports all Redis data types (string, hash, list, set, sorted set) with auto-detection or explicit type specification.

## Use Cases

- **Session data**: Extract user session hashes for analysis
- **Cache inspection**: Sample cached values for data quality checks
- **Feature store**: Read feature vectors stored as Redis hashes
- **Leaderboards**: Extract sorted set rankings

## Configuration

### Read all string keys

```yaml
type: dagster_component_templates.RedisReaderComponent
attributes:
  asset_name: redis_config_values
  key_pattern: "config:*"
  data_type: string
```

### Read hash keys (e.g. user profiles)

```yaml
type: dagster_component_templates.RedisReaderComponent
attributes:
  asset_name: user_profiles
  host_env_var: REDIS_HOST
  port: 6379
  key_pattern: "user:*"
  data_type: hash
  limit: 5000
  group_name: sources
```

### Auto-detect data types

```yaml
type: dagster_component_templates.RedisReaderComponent
attributes:
  asset_name: mixed_redis_keys
  key_pattern: "app:*"
  data_type: auto
```

## Output Schema

| Column | Description |
|--------|-------------|
| `key` | Redis key name |
| `value` | Key value (for string, list, set, zset types) |
| `type` | Redis data type |
| `[hash fields]` | Individual hash fields (for hash type only) |

## Environment Variables

| Variable | Description |
|----------|-------------|
| `REDIS_HOST` | Redis host address |
| `REDIS_PASSWORD` | Redis password (if AUTH enabled) |

## Dependencies

- `pandas>=1.5.0`
- `redis>=4.0.0`
