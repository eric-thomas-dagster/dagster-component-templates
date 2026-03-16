# RedisResourceComponent

A Dagster component that provides a Redis `ConfigurableResource` backed by `redis-py`.

## Installation

```
pip install "redis>=4.0.0"
```

## Configuration

```yaml
type: redis_resource.component.RedisResourceComponent
attributes:
  resource_key: redis_resource
  host: localhost
  port: 6379
  db: 0
  password_env_var: REDIS_PASSWORD
  ssl: false
```

## Auth

Set `password_env_var` to the name of an environment variable containing the Redis password. Alternatively, set `url_env_var` to the name of an environment variable holding a full Redis URL (e.g. `redis://:password@host:6379/0`) — this overrides `host`, `port`, and `db`.
