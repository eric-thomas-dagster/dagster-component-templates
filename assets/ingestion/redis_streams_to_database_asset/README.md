# Redis Streams to Database Asset

Reads entries from a Redis Stream using XREAD (or XREADGROUP for consumer group mode) and writes them to a database table via SQLAlchemy. Each entry's fields become columns. The `_stream_id` field is automatically added.

## Required packages

```
redis>=4.6.0
pandas>=1.3.0
sqlalchemy>=2.0.0
```

## Fields

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `asset_name` | str | yes | — | Dagster asset name |
| `redis_url_env_var` | str | yes | — | Env var with Redis URL (redis://host:6379/0) |
| `stream_name` | str | yes | — | Redis stream name (key) |
| `consumer_group` | str | no | null | Consumer group name (uses XREADGROUP if set) |
| `consumer_name` | str | no | `dagster-worker` | Consumer name for XREADGROUP |
| `database_url_env_var` | str | yes | — | Env var with SQLAlchemy database URL |
| `table_name` | str | yes | — | Destination table name |
| `schema_name` | str | no | null | Destination schema name |
| `if_exists` | str | no | `append` | fail, replace, or append |
| `max_entries` | int | no | 10000 | Max stream entries to read per run |
| `block_ms` | int | no | 2000 | XREAD block timeout in milliseconds |
| `column_mapping` | dict | no | null | Rename columns: `{old: new}` |
| `group_name` | str | no | `ingestion` | Asset group name |
| `partition_type` | str | no | `none` | none, daily, weekly, or monthly |
| `partition_start_date` | str | no | null | Partition start date YYYY-MM-DD |

## Example

```yaml
type: dagster_component_templates.RedisStreamsToDatabaseAssetComponent
attributes:
  asset_name: redis_events_ingest
  redis_url_env_var: REDIS_URL
  stream_name: events
  database_url_env_var: DATABASE_URL
  table_name: raw_events
  max_entries: 10000
```

## Asset Dependencies & Lineage

This component supports a `deps` field for declaring upstream Dagster asset dependencies:

```yaml
attributes:
  # ... other fields ...
  deps:
    - raw_orders              # simple asset key
    - raw/schema/orders       # asset key with path prefix
```

`deps` draws lineage edges in the Dagster asset graph without loading data at runtime. Use it to express that this asset depends on upstream tables or assets produced by other components.

Dependencies can also be wired externally via `map_resolved_asset_specs()` in `definitions.py` — the same approach used by [Dagster Designer](https://github.com/eric-thomas-dagster/dagster_designer).
