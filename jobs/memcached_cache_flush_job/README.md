# Memcached Cache Flush Job

Flush the entire Memcached instance (`flush_all`) or delete a specific list of keys as a Dagster job. Optionally schedule via cron. Backed by `pymemcache`.

## Installation

```
pip install "pymemcache>=4.0.0"
```

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `job_name` | `str` | — | Dagster job name |
| `schedule` | `str` | — | Cron schedule (None = no schedule) |
| `default_status` | `str` | `"STOPPED"` | STOPPED or RUNNING |
| `tags` | `dict` | — | Dagster job tags |
| `host` | `str` | `"localhost"` | Memcached host |
| `port` | `int` | `11211` | Memcached port |
| `keys` | `List[str]` | — | Explicit keys to delete. When empty, calls `flush_all`. |
| `timeout_seconds` | `float` | `3.0` | Connect + read/write timeout |

## Memcached vs Redis flushing

Memcached does not support SCAN or pattern-match. The wire protocol is only `get` / `set` / `delete` / `flush_all`. So:

- **Enumerate keys explicitly** in the `keys:` list to delete specific entries.
- **Omit `keys:`** (or leave it empty) to call `flush_all` — which nukes everything in the cache.

If you need glob-based invalidation (`session:*`, `user:123:*`), use Redis instead — the `redis_resource` + `cache_invalidation_job` combo supports SCAN + pattern-match deletes.

## Configuration

```yaml
type: memcached_cache_flush_job.component.MemcachedCacheFlushJobComponent
attributes:
  job_name: memcached_flush_hourly
  schedule: "0 * * * *"
  default_status: STOPPED
  host: memcached.internal
  port: 11211
  keys:
    - "session:demo"
    - "user_prefs:cache"
```

## Common use cases

- **Post-deploy invalidation**: fire the job as part of a deploy Dagster sensor. New code shouldn't read stale cache.
- **Data-refresh sync**: schedule right after your daily warehouse ETL — cache invalidations align with fresh data.
- **Emergency wipe**: turn on the job manually via the UI when cache corruption is suspected.
