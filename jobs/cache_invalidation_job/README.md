# CacheInvalidationJobComponent

Flush keys from a Redis (or compatible) cache as a job — pattern-based or full FLUSHDB.

## Dependencies
- `redis`

## What this is
A **Dagster job** (no asset materialized) — the component installs a `dg.job`
plus an optional schedule. Lineage graph stays clean; the work happens
behind the scenes.

## See also
- [Schema](schema.json) · [Example config](example.yaml)
