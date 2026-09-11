# `CachedAssetComponent` + `@cached` decorator

Content-addressable cache for expensive Python compute. Skip re-running when the cache is warm; run + save when cold. Save real money (LLM tokens, warehouse credits, GPU time) on assets whose inputs haven't changed.

## Cache key composition

Every materialization derives a stable key from:

- **Asset key** — different assets, different keys.
- **`code_version`** — bump this to invalidate cache. Also set as `code_version` on the underlying `@dg.asset` so downstream sees the version bump via Dagster's built-in change detection.
- **`partition_key`** — different partitions, different keys.
- **Optional `key_fn: 'mod:fn'`** — user-provided function whose result is mixed into the key. Perfect for "invalidate when this external config changes."

Hashed to 24 chars → cache file at `{cache_dir}/{key}.parquet`.

## Storage

`cache_dir` accepts:
- Local paths (`/tmp/cache/`, `~/.dagster_cache/reports/`)
- fsspec URIs — `s3://bucket/cache/`, `gs://bucket/cache/`, `abfs://container/cache/`

Cloud URIs make the cache cross-worker + cross-run (perfect for Dagster+ Serverless).

## Two shapes

| Shape | Use when |
|---|---|
| **`CachedAssetComponent`** (YAML) | Define a new cached asset |
| **`@cached` decorator** (Python) | Wrap an existing `@dg.asset` |

## What's Dagster-specific here

- **Cache key includes `code_version`** — Dagster's own change-detection primitive drives invalidation.
- **Cache key includes `partition_key`** — each partition has its own cache slot.
- **Hit/miss visibility** — every materialization emits an `AssetObservation(tags={cached_asset_status: hit|miss, cache_key, cache_path})`. Searchable in the event log. Agent planners + dashboards can compute hit rates over any time window.
- **Materialization metadata** is typed (`MetadataValue.path` for cache_path, `MetadataValue.int` for row count) so the UI renders correctly.

## Full YAML example

```yaml
type: dagster_community_components.CachedAssetComponent
attributes:
  asset_name: expensive_report

  compute:
    kind: python
    python: "my_project.reports:build_expensive"

  cache_dir: "s3://my-cache/reports/"
  code_version: "1.0.0"
  ttl_seconds: 86400
  format: parquet

  # Optional: mix a runtime value into the cache key
  key_fn: "my_project.reports:cache_key_extras"
```

`cache_key_extras` might read from a config file, git commit hash, etc:

```python
def cache_key_extras(context):
    return open("/etc/report_config.yaml").read()
```

## `@cached` decorator

```python
import dagster as dg
from dagster_community_components import cached

@dg.asset(code_version="1.2")
@cached(
    cache_dir="s3://my-cache/orders/",
    code_version="1.2",           # matches asset's code_version
    ttl_seconds=3600,             # 1 hour
    format="parquet",
    key_fn="my_project.cache:key_from_config",  # optional
)
def orders(context):
    return expensive_build()
```

## Invalidation levers

- **Bump `code_version`** — most common, tracks alongside the asset.
- **Wait for TTL** — cache staleness is a "just wait it out" recovery.
- **Change `key_fn` output** — external config invalidation.
- **Manual bust** — delete the parquet file at the cache_path.

Delete = full manual reset. Combine with a scheduled cleanup for retention.

## Eviction policies

TTL is one axis (age-based); large deployments also want size-based caps. Set either or both of:

- **`max_entries: N`** — after every miss-write, if `cache_dir/` holds more than N files, delete oldest (by mtime) until the count is back to N.
- **`max_bytes: X`** — same mechanic, tallied by file size.

Both caps can be set simultaneously — eviction runs until BOTH are satisfied.

```python
@dg.asset(code_version="1.0")
@cached(
    cache_dir="/var/dagster_cache/orders/",
    max_entries=100,               # keep 100 most-recent parquets
    max_bytes=1_000_000_000,       # AND stay under 1 GB total
)
def orders(context):
    return build()
```

Same fields on the YAML component:

```yaml
type: dagster_community_components.CachedAssetComponent
attributes:
  asset_name: orders
  compute:
    kind: python
    python: "my_project.reports:build_orders"
  cache_dir: /var/dagster_cache/orders/
  max_entries: 100
  max_bytes: 1_000_000_000
```

When files are evicted, the log records `[cached] LRU evicted N files (freed X bytes)` and an `AssetObservation(tags={cached_asset_lru_evicted: N})` is emitted so it's searchable in the event log.

**v1 caveat:** LRU eviction runs on the LOCAL filesystem only. If `cache_dir` is an `s3://` / `gs://` / `abfs://` URI, eviction is skipped with a warning — cross-provider mtime scans are expensive and best handled via bucket lifecycle rules for now.

## Composes with

- **`@data_contract`** — validate the CACHED result against the contract. If the cache was corrupted, the contract catches it.
- **`@lifecycle`** — audit the cached result before publishing to prod.
- **`@smart_retry`** — retry transient failures during compute (only on cache miss).

## CLI demos using this template

| Demo | Setup script | What it shows |
|---|---|---|
| [`cached_asset.md`](https://github.com/eric-thomas-dagster/dagster-community-components-cli/blob/main/examples/cached_asset.md) | [`setup_cached_asset_demo.sh`](https://github.com/eric-thomas-dagster/dagster-community-components-cli/blob/main/examples/setup_cached_asset_demo.sh) | 100% offline. Three back-to-back runs of one `@cached` `@dg.asset`: RUN 1 MISS (writes parquet, ~3s) → RUN 2 HIT (loads parquet, near-zero) → RUN 3 MISS after `code_version` bump (new key, ~3s). Ends with an event-log query that reads cache metadata for a hit/miss dashboard. |

```bash
curl -fsSL https://raw.githubusercontent.com/eric-thomas-dagster/dagster-community-components-cli/main/examples/setup_cached_asset_demo.sh | bash
```

## What's not in v1 (roadmap)

- **Multi-format inputs** — right now the cache stores `pandas.DataFrame`. Add object / pickle / arrow support.
- **Cross-asset dedup** — if two assets produce the same content, cache once and both hit.
- **Warm-on-startup** — pre-populate the cache from prior runs' materializations without waiting for the first request.
- **LRU eviction on cloud storage** — v1 evicts local FS only; extend to fsspec URIs (S3 / GCS / ABFS) with provider-aware mtime scans.

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `cache_dir` | `str` | Where cached parquets live. Local path or fsspec URI. |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | — | Dagster asset name. Required when NOT using `wraps:` (inherited from inner in wraps mode). |
| `code_version` | `str` | — | Version string mixed into the cache key. Bump to invalidate cache. Also set as `code_version` on the asset so downstream sees a bump. |
| `group_name` | `str` | — | — |
| `description` | `str` | — | — |
| `owners` | `List[str]` | — | — |
| `tags` | `Dict[str, str]` | — | — |
| `kinds` | `List[str]` | — | Asset kinds. Default: ['python', 'cache']. |

### Source / target

| Field | Type | Default | Description |
|---|---|---|---|
| `format` | `str` | `"parquet"` | Cache file format: parquet \| csv \| json. |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `upstream_asset_key` | `str` | — | — |
| `compute` | `Dict[str, Any]` | — | `{kind: python, python: 'mod:fn'}`. Returns pandas DataFrame. Mutually exclusive with `wraps`. |
| `wraps` | `Dict[str, Any]` | — | Wrap another DCC component's assets with caching instead of defining new compute. Shape: `{type: 'dagster_community_components.<Component>', attributes: {...}}`. Inner asset must return pandas.DataFrame. Mutually exclusive with `compute`. |
| `ttl_seconds` | `float` | — | Cache expiry. If the cached file's mtime is older than this, treat as miss. |
| `key_fn` | `str` | — | Optional `mod:fn` callable mixed into the cache key. |
| `max_entries` | `int` | — | LRU eviction: cap total number of cached parquets in `cache_dir/`. When set, after every miss-write we scan the dir, sort by mtime ascending (oldest = LRU), and delete the oldest files until count <= max_entries. |
| `max_bytes` | `int` | — | LRU eviction: cap total bytes in `cache_dir/`. Same mechanic as max_entries but tallies file sizes. Both can be set simultaneously (whichever cap is hit first triggers eviction). |

[//]: # (FIELDS:END)
