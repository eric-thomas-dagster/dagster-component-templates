# `TaskAssetComponent` + `@task` + `@task_asset` + `child_step`

Dynamic, runtime-declared sub-steps for Dagster assets. Three decorators / one YAML component / one context manager — each solving a different shape.

## The four shapes at a glance

| | `@task` (alone) | `@task_asset` | `TaskAssetComponent` (YAML) | `child_step` (context manager) |
|---|---|---|---|---|
| Discover work at runtime | ✅ | ✅ | ✅ | ✅ |
| Branch on real task results | ✅ | ❌ (calls return `None` at record time) | ❌ | ✅ |
| Arbitrary nesting depth | ✅ | 1 layer | N compile-time-known layers | ✅ |
| Graph shows individual calls as nodes | ❌ (log tab only) | ✅ | ✅ | ❌ (log tab only) |
| Parallel execution | ❌ (sequential) | ✅ | ✅ | ❌ (sequential) |
| Runtime state / durations queryable | ✅ | ✅ | ✅ | ✅ |
| Result caching (survives failure retry) | ✅ | via `@task` inside | ❌ | ❌ |

Pick per use case. Everything below composes freely.

## `@task` — imperative, log-attribution

Wrap any Python function. Call it normally from inside a `@dg.asset`. Each call emits `STEP_START` / `STEP_SUCCESS` / `STEP_FAILURE` events with a hierarchical step_key like `<parent>[task.<name>.<seq>]`. Log tab shows the nested structure with real durations + status.

```python
from dagster_community_components import task
import dagster as dg

@task
def parse_url(context, url):
    ...

@task
def parse_text(context, block):
    for url in extract_urls(block):
        parse_url(context, url)      # nested — stacked step_key

@dg.asset
def parse_document(context):
    doc = load()
    for block in doc["blocks"]:
        parse_text(context, block)   # imperative, real results
```

Perfect for: agentic tool-use loops, API pagination, ML early stopping, recursive parsers — anywhere branching on task RESULT matters more than graph render.

### Call-site name override — `task_name=`

Every `@task` call accepts a `task_name` keyword to override the default step name — useful when the same task fans over a collection and you want each call to render with a meaningful identifier:

```python
for i, block in enumerate(doc["blocks"]):
    parse_text(context, block, task_name=f"text_{i}")   # step_key includes text_0, text_1, ...
    for j, url in enumerate(extract_urls(block)):
        parse_url(context, url, task_name=f"url_{i}_{j}")
```

Without `task_name`, calls auto-suffix by seq (`_1`, `_2`, ...) — fine for one-off calls, ugly for fan-outs.

### Caching — full Prefect `@task` parity

Every Prefect `@task` cache feature has a `@task` in DCC equivalent, plus a few Dagster-native extras. Simplest form — zero-config:

```python
from dagster_community_components import task
import dagster as dg

@task(cache=True)                  # auto-hash inputs + filesystem backend
def parse_url(context, url):
    return expensive_scrape(url)

@dg.asset
def parse_document(context):
    for url in urls:
        parse_url(context, url)    # cached per URL — same inputs = hit
```

`cache=True` auto-configures a `FilesystemTaskCache` at `/tmp/dagster_task_cache/<task_name>/` and enables input-hashed cache keys (Prefect's `task_input_hash` default). Zero decorator params required for common cases.

#### Composable cache policy — Prefect `CachePolicy` parity

Compose behavior with `+`:

```python
from dagster_community_components import task, INPUTS, TASK_SOURCE, CROSS_RUN, NO_CACHE
from datetime import timedelta

@task(cache=True, cache_policy=INPUTS + TASK_SOURCE, cache_expiration=timedelta(hours=1))
def parse_url(context, url):
    return expensive_scrape(url)
```

| Building block | Effect |
|---|---|
| `INPUTS` | Hash args/kwargs into the cache key (Prefect's `task_input_hash`). |
| `TASK_SOURCE` | Include the function's source-code hash — invalidates cache when the function body changes. |
| `ROOT_RUN` | Scope to `root_run_id` (default; survives re-execute-from-failure). |
| `RUN_ONLY` | Scope to the specific `run_id` (this run only). |
| `CROSS_RUN` | No run scoping — cache shared across all runs ("parse this URL once ever"; Prefect's default). |
| `NO_CACHE` | Disable caching entirely, even if `cache=` is set. |

Combine freely: `INPUTS + TASK_SOURCE + CROSS_RUN` = inputs-hashed, invalidates on source edits, shared across runs.

#### Refresh overrides — decorator + per-run + per-call

Three ways to bypass a hit:

```python
# 1. Per-task always-refresh (Prefect refresh_cache=True at decorator level):
@task(cache=True, refresh_cache=True)
def probe(context):
    ...

# 2. Per-run override (via Dagster run tag — Prefect .submit(refresh_cache=True)):
#    dg launch --assets my_asset --tags refresh_cache=true
#    dg.materialize([my_asset], tags={"refresh_cache": "true"})

# 3. Per-call bypass (this specific invocation):
result = parse_url(context, url, task_no_cache=True)
```

#### `cache_expiration` — accepts `timedelta`

```python
from datetime import timedelta

@task(cache=True, cache_expiration=timedelta(hours=1))     # Prefect shape
def parse_url(context, url): ...

@task(cache=True, cache_ttl_seconds=3600)                  # Original DCC name — kept for compat
def parse_url(context, url): ...
```

#### Custom cache_key_fn — override the auto-hash

If your inputs need special hashing (a DataFrame's content, a normalized URL, etc.):

```python
@task(cache=True, cache_key_fn=lambda ctx, url: url.lower().rstrip("/"))
def parse_url(context, url): ...
```

Explicit `cache_key_fn` wins over `INPUTS` when both are set. `TASK_SOURCE`, run scoping, and refresh signals still apply on top.

Cache hits still render — the child_step wraps the (skipped) execution and the log line is tagged `[cache_hit]`, so the graph node still shows up with near-zero duration.

#### Cache backends

- `FilesystemTaskCache(base_dir, ttl_seconds=None, max_entries=None, max_bytes=None)` — pickle files under `<base>/<sha256(key)>.pkl`. Optional LRU eviction: `max_entries` and/or `max_bytes` cap the cache dir, evicting oldest files (by mtime) after each put.
- `IOManagerBackedTaskCache(io_manager, ttl_seconds=None)` — **wraps any Dagster IOManager as a cache backend**. Reuse an existing s3_pickle / gcs_pickle / azure_blob_pickle IO manager as the cache store — no separate cache storage to provision.
- Custom — implement `TaskCache.get(key) → value | TaskCache.MISS` + `put(key, value)`.

Alternative to `cache=`: supply `cache_resource="task_cache"` and declare the resource on the parent asset.

### Prefect `@task` ↔ DCC `@task` parity

| Prefect `@task` feature | DCC `@task` |
|---|---|
| `cache_key_fn` | `cache_key_fn=` (explicit) or `INPUTS` in `cache_policy` (auto-hash) |
| `cache_expiration` | `cache_expiration=` (accepts `int` / `float` / `timedelta`) |
| `cache_policy` | `cache_policy=` — composable `CachePolicy` with `+` |
| `refresh_cache=True` | `refresh_cache=True` (decorator) OR `--tags refresh_cache=true` (per-run) OR `task_no_cache=True` (per-call) |
| `task_input_hash` (default) | `INPUTS` (module-level constant) — used automatically when `cache=True` |
| `TASK_SOURCE` | `TASK_SOURCE` (module-level constant) |
| Cross-run cache | `CROSS_RUN` (opt-in; Dagster defaults to `ROOT_RUN` for re-execute-from-failure survival) |
| Persistent result / result storage | `IOManagerBackedTaskCache` on any Dagster IO manager |
| Custom serializer | Comes from the IO manager choice |
| Async task functions | `@task` auto-detects `async def` and wraps for sync execution |
| `retry_condition_fn` (retry on specific exceptions) | `retry_condition_fn=lambda ctx, exc: isinstance(exc, HTTPError)` + `max_retries=N` |
| Concurrency limits (tag-based) | `concurrency_pool="gpu" + max_concurrent=3` (in-process semaphore) |

### Async tasks

Native Python `async def` functions work as `@task` — the decorator detects them and wraps for sync execution:

```python
@task(cache=True)
async def fetch_url(context, url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            return await resp.text()

@dg.asset
def scrape(context):
    return fetch_url(context, "https://example.com")   # runs the coroutine synchronously
```

Uses `asyncio.run()` when called from a sync context (Dagster ops); isolates in a thread when nested inside an existing event loop.

### `retry_condition_fn` — retry on specific exceptions

Prefect parity — decide whether to retry based on the exception:

```python
import requests

@task(
    retry_condition_fn=lambda context, exc: isinstance(exc, requests.HTTPError) and exc.response.status_code in {429, 502, 503},
    max_retries=3,
    retry_delay_seconds=5.0,
)
def call_api(context, endpoint):
    r = requests.get(endpoint)
    r.raise_for_status()
    return r.json()
```

When the predicate returns True, the task raises `dg.RetryRequested(max_retries=..., seconds_to_wait=...)` — Dagster retries the whole step with fresh state. When False (or predicate itself raises), the original exception propagates.

### Concurrency pools — cap simultaneous invocations

Prefect's tag-based concurrency limits — cap how many `@task` calls run at once across all tasks sharing a pool name:

```python
@task(concurrency_pool="gpu", max_concurrent=3)
def gpu_inference(context, batch):
    return heavy_model.predict(batch)

@task(concurrency_pool="gpu", max_concurrent=3)   # SAME pool, SAME cap
def gpu_embedding(context, texts):
    return embedding_model.encode(texts)
```

Both tasks share one pool of 3 slots — at most 3 concurrent invocations across both. All tasks binding a given pool_name must agree on `max_concurrent` (mismatch raises at decoration time).

Backed by a `threading.Semaphore` — **in-process concurrency control**. For cross-run / cross-process pools (e.g., "5 GPU licenses shared across every run in the fleet"), use the `rpa_queue_concurrency_lock` component — event-log-backed, works across processes.

### `child_step` — the primitive underneath

`@task` is sugar over `child_step(context, op_name, mapping_key=None)`. Use it directly when you want log-tab structure without wrapping a function:

```python
from dagster_community_components import child_step

@dg.asset
def scrape_batch(context, urls):
    for url in urls:
        with child_step(context, "scrape", mapping_key=url.replace("/", "_")):
            fetch(url)
```

Step_key shape: `<parent>.<op_name>[<mapping_key>]` — mirrors Dagster's `DynamicOutput` convention. If `mapping_key` isn't supplied, an auto-seq is used. Arbitrary depth of nesting via a contextvars-backed stack (thread- and async-safe).

## `@task_asset` — imperative + graph fan-out

Same imperative style as `@task`, but calls inside are RECORDED and fanned out via `DynamicOutput` after the body finishes. Every call becomes a distinct graph node under `<asset>.run_task[<name>]`.

```python
from dagster_community_components import task, task_asset
import dagster as dg

@task
def parse_title(context, doc): ...

@task
def parse_url(context, url): ...

@task_asset(
    group_name="parsing",
    description="Per-doc extraction with runtime fan-out",
)
def parse_document(context):
    doc = load()
    parse_title(context, doc)                # → graph node run_task[parse_title]
    for i, block in enumerate(doc["blocks"]):
        if block["kind"] == "text":
            parse_text(context, block, task_name=f"text_{i}")
            for j, url in enumerate(extract_urls(block)):
                parse_url(context, url, task_name=f"url_{i}_{j}")
```

Result: one graph per doc, `scan → run_task[?] → collect`, with `run_task[?]` fanning out into one node per recorded `@task` call. Real graph render, real per-call durations, parallel execution.

**Constraint**: `@task` calls inside `@task_asset` return `None` at record time — you can't branch on their return value. If you need branching, use `@task` outside a `@task_asset`.

### `@task_asset` signature

| Arg | Default | What |
|---|---|---|
| `name` | fn.__name__ | Asset name |
| `group_name` | none | Dagster asset group |
| `description` | none | Asset description |
| `partitions_def` | none | Any Dagster `PartitionsDefinition` — one run per partition |

### Composes with partitions

```python
docs_partitions = dg.StaticPartitionsDefinition([...])

@task_asset(partitions_def=docs_partitions)
def parse_document(context):
    doc = load(context.partition_key)   # one doc per partition
    parse_title(context, doc)
    for block in doc["blocks"]: ...
```

Backfill 100 partitions → 100 runs, each with its own runtime-discovered fan-out shape. Standard Dagster ergonomics.

## `TaskAssetComponent` — YAML shape for layered pipelines

For "scan → process → enrich → …" pipelines with N compile-time-known layers where each layer's fan-out width is runtime-discovered. Auto-scaffolds `collect + re-emit` bridges between layers so any depth is legal (Dagster rejects two dynamic outputs feeding one op — the bridge sidesteps that).

```yaml
type: dagster_community_components.TaskAssetComponent
attributes:
  asset_name: parse_document
  upstream_asset_key: raw_documents      # optional upstream DataFrame/list

  layers:
    - name: scan
      compute: "my_project.parser:scan_doc"
    - name: process_block
      compute: "my_project.parser:process_block"
    - name: parse_url
      compute: "my_project.parser:parse_url"

  terminal: "my_project.parser:summarize"

  group_name: parsing
  description: Multi-layer parse pipeline with runtime fan-out per layer.
  owners: ["team:data-platform"]
  tags: { domain: content }
  kinds: [python, task, custom-parser]
```

Graph: `scan → process_block[?] → bridge → parse_url[?] → terminal`. Each `[?]` fans out at runtime.

### Layer callables

Every layer's `compute` is a `mod:fn` reference. Signatures:

- **First layer** (the scan) — `(context) -> iterable of (task_name, task_spec)`. `task_spec` is any JSON-safe object — passed as input to the next layer.
- **Middle layers** — `(context, task_spec) -> list of (task_name, task_spec)` to fan out to the next layer, OR return a plain value (terminal for this branch).
- **Terminal** (optional `terminal:` field) — `(context, results: list) -> asset_value`. Runs once after the final layer collects.

## CLI demos using this template

- **[`task_asset` walkthrough](https://github.com/eric-thomas-dagster/dagster-community-components-cli/blob/main/examples/task_asset.md)** — all four shapes side-by-side in one project, 100% offline. Scaffolds `@task` alone (9 nested synthetic events), `@task_asset` (9 real graph fan-out nodes), `TaskAssetComponent` YAML (scan → 6 mapped `parse_url_process` nodes → terminal reducer), and `@task` + `FilesystemTaskCache` (cache pickle files under `.task_cache/`). Setup script:

  ```bash
  curl -fsSL https://raw.githubusercontent.com/eric-thomas-dagster/dagster-community-components-cli/main/examples/setup_task_asset_demo.sh | bash
  ```

## What's NOT supported

**Sequential graph nodes where N is runtime-unknown AND each depends on prior's REAL output** — every workaround has an honest cost (RetryRequested conflates with retry semantics; fabricated events are log-only; pre-declared slots pollute the graph with SKIPPED nodes). Tracked in [docs/FEEDBACK_dynamic_ops_in_run_graph.md](../../../docs/FEEDBACK_dynamic_ops_in_run_graph.md) as a Dagster core ask.

For agentic loops / pagination / early stopping today: use `@task` (log attribution only).

## Composes with other DCC decorators

- **`@smart_retry`** — retry classification on individual `@task` fns.
- **`@sla`, `@timeout`** — enforce SLA / hard deadlines on the whole asset.
- **`@budget`** — LLM cost tracking for agentic pipelines.
- **`@snapshot`** — save the final `@task_asset` output.
- **`@cached`** — separate asset-level content-addressable cache (see `cached_asset`). Different job than `@task` caching — asset-scoped cross-run memoization vs sub-task retry-resumption.

## Public API

Exports from `dagster_community_components`:

- Decorators: `task`, `task_asset`
- Context manager: `child_step`
- Cache: `TaskCache` (protocol), `FilesystemTaskCache`, `IOManagerBackedTaskCache`
- Component: `TaskAssetComponent`

## `TaskAssetComponent` fields

### Required

| Field | Type | Description |
|---|---|---|
| `asset_name` | `str` | Dagster asset name |
| `layers` | `List[LayerSpec]` | Ordered processing layers. Each layer has `name` (str, becomes the mapped op's graph key) and `compute` (str, `mod:fn` reference to the callable) |

### Optional

| Field | Type | Description |
|---|---|---|
| `upstream_asset_key` | `str` | Upstream Dagster asset — routed as `upstream` input to the first-layer scan |
| `terminal` | `str` | `mod:fn` — receives `(context, all_results: list)`, returns the asset's final value |
| `group_name` | `str` | Dagster asset group |
| `description` | `str` | Asset description (default: auto-generated per layer count) |
| `owners` | `List[str]` | Dagster asset owners |
| `tags` | `Dict[str, str]` | Dagster asset tags |
| `kinds` | `List[str]` | Dagster asset kinds (default: `['python', 'task']`) |

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `name` | `str` | Layer name. Becomes the mapped op's key in the run graph. |
| `compute` | `str` | 'mod:fn' — the callable that processes items at this layer. |

[//]: # (FIELDS:END)
