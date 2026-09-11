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

### Caching — resumable on re-execute-from-failure

Pass `cache_key_fn` (and a cache backend) to `@task` and matching keys become cache hits. Cache keys are automatically scoped to the run's lineage via `root_run_id`:

- **Same failed run → resumed via "Re-execute from failure"** — succeeded `@task` calls from the prior attempt are cache hits; the failed step re-runs.
- **Net-new run** — fresh `root_run_id`, so no cache bleed from any prior run.

```python
from dagster_community_components import task, FilesystemTaskCache
import dagster as dg

_cache = FilesystemTaskCache(base_dir="/tmp/task_cache", ttl_seconds=3600)

@task(cache=_cache, cache_key_fn=lambda ctx, url: url)
def parse_url(context, url):
    return expensive_scrape(url)

@dg.asset
def parse_document(context):
    for url in urls:
        parse_url(context, url)   # cached per URL, survives run retry
```

Cache hits still render — the child_step wraps the (skipped) execution and the log line is tagged `[cache_hit]`, so the graph node still shows up with near-zero duration.

Without `cache_key_fn`, caching is off — the default. `cache_ttl_seconds` is optional; unset = never expire.

**Cache backends** (interchangeable — anything satisfying the `TaskCache` protocol):

- `FilesystemTaskCache(base_dir, ttl_seconds=None)` — pickle files under `<base>/<sha256(key)>.pkl`. Zero-config for local + shared-filesystem deployments.
- `IOManagerBackedTaskCache(io_manager, ttl_seconds=None)` — **wraps any Dagster IOManager as a cache backend**. If you already have an s3/gcs pickle IO manager for asset outputs, reuse it here for free — no separate cache storage to provision. Works with filesystem / s3_pickle / gcs_pickle out of the box; database-schema IO managers that require a real run_id row won't.
- Custom — implement the `TaskCache` protocol (`get(key) → value | TaskCache.MISS` + `put(key, value)`).

Alternative to `cache=`: supply `cache_resource="task_cache"` and declare the resource on the parent asset (Dagster filters undeclared resources from step context).

For cross-run memoization ("scrape this URL once ever"), use a real Dagster asset with an IO manager, not `@task` caching — this is deliberate.

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
