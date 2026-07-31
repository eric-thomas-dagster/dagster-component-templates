# dynamic_fanout_asset

**Generic DynamicOut fan-out as a Dagster asset.** The asset-lineage sibling of `dynamic_fanout_job`. User provides 3 Python callables (`discover` / `process` / `collect` via `module:function` paths in YAML); the component builds the ops, wires them into a `@graph_asset`, and emits a single asset whose materialization is the collected result of parallel per-item processing.

**How it works:**

```
<upstream_asset_key>  (optional)
      │
      ▼  @graph_asset  <asset_name>
            _discover  ← @op(out=DynamicOut()), yields N items
              │
              ├── _process[key_1]  ← @op via .map(), runs in parallel
              ├── _process[key_2]
              └── _process[key_N]
              │
            _collect(results)      ← optional aggregation via .collect()
      │
      ▼  materialized value → consumed by downstream assets
```

**Set `mapping_key_field`** when items are dicts and one field is a stable id — DynamicOutput mapping_keys become stable across retries.

**When to reach for this vs [`dynamic_fanout_job`](../../../jobs/dynamic_fanout_job):**
- Fan-out result is a data artifact your downstream assets consume → **this component**.
- Fan-out is a side-effect-only batch (write to a queue, hit an API for each item) → the job version.

**When to reach for this vs partitioned assets:**
- Items are stable, addressable, worth tracking in the catalog long-term (per-tenant, per-date, per-region) → partitioned asset.
- Items are transient within a run (URLs in a queue, today's tickets) → this component.

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `asset_name` | `str` | Name of the emitted @graph_asset. |
| `discover_callable_path` | `str` | `"module:function"` returning an iterable of items. Signature: `discover(upstream=..., **discover_kwargs) → List[item]`. |
| `process_callable_path` | `str` | `"module:function"` that takes one item and returns a result. Signature: `process(item, **process_kwargs)`. |

### Callable configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `discover_kwargs` | `Dict[str, Any]` | — | Static kwargs passed to `discover`. |
| `process_kwargs` | `Dict[str, Any]` | — | Static kwargs merged into each `process` call. |
| `collect_callable_path` | `str` | — | Optional `"module:function"` that takes the list of results and returns the asset's final value. Omit to materialize the raw list. |
| `mapping_key_field` | `str` | — | If items are dicts, use this field as the DynamicOutput mapping_key (stable per-item retries). |

### Lineage

| Field | Type | Default | Description |
|---|---|---|---|
| `upstream_asset_key` | `str` | — | Optional asset to depend on. Its materialized value passes into `discover` as `upstream=<value>`. |

### Runtime

| Field | Type | Default | Description |
|---|---|---|---|
| `retry_max_retries` | `int` | — | Per-item retry max. |
| `retry_delay_seconds` | `int` | `1` | Seconds between retries. |
| `retry_backoff` | `str` | `"exponential"` | `"linear"` \| `"exponential"`. |
| `max_concurrent_tag_value` | `str` | — | If set, applies `dagster/concurrency_key=<value>` to the process op so an instance-level concurrency limit governs parallelism. |
| `fail_on_empty` | `bool` | `false` | Fail the graph if discover returns no items. |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `group_name` | `str` | — | Asset group. |
| `description` | `str` | — | Asset description. |
| `owners` | `List[str]` | — | Asset owners. |
| `tags` | `Dict[str, str]` | — | Extra asset tags. |
| `kinds` | `List[str]` | `['fanout', 'transform']` | Asset kinds. |

## Related components

- [`dynamic_fanout_job`](../../../jobs/dynamic_fanout_job) — the job-lineage sibling (same runtime shape, produces a `@dg.job` instead of an asset).
- [`llm_multi_path_router`](../../ai/llm_multi_path_router) — domain-specific router agent that uses the same fan-out pattern internally when in `fanout_mode`.
