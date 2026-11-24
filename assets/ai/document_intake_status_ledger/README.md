# document_intake_status_ledger

A durable, queryable **per-file status ledger** for batch document intake — ingesting a large batch (hundreds to thousands of files) and
needing to know at a glance: *how many files are done? which ones failed, and why? which are stuck mid-processing?*

[`sensors/s3_monitor`](../../../sensors/s3_monitor/README.md) already detects new S3 objects and fires a `RunRequest` per file carrying
`{bucket, key, size, etag, last_modified, prefix, region}` in run_config (or registers each file as a dynamic partition) — but it has no
durable status that survives across runs and sensor ticks. This component is that store, kept deliberately separate: it does not modify
`s3_monitor` (a widely-used, heavily-tested generic component), and it does not wire itself into any particular sensor or pipeline — it's a
small, composable store that your own asset/sensor code reads and writes, the same as any other Dagster resource.

## Five statuses, exactly

`discovered` → `processing` → `extracted` **or** `failed` (with a `detail` message) → `reviewed`. No transition validation is enforced
(it's a status store, not a state machine) — whichever status string you record is accepted, as long as it's one of the five.

## Storage

One local JSONL file. Writes are **append-only on disk** (crash-safe, concurrent-writer-safe — every write is one line); reads **collapse
duplicate `content_hash` keys to the last-written entry** ("latest write per key wins"). This is a *mutable per-file status*, not an event
log — contrast with `write_outcome_ledger_entry` / `read_outcome_ledger_entries` in
[`assets/ai/github_pr_tools/component.py`](../github_pr_tools/component.py), which is an append-only *event* log keyed by `run_id` that
returns every entry.

Each record is keyed by a stable **content hash**: pass a real one when you have it (an S3 `etag` is already a real per-object hash), or
pass `content_hash=None` with `source`/`identifier` set to derive a fallback via `compute_identity_hash(source, identifier)` —
`hashlib.sha256(f"{source}:{identifier}")`. Said honestly: that fallback is an **identity hash of the path**, not a content hash — it
changes if the path changes, not if the file's bytes change. Prefer a real content hash whenever one is available.

## Two pieces

1. **`DocumentIntakeStatusLedger`** — a `dg.ConfigurableResource` (same convention as this repo's other small "resource other components
   read/write" components, e.g. `PlaidResource`): `record_status`, `get_status`, `list_by_status`, `summary`. Registered under
   `resource_key` so your own asset/sensor code can depend on it via `required_resource_keys` and `getattr(context.resources, resource_key)`.
2. **`DocumentIntakeStatusLedgerComponent`** — registers that resource plus a summary asset (`{ledger_name}_intake_summary`) that
   materializes `summary()` with per-status counts as `MetadataValue`, so the counts show up in the Dagster UI without a separate
   dashboard — plus an optional asset check flagging files stuck in `processing` too long.

```yaml
type: dagster_community_components.DocumentIntakeStatusLedgerComponent
attributes:
  ledger_name: invoice_batch_intake
  ledger_path: ./state/invoice_batch_intake.jsonl
  resource_key: invoice_batch_ledger
  stuck_after_minutes: 30
  stuck_severity: WARN
  cron_schedule: "*/10 * * * *"
```

## How it connects to s3_monitor

This component doesn't orchestrate the wiring — **your own downstream asset code** calls into the resource as it processes each file.
`s3_monitor`'s `RunRequest` run_config already carries `etag` (a real per-object content hash):

```python
import dagster as dg

@dg.asset(required_resource_keys={"invoice_batch_ledger"})
def extract_invoice(context: dg.AssetExecutionContext, config: MyRunConfig):
    ledger = context.resources.invoice_batch_ledger
    ledger.record_status(config.etag, "discovered", source=config.bucket, identifier=config.key)
    ledger.record_status(config.etag, "processing")
    try:
        result = do_extraction(config.bucket, config.key)
        ledger.record_status(config.etag, "extracted")
    except Exception as e:
        ledger.record_status(config.etag, "failed", detail=str(e))
        raise
    return result
```

Nothing here requires `s3_monitor` specifically — any sensor or asset that can hand you a stable identifier per file works the same way.

## What you get

| Output | |
|---|---|
| Resource API | `record_status(content_hash, status, *, source=None, identifier=None, detail=None)`, `get_status(content_hash)`, `list_by_status(status, *, stuck_after_minutes=None)`, `summary(*, recent_failed_cap=None)` |
| Asset `{ledger_name}_intake_summary` | materializes `summary()`'s dict (`counts`, `total`, `recent_failed`) |
| Metadata | `count_discovered` / `count_processing` / `count_extracted` / `count_failed` / `count_reviewed`, `total_count`, `recent_failed` (capped list of the most recent failures with their `detail`) |
| Asset check `no_stuck_processing` (opt-in via `stuck_after_minutes`) | fails (WARN or ERROR) when any file has been in `processing` longer than `stuck_after_minutes`; lists the stuck content hashes |
| Schedule (optional) | `cron_schedule` re-materializes the summary asset so it stays current |

**Verified:** real `tests/` — record/get round trip, latest-write-wins collapsing when the same `content_hash` is recorded twice,
`list_by_status` filtering, the `stuck_after_minutes` filter (a backdated entry written directly to the ledger file), `summary()`'s counts
and failed-list capping, an invalid status string raising a clear error, and a real `dg.materialize()` round trip through the summary
asset and its stuck-processing check. Pure local file I/O — nothing to mock.

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Connection

| Field | Type | Default | Description |
|---|---|---|---|
| `resource_key` | `str` | `"document_intake_status_ledger"` | Dagster resource key the ledger is registered under -- reference it from your own asset/sensor code via required_resource_keys. |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `group_name` | `str` | `"ai"` | Asset group |
| `description` | `str` | — | Asset description |
| `owners` | `List[str]` | — | Asset owners (team:name or emails) |
| `kinds` | `List[str]` | — | Asset kinds for the catalog (default: ['ledger', 'status']) |
| `asset_tags` | `Dict[str, str]` | — | Tags added to the asset |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `ledger_name` | `str` | `"document_intake_status_ledger"` | Used to derive the default ledger_path ('./{ledger_name}.jsonl') and the default summary asset name ('{ledger_name}_intake_summary'). |
| `ledger_path` | `str` | — | Path to the local JSONL ledger file. Default: './{ledger_name}.jsonl'. Created (with parent directories) on first write. |
| `summary_asset_name` | `str` | — | Dagster asset name for the summary asset. Default: '{ledger_name}_intake_summary'. |
| `recent_failed_cap` | `int` | `20` | Max number of most-recent failed entries surfaced by the summary asset's metadata. |
| `stuck_after_minutes` | `float` | — | When set, the summary asset also emits an asset check that fails when any file has been in 'processing' longer than this many minutes. Unset (default): no check. |
| `stuck_severity` | `str` | `"WARN"` | Asset check severity when files are stuck: 'WARN' or 'ERROR'. Only used when stuck_after_minutes is set. |
| `cron_schedule` | `str` | — | Optional cron expression; when set, a schedule re-materializes the summary asset on this cadence (e.g. '*/5 * * * *'). |

[//]: # (FIELDS:END)
