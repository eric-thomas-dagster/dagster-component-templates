# context_receipt_bigquery_sync

Ships **context receipts** from the local SQLite `ContextReceiptStore` into **BigQuery**, so a BigQuery shop can join retrievals, exclusions
and outcomes with the rest of its warehouse and compute item feedback with SQL.

```yaml
type: dagster_community_components.ContextReceiptBigQuerySyncComponent
attributes:
  store_resource_key: context_receipts     # ContextReceiptStoreComponent resource
  bigquery_resource_key: bigquery          # dagster_gcp BigQueryResource
  dataset: my-project.context_receipts
  cron_schedule: "*/10 * * * *"
```

## What lands in BigQuery

The five canonical receipt tables (DDL created with `IF NOT EXISTS`, identical columns to the store): `context_receipts`,
`context_receipt_items`, `context_receipt_excluded`, `context_receipt_events` (all event types, `payload_json` as text -- read with `JSON_VALUE`),
and `context_item_scores` (created, **not** populated: compute feedback/priors in SQL). Plus `context_sync_state(component_name, receipt_cursor, event_cursor, updated_at)`.
Timestamps are `TIMESTAMP`. Use `table_prefix` to namespace.

## How it works

- **Cursors live in BigQuery** (`context_sync_state`), so the sync runs from any host or an ephemeral runner. To force a full re-sync delete the state row.
- **Idempotent.** Each batch is loaded, then the cursor is advanced. A crash in between replays the batch: receipts/items/excluded are replaced per
  `receipt_id` (`DELETE ... IN UNNEST(@ids)` then a load job); events are inserted only if their `event_id` is not already present.
  Loads use load jobs, not streaming inserts (streamed rows cannot be deleted for ~90 minutes). A reader can briefly see a receipt without its items
  while a batch is being replaced. Run one sync per dataset/`sync_name` at a time.
- **Events are complete, straight from the cursor.** `store.read_since` returns each page's receipts (+items, excluded) and *every* event
  (`outcome`, `evidence`, `output_attached`) with its real `event_id`, `occurred_at` and `payload_json`; `event_cursor` advances over all of them. The
  sync ships those rows as-is (no per-receipt re-fetch), so a late `evidence`/`output_attached` event on an old receipt is picked up on the next run
  like any other event. This needs a store whose `read_since` includes `events`.
- `reconcile_recent_receipts` is **deprecated and ignored** (default 0): it was a workaround for `read_since` not carrying non-outcome events. The field is
  still accepted so existing configs load (a non-zero value emits a DeprecationWarning).
- **Transient BigQuery errors are retried** per statement/load job, with exponential backoff and full jitter: `ServiceUnavailable`, `InternalServerError`,
  `TooManyRequests`, `BadGateway`, `GatewayTimeout`/`DeadlineExceeded` (or an HTTP 429/500/502/503/504 `code`), connection resets/timeouts, and a 403 whose
  `errors[0]['reason']` is `rateLimitExceeded` or `quotaExceeded` (recognised by class name/attribute; no google import). Bad SQL, 404, a real 403 and the
  like are never retried. `retry_max_attempts` (default 4; 1 disables) and `retry_max_backoff_seconds` (default 30) set the knobs; the final failure raises the
  original BigQuery error annotated with the attempt count (`.retry_attempts`, plus an exception note on Python 3.11+). Each load job carries a client-chosen
  job id reused across retries, so a timeout that arrives *after* BigQuery accepted the load resubmits as a 409 Conflict and waits on the existing job instead of
  appending the rows twice. (These are separate from the Dagster-level `retry_policy_*` fields, which re-run the whole asset.) A retried or crashed run is
  always safe to re-run: the cursor is written only after the load.
- Output metadata: rows shipped per table, cursors before/after, batches, `drained`, `lag_seconds` (now minus newest shipped timestamp), `load_jobs`, `retries`.

## Load-job quota: sizing `batch_size` and the schedule

BigQuery allows **1,500 load jobs per table per day** (project-wide quota page: "Load jobs per table per day"). Each batch submits at most one load job per table
(`context_receipts`, `_items`, `_excluded`, `_events`; empty ones are skipped) **plus one for `context_sync_state`** (the cursor), so the table that hits the quota
first is the state table: **jobs per table per day ~ batches per run x runs per day**. A run that drains in one batch costs ~1 job per table, so
`*/10 * * * *` (144 runs/day) is ~144/day: comfortable. The danger is a *backlog*: a run walks up to `max_batches` (default 50), so a first sync of a large store
with `batch_size: 1000` and 5M receipts would try 5,000 batches. The run reports `load_jobs` in metadata and logs a **WARNING** when a single run submits more
than 200. Rules of thumb:

- Pick `batch_size` so the normal steady-state run is 1 batch (receipts per run <= `batch_size`); a bigger `batch_size` (up to what a load job can take, e.g. 10,000+) costs
  nothing per job but makes each batch's DELETE/UNNEST ids list longer (ids are chunked at 5,000).
- Keep `max_batches x runs_per_day <= ~1,000` (leave headroom for retries and other writers to the dataset); e.g. `max_batches: 20` on a 30-minute schedule is at most 960/day.
- For a big initial backfill raise `batch_size` and run it on a slower schedule (or by hand) until `drained` is true, then switch to the frequent schedule.
- A retried load job reuses its job id and is counted once; a job that failed and was replaced counts again.

Standard fields: `group_name`, `kinds`, `owners`, `asset_tags`, `deps`, `freshness_max_lag_minutes`/`freshness_cron`, `retry_policy_*`, `cron_schedule`.
