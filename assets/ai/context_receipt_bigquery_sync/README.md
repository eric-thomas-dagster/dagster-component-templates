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
- Output metadata: rows shipped per table, cursors before/after, batches, `drained`, `lag_seconds` (now minus newest shipped timestamp).

Standard fields: `group_name`, `kinds`, `owners`, `asset_tags`, `deps`, `freshness_max_lag_minutes`/`freshness_cron`, `retry_policy_*`, `cron_schedule`.
