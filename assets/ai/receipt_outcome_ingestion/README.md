# receipt_outcome_ingestion

Turns **real feedback data** into context-receipt outcomes. The receipts learning loop (`record_outcome` -> `item_feedback` ->
rerank priors -> the `recipe_outcomes` check, all in `resources/context_receipt_store`) only learns when something calls
`store.record_outcome`. This asset is that something: it reads feedback rows from a warehouse table, files, or an HTTP JSON API,
maps each row to an outcome, and records it -- idempotently and incrementally.

## Prerequisite: feedback rows must carry a receipt id

An outcome attaches to a **receipt**, so every feedback row needs the `receipt_id` the application got when it served the answer
(`record_retrieval` returns it; the gateway/rag asset records it). The app has to carry it through to wherever feedback is captured:
a column on the feedback table, a hidden field on the thumbs widget, a ticket custom field / `external_id`, message metadata for a Slack
bot. A feedback system that never saw the receipt id cannot be ingested -- there is no fuzzy matching. Rows that name an id the store
does not know are counted (`unknown_receipts`), never fatal.

## How it behaves

| | |
|---|---|
| **Idempotent** | Every outcome gets a deterministic `outcome_ref`. With `mapping.outcome_ref` (e.g. a user id) it is `<source label>:<key>`, so a changed rating by the same user **supersedes** the old one. Without it, it is a hash of (label, receipt, `occurred_at`, signal, value): re-reading identical feedback yields the same ref. Either way re-ingesting never double-counts. Two *byte-identical* ref-less rows collapse to one -- give the rows a key column if that matters. |
| **Incremental** | The cursor (max `occurred_at` handled) is the asset's own `cursor` materialization metadata, read back via `context.instance` -- no local files. It is bound into `{since}` (sql) / `since_param` (http) and also filtered in Python. Reads are **inclusive** (`>= cursor`): boundary rows are re-read, which is safe, but each re-read appends another event (the store is append-only; the *effective* outcome does not change). Without `mapping.occurred_at` there is no cursor and every run reads everything. |
| **Writes** | Synchronous, not `AsyncReceiptWriter`, so the cursor only advances past outcomes that are durably recorded. Against the BigQuery store each outcome is a query + load: keep `max_rows` modest there. |
| **Bad rows** | Missing receipt id, unmapped signal, value outside [-1,1] or the `scale`, non-numeric value, bad timestamp. With `on_bad_row: skip` (default) they are counted, with the first 5 in `bad_row_examples`; with `fail` the run raises **before recording anything**. |
| **Unknown receipts** | `unknown_receipts` + `unknown_receipt_examples`. `on_unknown_receipt: fail` records the known ones then raises. An unknown-receipt row **younger than `unknown_receipt_grace_hours`** (default 24, by its `occurred_at`) holds the cursor back to just before it and is retried next run (feedback can arrive before its receipt is durable: async writer, multi-host); it is counted in `unknown_receipts_pending`. Older ones are dropped and the cursor moves past them. Re-reading is safe (outcome_ref idempotency). With `max_rows`, a held row inside the batch stalls progress until it ages out of the grace window; `0` disables the retry. |
| **max_rows** | Oldest-first by `occurred_at`; the cursor lets following runs catch up. HTTP/SQL results are fully fetched before truncating. A batch is never cut inside a group of rows sharing one timestamp (it extends to the end of the group), and a batch of only re-read boundary rows extends to the next timestamp, so every run makes progress even when more than `max_rows` rows share a timestamp. |
| **max_pages / `order`** | `source.order: asc` (default, oldest-first API) or `desc` (newest-first). If `max_pages` stops the read, an `asc` run advances the cursor to the newest record read (the unread ones are newer). A `desc` run reads the *newest* pages, so unread records are *older*: the cursor does **not** advance unless the read reached the previous cursor (`cursor_held_reason: desc_truncated_before_previous_cursor`, plus a warning). On a newest-first API set `max_pages` high enough to reach the previous cursor (the first run has no cursor, so it needs the whole backlog). `read_truncated` is in the metadata. |

Materialization metadata: `rows_read`, `rows_before_cursor`, `outcomes_recorded`, `superseded_in_run`, `skipped_rows`, `unknown_receipts`
(+examples), `unknown_receipts_pending`, `bad_rows` (+examples), `cursor`, `cursor_held_reason`, `read_truncated`, `lag_seconds`.

## Mapping

- `receipt_id` (required): column / field. JSON/HTTP rows accept dotted paths (`satisfaction_rating.score`).
- `signal` (column) **or** `signal_value` (constant). A signal not in the store's `OUTCOME_SIGNALS` (`thumbs_up`, `thumbs_down`, `accepted`,
  `edited`, `escalated`, `reopened`, `closed_won`, `closed_lost`, `reformulated`) needs a numeric value.
- `value` (column): number in [-1, 1], optionally mapped with `scale: {min, max}` (linear: CSAT 1-5 -> -1..1; out-of-scale = bad row).
- `value_map`: `{raw: signal-or-number}`, matched case-insensitively and numerically (`5`, `5.0`, `"5"` all match). A string target is a
  signal name; a number target is the value. Applies to the `signal` column, or the `value` column when there is no signal column.
- `skip_values`: raw values to ignore silently (counted as `skipped_rows`), e.g. a survey that was `offered` but not answered.
- `outcome_ref`, `source` (label, default `feedback`), `occurred_at`.

## Presets (plain example YAML, no special code)

These show mapping shapes. Field names of third-party APIs are from memory of their docs -- check them against your account's actual payload.

**A `feedback` SQL table** (DuckDB / Snowflake / BigQuery-style resource):
```yaml
type: dagster_community_components.ReceiptOutcomeIngestionComponent
attributes:
  source:
    kind: sql
    resource_key: warehouse
    query: "SELECT receipt_id, rating, user_id, created_at FROM feedback WHERE created_at >= {since}"
  mapping:
    receipt_id: receipt_id
    value: rating
    scale: {min: 1, max: 5}
    signal_value: csat
    outcome_ref: user_id
    occurred_at: created_at
    source: app_feedback
```

**Zendesk CSAT via http_json** (tickets carry the receipt id as `external_id`; the CSAT endpoint alone has no custom fields):
```yaml
attributes:
  source:
    kind: http_json
    url: https://acme.zendesk.com/api/v2/search.json?query=type:ticket%20satisfaction:good%20satisfaction:bad
    headers_env: {Authorization: ZENDESK_BASIC_AUTH}   # env var holds "Basic <base64 email/token:key>"; never logged
    records_path: results
    next_url_path: next_page
  mapping:
    receipt_id: external_id
    signal: satisfaction_rating.score
    value_map: {good: thumbs_up, good_with_comment: thumbs_up, bad: thumbs_down, bad_with_comment: thumbs_down}
    skip_values: [offered, unoffered]
    outcome_ref: id
    occurred_at: updated_at
    source: zendesk_csat
```

**Slack-reaction export file** (one row per reaction, written by your Slack bot / export job):
```yaml
attributes:
  source: {kind: file, path: ./exports/slack_reactions/*.jsonl}
  mapping:
    receipt_id: receipt_id          # the bot posts the answer with the receipt id in message metadata
    signal: reaction
    value_map: {"+1": thumbs_up, thumbsup: thumbs_up, "-1": thumbs_down, thumbsdown: thumbs_down}
    outcome_ref: user               # one vote per user; changing the reaction supersedes
    occurred_at: ts
    source: slack_reactions
```

Other `http_json` options: `since_param` (+ `since_format: iso|epoch`), `page_param` (increments until an empty page), `next_url_path` (a next URL on another scheme/host is refused, and cross-host redirects are refused, so `headers_env` secrets are only ever sent to the configured host),
`max_pages` (default 50), `order` (`asc`|`desc`), `headers` (non-secret), `headers_env` (`{Header-Name: ENV_VAR}`; a missing variable fails the run without
echoing anything).

### Transient failures

- **`http_json`**: connection errors, timeouts (`source.timeout_seconds`, default 30) and HTTP 429/500/502/503/504 are retried with exponential backoff and **full jitter**;
  a `Retry-After` header (seconds or HTTP-date) is honoured, capped by `retry_max_backoff_seconds`. Any other 4xx (401/403/404...) fails immediately with an error naming the
  status (never the headers or secrets). Malformed JSON and a missing/ill-typed `records_path` are clear, non-retried errors. `retry_max_attempts` (default 4, 1 disables) and
  `retry_max_backoff_seconds` (default 30) are component fields.
- **A page that fails after its retries fails the run, and the cursor does not move.** The whole source read finishes before anything is recorded, and the cursor is written only
  by a successful run's materialization, so a run that dies on page 2 records nothing from page 1 and the next run starts from the old cursor (re-reading is safe: deterministic `outcome_ref`s).
- **`sql`**: connection-level errors (OSError/`OperationalError`/`InterfaceError`...) retry up to 3 tries; a bad query is never retried.
- **`file`**: jsonl lines that do not parse (corruption, a half-written last line) are counted as bad rows (`bad_rows`, honouring `on_bad_row`), not a crash. A csv/json/parquet file whose
  mtime/size changes while it is being read is re-read after a backoff, and fails with a clear error if it never settles.

Tested against a real `ContextReceiptStore`, real csv/jsonl/json/parquet files, a real DuckDB resource and a local HTTP server
(`python3 -m pytest assets/ai/receipt_outcome_ingestion/tests -q`).
