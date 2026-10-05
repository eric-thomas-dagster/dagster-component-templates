# Anthropic Batch Submit

Submits rows of an upstream DataFrame as an async **Anthropic Message Batches** job — the ~50%-cheaper, non-latency-sensitive endpoint (`POST /v1/messages/batches`), not the regular synchronous `messages.create` endpoint. Pairs with `anthropic_batch_status_sensor` and `anthropic_batch_results` to form a submit → poll → fetch pipeline that never ties up a Dagster worker waiting on a multi-hour batch:

- **`anthropic_batch_submit`** (this component) builds one request per row and submits the batch, or reattaches to an already-running one.
- **`anthropic_batch_status_sensor`** polls the LIVE batch status and fires a run once it has ended.
- **`anthropic_batch_results`** fetches and parses the finished batch's results.

```yaml
type: dagster_component_templates.AnthropicBatchSubmitComponent
attributes:
  asset_name: support_ticket_batch
  upstream_asset_key: raw_support_tickets
  prompt_column: body
  id_column: ticket_id
  model: claude-haiku-4-5-20251001
  system_prompt: "Classify the sentiment of this support ticket as positive, neutral, or negative. Respond with one word only."
  max_tokens: 50
  wait_for_completion: false
```

## Two modes

- **`wait_for_completion: false` (default)** — submit (or reattach), write a small manifest DataFrame (`batch_id`, `processing_status`, `request_count`), and return immediately. No worker sits idle polling a batch that can legitimately take hours. The real results are fetched later by `anthropic_batch_results` once `anthropic_batch_status_sensor` confirms `processing_status == "ended"`.
- **`wait_for_completion: true`** — poll in-process every `poll_interval_seconds` until the batch ends (or raise after `timeout_seconds`), then parse and return the full results DataFrame inline. Useful for small batches or synchronous testing where simplicity beats worker efficiency.

## Idempotency: content-addressed retry-reattach

Anthropic's `messages.batches.create()` has **no metadata parameter at all** (unlike OpenAI's Batch API, which has `metadata={}`) — there's no way to stash a tracking hash on the vendor side. So this component's idempotency key lives entirely in **Dagster's own materialization metadata**:

1. Every submission computes `prompts_hash = sha256(json.dumps([[custom_id, prompt_text], ...]))` — a deterministic fingerprint of exactly what's being sent.
2. Before submitting, the asset reads back its OWN prior materialization's metadata (`context.instance.get_latest_materialization_event(context.asset_key)` → `event.asset_materialization.metadata["batch_id"].text`).
3. If a prior `batch_id` exists **and** its `prompts_hash` matches the current one: this is a retry or a redundant re-materialization of identical content. The live batch is re-retrieved (`client.messages.batches.retrieve(prior_batch_id)`) and reused — **no resubmission**.
4. If a prior `batch_id` exists **and** the hash differs (the upstream data changed): the stale batch is canceled best-effort (`client.messages.batches.cancel(...)`, wrapped in try/except — cancellation is itself async on Anthropic's side, so this doesn't block) and a fresh batch is submitted.
5. If there's no prior `batch_id`: submit fresh.

This makes re-materializing the asset after a flaky run, a Dagster restart, or an unrelated upstream change behave sanely instead of either silently resubmitting duplicate (billable) work or silently skipping real changes.

## custom_id handling

Anthropic requires every batch request's `custom_id` to match `^[a-zA-Z0-9_-]{1,64}$`. Set `id_column` to a column whose values already fit that pattern (e.g. a numeric ticket ID); a non-matching value raises a clear `ValueError` naming the offending value rather than silently truncating or sanitizing user data. If `id_column` is unset, the row's positional index (stringified) is used instead.

## Prompt construction

Set exactly one of:
- `prompt_column` — the column holding the raw text to send as the user message, verbatim.
- `prompt_template` — a format-string using row values, e.g. `"Summarize this ticket: {body}"` (same convention as `litellm_batch_completion`'s `prompt_template` field — rendered via `str.format(**row_dict)`).

## Metadata written on every materialization

| Key | Meaning |
|---|---|
| `batch_id` | The live (submitted or reattached) batch's ID. |
| `prompts_hash` | The content fingerprint used for retry-reattach. |
| `processing_status` | `in_progress` / `canceling` / `ended` at the moment this run finished. |
| `request_count` | Number of rows submitted in this batch. |

## Verified API facts

- `client.messages.batches.create(requests=[{"custom_id": ..., "params": {"model": ..., "max_tokens": ..., "messages": [...]}}])` — plain dicts, not the typed `Request`/`MessageCreateParamsNonStreaming` constructors (safer across `anthropic` SDK versions).
- `processing_status` is `in_progress` → `canceling` → `ended` — **not** the same enum as OpenAI's per-batch `status` (there is no `completed`/`failed`/`expired` at the batch level; those only appear per-request in `request_counts` and each result's `result.type`).
- Results are fetched via the SDK helper `client.messages.batches.results(batch_id)` (handles the streaming JSONL parsing) — never by manually GETing `results_url`.
- Limits: 100,000 requests OR 256MB per batch, whichever comes first. Results are downloadable for 29 days after creation.

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `asset_name` | `str` | Output Dagster asset name |
| `upstream_asset_key` | `str` | Upstream asset key providing a DataFrame of prompts |

### Connection

| Field | Type | Default | Description |
|---|---|---|---|
| `api_key_env_var` | `str` | `"ANTHROPIC_API_KEY"` | Env var holding the Anthropic API key |

### Execution

| Field | Type | Default | Description |
|---|---|---|---|
| `wait_for_completion` | `bool` | `false` | If True, poll in-process until the batch ends and return the full parsed results DataFrame. If False (default), submit/reattach and return only a small manifest (batch_id, processing_status, request_count) — the real res… _(full docs in schema.json + component README)_ |
| `poll_interval_seconds` | `int` | `30` | Seconds between polls. Only used when wait_for_completion=True. |
| `timeout_seconds` | `int` | `3600` | Max seconds to wait for the batch to end before raising. Only used when wait_for_completion=True. |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `group_name` | `str` | — | Dagster asset group name |
| `owners` | `List[str]` | — | Asset owners — list of team names or email addresses, e.g. ['team:analytics', 'user@company.com'] |
| `asset_tags` | `Dict[str, str]` | — | Additional key-value tags to apply to the asset, e.g. {'domain': 'finance', 'tier': 'gold'} |
| `kinds` | `List[str]` | — | Asset kinds for the Dagster catalog, e.g. ['anthropic', 'python']. Auto-inferred from component name if not set. |
| `description` | `str` | — | Asset description shown in the Dagster catalog. |

### Retry policy

| Field | Type | Default | Description |
|---|---|---|---|
| `retry_policy_max_retries` | `int` | — | Max retries on asset failure. Defines a RetryPolicy. Useful for transient network failures, rate limits, etc. |
| `retry_policy_delay_seconds` | `int` | — | Seconds between retries (default 1). |
| `retry_policy_backoff` | `str` | `"exponential"` | Backoff strategy: 'linear' or 'exponential'. |

### Source / target

| Field | Type | Default | Description |
|---|---|---|---|
| `model` | `str` | `"claude-haiku-4-5-20251001"` | Anthropic model id for each batch request |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `prompt_column` | `Union[str, int]` | — | Column containing the raw text to send as the user message. Set this OR prompt_template, not both. |
| `prompt_template` | `str` | — | Format-string template using row values, e.g. "Summarize: {body}". Set this OR prompt_column, not both. |
| `id_column` | `Union[str, int]` | — | Column to use as each row's custom_id (must match Anthropic's ^[a-zA-Z0-9_-]{1,64}$ pattern — a non-matching value raises a clear error rather than being silently mangled). If unset, the row's positional index (stringified) is used. |
| `system_prompt` | `str` | — | System prompt applied to every request in the batch |
| `max_tokens` | `int` | `1000` | Maximum tokens per completion |
| `temperature` | `float` | `0.0` | Sampling temperature |

[//]: # (FIELDS:END)
