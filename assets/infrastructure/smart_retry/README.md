# `SmartRetryComponent`

Retry classification beyond Dagster's built-in `RetryPolicy`. Instead of retrying every failure equally (max_retries, delay, backoff), classify exceptions at compute time and retry only the ones you want.

**The pain**: `RetryPolicy` retries every failure — including `400 Bad Request` (client error, will never succeed no matter how many retries) alongside `429 Too Many Requests` (rate-limited, retry-with-backoff is the exact right response). No knob to say "retry these, fail immediately on those."

**The fix**: wrap the compute in this component. Rules classify each exception as `transient` (sleep + retry) or `permanent` (fail immediately, no wasted attempts).

## Rules in v1

### `http_status` — for HTTP errors

Matches when the raised exception has a `response.status_code` attribute (`requests.HTTPError`, `httpx.HTTPStatusError`, urllib `.code`).

```yaml
- kind: http_status
  transient_codes: [429, 500, 502, 503, 504]   # sleep + retry
  permanent_codes: [400, 401, 403, 404, 422]   # fail immediately
```

Codes not in either list fall through to the next rule.

### `exception_class` — for anything Python

Matches by dotted class path (or bare builtin name). Uses `isinstance` — subclasses match too.

```yaml
- kind: exception_class
  transient:
    - ConnectionError
    - TimeoutError
    - "requests.exceptions.ConnectionError"
    - "psycopg2.OperationalError"
  permanent:
    - ValueError
    - KeyError
    - "pydantic.ValidationError"
```

## Compute kinds

The wrapped work can be one of:

### `kind: python` — a Python callable

```yaml
compute:
  kind: python
  python: "my_project.enrichers:enrich_orders"
```

Callable signature: `fn()`, `fn(context)`, or `fn(context, upstream)`. The component picks the widest signature the function supports.

### `kind: shell` — a shell command

```yaml
compute:
  kind: shell
  cmd: "./scripts/enrich.sh --date=$DAGSTER_PARTITION_KEY"
  timeout_seconds: 300
```

`context.partition_key` gets threaded in as `DAGSTER_PARTITION_KEY` env var. Non-zero exit → `RuntimeError` (classification rules can catch this via `exception_class: RuntimeError` if you want it retryable).

### `kind: http` — one-off HTTP call

```yaml
compute:
  kind: http
  method: POST
  url: "https://api.example.com/enrich"
  headers: {authorization: "Bearer $API_TOKEN"}
  body: {batch_size: 100}
  timeout_seconds: 30
```

`resp.raise_for_status()` runs automatically — so the `http_status` rule can classify 4xx/5xx without extra work.

## Backoff

```yaml
retry_policy:
  max_attempts: 5
  backoff: exponential   # exponential | linear | fixed
  initial_delay_seconds: 1.0
  max_delay_seconds: 60.0
  jitter: true            # 50-100% of computed delay (thundering-herd protection)
```

Defaults: `max_attempts=3, backoff=exponential, initial=1.0, max=60.0, jitter=true`.

The sleep happens **inside the compute** (blocks the step), then classification decides retry vs fail. This is by design — Dagster's step-level `RetryPolicy` re-launches the whole step which is expensive; in-compute retry is cheaper for the same asset compute.

## Full example

```yaml
type: dagster_community_components.SmartRetryComponent
attributes:
  asset_name: enriched_orders
  upstream_asset_key: raw_orders

  compute:
    kind: python
    python: "my_project.enrichers:enrich_orders"

  retry_rules:
    - kind: http_status
      transient_codes: [429, 500, 502, 503, 504]
      permanent_codes: [400, 401, 403, 404, 422]
    - kind: exception_class
      transient: [ConnectionError, TimeoutError, "requests.exceptions.ConnectionError"]
      permanent: [ValueError, KeyError, "pydantic.ValidationError"]

  retry_policy:
    max_attempts: 5
    backoff: exponential
    initial_delay_seconds: 1.0
    max_delay_seconds: 60.0
    jitter: true

  group_name: ingest
  owners: [platform-team@example.com]
```

If `enrich_orders` raises `HTTPError(429)`: `http_status` rule matches → transient → sleep (1s + jitter) → retry. On second `HTTPError(429)`: sleep 2s → retry. Attempt 5: fail with `Failure` describing "exhausted 5 attempts."

If it raises `HTTPError(400)`: `http_status` rule matches → permanent → fail immediately on attempt 1. No wasted retries.

If it raises `KeyError`: `exception_class` rule matches → permanent → fail immediately.

If it raises `SomeOtherException`: no rule matches → treated as retryable (falls through to attempt limit).

## Metadata on the materialization

Every run reports:
- `attempts` (int) — how many attempts were tried
- `compute_kind` (str) — python / shell / http

On failure:
- `classification` — permanent / transient / unclassified
- `http_status` — status code if extractable, else -1
- `exception_class` — the raised exception's class name

## What's coming in v2

- **LLM classification** — a small LLM call ("is this error transient?") for unclassified errors. Gate behind `enable_llm_fallback` (costs one LLM roundtrip per unclassified error).
- **Sliding-window rate limiting** — "no more than 3 retries per 60-second window" to prevent runaway retry loops.
- **Circuit breaker** — after N total failures, refuse to attempt for M seconds. Prevents burning API budget on a downed dependency.

## `@smart_retry` decorator — wrap an EXISTING asset

The component above defines a new asset from YAML. To add retry to an asset defined elsewhere (Python decorator, another component's compute), use the decorator:

```python
import dagster as dg
from dagster_community_components import smart_retry

@dg.asset(ins={"raw": dg.AssetIn(key="raw_orders")})
@smart_retry(
    rules=[
        {"kind": "http_status",
         "transient_codes": [429, 500, 502, 503, 504],
         "permanent_codes": [400, 401, 403, 404, 422]},
        {"kind": "exception_class",
         "transient": ["ConnectionError", "TimeoutError"],
         "permanent": ["ValueError", "KeyError"]},
    ],
    max_attempts=5,
    backoff="exponential",
    initial_delay_seconds=1.0,
    max_delay_seconds=60.0,
    jitter=True,
)
def enriched_orders(context, raw):
    return call_api(raw)   # existing user code, unchanged
```

**Same classification + backoff engine** as the component. Args mirror the component's `retry_rules` + `retry_policy` fields.

Apply BEFORE `@dg.asset` (or `@dg.op`, `@dg.multi_asset`) so the compute is what gets wrapped. `context.log.*` is used for progress messages if the function's first positional arg has a `.log` attribute (standard Dagster asset signature).

On PERMANENT classification → raises `dg.Failure` immediately with classification metadata.
On exhausted retries → raises `dg.Failure` with attempt count + last classification.
On success → returns the underlying function's return value unchanged.

## Why not just use Dagster's `RetryPolicy`?

`RetryPolicy` retries EVERY failure the same way. That's fine for stateless compute failures ("worker died mid-step, retry"), but breaks for API failures where the code MEANS something. This component encodes what customers actually want: "retry only the errors that could plausibly succeed next time."

The two are complementary — use RetryPolicy for worker-level resilience (max_retries=1) and this component for compute-level classification.

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `asset_name` | `str` | Dagster asset name. |
| `compute` | `Dict[str, Any]` | How to run the actual work. Shape: `{kind: python\|shell\|http, ...kind-specific fields}`. python: `python: 'mod.path:func'`. shell: `cmd: '...'` or `cmd: [list]` + optional `timeout_seconds`. http: `method, url, headers… _(full docs in schema.json + component README)_ |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `group_name` | `str` | — | — |
| `description` | `str` | — | — |
| `owners` | `List[str]` | — | — |
| `tags` | `Dict[str, str]` | — | — |
| `kinds` | `List[str]` | — | Asset kinds. Default: ['python', 'retry']. |

### Partitions

| Field | Type | Default | Description |
|---|---|---|---|
| `partition_type` | `str` | — | — |
| `partition_start` | `str` | — | — |
| `partition_values` | `Any` | — | — |
| `partition_dimensions` | `List[Dict[str, Any]]` | — | — |

### Retry policy

| Field | Type | Default | Description |
|---|---|---|---|
| `retry_rules` | `List[Dict[str, Any]]` | `list()` | Ordered classification rules. Each: `{kind: http_status \| exception_class, ...}`. First matching rule wins. If no rule classifies, the error re-raises as-is (Dagster's default Failure handling applies). |
| `retry_policy` | `Dict[str, Any]` | — | Backoff config: `{max_attempts: 5, backoff: exponential\|linear\|fixed, initial_delay_seconds: 1.0, max_delay_seconds: 60.0, jitter: true}`. Defaults: max_attempts=3, backoff=exponential, initial=1.0, max=60.0, jitter=true. |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `upstream_asset_key` | `str` | — | Optional upstream asset the compute callable receives as second arg. |
| `dynamic_partition_name` | `str` | — | — |

[//]: # (FIELDS:END)
