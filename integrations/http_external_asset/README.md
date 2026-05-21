# HTTP External Asset

Generic wrapper for HTTP-driven external job runners — wrap an "internal job API," a less-common SaaS tool, or anything else with a `trigger → poll → fetch logs` shape as one or more Dagster assets. Configure trigger / status / log endpoints in YAML; the component runs the loop, surfaces structured metadata, and honors all standard Dagster per-asset attributes.

## When to use this — and when NOT to

**Use this when you don't have a dedicated registry component for your tool.** Good fits:

- Internal / in-house job APIs (your team's own service, no SDK).
- Less-common SaaS tools without a dedicated `*_run_asset` / `*_assets` component.
- GitHub Actions / Jenkins / CircleCI / Argo Workflows / Kestra / Azure DevOps style runners.
- Prototyping or wiring something up before investing in a real SDK wrapper.

**Don't use this when there's a dedicated component.** The registry already has tailored components that use real vendor SDKs / OAuth / metadata for these — always prefer them:

| Vendor | Use instead |
|---|---|
| Fivetran | `fivetran_assets`, `fivetran_sync_sensor`, `fivetran_sync_trigger_job` |
| Airbyte | `airbyte_assets`, `airbyte_sync_sensor`, `airbyte_sync_trigger_job` |
| dbt Cloud | `dbt_run_job`, `dbt_cloud_job_sensor`, `dbt_cloud_resource` |
| Matillion | `matillion_run_asset`, `matillion_job_sensor` |
| Rivery | `rivery_run_asset`, `rivery_job_sensor` |
| Precisely | `precisely_run_asset`, `precisely_job_sensor` |
| Coalesce | `coalesce_project`, `coalesce_job_sensor` |
| Autosys | `autosys_asset` |
| Dataiku | `dataiku_asset` |
| Databricks | `databricks_asset_bundle`, `databricks_io_manager`, `databricks_resource` |

## Required packages

```
dagster>=1.8.0
httpx>=0.24.0
jinja2>=3.0.0
jsonpath-ng>=1.6.0
```

## Top-level fields

| Field | Required | Default | Description |
|---|---|---|---|
| `base_url` | Yes | — | Base URL prepended to every endpoint path. Supports Jinja templating. |
| `auth_resource_key` | No | `null` | Resource key of an `HttpAuthResource` subclass. Resource must be wired in your `Definitions` resources dict. |
| `assets` | Yes | — | List of asset specs. At least one. |

## Per-asset fields

Standard Dagster per-asset attributes are at the top level; HTTP-specific config lives under `trigger` / `status` / `logs`.

| Field | Required | Description |
|---|---|---|
| `key` | Yes | Dagster asset key. Use slashes for path-style keys. |
| `description` / `group_name` / `kinds` / `tags` / `metadata` / `owners` / `deps` | No | Standard Dagster asset attributes. |
| `automation_condition` | No | Factory call as a Python expression — e.g. `"on_cron('0 2 * * *')"`, `"eager()"`. |
| `retry_policy` | No | `{max_retries, delay, backoff: linear\|exponential, jitter: full\|plus_minus}`. |
| `code_version` | No | Asset code version string. |
| `partition_type`, `partition_start`, `partition_values`, `dynamic_partition_name`, `partition_dimensions` | No | Canonical registry partition shape. |
| `trigger` | Yes | HTTP request that starts the external run. |
| `status` | Yes | Status-poll spec. |
| `logs` | No | Optional log-fetch + pattern-extraction spec. |

### `trigger`

| Field | Required | Default | Description |
|---|---|---|---|
| `method` | No | `POST` | HTTP method. |
| `path` | Yes | — | Path appended to `base_url`. Supports Jinja templating + `{placeholder}` substitution from `path_params`. |
| `path_params` | No | `{}` | Map substituted into `{placeholder}` segments of `path`. |
| `query_params` | No | `{}` | Query-string parameters. |
| `headers` | No | `{}` | Request headers (templated). |
| `body` | No | `null` | String → sent verbatim (or detected as JSON); dict/list → JSON-serialized; null → no body. |
| `run_id` | Yes | — | Extractor that pulls the external run-id out of the trigger response. |

### `status`

| Field | Required | Default | Description |
|---|---|---|---|
| `method` | No | `GET` | HTTP method. |
| `path` | Yes | — | Status URL. `{run_id}` is auto-substituted from the trigger response. |
| `poll_interval_seconds` | No | `15` | Seconds between polls. |
| `timeout_seconds` | No | `3600` | Max wall-clock seconds to wait for a terminal status. |
| `is_terminal` | Yes | — | Condition that evaluates true when the external run has reached a terminal state. |
| `is_success` | No | — | Condition that, given a terminal response, evaluates true when the run succeeded. Strongly recommended; without it, the component falls back to a heuristic and logs a warning. |
| `metadata` | No | `{}` | Map of `{metadata_key: extractor}`. Extracted values become Dagster materialization metadata. |

### `logs`

Optional. Skipped if absent.

| Field | Required | Default | Description |
|---|---|---|---|
| `method` | No | `GET` | HTTP method. |
| `path` | Yes | — | Log endpoint path. `{run_id}` is auto-substituted. |
| `next_cursor` | No | — | Pagination cursor extractor. If present, the component pages until exhausted or `max_total_bytes`. |
| `max_total_bytes` | No | `10485760` (10 MB) | Hard cap on total bytes fetched. |
| `patterns` | No | `[]` | List of `{name, regex, group?, cast?, mode}` — extracted values become metadata. `mode`: `first_match` \| `count` \| `exists`. |
| `failure_patterns` | No | `[]` | Regex list — if any match, override `is_success` → False even if status said success. |

## Condition language

Conditions appear in `trigger.run_id`, `status.is_terminal`, `status.is_success`, `status.metadata.<key>`, and `logs.next_cursor`.

A condition has **exactly one** extractor source plus an optional operator, **or** a boolean composition (`any_of` / `all_of` / `not`).

```yaml
# JSONPath extraction + equals operator
is_success:
  jsonpath: $.status
  equals: succeeded

# Regex with capture group
run_id:
  source: trigger_response       # or status_response | logs | headers
  regex: "run-id: (\\S+)"
  group: 1

# Header lookup (case-insensitive)
next_cursor:
  source: headers
  header: X-Next-Cursor

# Composition
is_terminal:
  any_of:
    - jsonpath: $.status
      equals: succeeded
    - jsonpath: $.status
      equals: failed
    - jsonpath: $.completed_at
      exists: true
```

### Sources

`status_response` (default) | `trigger_response` | `logs` | `headers`.

### Operators (use exactly one)

`equals: <value>` · `in: [a, b, c]` · `matches: "regex"` · `exists: true|false` · `truthy: true|false` · `gt: <num>` · `lt: <num>`.

If no operator is given, the extractor returns the raw value (used by `metadata` and `run_id`).

### Boolean composition

`any_of: [<cond>, ...]` · `all_of: [<cond>, ...]` · `not: <cond>`. Cannot be combined with leaf extractors at the same level.

## Templating context

Strings under `path` / `path_params` / `query_params` / `headers` / `body` are rendered with Jinja2. Available variables:

- `{{ partition_key }}` — current partition key, or `None` for non-partitioned assets.
- `{{ run_id }}` — Dagster run id during trigger; the **external** run id during status/logs polling.
- `{{ asset_key }}` — slash-joined asset key string.
- `{{ now() }}` — current UTC timestamp ISO string.
- `{{ env.<VAR> }}` — process environment variables.
- `{{ secrets.<KEY> }}` — passed via the resource (currently empty by default; populate via a custom auth resource).
- `{{ deps['<asset_key>'].metadata.<key> }}` — latest materialization metadata of an upstream dep.
- `{{ deps['<asset_key>'].run_id }}` — latest materialization run id of an upstream dep.

### `from_python` escape hatch

For values that templating can't express, drop in:

```yaml
body:
  from_python: "mypkg.helpers:build_request_body"
```

The function receives an `HttpTriggerContext` and returns the value. Useful for HMAC signing, dynamic body assembly, or fetching short-lived tokens.

## Auth resources

Configure auth via a Dagster resource. Built-ins:

- `BearerTokenAuth(token=...)` — `Authorization: Bearer <token>`
- `BasicAuth(username=..., password=...)` — HTTP Basic
- `HeaderAuth(header_name=..., header_value=...)` — custom header (e.g. `X-API-Key`)

Use Dagster `EnvVar()` for secrets:

```python
# definitions.py
from dagster import EnvVar
from dagster_component_templates.HttpExternalAssetComponent import BearerTokenAuth

resources = {
    "api_auth": BearerTokenAuth(token=EnvVar("MY_API_TOKEN")),
}
```

Then reference the key from YAML:

```yaml
auth_resource_key: api_auth
```

For HMAC signing, OAuth refresh, etc., subclass `HttpAuthResource` and override `apply_auth(headers, params) -> (headers, params)`.

<!-- FIELDS:START - auto-generated by tools/regen_readme_fields.py -->

## Fields

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `source` | `ConditionSource` | `"status_response"` | Where to pull the value from. |
| `group` | `int` | — | Capture group for regex extractor. |
| `in` | `List[Any]` | — | — |
| `not` | `Any` | — | — |

<!-- FIELDS:END -->

## Example YAML

```yaml
type: dagster_component_templates.HttpExternalAssetComponent
attributes:
  base_url: https://api.example.com
  auth_resource_key: api_auth
  assets:
    - key: external/orders_sync
      description: Triggers an external orders-sync job and polls until terminal.
      group_name: external
      kinds: [http]
      tags:
        owner: data-platform

      trigger:
        method: POST
        path: /v1/jobs
        body:
          job_type: orders_sync
          partition: "{{ partition_key }}"
        run_id:
          jsonpath: $.id

      status:
        method: GET
        path: /v1/jobs/{run_id}
        poll_interval_seconds: 15
        timeout_seconds: 3600
        is_terminal:
          jsonpath: $.status
          in: [succeeded, failed, cancelled, errored]
        is_success:
          jsonpath: $.status
          equals: succeeded
        metadata:
          rows_processed:
            jsonpath: $.metrics.rows_processed
          duration_ms:
            jsonpath: $.metrics.duration_ms

      logs:
        path: /v1/jobs/{run_id}/logs
        patterns:
          - name: error_count
            regex: "ERROR"
            mode: count
        failure_patterns:
          - "OutOfMemoryError"
```

## Idempotency

When a Dagster run is retried (e.g. `RetryPolicy` triggers), the component checks the Dagster run tags for a previously-stored `external_run_id`. If found, it **skips the trigger** and resumes polling the existing external run instead of starting a new one. This prevents duplicate runs in the upstream system on flaky-network retries.

## Lineage

Use the standard `deps:` field on each asset for upstream lineage. Templated body / params can reference upstream metadata via `{{ deps['<key>'].metadata.<field> }}`.

```yaml
assets:
  - key: external/load_orders
    deps:
      - raw/orders_landed
    trigger:
      body:
        as_of_partition: "{{ deps['raw/orders_landed'].metadata.last_partition }}"
      run_id:
        jsonpath: $.run_id
    status: { ... }
```

## Why use this

- **One config, one ingestion contract.** No bespoke Python per external service.
- **Honors Dagster-native attributes** — partitions, retries, tags, owners, automation, code_version — same as any first-class asset.
- **Idempotent retries** prevent duplicate external runs.
- **Failure-pattern overrides** let you trust HTTP success but flag soft failures hiding in logs.
- **`from_python:` escape hatch** for the rare case where YAML can't express the request shape (HMAC signing, token rotation, etc.).
