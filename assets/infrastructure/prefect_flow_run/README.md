# prefect_flow_run

Trigger a Prefect deployment and materialize the flow run as a Dagster asset. Materializing this asset creates a Prefect flow run via `prefect.deployments.run_deployment` and — by default — waits for it to complete. Flow run id, state, state message, and parameters land in the asset's materialization metadata.

**Story**: Dagster owns the schedule, the partition (per-tenant / per-day / per-file), and the asset catalog. Prefect owns the flow's per-run work — including durable execution and runtime-decided task graphs. Each does what it's best at.

Works against:
- **Local server**: `prefect server start` → default `api_url` `http://127.0.0.1:4200/api`.
- **Prefect Cloud**: set `api_url` + `api_key_env_var`.

**Requires Prefect 3.0+.** Verified directly: Prefect 2's client schema classes (`LogFilter`, etc.) aren't Pydantic v2 models the way Prefect 3's are — `stream_logs`/`stream_artifacts` won't work against a 2.x server as written.

## Common shapes

- **Unpartitioned trigger** — cron-driven or one-shot. Set `parameters` inline, `wait_for_result: true`.
- **Per-partition trigger** — one Prefect flow run per Dagster partition (per date, per tenant, per file). Reference `{partition_key}` inside parameter values.
- **Fire-and-forget** — set `wait_for_result: false` and pair with `prefect_flow_run_sensor` downstream to react to completions.

## Templating

String parameter values (and `flow_run_name`) support `{partition_key}`, `{run_id}`, and `{partition_window_start}` / `{partition_window_end}` (ISO 8601, time-window partitions only — empty string otherwise). Non-string values pass through unchanged.

## Failure semantics

- `wait_for_result: true` + `fail_on_flow_run_failure: true` (default): the Dagster asset fails if the Prefect flow ends in FAILED/CRASHED/CANCELLED. The state message is in the metadata + the raised failure.
- `wait_for_result: true` + `fail_on_flow_run_failure: false`: asset always materializes; inspect state in metadata.
- `wait_for_result: false`: asset materializes immediately after submitting the flow run; downstream check the state via a sensor or another asset.

## Cancellation

`forward_termination` (default `true`): if the Dagster run is terminated/interrupted while waiting, the Prefect flow run is cancelled via the plain Prefect SDK — no `dagster-prefect` dependency needed. This mirrors `dagster-prefect`'s own Pipes client behavior of the same name, built independently on `client.set_flow_run_state(id, Cancelling())`. Live-verified: a SIGINT mid-wait produces a Prefect flow run that reaches `CANCELLED`, confirmed via the Prefect API.

## Observability without a shared filesystem or blob store

The official `dagster-prefect` Pipes client's default message reader is a temp file — it only works when the Dagster step and the Prefect worker share a filesystem, which isn't true for Dagster+ or any worker on separate infrastructure. Without a reachable reader, `dagster-prefect`'s own docs say the asset still materializes on success, but silently *without* the metadata/logs/checks the flow reported — a quiet failure mode, not an error.

- `stream_logs` (default `false`): forwards the flow's own Prefect log lines into the Dagster run log while waiting, via `read_logs` with a timestamp cursor. Live-verified: INFO/WARNING lines from inside the flow appear in the Dagster run log in near-real-time, each exactly once.
- `stream_artifacts` (default `false`): forwards Prefect artifacts the flow creates (`create_markdown_artifact`, `create_table_artifact`, `create_progress_artifact`, `create_link_artifact`, `create_image_artifact` — calls the flow may already be making, no Dagster-awareness required) as `AssetObservation` events, mapped onto the matching `MetadataValue` type (markdown → `md`, table → `json`, progress → `float`, image/link → `url`). Live-verified against a real server.

Both cost one extra API call per poll tick; both read from the same store the Prefect UI itself reads from — no S3/GCS bucket, no shared volume.

## Check convention

`check_names` (requires `stream_artifacts: true`, `wait_for_result: true`, `execution_mode: poll` — validated at build time) turns a matching Prefect table artifact into a real `AssetCheckResult`. The flow writes `create_table_artifact(key="row-count-check", table=[{"passed": True, "rows": 1200}])` — a one-row table, Prefect's own artifact shape — and declares `check_names: [row_count_check]`.

The KEY translation is required, not cosmetic: Prefect artifact keys must be lowercase letters/digits/dashes (Prefect rejects underscores), while Dagster check names must match `^[A-Za-z0-9_]+$` (Dagster rejects dashes) — verified directly, these two systems' naming rules conflict. This component translates automatically (dashes read as underscores) so `check_names: [row_count_check]` matches an artifact keyed `row-count-check`.

A declared check that never gets a matching artifact on a given run is reported as `passed=False` with an explanatory description, not silently skipped — verified directly that Dagster hard-fails the whole step (`DagsterStepOutputNotFoundError`, a confusing engine error) if a declared check gets no result at all, so this component always reports *something* for every declared name.

## Pipes mode (`execution_mode: pipes`)

Opt-in delegation to the official `dagster-prefect` package's `PipesPrefectDeploymentClient`. Requires `pip install dagster-prefect` and the flow to open a Pipes session (`open_dagster_pipes()`) — a real code change, unlike the default `poll` mode. Useful when a flow wants to report arbitrary typed metadata or asset checks mid-run through Pipes' own protocol rather than through the artifact convention above. `timeout_seconds`, `fail_on_flow_run_failure`, `stream_logs`, `stream_artifacts`, and `check_names` are all ignored in this mode (logged as warnings) — Pipes has its own equivalents.

## Related

- [`prefect_background_task`](../prefect_background_task) — the same trigger-and-observe shape for a single Prefect `@task` instead of a whole `@flow`.
- [`prefect_resource`](../../../resources/prefect_resource) — optional shared connection resource.
- [`prefect_flow_run_sensor`](../../../sensors/prefect_flow_run_sensor) — react to Prefect flow completions from Dagster.

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `asset_name` | `str` | Dagster asset name (or '/' path). |
| `deployment_name` | `str` | Prefect deployment name in 'flow_name/deployment_name' format. |

### Connection

| Field | Type | Default | Description |
|---|---|---|---|
| `api_key_env_var` | `str` | — | Env var holding a Prefect Cloud API key. Leave unset for local server. |

### Execution

| Field | Type | Default | Description |
|---|---|---|---|
| `parameters` | `Dict[str, Any]` | — | Parameters passed to the flow run. String values are templated: `{partition_key}` substitutes the current partition key; `{run_id}` substitutes the Dagster run_id; `{partition_window_start}` / `{partition_window_end}` su… _(full docs in schema.json + component README)_ |
| `timeout_seconds` | `int` | — | Only used when wait_for_result=True. Max seconds to wait for the flow run. None = wait indefinitely. If exceeded, the asset raises. |
| `poll_interval_seconds` | `float` | `5.0` | Only used when wait_for_result=True. Seconds between polls. |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `tags` | `List[str]` | — | Tags applied to the created flow run. |
| `group_name` | `str` | — | Asset group. |
| `description` | `str` | — | Asset description. |
| `owners` | `List[str]` | — | Asset owners. |
| `asset_tags` | `Dict[str, str]` | — | Extra asset tags. |
| `kinds` | `List[str]` | — | Asset kinds. Defaults to ['prefect']. |
| `deps` | `List[str]` | — | — |

### Freshness

| Field | Type | Default | Description |
|---|---|---|---|
| `freshness_max_lag_minutes` | `int` | — | — |
| `freshness_cron` | `str` | — | — |

### Partitions

| Field | Type | Default | Description |
|---|---|---|---|
| `partition_type` | `str` | — | 'daily'\|'weekly'\|'monthly'\|'hourly'\|'static'\|'dynamic'\|'multi'\|None |
| `partition_start` | `str` | — | — |
| `partition_values` | `str` | — | — |
| `partition_dimensions` | `List[Dict[str, Any]]` | — | — |

### Retry policy

| Field | Type | Default | Description |
|---|---|---|---|
| `retry_policy_max_retries` | `int` | — | — |
| `retry_policy_delay_seconds` | `int` | — | — |
| `retry_policy_backoff` | `str` | `"exponential"` | — |

### Source / target

| Field | Type | Default | Description |
|---|---|---|---|
| `stream_logs` | `bool` | `false` | Only in execution_mode='poll'. Forward the flow's own Prefect logs into the Dagster run log while waiting, via Prefect's read_logs API — no shared filesystem or blob store required (unlike dagster-prefect's Pipes message… _(full docs in schema.json + component README)_ |
| `stream_artifacts` | `bool` | `false` | Only in execution_mode='poll'. Forward Prefect artifacts the flow creates (create_markdown_artifact, create_table_artifact, create_progress_artifact, create_link_artifact, create_image_artifact — calls the flow may alrea… _(full docs in schema.json + component README)_ |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `wait_for_result` | `bool` | `true` | Wait for the flow run to reach a terminal state before the Dagster asset finishes materializing. When False, the asset returns immediately after submitting the flow run — use this for fire-and-forget triggers and pair wi… _(full docs in schema.json + component README)_ |
| `flow_run_name` | `str` | — | Optional flow run name. Templated with `{partition_key}` and `{run_id}` like parameter values. |
| `api_url` | `str` | `"http://127.0.0.1:4200/api"` | Prefect API URL. Default is local server at :4200. |
| `ui_url` | `str` | — | Base URL of the Prefect UI, used to build the 'Prefect Run URL' materialization metadata link. Defaults to api_url with its trailing '/api' stripped, which is correct for a local/self-hosted server. Prefect Cloud serves… _(full docs in schema.json + component README)_ |
| `fail_on_flow_run_failure` | `bool` | `true` | When True and wait_for_result=True, the Dagster asset fails if the Prefect flow run ends in a non-COMPLETED state (FAILED, CRASHED, CANCELLED). When False, the asset always materializes successfully — inspect the state i… _(full docs in schema.json + component README)_ |
| `forward_termination` | `bool` | `true` | When wait_for_result=True, cancel the Prefect flow run if the Dagster run is terminated/interrupted while waiting. Uses the plain Prefect SDK (no dagster-prefect dependency) — same behavior dagster-prefect's Pipes client… _(full docs in schema.json + component README)_ |
| `check_names` | `List[str]` | — | Requires stream_artifacts=True, wait_for_result=True, and execution_mode='poll' (validated at build time). Declares these as AssetCheckSpecs on the asset. Convention: the flow writes a table artifact (create_table_artifa… _(full docs in schema.json + component README)_ |
| `execution_mode` | `str` | `"poll"` | 'poll' (default) — trigger + poll via the plain Prefect SDK, zero flow code changes required. 'pipes' — delegate to dagster-prefect's PipesPrefectDeploymentClient for in-flight metadata/log streaming; requires `pip insta… _(full docs in schema.json + component README)_ |
| `dynamic_partition_name` | `str` | — | — |

[//]: # (FIELDS:END)
