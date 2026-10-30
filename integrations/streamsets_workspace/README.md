# `StreamSetsWorkspaceComponent`

Wrap **StreamSets Control Hub** Jobs as Dagster assets — the control plane for StreamSets DataOps Platform (Data Collector / Transformer pipelines deployed as Jobs). One Dagster asset per configured job, with an optional status-change observation sensor.

## When to use this

- You run CDC or streaming ETL pipelines on StreamSets and want Dagster to own their start/stop lifecycle and have visibility into job status.
- You're consolidating orchestration onto Dagster and want StreamSets jobs to appear in the same asset graph as their downstream warehouse tables.
- You want a `job_active` health check wired into Dagster's asset-check surface instead of watching the Control Hub UI separately.

## Important: StreamSets jobs are usually continuous, not batch

Unlike `ssis_workspace` or `sql_server_stored_procedure_workspace` (which run a package/procedure to completion), a StreamSets job is typically a **continuously-running streaming pipeline** — it doesn't "finish" the way a batch job does. So `action: start` here means **"ensure this job is running"** (start it, then poll until it reaches `ACTIVE`), the same reconcile-to-running semantics `mirth_connect_workspace`'s `action: deploy` uses for its own continuously-running HL7 channels — not "run it and wait for it to complete."

## What's verified vs. well-corroborated

This component is upfront about the gap between what's independently confirmed against official documentation and what's well-corroborated by multiple independent third-party sources but not from a reference this component's author could directly verify — the same transparency convention `mirth_connect_workspace` uses for its own REST surface.

**Verified** against IBM's own Control Hub docs (`ibm.com/docs/SSBH9Y`, "Enabling Access to the REST APIs"):
- Required headers on every call: `Content-Type: application/json`, `X-Requested-By: <any client name>`, `X-SS-REST-CALL: true`, `X-SS-App-Component-Id: <credential ID>`, `X-SS-App-Auth-Token: <auth token>`.
- API credentials are created in Control Hub UI (Manage > API Credentials); the auth token is shown exactly once and cannot be retrieved later — regenerating keeps the same Component ID but issues a new token. Max 10 credentials per user.

**Verified** against StreamSets/IBM's own Job Status documentation: job status is one of `ACTIVE` / `INACTIVE` / `ACTIVATING` / `DEACTIVATING`, each with an "... with error" variant surfaced via a `color` field. `ACTIVATING`/`DEACTIVATING` are pure transition states — no job action is valid while in either.

**Well-corroborated but not independently confirmed** against an official reference: the literal Job Runner endpoint paths —

```
POST {base}/jobrunner/rest/v1/job/{jobId}/start
POST {base}/jobrunner/rest/v1/job/{jobId}/stop
GET  {base}/jobrunner/rest/v1/job/{jobId}/currentStatus
```

Multiple independent third-party sources (an MCP-server implementation guide, a SoftwareAG webMethods connector's documented service names) agree on this shape, but no official StreamSets/IBM reference page's literal path string could be confirmed. Each path lives in exactly one method on `StreamSetsTransport` (`start_job`/`stop_job`/`get_job_status`) — if your Control Hub install uses a different path, patch those three methods, not the asset/sensor logic around them.

There's also no independently-confirmed "list all jobs" endpoint, so (matching `mirth_connect_workspace`'s and `qlik_cloud_workspace`'s own precedent for APIs without a confirmed full-catalog listing call) jobs are declared explicitly via `jobs:`, not discovered. Similarly, no officially-confirmed metrics/throughput field exists on `currentStatus`, so the polling sensor below tracks **status changes only** — a deliberate scope limit, not an oversight.

## Jobs — explicit list, per-job overrides

```yaml
jobs:
  - job_id: "8f6e1f2a-1234-5678-9abc-def012345678:acme-org"
    asset_key: streamsets/orders_cdc_pipeline
  - job_id: "1a2b3c4d-1234-5678-9abc-def012345678:acme-org"
    asset_key: streamsets/clickstream_pipeline
    deps: [streamsets/orders_cdc_pipeline]

assets_by_job:
  "8f6e1f2a-1234-5678-9abc-def012345678:acme-org":
    key: cdc/orders
    group_name: cdc
```

`assets_by_job` is keyed by `job_id`, mirroring the official `dagster-databricks` `DatabricksWorkspaceComponent.assets_by_task_key` shape (also used by `mirth_connect_workspace.assets_by_channel` / `qlik_cloud_workspace.assets_by_app`): `key`/`group_name`/`description`/`kinds`/`owners` replace; `deps`/`metadata`/`tags` merge.

## Actions

- **`action: noop` (default)** — external asset shape, materialize does nothing.
- **`action: start`** — materialize calls the Job Runner start endpoint, then polls `currentStatus` until it reaches `ACTIVE` (raising if it instead lands on `INACTIVE` — the start failed — or times out while `ACTIVATING`).

## Observing status changes

```yaml
polling_sensor: true
check_interval_seconds: 60
```

One shared sensor polls every declared job's `currentStatus` and emits `AssetObservation` only when the status actually changed since the last tick (first sighting is baseline-only — no spurious bootstrap event). Useful for catching a job someone stopped outside of Dagster, or one that transitioned into an error variant.

## Health check

```yaml
require_active: true
```

Attaches a `job_active` asset_check per job that fails when the job's current status isn't `ACTIVE`.

## Demo mode — no Control Hub required

`demo_mode: true` replaces every real REST call with a deterministic synthetic equivalent — start always lands on `ACTIVE`, and the polling sensor occasionally simulates an `ACTIVATING` blip (seeded per job_id) so you can exercise the observation path too. Use it to smoke-test `assets_by_job`, the sensor, and the check with no real Control Hub instance available.

## Companion components

- **`mirth_connect_workspace`** — the closest sibling in shape: another continuously-running integration-engine pipeline with reconcile-to-running `action` semantics and a status/counter polling sensor.
- **`snowflake_workspace` / `sql_transform`** — the natural downstream once a StreamSets CDC pipeline lands data in a warehouse.

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Execution

| Field | Type | Default | Description |
|---|---|---|---|
| `poll_interval_seconds` | `int` | `5` | Seconds between status polls after start. Only used when action=start. |
| `timeout_seconds` | `int` | `120` | Max seconds to wait for a job to reach ACTIVE before failing the run. Only used when action=start. |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `group_name` | `str` | — | Dagster asset group for jobs without their own group_name. |
| `tags` | `Union[Dict[str, Any], List[str]]` | — | Catalog tags applied to every job asset (merged with each job's own tags). |
| `owners` | `List[str]` | — | Default asset owners for jobs without their own. |

### Sensor configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `sensor_name` | `str` | `"streamsets_workspace_observer"` | Sensor name. Only used when polling_sensor=True. |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `control_hub_url_env_var` | `str` | `"STREAMSETS_CONTROL_HUB_URL"` | Env var holding the Control Hub base URL, e.g. https://cloud.streamsets.com (SaaS) or your on-prem Control Hub URL. |
| `component_id_env_var` | `str` | `"STREAMSETS_COMPONENT_ID"` | Env var with the API credential's Component ID (Control Hub > Manage > API Credentials). |
| `auth_token_env_var` | `str` | `"STREAMSETS_AUTH_TOKEN"` | Env var with the API credential's Auth Token. Shown once at credential creation -- regenerate the credential if lost (keeps the same Component ID, issues a new token). |
| `jobs` | `List[StreamSetsJobSpec]` | `list()` | Control Hub Jobs to wrap as Dagster assets, one entry per job. |
| `assets_by_job` | `Dict[str, Dict[str, Any]]` | — | Override the asset spec for specific jobs by job_id. Mirrors the official dagster-databricks `DatabricksWorkspaceComponent.assets_by_task_key` shape (same convention `mirth_connect_workspace.assets_by_channel` / `qlik_cl… _(full docs in schema.json + component README)_ |
| `action` | `str` | `"noop"` | noop (default) = external asset, declare-only, materialize does nothing. start = materialize starts the job and polls currentStatus until it reaches ACTIVE (or raises if it instead lands on INACTIVE, meaning the start fa… _(full docs in schema.json + component README)_ |
| `polling_sensor` | `bool` | `false` | If true, adds ONE sensor (not one per job) that polls each job's currentStatus and emits AssetObservation only when the status changed since the last tick (e.g. a job stopped externally, or transitioned to an error varia… _(full docs in schema.json + component README)_ |
| `check_interval_seconds` | `int` | `60` | Seconds between sensor polls. Only used when polling_sensor=True. |
| `require_active` | `bool` | `false` | If true, attach a `job_active` asset_check per job that fails when the job's current status isn't ACTIVE. |
| `demo_mode` | `bool` | `false` | Skip the real Control Hub connection entirely. Start/stop and currentStatus all return synthetic, deterministic values instead of calling the REST API. Use to smoke-test the whole shape (assets_by_job, the sensor, the ch… _(full docs in schema.json + component README)_ |

[//]: # (FIELDS:END)
