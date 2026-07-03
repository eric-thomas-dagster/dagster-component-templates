# temporal_workflow_trigger

Start a Temporal Workflow from a Dagster asset. Uses the `temporalio` Python SDK (gRPC) — same client whether you're pointed at a local dev-server, a self-hosted cluster, or Temporal Cloud. When `wait_for_result: true` (default), the asset blocks until the workflow completes and stores its return value in materialization metadata.

## When to use this

**Case B — Dagster owns the schedule.** A Dagster schedule / sensor / manual materialization triggers a Temporal Workflow run. Great for:

- **Long-running / durable pipelines** where retries, timers, and state need to survive restarts. Temporal was built for this; Dagster orchestrates the boundary.
- **Existing Temporal workflows** you want to expose in the Dagster catalog with proper lineage.
- **Mixing execution engines** — some steps are Dagster assets, others are Temporal workflows.

If Temporal owns the schedule (a Temporal `Schedule` or cron trigger fires the workflow), use [`external_temporal_workflow`](../../external_assets/external_temporal_workflow) + [`temporal_workflow_sensor`](../../../sensors/temporal_workflow_sensor) instead.

## Example

Local dev-server (`temporal server start-dev`):

```yaml
type: dagster_community_components.TemporalWorkflowTriggerComponent
attributes:
  asset_key: temporal/etl/nightly_aggregation
  workflow_type: NightlyAggregationWorkflow
  task_queue: etl-tq
  workflow_id: "nightly-agg-{run_id}"
  workflow_arg:
    date: "2026-05-27"
    region: us-east-1
  target_host: localhost:7233
  namespace: default
```

Temporal Cloud with API-key auth:

```yaml
type: dagster_community_components.TemporalWorkflowTriggerComponent
attributes:
  asset_key: temporal/etl/nightly
  workflow_type: NightlyAggregationWorkflow
  task_queue: etl-tq
  workflow_id: "nightly-{run_id}"
  target_host: myns.tmprl.cloud:7233
  namespace: myns.abcde
  api_key_env_var: TEMPORAL_CLOUD_API_KEY
```

Temporal Cloud with mTLS:

```yaml
attributes:
  # ...
  target_host: myns.tmprl.cloud:7233
  namespace: myns.abcde
  tls_cert_env_var: TEMPORAL_TLS_CERT   # PEM-encoded client cert
  tls_key_env_var:  TEMPORAL_TLS_KEY    # PEM-encoded private key
```

## Fields

| Field | Type | Description |
|---|---|---|
| `asset_key` | string | Dagster asset key, `/`-separated. |
| `workflow_type` | string | Registered Workflow class name on the worker. |
| `task_queue` | string | Task queue the workflow's worker polls. |
| `workflow_id` | string | Workflow ID. Supports `{run_id}`, `{partition_key}`, `{partition_keys.<dim>}` substitution. |
| `workflow_arg` | any | Convenience: single input arg (dict/str/int). Common single-arg case. |
| `workflow_args` | list | Positional args (multi-arg workflows). If both are set, `workflow_arg` is appended. |
| `wait_for_result` | bool | Default `true`. When `false`, fire-and-forget: workflow_id + run_id land in metadata; pair with the sensor for terminal-status detection. |
| `result_wait_timeout_seconds` | int | Cap on how long to wait for the result. |
| `task_timeout_seconds` / `execution_timeout_seconds` / `run_timeout_seconds` | int | Temporal timeouts. |
| `target_host` | string | Frontend host:port. Default `localhost:7233`. Cloud: `<ns>.tmprl.cloud:7233`. |
| `namespace` | string | Temporal namespace. Default `default`. |
| `api_key_env_var` | string | Env var with a Temporal Cloud API key. |
| `tls_cert_env_var` / `tls_key_env_var` / `tls_server_root_ca_env_var` | string | mTLS env vars (PEM-encoded). |
| `group_name` / `description` / `owners` / `tags` / `kinds` / `deps` | — | Standard Dagster catalog fields. |

## Materialization metadata

- `temporal_workflow_id` — the resolved workflow ID (post-substitution).
- `temporal_run_id` — the specific run ID.
- `temporal_workflow_type` / `temporal_task_queue` / `temporal_namespace` / `temporal_target_host`.
- `temporal_result` (only when `wait_for_result: true`) — the workflow's return value.

## Pairing with the sensor

When `wait_for_result: false`, Dagster starts the workflow and returns immediately. To observe terminal status, deploy a [`temporal_workflow_sensor`](../../../sensors/temporal_workflow_sensor) with `asset_event_type: observation` pointed at the same `asset_key` — it will emit an `AssetObservation` when Temporal reports success/failure.

Do NOT pair the trigger with `external_temporal_workflow` on the same `asset_key` — those cover different schedule-ownership cases.

## Requirements

```
temporalio>=1.7.0
```

For Temporal Cloud API-key auth, `temporalio>=1.8.0` is recommended.
