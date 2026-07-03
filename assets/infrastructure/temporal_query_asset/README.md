# temporal_query_asset

Read live state from a running Temporal workflow via a Query — as a Dagster asset. Materialization value IS whatever the workflow's Query handler returns.

## What's a Query, quickly

Temporal Queries are Temporal's synchronous read API for running workflows:
- **Addressed by `workflow_id`** — the currently-running run receives it.
- **Read-only** — Queries cannot mutate workflow state or side-effect.
- **Synchronous return** — the caller blocks until the workflow's Query handler runs and returns.
- **Cheap** — Queries execute inside the workflow's own event loop; no history event is written.

## When to use

Long-lived workflows that hold live in-memory state you want to expose downstream:
- **Batch aggregator** → materialize "current pending count" and "current batch size" as Dagster assets consumed by dashboards.
- **Approval router** → materialize "pending approvals list" as a DataFrame asset for reporting.
- **Streaming pipeline** → materialize "last N records processed" as a smoke-test artifact.
- **Long-running agent** → materialize "current transcript" or "confidence score" for observability.

Because Queries are read-only, this asset is safe at any cadence (schedule, sensor, upstream trigger, manual).

If you want to *start* a workflow, use [`temporal_workflow_trigger`](../temporal_workflow_trigger). If you want to *push* state INTO a workflow, use [`temporal_signal_asset`](../temporal_signal_asset). This component is for **read pulls**.

## Example

```yaml
type: dagster_community_components.TemporalQueryAssetComponent
attributes:
  asset_key: temporal/approvals/pending
  workflow_id: approval-router-v1
  query_name: get_pending
```

The workflow's Query handler:

```python
# On the Temporal worker side.
@workflow.defn
class ApprovalRouterWorkflow:
    def __init__(self):
        self._pending: list[dict] = []

    @workflow.query
    def get_pending(self) -> list[dict]:
        return list(self._pending)

    @workflow.query
    def stats(self) -> dict:
        return {"pending_count": len(self._pending)}
```

The Dagster asset materializes with the returned list — downstream Dagster assets can `deps: [temporal/approvals/pending]` and consume it directly.

## Fields

| Field | Type | Description |
|---|---|---|
| `asset_key` | string | Dagster asset key, `/`-separated. |
| `workflow_id` | string | Target workflow ID. |
| `query_name` | string | Query handler name defined on the workflow. |
| `query_arg` | any | Convenience: single arg (dict/str/int). `{run_id}` / `{partition_key}` / `{partition_keys.<dim>}` are substituted in string fields (recurses into dicts/lists). |
| `query_args` | list | Positional args (multi-arg queries). |
| `query_timeout_seconds` | int | Default 30. Cap on how long to wait for the response. |
| `target_host` / `namespace` | string | Temporal frontend + namespace. |
| `api_key_env_var` / `tls_cert_env_var` / `tls_key_env_var` / `tls_server_root_ca_env_var` | string | Temporal Cloud / self-hosted auth. |
| `group_name` / `description` / `owners` / `tags` / `kinds` / `deps` | — | Standard catalog fields. |

## Materialization metadata

- `temporal_workflow_id`, `temporal_query_name`, `temporal_namespace`, `temporal_target_host`
- `query_result` — the response, rendered as JSON (dict/list) or truncated text (scalars)
- `query_result_count` — list length (when result is a list)

The asset's actual materialized VALUE is whatever the Query returned — full fidelity, no truncation.

## Failure modes

- **Workflow not running** → SDK raises immediately.
- **Query handler not registered** → SDK raises with `Unknown query`.
- **Query timeout** → this component wraps with `asyncio.wait_for`; a hung workflow will time out at `query_timeout_seconds`.
- **Query handler raises** → SDK propagates the exception; Dagster surfaces it as an asset failure.

## Combining with signals

The signal + query pair is the workhorse of live workflow interaction:

```yaml
# 1) Nightly: flush the batch (write)
- type: dagster_community_components.TemporalSignalAssetComponent
  attributes:
    asset_key: temporal/batch/flush
    workflow_id: order-aggregator-v2
    signal_name: flush_batch

# 2) Every 15 min: snapshot current state (read)
- type: dagster_community_components.TemporalQueryAssetComponent
  attributes:
    asset_key: temporal/batch/stats
    workflow_id: order-aggregator-v2
    query_name: stats
    deps: ["temporal/batch/flush"]
```

## Requirements

```
temporalio>=1.7.0
```

## Related

- [`temporal_workflow_trigger`](../temporal_workflow_trigger) — start a workflow.
- [`temporal_signal_asset`](../temporal_signal_asset) — push state into a workflow.
- [`temporal_workflow_sensor`](../../../sensors/temporal_workflow_sensor) — observe terminal state.
