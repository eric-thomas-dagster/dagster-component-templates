# temporal_signal_asset

Push state INTO a running Temporal workflow via a Signal — as a Dagster asset. This is the write side of the Dagster ↔ Temporal integration.

## What's a Signal, quickly

Temporal Signals are the canonical way for outside code to send events to a running Workflow. Signals are:
- **Addressed by `workflow_id`** — not by run_id. Whatever run of that workflow is active receives it.
- **Fire-and-forget** from the caller's side (durably queued by Temporal until the workflow's Signal handler picks them up).
- **Typed by name** — the workflow declares `@workflow.signal` handlers by name; you send by that name.

## When to use

Long-lived Temporal workflows that need external nudges:
- **Batch aggregator** — workflow accumulates orders indefinitely; nightly Dagster asset signals `flush_batch`.
- **Approval router** — workflow waits for approver decisions; Dagster asset signals `record_approval` when an upstream reviewer commits.
- **Agent supervisor** — long-lived agent workflow; Dagster asset signals `add_prompt`/`shutdown_gracefully` from downstream logic.
- **Human-in-the-loop** — a Dagster asset that materializes when a person clicks "approve"; the materialization signals into a pending workflow.

If you want to *start* a workflow, use [`temporal_workflow_trigger`](../temporal_workflow_trigger). If you want to *read* current state from one, use [`temporal_query_asset`](../temporal_query_asset). This component is for **write pushes**.

## Example

```yaml
type: dagster_community_components.TemporalSignalAssetComponent
attributes:
  asset_key: temporal/batch/flush_signal
  workflow_id: order-aggregator-v2
  signal_name: flush_batch
  signal_arg:
    reason: nightly
    requested_at: "{run_id}"
```

The workflow's Signal handler:

```python
# Python worker code — this is on the Temporal side, not Dagster.
@workflow.defn
class OrderAggregatorWorkflow:
    def __init__(self):
        self._orders: list = []
        self._flush_now = False

    @workflow.signal
    def flush_batch(self, args: dict) -> None:
        workflow.logger.info(f"Received flush_batch: {args}")
        self._flush_now = True

    @workflow.run
    async def run(self):
        while True:
            await workflow.wait_condition(lambda: self._flush_now or len(self._orders) >= 1000)
            # ... flush + reset ...
            self._flush_now = False
```

## Fields

| Field | Type | Description |
|---|---|---|
| `asset_key` | string | Dagster asset key, `/`-separated. |
| `workflow_id` | string | Target workflow ID (whichever run is active receives the signal). |
| `signal_name` | string | Signal handler name defined on the workflow. |
| `signal_arg` | any | Convenience: single arg. `{run_id}`, `{partition_key}`, `{partition_keys.<dim>}` are substituted inside string fields (recurses into dicts/lists). |
| `signal_args` | list | Positional args (multi-arg signals). |
| `target_host` / `namespace` | string | Temporal frontend + namespace. |
| `api_key_env_var` / `tls_cert_env_var` / `tls_key_env_var` / `tls_server_root_ca_env_var` | string | Temporal Cloud / self-hosted auth. |
| `group_name` / `description` / `owners` / `tags` / `kinds` / `deps` | — | Standard catalog fields. |

## Materialization metadata

- `temporal_workflow_id`, `temporal_signal_name`, `temporal_namespace`, `temporal_target_host`
- `signal_sent_at` — UTC ISO timestamp
- `signal_args_count`, `signal_args_preview` (JSON or truncated text)

## Failure modes

- **Workflow not running** → the SDK raises. Materialization fails; Dagster surfaces the error. (Temporal doesn't queue signals for non-existent workflows.)
- **Wrong signal name** → the workflow's Signal handler simply won't fire; the signal is queued but ignored. Prefer descriptive `signal_name` and version handlers carefully.
- **Auth failure** → SDK raises immediately.

## Requirements

```
temporalio>=1.7.0
```

## Related

- [`temporal_workflow_trigger`](../temporal_workflow_trigger) — start a workflow.
- [`temporal_query_asset`](../temporal_query_asset) — read live state.
- [`temporal_workflow_sensor`](../../../sensors/temporal_workflow_sensor) — observe terminal state.
