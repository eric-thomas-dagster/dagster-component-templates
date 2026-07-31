# prefect_flow_run

Trigger a Prefect deployment and materialize the flow run as a Dagster asset. Materializing this asset creates a Prefect flow run via `prefect.deployments.run_deployment` and — by default — waits for it to complete. Flow run id, state, state message, and parameters land in the asset's materialization metadata.

**Story**: Dagster owns the schedule, the partition (per-tenant / per-day / per-file), and the asset catalog. Prefect owns the flow's per-run work — including durable execution and runtime-decided task graphs. Each does what it's best at.

Works against:
- **Local server**: `prefect server start` → default `api_url` `http://127.0.0.1:4200/api`.
- **Prefect Cloud**: set `api_url` + `api_key_env_var`.

## Common shapes

- **Unpartitioned trigger** — cron-driven or one-shot. Set `parameters` inline, `wait_for_result: true`.
- **Per-partition trigger** — one Prefect flow run per Dagster partition (per date, per tenant, per file). Reference `{partition_key}` inside parameter values.
- **Fire-and-forget** — set `wait_for_result: false` and pair with `prefect_flow_run_sensor` downstream to react to completions.

## Templating

String parameter values (and `flow_run_name`) support `{partition_key}` and `{run_id}` substitution. Non-string values pass through unchanged.

## Failure semantics

- `wait_for_result: true` + `fail_on_flow_run_failure: true` (default): the Dagster asset fails if the Prefect flow ends in FAILED/CRASHED/CANCELLED. The state message is in the metadata + the raised failure.
- `wait_for_result: true` + `fail_on_flow_run_failure: false`: asset always materializes; inspect state in metadata.
- `wait_for_result: false`: asset materializes immediately after submitting the flow run; downstream check the state via a sensor or another asset.

## Related

- [`prefect_resource`](../../../resources/prefect_resource) — optional shared connection resource.
- [`prefect_flow_run_sensor`](../../../sensors/prefect_flow_run_sensor) — react to Prefect flow completions from Dagster.
