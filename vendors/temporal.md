# Temporal

[Temporal](https://temporal.io/) is a durable-execution platform for long-running workflows — retries, timers, signals, and state that survive process restarts and datacenter failures. Common landings: batch aggregators, approval routers, streaming pipelines, human-in-the-loop workflows, long-lived LLM agents.

Temporal is a **workflow engine**, not a data orchestrator. Dagster's role in a Temporal-adopting stack is the connective tissue: put durable workflows into the catalog with lineage, gate downstream data work on their state, and drive them from Dagster's schedule / sensor / partition model. The community registry ships **5 Temporal components** covering all four modes of Dagster ↔ Temporal interaction.

## Four modes at a glance

|                              | one-shot workflow                            | long-lived workflow                 |
|------------------------------|----------------------------------------------|-------------------------------------|
| **fire from Dagster**        | `temporal_workflow_trigger`                  | `temporal_signal_asset`             |
| **read from Dagster**        | `temporal_workflow_sensor` + `external_temporal_workflow` | `temporal_query_asset`  |

## Components

| Component | Category | What it does | Validation |
|---|---|---|---|
| [`temporal_workflow_trigger`](https://dagster-component-ui.vercel.app/c/temporal_workflow_trigger) | infrastructure | Materializable asset that starts a Temporal workflow via the `temporalio` SDK (gRPC). Waits for the result by default; captures workflow_id, run_id, and result in asset metadata. Works against local dev-server, self-hosted clusters, or Temporal Cloud. | `live` |
| [`temporal_workflow_sensor`](https://dagster-component-ui.vercel.app/c/temporal_workflow_sensor) | sensor | Polls Temporal Visibility (`workflow_id` or `list_filter`) for terminal state. On `COMPLETED`, emits `AssetMaterialization` / `AssetObservation` + fires a downstream job. On `FAILED` / `CANCELED` / `TERMINATED` / `TIMED_OUT`, surfaces the status as `skip_reason`. | `live` |
| [`external_temporal_workflow`](https://dagster-component-ui.vercel.app/c/external_temporal_workflow) | external asset | Declare-only `AssetSpec` — puts a Temporal workflow (or Schedule) in the Dagster catalog with clickable Temporal UI URL. Downstream `deps:` work, lineage shows up. No execution. | `live` |
| [`temporal_signal_asset`](https://dagster-component-ui.vercel.app/c/temporal_signal_asset) | infrastructure | Dagster asset that PUSHES state into a running long-lived workflow via a Signal (e.g. `add_order`, `flush_batch`, `shutdown`). Fire-and-forget from the SDK's side; captures signal name + args preview in metadata for lineage. | `live` |
| [`temporal_query_asset`](https://dagster-component-ui.vercel.app/c/temporal_query_asset) | infrastructure | Dagster asset that PULLS live state from a running workflow via a Query (read-only). The Query's return value IS the materialized asset value — perfect for exposing pending items / current counters / accumulated state as first-class Dagster assets. | `live` |

## Pairing patterns

**Case A — Temporal owns the schedule (observe-only).** Vercel-hosted workflow schedules, application-code-driven `start_workflow` calls, cron in Temporal Schedule. Pair `external_temporal_workflow` + `temporal_workflow_sensor` with `asset_event_type: materialization`. Same `asset_key` across both.

**Case B — Dagster owns the schedule.** Dagster asset triggers a workflow (potentially per partition). Pair `temporal_workflow_trigger` + `temporal_workflow_sensor` with `asset_event_type: observation` if `wait_for_result: false`, or omit the sensor when the trigger waits synchronously.

**Case C — Long-lived workflow interaction.** Long-running workflow with `@signal` and `@query` handlers. Use `temporal_signal_asset` to push events in and `temporal_query_asset` to materialize its current state as a Dagster asset. Both work orthogonally to any of the schedule-ownership cases.

**Do NOT mix** `external_temporal_workflow` and `temporal_workflow_trigger` on the same `asset_key` — they represent different schedule ownership and would double-count materializations.

## Connection / auth — quick reference

| Surface | Setting | Notes |
|---|---|---|
| Frontend host | `target_host: localhost:7233` | Local dev-server default. Temporal Cloud: `<namespace>.tmprl.cloud:7233`. |
| Namespace | `namespace: default` | Local dev. Temporal Cloud: `<namespace>.<account_id>` (e.g. `myns.abcde`). |
| Auth: local dev | none | `temporal server start-dev` runs anonymous. |
| Auth: Temporal Cloud (API key) | `api_key_env_var: TEMPORAL_CLOUD_API_KEY` | Requires `temporalio>=1.8.0`. |
| Auth: mTLS | `tls_cert_env_var` + `tls_key_env_var` | Traditional Temporal Cloud auth; always supported. |
| Custom root CA | `tls_server_root_ca_env_var` | Rarely needed on Temporal Cloud. |

All 5 components accept the same auth fields — swap the pair to point at Cloud vs. dev vs. self-hosted with zero code change.

## Walkthroughs

- [Temporal Workflow (trio: trigger + external + sensor)](https://github.com/eric-thomas-dagster/dagster-community-components-cli/blob/main/examples/temporal_workflow.md) — full E2E via local dev-server + a real Python worker + SWAPI activity.
- [Temporal Signal + Query](https://github.com/eric-thomas-dagster/dagster-community-components-cli/blob/main/examples/temporal_signal_query.md) — long-lived `OrderBatchWorkflow` with `@signal` + `@query` handlers; scripted `query → signal add → query → signal flush → query` sequence with visible state transitions.

## Where Dagster adds value that Temporal alone doesn't

- **Catalog placement + lineage.** Temporal workflows become first-class Dagster assets. Downstream dbt models / SQL transforms / dashboards depend on them via `deps:`.
- **Cross-tool orchestration.** Temporal has no notion of "wait for the S3 file to arrive" or "gate on Snowflake table freshness." Dagster does — Temporal workflows fit into wider DAGs alongside Snowflake / Databricks / dbt / Fivetran / Airbyte / etc.
- **Materializations on live state.** A running Temporal workflow's `pending_orders` list becomes a Dagster asset with lineage, freshness policies, and downstream `deps:`. Temporal alone offers Query but doesn't surface it in a catalog.
- **Signals from Dagster's world.** Long-lived Temporal workflows often need to react to *data-side* events (S3 landing, schedule tick, upstream asset materializing). Dagster is the natural source of those signals; `temporal_signal_asset` is the wire.

## Gotchas — most now handled by the components

- **Signals are addressed by `workflow_id`.** Whatever run is currently active receives them. If no run is active, the SDK raises — Temporal does NOT queue signals for non-existent workflows.
- **Queries are read-only.** They cannot mutate workflow state. The Query handler runs inside the workflow's event loop, so a slow/blocked workflow will stall Query calls.
- **`temporalio>=1.8.0` for Temporal Cloud API keys.** Earlier versions require mTLS. **All components now check this at runtime** and raise an actionable error if `api_key_env_var` is set with an old SDK.
- **Rate limits on Temporal Cloud Visibility.** `temporal_workflow_sensor` with `list_filter` calls `list_workflows`. **The sensor now auto-detects Cloud** (via `.tmprl.cloud` in `target_host`) and clamps `minimum_interval_seconds` to a 60s floor, with a warning — set `>= 60` explicitly to silence.
- **Namespace format on Cloud.** Full namespace is `<name>.<account_id>`, not just `<name>`. **All components now warn** when a Cloud host is paired with a `<name>`-only namespace.

## Compared to Argo

Argo Workflows is a Kubernetes-native workflow engine. Temporal is a language-native durable-execution engine (Python / Go / Java / TypeScript workers). They target different stacks — the community registry ships a similar trio for [Argo](argo.md). Same integration shape (external + sensor + trigger); different transport (Argo REST vs Temporal gRPC).
