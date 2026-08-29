# Prefect

[Prefect](https://www.prefect.io/) is a Python-native workflow engine — flows and tasks defined in code, executed on Python workers, with a hosted control plane (Prefect Cloud) or self-hosted Prefect Server. Common landings: ETL that lives in Python, background jobs, teams that grew up on the Prefect DSL, ML training loops, script-forward data science work.

Prefect is a **workflow engine**, not a data orchestrator. Framed differently: Prefect is closer to Temporal than to Dagster — both are durable-execution engines for code-centric workflows, and both benefit from being placed into Dagster's asset catalog rather than replacing it. Dagster's role in a Prefect-adopting stack is the connective tissue: give Prefect flows first-class asset identity with lineage, gate downstream data work on their completion, and drive them from Dagster's schedule / sensor / partition model.

The community registry ships **3 Prefect components** covering the trigger + observe + resource-registration surface — the same shape as [Temporal](temporal.md) and [Argo](argo.md).

## Positioning — Prefect vs. Temporal vs. Dagster

|                                    | Prefect                     | Temporal                     | Dagster                    |
|---|---|---|---|
| **Primary domain**                  | Python workflows            | Durable execution, any language | Data assets + lineage    |
| **Programming model**               | `@flow` / `@task` decorators | `@workflow` / `@activity` classes | `@asset` / `@op` decorators |
| **Execution model**                 | Python worker pool          | Language-native worker pool  | Compute per-asset         |
| **State survival**                  | Storage-backed              | Event-sourced replay         | Materialization events    |
| **Native asset lineage**            | No                          | No                           | **Yes** (first-class)    |
| **Native data-side triggers** (S3, DB, freshness) | No               | No                           | **Yes**                  |
| **Typical use case shift TO Dagster** | "we have flows but no lineage / data catalog" | "we run workflows but analysts want to see their outputs" | — |

The pattern is the same in both directions:
- **Prefect → Dagster** when the team hits "we don't know what data our flows produce or when."
- **Temporal → Dagster** when the team hits "we run durable workflows but want data-side triggers."

**Dagster doesn't replace either.** It puts them in the asset catalog and connects them to the rest of the data stack.

## Two modes of interaction

|                              | Trigger from Dagster                          | Observe from Dagster                       |
|------------------------------|-----------------------------------------------|--------------------------------------------|
| **Prefect deployment**       | `prefect_flow_run` (materializable asset)     | `prefect_flow_run_sensor` (terminal-state polling) |

## Components

| Component | Category | What it does | Validation |
|---|---|---|---|
| [`prefect_resource`](https://dagster-component-ui.vercel.app/c/prefect_resource) | resource | Registers a Dagster resource that configures the Prefect Python SDK for a specific Prefect instance (local server or Prefect Cloud). Used by `prefect_flow_run` and `prefect_flow_run_sensor`, or referenced directly. | `live` |
| [`prefect_flow_run`](https://dagster-component-ui.vercel.app/c/prefect_flow_run) | infrastructure | Materializable asset that triggers a Prefect deployment and (optionally) waits for completion. Captures `flow_run_id` + `state` in materialization metadata. Works against local Prefect server and Prefect Cloud. | `live` |
| [`prefect_flow_run_sensor`](https://dagster-component-ui.vercel.app/c/prefect_flow_run_sensor) | sensor | Dagster sensor watching Prefect's API for flow runs entering terminal states (`Completed` / `Failed` / `Crashed` / `Cancelled`). Launches a Dagster job per new completion. Filters by `flow_name` / `deployment_name` / state list. Bridges Prefect-owned upstream work into the Dagster catalog. | `live` |

## Pairing patterns

**Case A — Prefect owns the schedule (observe-only).** Prefect Cloud deployment schedules, application-code-driven `run_deployment` calls, cron in Prefect. Pair `prefect_resource` + `prefect_flow_run_sensor` — the sensor emits `AssetMaterialization` events keyed to whatever asset represents the flow's output, and Dagster downstream work triggers off those materializations.

**Case B — Dagster owns the schedule.** Dagster asset triggers a Prefect deployment (potentially per partition). Use `prefect_flow_run` as a Dagster asset with `wait_for_completion: true`. Materialization completes when the Prefect run reaches a terminal state. Downstream Dagster assets get lineage back to the Prefect run automatically.

**Case C — Both worlds.** Use both patterns simultaneously — some flows Dagster triggers, others Prefect owns. `prefect_flow_run` and `prefect_flow_run_sensor` share the same `prefect_resource` and can both target the same asset keys.

## Connection / auth — quick reference

| Surface | Setting | Notes |
|---|---|---|
| Local Prefect server | `api_url: http://localhost:4200/api` | Default local dev server. No auth. |
| Prefect Cloud | `api_url: https://api.prefect.cloud/api/accounts/<account_id>/workspaces/<workspace_id>` | Full workspace URL. |
| Auth: Cloud | `api_key_env_var: PREFECT_API_KEY` | Personal or service account key. |
| Auth: self-hosted with basic auth | `api_key_env_var: PREFECT_SERVER_API_KEY` | Same field, custom-issued key. |

All 3 components accept the same `prefect_resource` — swap the resource's config to point at Cloud vs. local vs. self-hosted with zero component-level change.

## Where Dagster adds value that Prefect alone doesn't

- **Catalog placement + lineage.** Prefect flows become first-class Dagster assets. Downstream dbt models / SQL transforms / dashboards depend on them via `deps:`. Prefect's own UI shows flow runs; Dagster's shows the whole data graph the flow sits in.
- **Data-side triggers.** Prefect has no notion of "wait for the S3 file to arrive" or "gate on Snowflake table freshness." Dagster's sensors + `AutomationCondition` do — Prefect flows fit into wider DAGs alongside Snowflake / Databricks / dbt / Fivetran / Airbyte / etc.
- **Cross-tool orchestration.** A Prefect flow that lands data in Snowflake can trigger downstream dbt Cloud runs via `dbt_cloud_trigger_job`, refresh Power BI datasets via `dagster-powerbi`, and gate on freshness via `freshness_check`. None of that is Prefect's job; all of it is Dagster's.
- **Partitioning.** Dagster's asset-partitioning model lets one `prefect_flow_run` component fan out to per-day / per-tenant runs of the same Prefect deployment — with Dagster tracking which partitions succeeded, retried, or need backfill. Prefect handles per-run state; Dagster handles the collection.

## Gotchas — most handled by the components

- **Prefect flow-run state IS the completion signal.** `prefect_flow_run_sensor` watches for state transitions to `Completed` (success) / `Failed` / `Crashed` / `Cancelled` (terminal). The sensor's cursor tracks the last flow-run ID seen; late-arriving runs still fire.
- **Prefect Cloud URL format.** The `api_url` for Prefect Cloud must include the full account + workspace path: `https://api.prefect.cloud/api/accounts/<account_id>/workspaces/<workspace_id>`. Bare `https://api.prefect.cloud/api` doesn't work.
- **Deployment vs flow.** `prefect_flow_run` triggers a deployment (by name), not a bare flow. Prefect's model requires a deployment to specify infrastructure + parameters + schedule; the component follows that convention.
- **Parameters vs config.** Flow parameters go in the component's `parameters:` dict, not run config. The Dagster launchpad exposes these for override on manual re-run.

## Compared to Temporal

Both are durable-execution workflow engines that pair with Dagster the same way (`external_asset` + `trigger` + `sensor` trio). Choose based on programming model:
- **Prefect** — Python-first, `@flow` / `@task` decorator DSL, hosted control plane. Best for teams already writing Python data code.
- **Temporal** — language-agnostic, `@workflow` / `@activity` class model, event-sourced replay. Best for teams needing multi-language durable execution or long-lived interactive workflows with signals + queries.

Not competitive with each other — they solve slightly different needs. Both benefit from Dagster's catalog role.

## Design note — no `prefect_workspace` component

Prefect deployments are code-defined. A `prefect_workspace`-style discovery component (mirroring `snowflake_workspace` / `hvr_hub_workspace`) would double-report the same objects Dagster-native discovery already finds, and adds no useful lineage — the flows are already Python modules the customer owns. Use `prefect_flow_run` (Dagster owns the schedule) or `prefect_flow_run_sensor` (Prefect owns the schedule) — those cover the real integration points.

## Walkthroughs

- [Dagster orchestrates Prefect](https://github.com/eric-thomas-dagster/dagster-community-components-cli/blob/main/examples/dagster_orchestrates_prefect.md) — end-to-end: Dagster project + local Prefect server + `prefect_resource` + `prefect_flow_run` (Dagster partitions per file → triggers Prefect flow per file) + `prefect_flow_run_sensor` (polls for completions and triggers downstream Dagster processing). Zero credentials, laptop-runnable.
