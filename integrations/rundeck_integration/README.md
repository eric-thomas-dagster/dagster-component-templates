# RundeckIntegrationComponent

**Rundeck** (OSS / Enterprise) REST API integration. Each declared Rundeck Job becomes a **daily-partitioned Dagster asset** with a retry policy. Ships with 5 operational ops + jobs (restart / disable / enable / abort / reconcile), 2 sensors (external-execution monitor + inbound trigger), and an hourly reconciliation schedule.

`demo_mode: true` (default) simulates the whole REST API lifecycle on stdout — the component runs end-to-end with zero external dependencies. Flip to `false` and set `RUNDECK_API_TOKEN` to hit a real Rundeck instance.

## What you get

```
Dagster catalog after `dg dev`:

Assets   ── rundeck_eod_settlement              [daily partitioned, kinds: python, rundeck]
         ── rundeck_regulatory_extract          [daily partitioned, kinds: python, rundeck]
         ── rundeck_settlement_table            [source, kinds: rundeck, database]  (optional)

Jobs     ── rundeck_restart_execution           (run config: {job_id})
         ── rundeck_disable_job                 (run config: {job_id})
         ── rundeck_enable_job                  (run config: {job_id})
         ── rundeck_abort_execution             (run config: {execution_id})
         ── rundeck_reconciliation              (per-project drift report)

Sensors  ── rundeck_external_execution_monitor  (poll every 60s)
         ── rundeck_inbound_trigger             (Rundeck -> Dagster GraphQL)

Schedules ── rundeck_reconciliation_schedule    (cron: 0 * * * *)
```

## Integration pattern

```
       ┌──────────────────────────────────────────────────────────┐
       │                    Rundeck server                        │
       │                                                          │
       │  Project: DAILY_BATCH                                    │
       │   ├─ eod_settlement       (node filter: settlement-*)    │
       │   └─ regulatory_extract   (node filter: compliance-*)    │
       │                                                          │
       │  REST API  (X-Rundeck-Auth-Token: rdk-...)               │
       └──────────────────────────────┬───────────────────────────┘
                                      │
        Dagster -> Rundeck            │ Rundeck -> Dagster
        POST /api/47/job/{id}/        │ POST https://dagster.cloud/graphql
             executions               │ mutation { launchRun(...) }
        GET  /api/47/execution/{id}   │ (Rundeck job step: script / http)
        GET  /api/47/execution/{id}/  │
             output                   │
                                      ▼
       ┌──────────────────────────────────────────────────────────┐
       │      RundeckIntegrationComponent (one YAML)              │
       │                                                          │
       │  daily-partitioned assets  +  RetryPolicy(max_retries=2) │
       │  op jobs (restart / disable / enable / abort / reconcile)│
       │  sensors (external-execution monitor + inbound trigger)  │
       │  hourly reconciliation schedule                          │
       └──────────────────────────────────────────────────────────┘
```

## Try it in one command

```bash
bash setup_rundeck_integration_demo.sh
```

Scaffolds a Dagster project, installs the component in demo mode, materializes one partition end-to-end, and prints the full simulated REST API call trace (AUTH → SUBMIT → POLL × 5 → OUTPUT → DONE).

Then open the Dagster UI:

```bash
cd rundeck-demo
uv run dg dev
```

## Point at a real Rundeck instance

Create an API token in the Rundeck UI (User Profile → User API Tokens → Generate New Token), then:

```bash
export RUNDECK_API_TOKEN='rdk-<your-token>'
```

Set `demo_mode: false` and override `endpoint` / `api_version` to match your install:

```yaml
attributes:
  demo_mode: false
  endpoint: "https://rundeck.prod.internal:4443"
  api_version: 47
  ...
```

> **API-version caveat.** Rundeck's API version defaults to `47` (current 2026). Older installs use lower ints — check `GET /api/{v}/system/info` and set `api_version` to the highest your server supports.

## The two-way trigger story

Rundeck and Dagster both need to be able to start work in the other. This component wires both directions:

**Dagster -> Rundeck** (assets):
Each declared Rundeck Job becomes a daily-partitioned Dagster asset. Materializing the asset fires the job via `POST /api/{v}/job/{job_id}/executions`, polls `GET /api/{v}/execution/{id}` until terminal state (`succeeded` / `failed` / `aborted` / `timedout` / `missed`), and retrieves output entries via `GET /api/{v}/execution/{id}/output`.

**Rundeck -> Dagster** (inbound trigger sensor):
Production wire-up: your Rundeck job's last step (script or http step) calls Dagster's GraphQL API with a `launchRun` mutation. The `rundeck_inbound_trigger` sensor confirms the trigger was received and produces observable ticks in the Dagster UI.

## Operational ops — Dagster as the pane of glass

Five ops ship as Dagster jobs so you can restart / disable / enable / abort / reconcile Rundeck executions directly from the Dagster UI:

| Job | Config | Purpose |
|---|---|---|
| `rundeck_restart_execution` | `{job_id}` | Re-fire a Rundeck job (Rundeck has no per-execution rerun — you re-launch the job) |
| `rundeck_disable_job` | `{job_id}` | Disable a job so no new executions launch (`POST /job/{id}/execution/disable`) |
| `rundeck_enable_job` | `{job_id}` | Re-enable a previously-disabled job |
| `rundeck_abort_execution` | `{execution_id}` | Abort a running execution |
| `rundeck_reconciliation` | — | Compare Rundeck state vs Dagster state per project; report drift |

The `rundeck_reconciliation_schedule` runs the reconciliation job every hour (STOPPED by default — toggle in the UI when ready).

## Terminology cheat-sheet — Control-M / RunMyJobs vs Rundeck

For teams running two schedulers side-by-side (or migrating between them):

| Control-M | RunMyJobs | Rundeck |
|---|---|---|
| Job | JobDefinition | Job |
| Folder | Application | Project |
| Agent / Host | Queue | Node / Node Filter |
| ODATE | scheduledTime | argString option (e.g. `-runDate <date>`) |
| runId | processId | executionId |
| "Ended OK" / "Ended Not OK" | "Completed" / "Error" | "succeeded" / "failed" |

The [`runmyjobs_integration`](../runmyjobs_integration/README.md) and [`controlm_integration`](../controlm_integration/README.md) sister components have the same asset / op / sensor / schedule surface, so a shop running multiple schedulers can present a single Dagster pane of glass over all of them.

> **Rundeck-specific note:** Rundeck's "hold" analog is **per-job** (`disable_rundeck_job`), not per-project. Rundeck has no first-class notion of "hold all jobs in a project" — the closest equivalent is to disable each job individually, or use a scheduler-level maintenance mode. For bulk operations, loop the `rundeck_disable_job` op over your project's job list.

## Hybrid deployment vs migration

Designed for the **hybrid** shape — Rundeck keeps owning the runbook automation and node-fleet orchestration that only Rundeck can own (SSH fan-out, script libraries, ACL-gated ops runbooks), Dagster owns the cloud/analytics/AI pipeline, both share a single Dagster UI with correct lineage.

Not a Rundeck killer. If you're doing a full migration off Rundeck, the `airflow_dag_proxy` / `sql_transform` / `snowflake_workspace` patterns fit better — this component keeps Rundeck in the loop.

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Connection

| Field | Type | Default | Description |
|---|---|---|---|
| `endpoint` | `str` | `"http://localhost:4440"` | Rundeck base URL (used when demo_mode=false). |
| `api_version` | `int` | `47` | Rundeck API version (v47 current as of 2026; older installs use lower ints). |

### Execution

| Field | Type | Default | Description |
|---|---|---|---|
| `poll_interval_seconds` | `int` | `10` | Seconds between Rundeck execution status polls. |
| `max_retries` | `int` | `2` | Dagster-side asset retries on failure. |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `group_name` | `str` | `"rundeck_integration"` | Dagster asset group for the job assets. |

### Partitions

| Field | Type | Default | Description |
|---|---|---|---|
| `partition_start_date` | `str` | `"2024-01-01"` | Start date for the daily partitioned assets. |

### Retry policy

| Field | Type | Default | Description |
|---|---|---|---|
| `retry_delay_seconds` | `int` | `60` | Delay between Dagster asset retries. |

### Source / target

| Field | Type | Default | Description |
|---|---|---|---|
| `source_tables` | `List[RundeckSourceTableSpec]` | `list()` | Tables loaded by Rundeck executions that Dagster observes (adds to lineage). |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `demo_mode` | `bool` | `true` | Simulate the REST API on stdout (no external calls). |
| `jobs` | `List[RundeckJobSpec]` | `list()` | Rundeck Jobs to wrap as daily-partitioned Dagster assets. |
| `upstream_deps` | `List[str]` | `list()` | Upstream asset keys (slash-separated for nested keys) that must complete before jobs run. |
| `rundeck_token_env` | `str` | `"RUNDECK_API_TOKEN"` | Env var holding the Rundeck API token. |
| `poll_timeout_seconds` | `int` | `3600` | Total seconds to wait for terminal status before failing. |
| `output_retrieval` | `bool` | `true` | Retrieve execution output entries on completion. |
| `reconciliation_cron` | `str` | `"0 * * * *"` | Cron for the Rundeck vs Dagster state reconciliation job. |

[//]: # (FIELDS:END)

## Example YAML

```yaml
type: dagster_community_components.RundeckIntegrationComponent

attributes:
  demo_mode: true
  endpoint: "http://localhost:4440"
  api_version: 47

  group_name: rundeck_integration
  rundeck_token_env: RUNDECK_API_TOKEN

  max_retries: 2
  retry_delay_seconds: 60

  jobs:
    - job_id: 3d7f9e2a-1c4b-4a5f-8d2e-6f8a1b2c3d4e
      asset_name: rundeck_eod_settlement
      project: DAILY_BATCH
      arg_string: "-runDate {{ partition_key }} -mode full"

    - job_id: 7a2b8c1d-9e4f-4b6a-8c5d-2f1e3a4b5c6d
      asset_name: rundeck_regulatory_extract
      project: DAILY_BATCH
      arg_string: "-runDate {{ partition_key }} -region ALL"

  # Optional — bring Rundeck-managed tables into Dagster's lineage
  source_tables:
    - table_name: "CORE_BANKING.SETTLEMENT_LEDGER"
      asset_name: rundeck_settlement_table
      produced_by: rundeck_eod_settlement
```

## REST API endpoints (Rundeck v47 surface)

```
POST /api/{v}/job/{job_id}/executions            — run a job (body: argString, options, asUser, filter?)
GET  /api/{v}/execution/{id}                     — poll execution status
GET  /api/{v}/execution/{id}/output              — output entries (log + level)
POST /api/{v}/execution/{id}/abort               — abort a running execution
POST /api/{v}/job/{id}/execution/disable         — disable a job (no new executions)
POST /api/{v}/job/{id}/execution/enable          — re-enable a disabled job
GET  /api/{v}/project/{project}/executions       — list executions (params: status, recentFilter, max)
```

Verify against your Rundeck version's API reference — the surface has evolved across v14 → v47.

## Requirements

```
dagster
requests
```

## References

- Rundeck API docs: <https://docs.rundeck.com/docs/api/>
- Rundeck GitHub: <https://github.com/rundeck/rundeck>
