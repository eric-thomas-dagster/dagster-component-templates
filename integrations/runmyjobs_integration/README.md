# RunMyJobsIntegrationComponent

Redwood **RunMyJobs** (aka SAP Redwood Scheduler / RMJ) REST API integration. Each declared JobDefinition becomes a **daily-partitioned Dagster asset** with a retry policy. Ships with 5 operational ops + jobs (restart / hold / release / kill / reconcile), 2 sensors (external-execution monitor + inbound trigger), and an hourly reconciliation schedule.

`demo_mode: true` (default) simulates the whole REST API lifecycle on stdout — the component runs end-to-end with zero external dependencies. Flip to `false` and set `RUNMYJOBS_USER` / `RUNMYJOBS_PASSWORD` to hit a real RunMyJobs instance.

## What you get

```
Dagster catalog after `dg dev`:

Assets   ── rmj_eod_settlement                 [daily partitioned, kinds: python, runmyjobs]
         ── rmj_regulatory_extract             [daily partitioned, kinds: python, runmyjobs]
         ── rmj_settlement_table               [source, kinds: runmyjobs, database]  (optional)

Jobs     ── runmyjobs_restart_process          (run config: {process_id})
         ── runmyjobs_hold_application         (run config: {application, queue})
         ── runmyjobs_release_processes        (run config: {application})
         ── runmyjobs_kill_process             (run config: {process_id})
         ── runmyjobs_reconciliation           (drift report)

Sensors  ── runmyjobs_external_execution_monitor   (poll every 60s)
         ── runmyjobs_inbound_trigger              (RMJ -> Dagster GraphQL)

Schedules ── runmyjobs_reconciliation_schedule (cron: 0 * * * *)
```

## Integration pattern

```
       ┌──────────────────────────────────────────────────────────┐
       │           Redwood RunMyJobs scheduler                    │
       │                                                          │
       │  Application: DAILY_BATCH                                │
       │   ├─ EOD_SETTLEMENT (queue: prod_queue)                  │
       │   └─ REG_EXTRACT    (queue: compliance_queue)            │
       │                                                          │
       │  REST API  (Authorization: Basic base64(user:pass))      │
       └──────────────────────────────┬───────────────────────────┘
                                      │
        Dagster -> RunMyJobs          │ RunMyJobs -> Dagster
        POST /scheduler/api/submitjob │ POST https://dagster.cloud/graphql
        GET  /scheduler/api/          │ mutation { launchRun(...) }
             processes/{id}           │
        POST /scheduler/api/          │
             processes/{id}/events    │
                                      ▼
       ┌──────────────────────────────────────────────────────────┐
       │      RunMyJobsIntegrationComponent (one YAML)            │
       │                                                          │
       │  daily-partitioned assets  +  RetryPolicy(max_retries=2) │
       │  op jobs (restart / hold / release / kill / reconcile)   │
       │  sensors (external-execution monitor + inbound trigger)  │
       │  hourly reconciliation schedule                          │
       └──────────────────────────────────────────────────────────┘
```

## Try it in one command

```bash
bash setup_runmyjobs_integration_demo.sh
```

Scaffolds a Dagster project, installs the component in demo mode, materializes one partition end-to-end, and prints the full simulated REST API call trace (AUTH → SUBMIT → POLL × 5 → STDOUT → DONE → EVENT).

Then open the Dagster UI:

```bash
cd runmyjobs-demo
uv run dg dev
```

## Point at a real RunMyJobs instance

```bash
export RUNMYJOBS_USER=svc_dagster
export RUNMYJOBS_PASSWORD='<your-password>'
```

Set `demo_mode: false` and override `endpoint` to your RMJ REST base URL:

```yaml
attributes:
  demo_mode: false
  endpoint: "https://runmyjobs.prod.internal:8443/RunMyJobs/api-rest"
  ...
```

> **API-path caveat.** RunMyJobs REST paths shift across versions (v6 vs v9 vs SAP-branded builds vary in prefix and payload shape). The URIs in this component target the modern JSON REST surface. If your instance uses a different prefix (`/scheduler/api/v1/…` vs `/scheduler/api/…` vs a fully-custom `/api-rest/` path), either bake the prefix into `endpoint` or fork `_execute_runmyjobs` in `component.py` to match. Demo mode is unaffected — the whole simulator runs on stdout.

## The two-way trigger story

RunMyJobs and Dagster both need to be able to start work in the other. This component wires both directions:

**Dagster -> RunMyJobs** (assets):
Each declared JobDefinition becomes a daily-partitioned Dagster asset. Materializing the asset submits the job to RunMyJobs via `POST /scheduler/api/submitjob`, polls `GET /scheduler/api/processes/{id}` until terminal state, retrieves stdout, and posts a `DAGSTER_JOB_COMPLETE` event back to RunMyJobs.

**RunMyJobs -> Dagster** (inbound trigger sensor):
Production wire-up: your RunMyJobs process's post-step calls Dagster's GraphQL API with a `launchRun` mutation. The `runmyjobs_inbound_trigger` sensor confirms the trigger was received and produces observable ticks in the Dagster UI.

## Operational ops — Dagster as the pane of glass

Five ops ship as Dagster jobs so you can restart / hold / release / kill / reconcile RunMyJobs processes directly from the Dagster UI:

| Job | Config | Purpose |
|---|---|---|
| `runmyjobs_restart_process` | `{process_id}` | Rerun a failed process (`POST /processes/{id}/rerun`) |
| `runmyjobs_hold_application` | `{application, queue}` | Hold all processes in an application on a queue |
| `runmyjobs_release_processes` | `{application}` | Release held processes |
| `runmyjobs_kill_process` | `{process_id}` | Kill a running process |
| `runmyjobs_reconciliation` | — | Compare RunMyJobs state vs Dagster state; report drift |

The `runmyjobs_reconciliation_schedule` runs the reconciliation job every hour (STOPPED by default — toggle in the UI when ready).

## Terminology cheat-sheet — Control-M vs RunMyJobs

For teams running both (or migrating between them):

| Control-M | RunMyJobs |
|---|---|
| Job | JobDefinition |
| Folder | Application |
| Agent / Host | Queue |
| ODATE | scheduledTime |
| runId | processId |
| "Ended OK" / "Ended Not OK" | "Completed" / "Error" |

The [`controlm_integration`](../controlm_integration/README.md) sister component has the same asset / op / sensor / schedule surface, so a shop running both can present a single Dagster pane of glass over both schedulers.

## Hybrid deployment vs migration

Designed for the **hybrid** shape — RunMyJobs keeps owning the batch that only RMJ can own (SAP process chains, mainframe wrappers, legacy scheduling), Dagster owns the cloud/analytics/AI pipeline, both share a single Dagster UI with correct lineage.

Not a RunMyJobs killer. If you're doing a full migration off RMJ, the `airflow_dag_proxy` / `sql_transform` / `snowflake_workspace` patterns fit better — this component keeps RMJ in the loop.

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Connection

| Field | Type | Default | Description |
|---|---|---|---|
| `endpoint` | `str` | `"https://runmyjobs.internal:8443/RunMyJobs/api-rest"` | RunMyJobs REST API base URL (used when demo_mode=false). |

### Execution

| Field | Type | Default | Description |
|---|---|---|---|
| `poll_interval_seconds` | `int` | `10` | Seconds between RunMyJobs process status polls. |
| `max_retries` | `int` | `2` | Dagster-side asset retries on failure. |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `group_name` | `str` | `"runmyjobs_integration"` | Dagster asset group for the job assets. |

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
| `source_tables` | `List[RunMyJobsSourceTableSpec]` | `list()` | Tables loaded by RunMyJobs that Dagster observes (adds to lineage). |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `demo_mode` | `bool` | `true` | Simulate the REST API on stdout (no external calls). |
| `jobs` | `List[RunMyJobsJobSpec]` | `list()` | RunMyJobs JobDefinitions to wrap as daily-partitioned Dagster assets. |
| `upstream_deps` | `List[str]` | `list()` | Upstream asset keys (slash-separated for nested keys) that must complete before jobs run. |
| `runmyjobs_user_env` | `str` | `"RUNMYJOBS_USER"` | Env var holding the RunMyJobs REST username. |
| `runmyjobs_password_env` | `str` | `"RUNMYJOBS_PASSWORD"` | Env var holding the RunMyJobs REST password. |
| `poll_timeout_seconds` | `int` | `3600` | Total seconds to wait for terminal status before failing. |
| `stdout_retrieval` | `bool` | `true` | Retrieve process stdout on completion. |
| `reconciliation_cron` | `str` | `"0 * * * *"` | Cron for the RunMyJobs vs Dagster state reconciliation job. |

[//]: # (FIELDS:END)

## Example YAML

```yaml
type: dagster_community_components.RunMyJobsIntegrationComponent

attributes:
  demo_mode: true
  endpoint: "https://runmyjobs.internal:8443/RunMyJobs/api-rest"

  group_name: runmyjobs_integration
  runmyjobs_user_env: RUNMYJOBS_USER
  runmyjobs_password_env: RUNMYJOBS_PASSWORD

  max_retries: 2
  retry_delay_seconds: 60

  jobs:
    - job_definition: EOD_BATCH_SETTLEMENT
      asset_name: rmj_eod_settlement
      application: DAILY_BATCH
      partition_type: CORE_BANKING
      sub_partition_type: SETTLEMENT
      queue: prod_queue

    - job_definition: REGULATORY_EXTRACT
      asset_name: rmj_regulatory_extract
      application: DAILY_BATCH
      partition_type: COMPLIANCE
      sub_partition_type: REGULATORY_REPORTING
      queue: compliance_queue

  # Optional — bring RunMyJobs-managed tables into Dagster's lineage
  source_tables:
    - table_name: "CORE_BANKING.SETTLEMENT_LEDGER"
      asset_name: rmj_settlement_table
      produced_by: rmj_eod_settlement
```

## REST API endpoints (modern JSON surface)

```
POST /scheduler/api/submitjob                    — submit a JobDefinition
GET  /scheduler/api/processes/{id}               — poll process status
GET  /scheduler/api/processes/{id}/stdout        — process output
POST /scheduler/api/processes/{id}/events        — post feedback event
POST /scheduler/api/processes/{id}/rerun         — restart a failed process
POST /scheduler/api/processes/{id}/kill          — kill a running process
POST /scheduler/api/applications/{app}/hold      — hold all processes in an application
POST /scheduler/api/applications/{app}/release   — release held processes
GET  /scheduler/api/processes?since=1h&limit=200 — list processes for reconciliation
```

Verify against your RMJ version's REST reference — the JSON surface has evolved across Redwood 6.x / 9.x / SAP-branded builds.

## Requirements

```
dagster
requests
```

## References

- Redwood RunMyJobs docs: <https://documentation.runmyjobs.cloud/>
- Redwood developer portal: <https://www.redwood.com/product/runmyjobs/>
