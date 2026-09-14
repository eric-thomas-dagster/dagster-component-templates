# IWSIntegrationComponent

IBM **Workload Scheduler** (IWS / formerly Tivoli Workload Scheduler / TWS) REST API integration. Each declared IWS Job becomes a **daily-partitioned Dagster asset** with a retry policy. Ships with 5 operational ops + jobs (rerun / hold / release / kill / reconcile), 2 sensors (external-execution monitor + inbound trigger), and an hourly reconciliation schedule.

`demo_mode: true` (default) simulates the whole REST API lifecycle on stdout — the component runs end-to-end with zero external dependencies. Flip to `false` and set `IWS_USER` / `IWS_PASSWORD` to hit a real IWS instance.

## What you get

```
Dagster catalog after `dg dev`:

Assets   ── iws_eod_settle                     [daily partitioned, kinds: python, iws]
         ── iws_reg_extract                    [daily partitioned, kinds: python, iws]
         ── iws_settlement_table               [source, kinds: iws, database]  (optional)

Jobs     ── iws_rerun_job                      (run config: {job_id})
         ── iws_hold_job                       (run config: {job_id})
         ── iws_release_job                    (run config: {job_id})
         ── iws_kill_job                       (run config: {job_id})
         ── iws_reconciliation                 (drift report)

Sensors  ── iws_external_execution_monitor     (poll every 60s)
         ── iws_inbound_trigger                (IWS -> Dagster GraphQL)

Schedules ── iws_reconciliation_schedule       (cron: 0 * * * *)
```

## Integration pattern

```
       ┌──────────────────────────────────────────────────────────┐
       │        IBM Workload Scheduler (IWS / TWS)                │
       │                                                          │
       │  Application (JobStream): DAILY_BATCH                    │
       │   ├─ EOD_SETTLE  (workstation: CPU1-MASTER)              │
       │   └─ REG_EXTRACT (workstation: CPU2-COMPLIANCE)          │
       │                                                          │
       │  REST API  (Authorization: Basic base64(user:pass))      │
       │  Distributed: /twsd/v1        z/OS: /twsz/v1             │
       └──────────────────────────────┬───────────────────────────┘
                                      │
        Dagster -> IWS                │ IWS -> Dagster
        POST /plan/current/jobstream  │ POST https://dagster.cloud/graphql
        GET  /plan/current/           │ mutation { launchRun(...) }
             job/{jobId}              │
        GET  /plan/current/           │
             job/{jobId}/stdlist      │
                                      ▼
       ┌──────────────────────────────────────────────────────────┐
       │        IWSIntegrationComponent (one YAML)                │
       │                                                          │
       │  daily-partitioned assets  +  RetryPolicy(max_retries=2) │
       │  op jobs (rerun / hold / release / kill / reconcile)     │
       │  sensors (external-execution monitor + inbound trigger)  │
       │  hourly reconciliation schedule                          │
       └──────────────────────────────────────────────────────────┘
```

## Try it in one command

```bash
bash setup_iws_integration_demo.sh
```

Scaffolds a Dagster project, installs the component in demo mode, materializes one partition end-to-end, and prints the full simulated REST API call trace (AUTH → SUBMIT → POLL × 5 → STDLIST → DONE).

Then open the Dagster UI:

```bash
cd iws-demo
uv run dg dev
```

## Point at a real IWS instance

```bash
export IWS_USER=svc_dagster
export IWS_PASSWORD='<your-password>'
```

Set `demo_mode: false` and override `endpoint` to your IWS REST base URL:

```yaml
attributes:
  demo_mode: false
  # Distributed engine
  endpoint: "https://iws.prod.internal:31116/twsd/v1"
  # or z/OS engine
  # endpoint: "https://iws-zos.prod.internal:31116/twsz/v1"
  ...
```

> **API-path caveat.** IWS REST paths differ between the **distributed engine** (`/twsd/v1`) and the **z/OS engine** (`/twsz/v1`), and payload shapes have evolved across product versions. The URIs in this component target the modern JSON REST surface. If your instance uses a different prefix or requires additional query params (e.g. plan selector, engine name), either bake it into `endpoint` or fork `_execute_iws` in `component.py` to match. Demo mode is unaffected — the whole simulator runs on stdout.

## Docker / eval images

IBM publishes IWS / Workload Automation container images under `icr.io/wa-container/*` (e.g. `icr.io/wa-container/wa-server-distr`). These are **BYOL / license-gated** for IBM customers — you'll need an entitlement key to pull them. The `demo_mode` simulator ships end-to-end with **zero license required** so evaluators can drive the full component surface without any IBM entitlements.

## The two-way trigger story

IWS and Dagster both need to be able to start work in the other. This component wires both directions:

**Dagster -> IWS** (assets):
Each declared IWS Job becomes a daily-partitioned Dagster asset. Materializing the asset submits the job to IWS via `POST /plan/current/jobstream`, polls `GET /plan/current/job/{jobId}` until terminal state (`Succ` / `Abend` / `Cancelled`), and retrieves the stdlist.

**IWS -> Dagster** (inbound trigger sensor):
Production wire-up: your IWS job's post-step calls Dagster's GraphQL API with a `launchRun` mutation. The `iws_inbound_trigger` sensor confirms the trigger was received and produces observable ticks in the Dagster UI.

## Operational ops — Dagster as the pane of glass

Five ops ship as Dagster jobs so you can rerun / hold / release / kill / reconcile IWS jobs directly from the Dagster UI:

| Job | Config | Purpose |
|---|---|---|
| `iws_rerun_job` | `{job_id}` | Rerun a failed IWS job (`POST /job/{id}/action/rerun`) |
| `iws_hold_job` | `{job_id}` | Hold an IWS job (`POST /job/{id}/action/hold`) |
| `iws_release_job` | `{job_id}` | Release a held IWS job (`POST /job/{id}/action/release`) |
| `iws_kill_job` | `{job_id}` | Kill a running IWS job (`POST /job/{id}/action/kill`) |
| `iws_reconciliation` | — | Compare IWS plan vs Dagster state; report drift + alert on `Abend` |

The `iws_reconciliation_schedule` runs the reconciliation job every hour (STOPPED by default — toggle in the UI when ready).

## Terminology cheat-sheet — Control-M vs RunMyJobs vs IWS

For teams running multiple schedulers (or migrating between them):

| Control-M | RunMyJobs | IBM Workload Scheduler |
|---|---|---|
| Job | JobDefinition | Job |
| Folder | Application | Application (JobStream) |
| Agent / Host | Queue | Workstation |
| ODATE | scheduledTime | scheduled time / IA |
| runId | processId | jobId |
| "Ended OK" | "Completed" | "Succ" |
| "Ended Not OK" | "Error" | "Abend" |

The sister components ship with the same asset / op / sensor / schedule surface, so a shop running multiple schedulers can present a single Dagster pane of glass over all of them:

- [`runmyjobs_integration`](../runmyjobs_integration/README.md) — Redwood RunMyJobs / SAP Redwood Scheduler
- [`controlm_integration`](../controlm_integration/README.md) — BMC Control-M

## Hybrid deployment vs migration

Designed for the **hybrid** shape — IWS keeps owning the batch that only IWS can own (z/OS JCL, mainframe wrappers, legacy calendars), Dagster owns the cloud/analytics/AI pipeline, both share a single Dagster UI with correct lineage.

Not an IWS killer. If you're doing a full migration off IWS, the `airflow_dag_proxy` / `sql_transform` / `snowflake_workspace` patterns fit better — this component keeps IWS in the loop.

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Connection

| Field | Type | Default | Description |
|---|---|---|---|
| `endpoint` | `str` | `"https://iws.internal:31116/twsd/v1"` | IWS REST API base URL (used when demo_mode=false). Distributed engine uses /twsd/v1; z/OS engine uses /twsz/v1. |

### Execution

| Field | Type | Default | Description |
|---|---|---|---|
| `poll_interval_seconds` | `int` | `10` | Seconds between IWS job status polls. |
| `max_retries` | `int` | `2` | Dagster-side asset retries on failure. |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `group_name` | `str` | `"iws_integration"` | Dagster asset group for the job assets. |

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
| `source_tables` | `List[IWSSourceTableSpec]` | `list()` | Tables loaded by IWS that Dagster observes (adds to lineage). |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `demo_mode` | `bool` | `true` | Simulate the REST API on stdout (no external calls). |
| `jobs` | `List[IWSJobSpec]` | `list()` | IWS Jobs to wrap as daily-partitioned Dagster assets. |
| `upstream_deps` | `List[str]` | `list()` | Upstream asset keys (slash-separated for nested keys) that must complete before jobs run. |
| `iws_user_env` | `str` | `"IWS_USER"` | Env var holding the IWS REST username. |
| `iws_password_env` | `str` | `"IWS_PASSWORD"` | Env var holding the IWS REST password. |
| `poll_timeout_seconds` | `int` | `3600` | Total seconds to wait for terminal status before failing. |
| `stdlist_retrieval` | `bool` | `true` | Retrieve job stdlist on completion. |
| `reconciliation_cron` | `str` | `"0 * * * *"` | Cron for the IWS vs Dagster state reconciliation job. |

[//]: # (FIELDS:END)

## Example YAML

```yaml
type: dagster_community_components.IWSIntegrationComponent

attributes:
  demo_mode: true
  endpoint: "https://iws.internal:31116/twsd/v1"

  group_name: iws_integration
  iws_user_env: IWS_USER
  iws_password_env: IWS_PASSWORD

  max_retries: 2
  retry_delay_seconds: 60

  jobs:
    - job_name: EOD_SETTLE
      asset_name: iws_eod_settle
      application: DAILY_BATCH
      workstation: CPU1-MASTER
      alias_name: CORE_BANKING_SETTLEMENT

    - job_name: REG_EXTRACT
      asset_name: iws_reg_extract
      application: DAILY_BATCH
      workstation: CPU2-COMPLIANCE
      alias_name: COMPLIANCE_REGULATORY

  # Optional — bring IWS-managed tables into Dagster's lineage
  source_tables:
    - table_name: "CORE_BANKING.SETTLEMENT_LEDGER"
      asset_name: iws_settlement_table
      produced_by: iws_eod_settle
```

## REST API endpoints (modern JSON surface)

```
POST /plan/current/jobstream                     — submit a job / jobstream
GET  /plan/current/job/{jobId}                   — poll job status
GET  /plan/current/job/{jobId}/stdlist           — job log / stdlist
POST /plan/current/job/{jobId}/action/rerun      — rerun a failed job
POST /plan/current/job/{jobId}/action/hold       — hold a job
POST /plan/current/job/{jobId}/action/release    — release a held job
POST /plan/current/job/{jobId}/action/kill       — kill a running job
GET  /plan/current/job?status=Succ,Abend&limit=200 — list jobs for reconciliation
```

Statuses: `Waiting`, `Ready`, `Held`, `Running`, `Succ`, `Abend`, `Cancelled` (terminal: `Succ`, `Abend`, `Cancelled`).

Verify against your IWS version's REST reference — the JSON surface differs between the distributed engine (`/twsd/v1`) and the z/OS engine (`/twsz/v1`), and has evolved across product versions.

## Requirements

```
dagster
requests
```

## References

- IBM Workload Automation docs: <https://www.ibm.com/docs/en/workload-automation>
- IBM Workload Scheduler product page: <https://www.ibm.com/products/workload-automation>
- IWS container images (BYOL): `icr.io/wa-container/wa-server-distr`
