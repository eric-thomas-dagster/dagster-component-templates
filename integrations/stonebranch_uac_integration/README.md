# StonebranchUACIntegrationComponent

Stonebranch **Universal Automation Center** (UAC — Universal Controller + Universal Agent) REST API integration. Each declared Task becomes a **daily-partitioned Dagster asset** with a retry policy. Ships with 5 operational ops + jobs (rerun / hold / release / cancel / reconcile), 2 sensors (external-execution monitor + inbound trigger), and an hourly reconciliation schedule.

`demo_mode: true` (default) simulates the whole REST API lifecycle on stdout — the component runs end-to-end with zero external dependencies. Flip to `false` and set `STONEBRANCH_USER` / `STONEBRANCH_PASSWORD` to hit a real Stonebranch UAC instance.

## What you get

```
Dagster catalog after `dg dev`:

Assets   ── sb_eod_settlement                     [daily partitioned, kinds: python, stonebranch]
         ── sb_regulatory_extract                 [daily partitioned, kinds: python, stonebranch]
         ── sb_settlement_table                   [source, kinds: stonebranch, database]  (optional)

Jobs     ── stonebranch_rerun_task_instance       (run config: {sys_id})
         ── stonebranch_hold_task_instance        (run config: {sys_id})
         ── stonebranch_release_task_instance     (run config: {sys_id})
         ── stonebranch_cancel_task_instance      (run config: {sys_id})
         ── stonebranch_reconciliation            (drift report)

Sensors  ── stonebranch_external_execution_monitor   (poll every 60s)
         ── stonebranch_inbound_trigger              (UAC -> Dagster GraphQL)

Schedules ── stonebranch_reconciliation_schedule (cron: 0 * * * *)
```

## Integration pattern

```
       ┌──────────────────────────────────────────────────────────┐
       │      Stonebranch Universal Automation Center             │
       │      (Universal Controller + Universal Agent)            │
       │                                                          │
       │  Workflow: DAILY_BATCH                                   │
       │   ├─ EOD_SETTLEMENT       (agent: ux-agent-01)           │
       │   └─ REGULATORY_EXTRACT   (agent: ux-agent-compliance)   │
       │                                                          │
       │  REST API  (Authorization: Basic base64(user:pass))      │
       └──────────────────────────────┬───────────────────────────┘
                                      │
        Dagster -> Stonebranch UAC    │ Stonebranch UAC -> Dagster
        POST /resources/task/         │ POST https://dagster.cloud/graphql
             ops-task-launch          │ mutation { launchRun(...) }
        GET  /resources/taskinstance/ │
             {sysId}                  │
        POST /resources/taskinstance/ │
             {sysId}/events           │
                                      ▼
       ┌──────────────────────────────────────────────────────────┐
       │   StonebranchUACIntegrationComponent (one YAML)          │
       │                                                          │
       │  daily-partitioned assets  +  RetryPolicy(max_retries=2) │
       │  op jobs (rerun / hold / release / cancel / reconcile)   │
       │  sensors (external-execution monitor + inbound trigger)  │
       │  hourly reconciliation schedule                          │
       └──────────────────────────────────────────────────────────┘
```

## Try it in one command

```bash
bash setup_stonebranch_uac_integration_demo.sh
```

Scaffolds a Dagster project, installs the component in demo mode, materializes one partition end-to-end, and prints the full simulated REST API call trace (AUTH → LAUNCH → POLL × 5 → OUTPUT → DONE → EVENT).

Then open the Dagster UI:

```bash
cd stonebranch-uac-demo
uv run dg dev
```

## Point at a real Stonebranch UAC instance

Stonebranch offers a demo Docker image (`stonebranch/uac-demo`) — access may require a Stonebranch trial account. The demo defaults (`ops.admin` / `admin`) match that image. If you have it running locally:

```bash
export STONEBRANCH_USER=ops.admin
export STONEBRANCH_PASSWORD='admin'
```

Set `demo_mode: false` and (if needed) override `endpoint`:

```yaml
attributes:
  demo_mode: false
  endpoint: "http://localhost:8080/uc"
  ...
```

If you don't have access to the Docker image, the `demo_mode: true` simulator ships end-to-end with zero-license — the whole component runs on stdout.

> **API-path caveat.** Stonebranch UAC REST paths shift across versions. UAC 7.x+ ships the modern JSON REST surface at `/uc/resources/…`; older builds required XML payloads and used different prefixes (`/uc/ws/…`). The URIs in this component target the modern JSON surface. If your instance uses a different prefix, either bake it into `endpoint` or fork `_execute_stonebranch` in `component.py` to match. Demo mode is unaffected — the whole simulator runs on stdout.

## The two-way trigger story

Stonebranch UAC and Dagster both need to be able to start work in the other. This component wires both directions:

**Dagster -> Stonebranch UAC** (assets):
Each declared Task becomes a daily-partitioned Dagster asset. Materializing the asset launches the task via `POST /resources/task/ops-task-launch?taskname=...`, polls `GET /resources/taskinstance/{sysId}` until terminal state, retrieves output, and posts a `DAGSTER_TASK_COMPLETE` event back to UAC.

**Stonebranch UAC -> Dagster** (inbound trigger sensor):
Production wire-up: your UAC task's post-step calls Dagster's GraphQL API with a `launchRun` mutation. The `stonebranch_inbound_trigger` sensor confirms the trigger was received and produces observable ticks in the Dagster UI.

## Operational ops — Dagster as the pane of glass

Five ops ship as Dagster jobs so you can rerun / hold / release / cancel / reconcile Stonebranch UAC task instances directly from the Dagster UI:

| Job | Config | Purpose |
|---|---|---|
| `stonebranch_rerun_task_instance` | `{sys_id}` | Rerun a task instance (`POST /taskinstance/{sysId}/ops-task-rerun`) |
| `stonebranch_hold_task_instance` | `{sys_id}` | Hold a task instance (`POST /taskinstance/{sysId}/ops-task-hold`) |
| `stonebranch_release_task_instance` | `{sys_id}` | Release a held task instance |
| `stonebranch_cancel_task_instance` | `{sys_id}` | Cancel a running task instance |
| `stonebranch_reconciliation` | — | Compare UAC state vs Dagster state; report drift |

The `stonebranch_reconciliation_schedule` runs the reconciliation job every hour (STOPPED by default — toggle in the UI when ready).

## Terminology cheat-sheet — Control-M vs RunMyJobs vs Stonebranch UAC

For teams running any combination (or migrating between them):

| Control-M | RunMyJobs | Stonebranch UAC |
|---|---|---|
| Job | JobDefinition | Task |
| Folder | Application | Workflow |
| Agent / Host | Queue | Universal Agent |
| ODATE | scheduledTime | scheduledTime |
| runId | processId | Task Instance (sysId) |
| "Ended OK" / "Ended Not OK" | "Completed" / "Error" | "Success" / "Failed" |

Sister components with the same asset / op / sensor / schedule surface:

- [`controlm_integration`](../controlm_integration/README.md) — BMC Control-M / Helix Control-M
- [`runmyjobs_integration`](../runmyjobs_integration/README.md) — Redwood RunMyJobs / SAP Redwood Scheduler

A shop running two or three of these can present a single Dagster pane of glass over every scheduler.

## Hybrid deployment vs migration

Designed for the **hybrid** shape — Stonebranch UAC keeps owning the batch that only UAC can own (Universal Agent mainframe wrappers, cross-platform file transfers, legacy scheduling with FTP/SFTP/z/OS agents), Dagster owns the cloud/analytics/AI pipeline, both share a single Dagster UI with correct lineage.

Not a Stonebranch UAC killer. If you're doing a full migration off UAC, the `airflow_dag_proxy` / `sql_transform` / `snowflake_workspace` patterns fit better — this component keeps UAC in the loop.

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Connection

| Field | Type | Default | Description |
|---|---|---|---|
| `endpoint` | `str` | `"http://localhost:8080/uc"` | Stonebranch UAC REST API base URL (used when demo_mode=false). |

### Execution

| Field | Type | Default | Description |
|---|---|---|---|
| `poll_interval_seconds` | `int` | `10` | Seconds between Stonebranch task instance status polls. |
| `max_retries` | `int` | `2` | Dagster-side asset retries on failure. |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `group_name` | `str` | `"stonebranch_uac_integration"` | Dagster asset group for the task assets. |

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
| `source_tables` | `List[StonebranchSourceTableSpec]` | `list()` | Tables loaded by Stonebranch tasks that Dagster observes (adds to lineage). |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `demo_mode` | `bool` | `true` | Simulate the REST API on stdout (no external calls). |
| `jobs` | `List[StonebranchTaskSpec]` | `list()` | Stonebranch UAC Tasks to wrap as daily-partitioned Dagster assets. |
| `upstream_deps` | `List[str]` | `list()` | Upstream asset keys (slash-separated for nested keys) that must complete before tasks run. |
| `stonebranch_user_env` | `str` | `"STONEBRANCH_USER"` | Env var holding the Stonebranch UAC REST username. |
| `stonebranch_password_env` | `str` | `"STONEBRANCH_PASSWORD"` | Env var holding the Stonebranch UAC REST password. |
| `poll_timeout_seconds` | `int` | `3600` | Total seconds to wait for terminal status before failing. |
| `output_retrieval` | `bool` | `true` | Retrieve task instance output on completion. |
| `reconciliation_cron` | `str` | `"0 * * * *"` | Cron for the Stonebranch vs Dagster state reconciliation job. |

[//]: # (FIELDS:END)

## Example YAML

```yaml
type: dagster_community_components.StonebranchUACIntegrationComponent

attributes:
  demo_mode: true
  endpoint: "http://localhost:8080/uc"

  group_name: stonebranch_uac_integration
  stonebranch_user_env: STONEBRANCH_USER
  stonebranch_password_env: STONEBRANCH_PASSWORD

  max_retries: 2
  retry_delay_seconds: 60

  jobs:
    - task_name: EOD_BATCH_SETTLEMENT
      asset_name: sb_eod_settlement
      workflow: DAILY_BATCH
      application: CORE_BANKING
      agent: ux-agent-01

    - task_name: REGULATORY_EXTRACT
      asset_name: sb_regulatory_extract
      workflow: DAILY_BATCH
      application: COMPLIANCE
      agent: ux-agent-compliance

  # Optional — bring Stonebranch-managed tables into Dagster's lineage
  source_tables:
    - table_name: "CORE_BANKING.SETTLEMENT_LEDGER"
      asset_name: sb_settlement_table
      produced_by: sb_eod_settlement
```

## REST API endpoints (modern JSON surface — UAC 7.x+)

```
POST /resources/task/ops-task-launch?taskname=...      — launch a Task
GET  /resources/taskinstance/{sysId}                   — poll task instance status
GET  /resources/taskinstance/{sysId}/output            — task instance output
POST /resources/taskinstance/{sysId}/events            — post feedback event
POST /resources/taskinstance/{sysId}/ops-task-rerun    — rerun a task instance
POST /resources/taskinstance/{sysId}/ops-task-hold     — hold a task instance
POST /resources/taskinstance/{sysId}/ops-task-release  — release a held task instance
POST /resources/taskinstance/{sysId}/ops-task-cancel   — cancel a task instance
GET  /resources/taskinstance/list?status=...           — list task instances for reconciliation
```

Verify against your UAC version's REST reference — older UAC builds (pre-7.x) used XML payloads and a different path structure.

## Requirements

```
dagster
requests
```

## References

- Stonebranch docs: <https://docs.stonebranch.com/>
- Stonebranch product page: <https://www.stonebranch.com/products/universal-automation-center>
