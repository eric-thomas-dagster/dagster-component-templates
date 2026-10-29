# ControlMIntegrationComponent

BMC Control-M Automation API integration. Each declared Control-M job becomes a **daily-partitioned Dagster asset** with a retry policy. Ships with 5 operational ops + jobs (restart / hold / free / kill / reconcile), 2 sensors (external-execution monitor + inbound trigger), and an hourly reconciliation schedule.

`demo_mode: true` (default) simulates the whole Automation API lifecycle on stdout — the component runs end-to-end with zero external dependencies. Flip to `false` and set `CONTROLM_USER` / `CONTROLM_PASSWORD` to hit a real Automation API server.

## What you get

```
Dagster catalog after `dg dev`:

Assets   ── controlm_eod_settlement            [daily partitioned, kinds: python, control-m]
         ── controlm_regulatory_extract        [daily partitioned, kinds: python, control-m]
         ── controlm_settlement_table          [source, kinds: control-m, database]  (optional)

Jobs     ── controlm_restart_job               (run config: {run_id})
         ── controlm_hold_folder               (run config: {folder, server})
         ── controlm_free_folder               (run config: {folder})
         ── controlm_kill_job                  (run config: {job_id})
         ── controlm_reconciliation            (drift report)

Sensors  ── controlm_external_execution_monitor   (poll every 60s)
         ── controlm_inbound_trigger              (Control-M -> Dagster GraphQL)

Schedules ── controlm_reconciliation_schedule  (cron: 0 * * * *)
```

## Integration pattern

```
       ┌──────────────────────────────────────────────────────────┐
       │              Control-M / z/OS mainframe                  │
       │                                                          │
       │  Folder: DAILY_BATCH                                     │
       │   ├─ EOD_BATCH_SETTLEMENT (CORE_BANKING/SETTLEMENT)      │
       │   └─ REGULATORY_EXTRACT   (COMPLIANCE/REG_REPORTING)     │
       │                                                          │
       │  Automation API v2  (POST /session/login -> Bearer JWT)  │
       └──────────────────────────────┬───────────────────────────┘
                                      │
        Dagster -> Control-M          │ Control-M -> Dagster
        POST /run/order               │ POST https://dagster.cloud/graphql
        GET  /run/jobs/status         │ mutation { launchRun(...) }
        POST /run/event/{runId}       │
                                      ▼
       ┌──────────────────────────────────────────────────────────┐
       │      ControlMIntegrationComponent (one YAML)             │
       │                                                          │
       │  daily-partitioned assets  +  RetryPolicy(max_retries=2) │
       │  op jobs (restart / hold / free / kill / reconcile)      │
       │  sensors (external-execution monitor + inbound trigger)  │
       │  hourly reconciliation schedule                          │
       └──────────────────────────────────────────────────────────┘
```

## Try it in one command

```bash
bash setup_controlm_integration_demo.sh
```

Scaffolds a Dagster project, installs the component in demo mode, materializes one partition end-to-end, and prints the full simulated Automation API call trace (LOGIN → SUBMIT → POLL × 5 → OUTPUT → DONE → FEEDBACK → LOGOUT).

Then open the Dagster UI:

```bash
cd controlm-demo
uv run dg dev
```

## Point at a real Control-M

```bash
export CONTROLM_USER=svc_dagster
export CONTROLM_PASSWORD='<your-password>'
```

Set `demo_mode: false` and (optionally) override `endpoint` to your Automation API URL:

```yaml
attributes:
  demo_mode: false
  endpoint: "https://controlm.prod.internal:8443/automation-api"
  ...
```

Everything else stays exactly as the demo — same job list, same partition shape, same ops.

## The two-way trigger story

Control-M and Dagster both need to be able to start work in the other. This component wires both directions:

**Dagster -> Control-M** (assets):
Each declared job becomes a daily-partitioned Dagster asset. Materializing the asset submits the job to Control-M via `POST /run/order`, polls `GET /run/jobs/status` until terminal state, retrieves the spool output, and posts a `DAGSTER_JOB_COMPLETE` event back to Control-M.

**Control-M -> Dagster** (inbound trigger sensor):
Production wire-up: your Control-M job's post-processing step calls Dagster's GraphQL API with a `launchRun` mutation. The `controlm_inbound_trigger` sensor confirms the trigger was received and produces observable ticks in the Dagster UI.

## Operational ops — Dagster as the pane of glass

Five ops ship as Dagster jobs so you can restart / hold / free / kill / reconcile Control-M jobs directly from the Dagster UI (or trigger them from Dagster+ Automations, or via GraphQL from any other tool):

| Job | Config | Purpose |
|---|---|---|
| `controlm_restart_job` | `{run_id}` | Restart a failed job (`POST /run/runNow`) |
| `controlm_hold_folder` | `{folder, server}` | Hold all jobs in a folder |
| `controlm_free_folder` | `{folder}` | Release held jobs |
| `controlm_kill_job` | `{job_id}` | Kill a running job |
| `controlm_reconciliation` | — | Compare Control-M state vs Dagster state; report drift |

The `controlm_reconciliation_schedule` runs the reconciliation job every hour (STOPPED by default — toggle in the UI when ready).

## Migrating from Control-M vs living alongside it

This component is designed for the **hybrid deployment** shape — Control-M keeps owning the batch that only Control-M can own (JCL submission, JES scheduling, mainframe agents), Dagster keeps owning the cloud/analytics/AI pipeline, and both are visible from the same Dagster UI with correct lineage.

Not a Control-M killer. If you're doing a full migration off Control-M, the `airflow_dag_proxy` / `sql_transform` / `snowflake_workspace` / etc. patterns are what you want — this component keeps Control-M in the loop.

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Connection

| Field | Type | Default | Description |
|---|---|---|---|
| `endpoint` | `str` | `"https://controlm.internal:8443/automation-api"` | Control-M Automation API base URL (used when demo_mode=false). |

### Execution

| Field | Type | Default | Description |
|---|---|---|---|
| `poll_interval_seconds` | `int` | `10` | Seconds between Control-M status polls. |
| `max_retries` | `int` | `2` | Dagster-side asset retries on failure. |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `group_name` | `str` | `"controlm_integration"` | Dagster asset group for the job assets. |

### Partitions

| Field | Type | Default | Description |
|---|---|---|---|
| `partition_start_date` | `str` | `"2024-01-01"` | Start date for the daily partitioned assets (ODATE origin). |

### Retry policy

| Field | Type | Default | Description |
|---|---|---|---|
| `retry_delay_seconds` | `int` | `60` | Delay between Dagster asset retries. |

### Source / target

| Field | Type | Default | Description |
|---|---|---|---|
| `source_tables` | `List[ControlMSourceTableSpec]` | `list()` | Tables loaded by Control-M that Dagster observes (adds to lineage). |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `demo_mode` | `bool` | `true` | Simulate the Automation API on stdout (no external calls). |
| `jobs` | `List[ControlMJobSpec]` | `list()` | Control-M jobs to wrap as daily-partitioned Dagster assets. |
| `upstream_deps` | `List[str]` | `list()` | Upstream asset keys (slash-separated for nested keys) that must complete before jobs run. |
| `controlm_user_env` | `str` | `"CONTROLM_USER"` | Env var holding the Automation API username. |
| `controlm_password_env` | `str` | `"CONTROLM_PASSWORD"` | Env var holding the Automation API password. |
| `poll_timeout_seconds` | `int` | `3600` | Total seconds to wait for terminal status before failing. |
| `spool_retrieval` | `bool` | `true` | Retrieve job spool output on completion. |
| `reconciliation_cron` | `str` | `"0 * * * *"` | Cron for the Control-M vs Dagster state reconciliation job. |

[//]: # (FIELDS:END)

## Example YAML

```yaml
type: dagster_community_components.ControlMIntegrationComponent

attributes:
  demo_mode: true
  endpoint: "https://controlm.internal:8443/automation-api"

  group_name: controlm_integration
  controlm_user_env: CONTROLM_USER
  controlm_password_env: CONTROLM_PASSWORD

  max_retries: 2
  retry_delay_seconds: 60

  jobs:
    - job_name: EOD_BATCH_SETTLEMENT
      asset_name: controlm_eod_settlement
      folder: DAILY_BATCH
      application: CORE_BANKING
      sub_application: SETTLEMENT
      host: ctm-agent-prod-01

    - job_name: REGULATORY_EXTRACT
      asset_name: controlm_regulatory_extract
      folder: DAILY_BATCH
      application: COMPLIANCE
      sub_application: REGULATORY_REPORTING
      host: ctm-agent-prod-02

  # Optional — bring Control-M-managed tables into Dagster's lineage
  source_tables:
    - table_name: "CORE_BANKING.SETTLEMENT_LEDGER"
      asset_name: controlm_settlement_table
      produced_by: controlm_eod_settlement
```

## Automation API endpoints

```
POST /session/login              — Bearer JWT
POST /run/order                  — submit a job
GET  /run/jobs/status?runId=…    — poll status
GET  {outputURI}                 — spool output
POST /run/event/{runId}          — post feedback event
POST /run/runNow                 — restart a failed job
DELETE /run/job/{id}/kill        — kill a running job
POST /session/logout             — end session
```

## Requirements

```
dagster
requests
```

## References

- Control-M Automation API docs: <https://docs.bmc.com/docs/automation-api>
- Automation API quickstart repo: <https://github.com/controlm/automation-api-quickstart>
