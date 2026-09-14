# JAMSIntegrationComponent

Fortra **JAMS Scheduler** (formerly HelpSystems JAMS) REST API integration. Each declared JAMS Job becomes a **daily-partitioned Dagster asset** with a retry policy. Ships with 5 operational ops + jobs (restart / hold / release / cancel / reconcile), 2 sensors (external-execution monitor + inbound trigger), and an hourly reconciliation schedule.

`demo_mode: true` (default) simulates the whole REST API lifecycle on stdout — the component runs end-to-end with zero external dependencies. JAMS runs on Windows Server with no public Docker image, so the simulator is the only zero-license way to smoke-test the full surface. Flip to `false` and set `JAMS_USER` / `JAMS_PASSWORD` to hit a real JAMS instance.

## What you get

```
Dagster catalog after `dg dev`:

Assets   ── jams_eod_settle                    [daily partitioned, kinds: python, jams]
         ── jams_regulatory_extract            [daily partitioned, kinds: python, jams]
         ── jams_settlement_table              [source, kinds: jams, database]  (optional)

Jobs     ── jams_restart_entry                 (run config: {entry_id})
         ── jams_hold_entry                    (run config: {entry_id})
         ── jams_release_entry                 (run config: {entry_id})
         ── jams_cancel_entry                  (run config: {entry_id})
         ── jams_reconciliation                (drift report)

Sensors  ── jams_external_execution_monitor    (poll every 60s)
         ── jams_inbound_trigger               (JAMS -> Dagster GraphQL)

Schedules ── jams_reconciliation_schedule      (cron: 0 * * * *)
```

## Integration pattern

```
       ┌──────────────────────────────────────────────────────────┐
       │           Fortra JAMS Scheduler (Windows Server)         │
       │                                                          │
       │  Folder: DAILY_BATCH                                     │
       │   ├─ EOD_SETTLE          (agent: WIN-AGT-01)             │
       │   └─ REGULATORY_EXTRACT  (agent: WIN-AGT-02)             │
       │                                                          │
       │  REST API  (Authorization: Basic base64(user:pass))      │
       └──────────────────────────────┬───────────────────────────┘
                                      │
        Dagster -> JAMS               │ JAMS -> Dagster
        POST /Jobs/{name}/Submit      │ POST https://dagster.cloud/graphql
        GET  /Entries/{entryId}       │ mutation { launchRun(...) }
        GET  /Entries/{entryId}/Log   │
        POST /Entries/{entryId}/      │
             Restart|Hold|Release|    │
             Cancel                   │
                                      ▼
       ┌──────────────────────────────────────────────────────────┐
       │      JAMSIntegrationComponent (one YAML)                 │
       │                                                          │
       │  daily-partitioned assets  +  RetryPolicy(max_retries=2) │
       │  op jobs (restart / hold / release / cancel / reconcile) │
       │  sensors (external-execution monitor + inbound trigger)  │
       │  hourly reconciliation schedule                          │
       └──────────────────────────────────────────────────────────┘
```

## Try it in one command

```bash
bash setup_jams_integration_demo.sh
```

Scaffolds a Dagster project, installs the component in demo mode, materializes one partition end-to-end, and prints the full simulated REST API call trace (AUTH → SUBMIT → POLL × 5 → LOG → DONE).

Then open the Dagster UI:

```bash
cd jams-demo
uv run dg dev
```

## Point at a real JAMS instance

```bash
export JAMS_USER=svc_dagster
export JAMS_PASSWORD='<your-password>'
```

Set `demo_mode: false` and override `endpoint` to your JAMS REST base URL:

```yaml
attributes:
  demo_mode: false
  endpoint: "https://jams.prod.internal/jams/rest/api"
  ...
```

> **API-path caveat.** JAMS REST paths vary across versions (6.x vs 7.x REST surfaces differ in prefix and payload shape). The URIs in this component target the modern `/jams/rest/api` JSON REST surface. If your JAMS instance uses a different prefix (e.g. `/api/rest/…` on older builds, or a fully-custom path), either bake the prefix into `endpoint` or fork `_execute_jams` in `component.py` to match. Some deployments also require a token exchange via `POST /Authentication` before Basic auth is accepted — that's a two-line addition to the executor. Demo mode is unaffected — the whole simulator runs on stdout.

> **No public Docker image.** Fortra JAMS runs on Windows Server (with SQL Server as the metadata store) and is not distributed as a public container image. The demo simulator is the only zero-license way to exercise the full surface; production wiring targets a real installed JAMS instance.

## The two-way trigger story

JAMS and Dagster both need to be able to start work in the other. This component wires both directions:

**Dagster -> JAMS** (assets):
Each declared JAMS Job becomes a daily-partitioned Dagster asset. Materializing the asset submits the job to JAMS via `POST /Jobs/{name}/Submit`, polls `GET /Entries/{entryId}` until terminal state, retrieves the log, and records duration.

**JAMS -> Dagster** (inbound trigger sensor):
Production wire-up: your JAMS job's post-step calls Dagster's GraphQL API with a `launchRun` mutation. The `jams_inbound_trigger` sensor confirms the trigger was received and produces observable ticks in the Dagster UI.

## Operational ops — Dagster as the pane of glass

Five ops ship as Dagster jobs so you can restart / hold / release / cancel / reconcile JAMS entries directly from the Dagster UI:

| Job | Config | Purpose |
|---|---|---|
| `jams_restart_entry` | `{entry_id}` | Rerun a failed entry (`POST /Entries/{id}/Restart`) |
| `jams_hold_entry` | `{entry_id}` | Hold an entry (`POST /Entries/{id}/Hold`) |
| `jams_release_entry` | `{entry_id}` | Release a held entry (`POST /Entries/{id}/Release`) |
| `jams_cancel_entry` | `{entry_id}` | Cancel a running entry (`POST /Entries/{id}/Cancel`) |
| `jams_reconciliation` | — | Compare JAMS state vs Dagster state; report drift |

The `jams_reconciliation_schedule` runs the reconciliation job every hour (STOPPED by default — toggle in the UI when ready).

## Terminology cheat-sheet — Control-M vs RunMyJobs vs JAMS

For teams running two or more (or migrating between them):

| Control-M | RunMyJobs | JAMS |
|---|---|---|
| Job | JobDefinition | Job |
| Folder | Application | Folder |
| Agent / Host | Queue | Agent |
| ODATE | scheduledTime | scheduledTime |
| runId | processId | Entry ID |
| "Ended OK" / "Ended Not OK" | "Completed" / "Error" | "Completed" / "Failed" |

Sister components with the exact same asset / op / sensor / schedule surface:

- [`controlm_integration`](../controlm_integration/README.md) — BMC Control-M
- [`runmyjobs_integration`](../runmyjobs_integration/README.md) — Redwood RunMyJobs / SAP Redwood

A shop running two or three schedulers can present a single Dagster pane of glass over all of them.

## Hybrid deployment vs migration

Designed for the **hybrid** shape — JAMS keeps owning the batch that only JAMS can own (Windows-centric workflows, SQL Server Agent wrappers, PowerShell chains, legacy on-prem scheduling), Dagster owns the cloud/analytics/AI pipeline, both share a single Dagster UI with correct lineage.

Not a JAMS killer. If you're doing a full migration off JAMS, the `airflow_dag_proxy` / `sql_transform` / `snowflake_workspace` patterns fit better — this component keeps JAMS in the loop.

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Connection

| Field | Type | Default | Description |
|---|---|---|---|
| `endpoint` | `str` | `"https://jams.internal/jams/rest/api"` | JAMS REST API base URL (used when demo_mode=false). |

### Execution

| Field | Type | Default | Description |
|---|---|---|---|
| `poll_interval_seconds` | `int` | `10` | Seconds between JAMS entry status polls. |
| `max_retries` | `int` | `2` | Dagster-side asset retries on failure. |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `group_name` | `str` | `"jams_integration"` | Dagster asset group for the job assets. |

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
| `source_tables` | `List[JAMSSourceTableSpec]` | `list()` | Tables loaded by JAMS that Dagster observes (adds to lineage). |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `demo_mode` | `bool` | `true` | Simulate the REST API on stdout (no external calls). |
| `jobs` | `List[JAMSJobSpec]` | `list()` | JAMS Jobs to wrap as daily-partitioned Dagster assets. |
| `upstream_deps` | `List[str]` | `list()` | Upstream asset keys (slash-separated for nested keys) that must complete before jobs run. |
| `jams_user_env` | `str` | `"JAMS_USER"` | Env var holding the JAMS REST username. |
| `jams_password_env` | `str` | `"JAMS_PASSWORD"` | Env var holding the JAMS REST password. |
| `poll_timeout_seconds` | `int` | `3600` | Total seconds to wait for terminal state before failing. |
| `log_retrieval` | `bool` | `true` | Retrieve entry log on completion. |
| `reconciliation_cron` | `str` | `"0 * * * *"` | Cron for the JAMS vs Dagster state reconciliation job. |

[//]: # (FIELDS:END)

## Example YAML

```yaml
type: dagster_community_components.JAMSIntegrationComponent

attributes:
  demo_mode: true
  endpoint: "https://jams.internal/jams/rest/api"

  group_name: jams_integration
  jams_user_env: JAMS_USER
  jams_password_env: JAMS_PASSWORD

  max_retries: 2
  retry_delay_seconds: 60

  jobs:
    - job_name: EOD_SETTLE
      asset_name: jams_eod_settle
      folder: DAILY_BATCH
      agent: WIN-AGT-01
      application: CORE_BANKING

    - job_name: REGULATORY_EXTRACT
      asset_name: jams_regulatory_extract
      folder: DAILY_BATCH
      agent: WIN-AGT-02
      application: COMPLIANCE

  # Optional — bring JAMS-managed tables into Dagster's lineage
  source_tables:
    - table_name: "CORE_BANKING.SETTLEMENT_LEDGER"
      asset_name: jams_settlement_table
      produced_by: jams_eod_settle
```

## REST API endpoints (modern JSON surface)

```
POST /Jobs/{name}/Submit                        — submit a JAMS Job
GET  /Entries/{entryId}                         — poll entry state
GET  /Entries/{entryId}/Log                     — entry log output
POST /Entries/{entryId}/Restart                 — restart a failed entry
POST /Entries/{entryId}/Hold                    — hold an entry
POST /Entries/{entryId}/Release                 — release a held entry
POST /Entries/{entryId}/Cancel                  — cancel a running entry
GET  /Entries?state=Completed,Failed&
     lastRunAfter=1h&pageSize=200               — list entries for reconciliation
POST /Authentication                            — (optional) exchange creds for token
```

State machine: `Queued -> Scheduled -> Executing -> {Completed | Failed | Cancelled | Held}`. Terminal: `Completed | Failed | Cancelled`.

Verify against your JAMS version's REST reference — the JSON surface has evolved between JAMS 6.x and 7.x builds.

## Requirements

```
dagster
requests
```

## References

- Fortra JAMS docs: <https://docs.jamsscheduler.com/>
- Fortra product page: <https://www.fortra.com/products/workload-automation-jams>
