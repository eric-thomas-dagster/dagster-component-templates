# ActiveBatchIntegrationComponent

**ActiveBatch** (now owned by Redwood, formerly Advanced Systems Concepts) REST API integration. Each declared ActiveBatch Job becomes a **daily-partitioned Dagster asset** with a retry policy. Ships with 5 operational ops + jobs (restart / hold / release / abort / reconcile), 2 sensors (external-execution monitor + inbound trigger), and an hourly reconciliation schedule.

`demo_mode: true` (default) simulates the whole REST API lifecycle on stdout — the component runs end-to-end with zero external dependencies. Flip to `false` and set `ACTIVEBATCH_USER` / `ACTIVEBATCH_PASSWORD` to hit a real ActiveBatch instance.

> **No Docker image.** ActiveBatch does not publish public Docker images — the software is Windows-native, licensed to Redwood customers, and typically installed on a Windows Server host with a SQL Server backend. The `demo_mode` simulator ships end-to-end zero-license so you can validate the wiring before pointing at your real ActiveBatch server.

## What you get

```
Dagster catalog after `dg dev`:

Assets    ── ab_eod_settlement                 [daily partitioned, kinds: python, activebatch]
          ── ab_regulatory_extract             [daily partitioned, kinds: python, activebatch]
          ── ab_settlement_table               [source, kinds: activebatch, database]  (optional)

Jobs      ── activebatch_restart_instance      (run config: {instance_id})
          ── activebatch_hold_instance         (run config: {instance_id})
          ── activebatch_release_instance      (run config: {instance_id})
          ── activebatch_abort_instance        (run config: {instance_id})
          ── activebatch_reconciliation        (drift report)

Sensors   ── activebatch_external_execution_monitor   (poll every 60s)
          ── activebatch_inbound_trigger              (ActiveBatch -> Dagster GraphQL)

Schedules ── activebatch_reconciliation_schedule (cron: 0 * * * *)
```

## Integration pattern

```
       ┌──────────────────────────────────────────────────────────┐
       │             ActiveBatch (Redwood) scheduler              │
       │                                                          │
       │  Plan: DAILY_BATCH                                       │
       │   ├─ EOD_SETTLEMENT       (queue: WIN-Q-01)              │
       │   └─ REGULATORY_EXTRACT   (queue: COMPLIANCE-Q-01)       │
       │                                                          │
       │  REST API  (Authorization: Basic base64(user:pass))      │
       └──────────────────────────────┬───────────────────────────┘
                                      │
        Dagster -> ActiveBatch        │ ActiveBatch -> Dagster
        POST /Objects/{id}/Triggers   │ POST https://dagster.cloud/graphql
        GET  /Instances/{id}          │ mutation { launchRun(...) }
        GET  /Instances/{id}/Log      │
        POST /Instances/{id}/Events   │
                                      ▼
       ┌──────────────────────────────────────────────────────────┐
       │      ActiveBatchIntegrationComponent (one YAML)          │
       │                                                          │
       │  daily-partitioned assets  +  RetryPolicy(max_retries=2) │
       │  op jobs (restart / hold / release / abort / reconcile)  │
       │  sensors (external-execution monitor + inbound trigger)  │
       │  hourly reconciliation schedule                          │
       └──────────────────────────────────────────────────────────┘
```

## Try it in one command

```bash
bash setup_activebatch_integration_demo.sh
```

Scaffolds a Dagster project, installs the component in demo mode, materializes one partition end-to-end, and prints the full simulated REST API call trace (AUTH → TRIGGER → POLL × 5 → LOG → DONE → EVENT).

Then open the Dagster UI:

```bash
cd activebatch-demo
uv run dg dev
```

## Point at a real ActiveBatch instance

```bash
export ACTIVEBATCH_USER=svc_dagster
export ACTIVEBATCH_PASSWORD='<your-password>'
```

Set `demo_mode: false` and override `endpoint` to your ActiveBatch REST base URL:

```yaml
attributes:
  demo_mode: false
  endpoint: "http://activebatch.prod.internal/absvc/api/v1"
  ...
```

> **API-path caveat.** ActiveBatch REST paths shift across versions (v11 / v12 / v13 shipped meaningful surface changes, and the pre-Redwood ABAT REST layer differed from the modern absvc surface). The URIs in this component target the modern JSON REST surface (`/absvc/api/v1/...`). If your instance uses a different prefix (e.g. `/absvc/api/v2/…` or the older `/ABatSvc/rest/…`), either bake the prefix into `endpoint` or fork `_execute_activebatch` in `component.py` to match. Windows-integrated authentication is also supported by ActiveBatch — this component uses HTTP Basic for portability; swap `_activebatch_headers` for `requests_negotiate_sspi.HttpNegotiateAuth` if you need SSPI/Kerberos. Demo mode is unaffected — the whole simulator runs on stdout.

## The two-way trigger story

ActiveBatch and Dagster both need to be able to start work in the other. This component wires both directions:

**Dagster -> ActiveBatch** (assets):
Each declared Job becomes a daily-partitioned Dagster asset. Materializing the asset triggers the job via `POST /Objects/{objectId}/Triggers`, polls `GET /Instances/{instanceId}` until terminal state, retrieves the instance log, and posts a `DAGSTER_JOB_COMPLETE` event back to ActiveBatch.

**ActiveBatch -> Dagster** (inbound trigger sensor):
Production wire-up: your ActiveBatch job's post-step calls Dagster's GraphQL API with a `launchRun` mutation. The `activebatch_inbound_trigger` sensor confirms the trigger was received and produces observable ticks in the Dagster UI.

## Operational ops — Dagster as the pane of glass

Five ops ship as Dagster jobs so you can restart / hold / release / abort / reconcile ActiveBatch instances directly from the Dagster UI:

| Job | Config | Purpose |
|---|---|---|
| `activebatch_restart_instance` | `{instance_id}` | Restart an instance (`POST /Instances/{id}/Restart`) |
| `activebatch_hold_instance` | `{instance_id}` | Place an instance on hold (`POST /Instances/{id}/Hold`) |
| `activebatch_release_instance` | `{instance_id}` | Release a held instance (`POST /Instances/{id}/Release`) |
| `activebatch_abort_instance` | `{instance_id}` | Abort a running instance (`POST /Instances/{id}/Abort`) |
| `activebatch_reconciliation` | — | Compare ActiveBatch state vs Dagster state; report drift |

The `activebatch_reconciliation_schedule` runs the reconciliation job every hour (STOPPED by default — toggle in the UI when ready).

## Terminology cheat-sheet — Control-M vs RunMyJobs vs ActiveBatch

For teams running more than one of these (or migrating between them):

| Control-M | RunMyJobs | ActiveBatch |
|---|---|---|
| Job | JobDefinition | Job (identified by numeric objectId) |
| Folder | Application | Plan |
| Agent / Host | Queue | Execution Queue |
| ODATE | scheduledTime | scheduledDate argument |
| runId | processId | instanceId |
| "Ended OK" / "Ended Not OK" | "Completed" / "Error" | "Succeeded" / "Failed" |

Sister components with the same asset / op / sensor / schedule surface:
- [`runmyjobs_integration`](../runmyjobs_integration/README.md) — Redwood RunMyJobs (SAP Redwood Scheduler)
- [`controlm_integration`](../controlm_integration/README.md) — BMC Control-M

A shop running any combination can present a single Dagster pane of glass over all three schedulers.

## Hybrid deployment vs migration

Designed for the **hybrid** shape — ActiveBatch keeps owning the batch that only ActiveBatch can own (Windows service orchestration, SQL Agent proxies, legacy scheduling, ERP job chains), Dagster owns the cloud/analytics/AI pipeline, both share a single Dagster UI with correct lineage.

Not an ActiveBatch killer. If you're doing a full migration off ActiveBatch, the `airflow_dag_proxy` / `sql_transform` / `snowflake_workspace` patterns fit better — this component keeps ActiveBatch in the loop.

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Connection

| Field | Type | Default | Description |
|---|---|---|---|
| `endpoint` | `str` | `"http://activebatch.internal/absvc/api/v1"` | ActiveBatch REST API base URL (used when demo_mode=false). |

### Execution

| Field | Type | Default | Description |
|---|---|---|---|
| `poll_interval_seconds` | `int` | `10` | Seconds between ActiveBatch instance status polls. |
| `max_retries` | `int` | `2` | Dagster-side asset retries on failure. |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `group_name` | `str` | `"activebatch_integration"` | Dagster asset group for the job assets. |

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
| `source_tables` | `List[ActiveBatchSourceTableSpec]` | `list()` | Tables loaded by ActiveBatch that Dagster observes (adds to lineage). |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `demo_mode` | `bool` | `true` | Simulate the REST API on stdout (no external calls). |
| `jobs` | `List[ActiveBatchJobSpec]` | `list()` | ActiveBatch Jobs to wrap as daily-partitioned Dagster assets. |
| `upstream_deps` | `List[str]` | `list()` | Upstream asset keys (slash-separated for nested keys) that must complete before jobs run. |
| `activebatch_user_env` | `str` | `"ACTIVEBATCH_USER"` | Env var holding the ActiveBatch REST username. |
| `activebatch_password_env` | `str` | `"ACTIVEBATCH_PASSWORD"` | Env var holding the ActiveBatch REST password. |
| `poll_timeout_seconds` | `int` | `3600` | Total seconds to wait for terminal state before failing. |
| `log_retrieval` | `bool` | `true` | Retrieve instance log on completion. |
| `reconciliation_cron` | `str` | `"0 * * * *"` | Cron for the ActiveBatch vs Dagster state reconciliation job. |

[//]: # (FIELDS:END)

## Example YAML

```yaml
type: dagster_community_components.ActiveBatchIntegrationComponent

attributes:
  demo_mode: true
  endpoint: "http://activebatch.internal/absvc/api/v1"

  group_name: activebatch_integration
  activebatch_user_env: ACTIVEBATCH_USER
  activebatch_password_env: ACTIVEBATCH_PASSWORD

  max_retries: 2
  retry_delay_seconds: 60

  jobs:
    - object_id: "12345"
      asset_name: ab_eod_settlement
      plan: DAILY_BATCH
      execution_queue: WIN-Q-01
      application: CORE_BANKING

    - object_id: "12346"
      asset_name: ab_regulatory_extract
      plan: DAILY_BATCH
      execution_queue: COMPLIANCE-Q-01
      application: COMPLIANCE

  # Optional — bring ActiveBatch-managed tables into Dagster's lineage
  source_tables:
    - table_name: "CORE_BANKING.SETTLEMENT_LEDGER"
      asset_name: ab_settlement_table
      produced_by: ab_eod_settlement
```

## REST API endpoints (modern JSON surface)

```
POST /Objects/{objectId}/Triggers      — trigger a Job (returns instanceId)
GET  /Instances/{instanceId}           — poll instance state
GET  /Instances/{instanceId}/Log       — instance log
POST /Instances/{instanceId}/Events    — post feedback event
POST /Instances/{instanceId}/Restart   — restart an instance
POST /Instances/{instanceId}/Hold      — place instance on hold
POST /Instances/{instanceId}/Release   — release a held instance
POST /Instances/{instanceId}/Abort     — abort a running instance
GET  /Instances?filter=state:Succeeded,Failed&recent=1h&limit=200
                                       — list recent instances (reconciliation)
```

Terminal states: `Succeeded`, `Failed`, `Aborted`, `Skipped`.

Verify against your ActiveBatch version's REST reference — the JSON surface has evolved across ActiveBatch v11 / v12 / v13 and the pre-Redwood ABAT REST layer.

## Requirements

```
dagster
requests
```

## References

- ActiveBatch (Redwood) product page: <https://www.advsyscon.com/en-us/activebatch>
- Redwood developer portal: <https://www.redwood.com/product/activebatch/>
