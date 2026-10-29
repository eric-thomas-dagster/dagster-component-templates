# OpConIntegrationComponent

**SMA Technologies / Continuous OpCon** workload-automation REST API integration. Each declared OpCon Daily Job becomes a **daily-partitioned Dagster asset** with a retry policy. Ships with 5 operational ops + jobs (force-restart / hold / release / cancel / reconcile), 2 sensors (external-execution monitor + inbound trigger), and an hourly reconciliation schedule.

`demo_mode: true` (default) simulates the whole REST API lifecycle (token exchange + scheduleactions + dailyjobs status polling) on stdout — the component runs end-to-end with zero external dependencies. OpCon is on-prem workload-automation software with no public Docker image or license-free trial, so the simulator is the only zero-license way to smoke-test the full surface. Flip to `false` and set `OPCON_USER` / `OPCON_PASSWORD` to hit a real OpCon instance.

## What you get

```
Dagster catalog after `dg dev`:

Assets   ── opcon_eod_ledger_close             [daily partitioned, kinds: python, opcon]
         ── opcon_regulatory_extract           [daily partitioned, kinds: python, opcon]
         ── opcon_gl_ledger_table              [source, kinds: opcon, database]  (optional)

Jobs     ── opcon_force_restart_job            (run config: {job_id})
         ── opcon_hold_job                     (run config: {job_id})
         ── opcon_release_job                  (run config: {job_id})
         ── opcon_cancel_job                   (run config: {job_id})
         ── opcon_reconciliation               (drift report)

Sensors  ── opcon_external_execution_monitor   (poll every 60s)
         ── opcon_inbound_trigger              (OpCon -> Dagster GraphQL)

Schedules ── opcon_reconciliation_schedule     (cron: 0 * * * *)
```

## Integration pattern

```
       ┌──────────────────────────────────────────────────────────┐
       │        SMA Technologies / Continuous OpCon (on-prem)      │
       │                                                          │
       │  Schedule: DAILY_FINANCE                                 │
       │   ├─ EOD_LEDGER_CLOSE     (machine: AS400-PROD-01)       │
       │   └─ REGULATORY_EXTRACT   (machine: AS400-PROD-01)       │
       │                                                          │
       │  REST API  (Authorization: Token <token> from /tokens)   │
       └──────────────────────────────┬───────────────────────────┘
                                      │
        Dagster -> OpCon              │ OpCon -> Dagster
        POST /tokens                  │ POST https://dagster.cloud/graphql
        POST /api/scheduleactions     │ mutation { launchRun(...) }
             {action: JOB:FORCERESTART}
        GET  /api/dailyjobs/{jId}/status
        GET  /api/dailyjobs/count_by_status
                                      ▼
       ┌──────────────────────────────────────────────────────────┐
       │        OpConIntegrationComponent (one YAML)                │
       │                                                          │
       │  daily-partitioned assets  +  RetryPolicy(max_retries=2) │
       │  op jobs (force-restart / hold / release / cancel /      │
       │           reconcile)                                     │
       │  sensors (external-execution monitor + inbound trigger)  │
       │  hourly reconciliation schedule                          │
       └──────────────────────────────────────────────────────────┘
```

## Try it in one command

```bash
bash setup_opcon_integration_demo.sh
```

Scaffolds a Dagster project, installs the component in demo mode, materializes one partition end-to-end, and prints the full simulated REST API call trace (AUTH → ACTION → POLL × 4 → EVENTS → DONE).

Then open the Dagster UI:

```bash
cd opcon-demo
uv run dg dev
```

## Point at a real OpCon instance

```bash
export OPCON_USER=svc_dagster
export OPCON_PASSWORD='<your-password>'
```

Set `demo_mode: false` and override `endpoint` to your OpCon REST base URL:

```yaml
attributes:
  demo_mode: false
  endpoint: "https://opcon.prod.internal"
  ...
```

> **API-path caveat.** OpCon has the richest publicly documented API in this integration family — a full hosted OpenAPI/Swagger reference (`help.smatechnologies.com/opcon/core/api/22-0`), a live Swagger UI on every instance (`/api-doc/index.html`), and even an official Java client on Maven Central. The endpoints this component calls (`POST /tokens`, `POST /api/scheduleactions` with `JOB:START`/`JOB:FORCERESTART`, `GET /api/dailyjobs/{jId}/status`, `GET /api/dailyjobs/count_by_status`) are confirmed from SMA/Continuous's own docs and community posts. The exact `JOB:HOLD` / `JOB:RELEASE` / `JOB:CANCEL` action names used by `hold_opcon_job` / `release_opcon_job` / `cancel_opcon_job` follow the same documented `JOB:<VERB>` convention as the confirmed START/FORCERESTART actions, but weren't individually confirmed from public sources — verify them (and every request-body field name) against your instance's own Swagger UI before relying on this in production.

> **No public Docker image.** OpCon is on-prem workload-automation software (traditionally mainframe/AS400/Windows-centric) with no public container image or license-free trial. The demo simulator is the only zero-license way to exercise the full surface; production wiring targets a real installed OpCon instance.

## The two-way trigger story

OpCon and Dagster both need to be able to start work in the other. This component wires both directions:

**Dagster -> OpCon** (assets):
Each declared OpCon Daily Job becomes a daily-partitioned Dagster asset. Materializing the asset exchanges credentials for a token (`POST /tokens`), force-starts the job via `POST /api/scheduleactions`, polls `GET /api/dailyjobs/{jId}/status` until terminal state, and records the job id + duration.

**OpCon -> Dagster** (inbound trigger sensor):
Production wire-up: an OpCon job (or a generic HTTP/REST job step) calls Dagster's GraphQL API with a `launchRun` mutation. The `opcon_inbound_trigger` sensor confirms the trigger was received and produces observable ticks in the Dagster UI.

## Operational ops — Dagster as the pane of glass

Five ops ship as Dagster jobs so you can force-restart / hold / release / cancel / reconcile OpCon daily jobs directly from the Dagster UI:

| Job | Config | Purpose |
|---|---|---|
| `opcon_force_restart_job` | `{job_id}` | Force-restart a job, ignoring dependencies (`JOB:FORCERESTART`, confirmed) |
| `opcon_hold_job` | `{job_id}` | Hold a job (`JOB:HOLD`, verify against your version) |
| `opcon_release_job` | `{job_id}` | Release a held job (`JOB:RELEASE`, verify against your version) |
| `opcon_cancel_job` | `{job_id}` | Cancel a running job (`JOB:CANCEL`, verify against your version) |
| `opcon_reconciliation` | — | `GET /api/dailyjobs/count_by_status` (confirmed endpoint); compare against Dagster state, report drift |

The `opcon_reconciliation_schedule` runs the reconciliation job every hour (STOPPED by default — toggle in the UI when ready).

## Hybrid deployment vs migration

Designed for the **hybrid** shape — OpCon keeps owning the batch that only OpCon can own (mainframe/AS400/Windows-centric legacy scheduling, cross-platform dependency chains), Dagster owns the cloud/analytics/AI pipeline, both share a single Dagster UI with correct lineage.

Not an OpCon killer. This component keeps OpCon in the loop as the workload-automation system of record.

Sister components with a similar asset / op / sensor / schedule surface for other workload-automation and scheduler platforms:

- [`jams_integration`](../jams_integration/README.md) — Fortra JAMS Scheduler
- [`controlm_integration`](../controlm_integration/README.md) — BMC Control-M
- [`goanywhere_integration`](../goanywhere_integration/README.md) — Fortra GoAnywhere MFT

A shop running two or three schedulers can present a single Dagster pane of glass over all of them.

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `job_name` | `str` | OpCon Job name (as registered on the OpCon schedule). |
| `asset_name` | `str` | Dagster asset name that wraps this Job. |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `description` | `str` | — | Prose description shown in the Dagster UI. |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `schedule_name` | `str` | — | OpCon Schedule the job belongs to. |
| `machine` | `str` | — | OpCon Machine (agent) the job runs on. |
| `department` | `str` | — | OpCon Department the job is organized under. |
| `force_restart` | `bool` | `true` | Use JOB:FORCERESTART (ignores dependencies, starts immediately) instead of JOB:START (honors dependencies). |

[//]: # (FIELDS:END)

## Example YAML

```yaml
type: dagster_community_components.OpConIntegrationComponent

attributes:
  demo_mode: true
  endpoint: "https://opcon.internal"

  group_name: opcon_integration
  opcon_user_env: OPCON_USER
  opcon_password_env: OPCON_PASSWORD

  max_retries: 2
  retry_delay_seconds: 60

  jobs:
    - job_name: EOD_LEDGER_CLOSE
      asset_name: opcon_eod_ledger_close
      schedule_name: DAILY_FINANCE
      machine: AS400-PROD-01
      force_restart: true

    - job_name: REGULATORY_EXTRACT
      asset_name: opcon_regulatory_extract
      schedule_name: DAILY_FINANCE
      machine: AS400-PROD-01

  # Optional — bring OpCon-managed tables into Dagster's lineage
  source_tables:
    - table_name: "FINANCE.GL_LEDGER"
      asset_name: opcon_gl_ledger_table
      produced_by: opcon_eod_ledger_close
```

## REST API endpoints

```
POST /tokens                              — exchange credentials for a short-lived token
POST /api/scheduleactions                 — {action: "JOB:START"|"JOB:FORCERESTART"|..., jobName/jId, scheduleName, jobDate} -> jId
GET  /api/dailyjobs                       — list daily jobs (rich filters: status, schedule, machine, dates, tags, dept, access code)
GET  /api/dailyjobs/{jId}                 — one daily job
GET  /api/dailyjobs/{jId}/status          — poll job status
GET  /api/dailyjobs/{jId}/events          — job event history
GET  /api/dailyjobs/count_by_status       — status counts, used for reconciliation
GET  /api/jobhistories                    — historical job runs
```

Terminal statuses: `Successful | Failed | Cancelled | Skipped`. Full reference (257 endpoints, OpenAPI/Swagger): `help.smatechnologies.com/opcon/core/api/22-0`, or `/api-doc/index.html` on your own instance.

## Requirements

```
dagster
requests
```

## References

- OpCon REST API reference (22.0): <https://help.smatechnologies.com/opcon/core/api/22-0>
- Getting Started with the OpCon API: <https://community.continuous.com/rest-api-40/getting-started-with-the-opcon-api-469>
- Official Java client: <https://github.com/SMATechnologies/opcon-rest-api-client-java>
