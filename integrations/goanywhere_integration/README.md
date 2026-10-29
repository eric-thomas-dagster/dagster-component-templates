# GoAnywhereIntegrationComponent

Fortra **GoAnywhere MFT** (Managed File Transfer) GACMD REST API integration. Each declared GoAnywhere Project becomes a **daily-partitioned Dagster asset** with a retry policy. Ships with 3 operational ops + jobs (rerun / dashboard summary / reconcile), 2 sensors (external-execution monitor + inbound trigger), and an hourly reconciliation schedule.

`demo_mode: true` (default) simulates the whole GACMD REST API lifecycle on stdout — the component runs end-to-end with zero external dependencies. GoAnywhere MFT does ship official Docker images (`fortrallc/goanywhere-mft`, `fortrallc/goanywhere-agent`, `fortrallc/goanywhere-gateway` on Docker Hub), but the admin console and REST API don't do anything useful until activated with a trial or paid license key — Fortra doesn't publish a license-free mode — so the simulator is still the only zero-license way to smoke-test the full surface. Flip to `false` and set `GOANYWHERE_USER` / `GOANYWHERE_PASSWORD` to hit a real GoAnywhere instance.

## What you get

```
Dagster catalog after `dg dev`:

Assets   ── goanywhere_partner_edi_inbound      [daily partitioned, kinds: python, goanywhere]
         ── goanywhere_regulatory_upload        [daily partitioned, kinds: python, goanywhere]
         ── goanywhere_partner_edi_landed       [source, kinds: goanywhere, file]  (optional)

Jobs     ── goanywhere_rerun_project            (run config: {project_name})
         ── goanywhere_dashboard                (service status / sessions / transfer summary)
         ── goanywhere_reconciliation           (drift report)

Sensors  ── goanywhere_external_execution_monitor  (poll every 60s)
         ── goanywhere_inbound_trigger             (GoAnywhere -> Dagster GraphQL)

Schedules ── goanywhere_reconciliation_schedule  (cron: 0 * * * *)
```

## Integration pattern

```
       ┌──────────────────────────────────────────────────────────┐
       │          Fortra GoAnywhere MFT (managed file transfer)   │
       │                                                          │
       │  Queue: SFTP_INBOUND                                     │
       │   ├─ PARTNER_EDI_INBOUND  (pickup + decrypt + validate)  │
       │  Queue: SFTP_OUTBOUND                                    │
       │   └─ REGULATORY_UPLOAD    (encrypted external upload)    │
       │                                                          │
       │  GACMD REST API  (Authorization: Basic base64(user:pass))│
       └──────────────────────────────┬───────────────────────────┘
                                      │
        Dagster -> GoAnywhere         │ GoAnywhere -> Dagster
        POST .../gacmd/v1/projects    │ POST https://dagster.cloud/graphql
        GET  .../gacmd/v1/jobs/{id}   │ mutation { launchRun(...) }
             ?jobStatusFormat=JSON    │ (Project post-step, or a file-arrival
                                      │  Trigger firing a Project independently)
                                      ▼
       ┌──────────────────────────────────────────────────────────┐
       │    GoAnywhereIntegrationComponent (one YAML)              │
       │                                                          │
       │  daily-partitioned assets  +  RetryPolicy(max_retries=2) │
       │  op jobs (rerun / dashboard summary / reconcile)         │
       │  sensors (external-execution monitor + inbound trigger)  │
       │  hourly reconciliation schedule                          │
       └──────────────────────────────────────────────────────────┘
```

## Try it in one command

```bash
bash setup_goanywhere_integration_demo.sh
```

Scaffolds a Dagster project, installs the component in demo mode, materializes one partition end-to-end, and prints the full simulated GACMD REST API call trace (AUTH → RUN → POLL × 4 → DONE).

Then open the Dagster UI:

```bash
cd goanywhere-demo
uv run dg dev
```

## Point at a real GoAnywhere instance

```bash
export GOANYWHERE_USER=svc_dagster
export GOANYWHERE_PASSWORD='<your-password-or-admin-api-key>'
```

Set `demo_mode: false` and override `endpoint` to your GoAnywhere REST base URL:

```yaml
attributes:
  demo_mode: false
  endpoint: "https://goanywhere.prod.internal/goanywhere/rest"
  ...
```

> **API-path caveat.** Fortra doesn't publish a single hosted GACMD REST API reference the way SMA does for OpCon — the shape here (`POST .../gacmd/v1/projects` with a `runParameters` body; `GET .../gacmd/v1/jobs/{jobId}?jobStatusFormat=JSON` returning Q/A/P/S/F/C) is confirmed from Fortra's own product/automation pages and forum posts, not a single authoritative spec. Verify exact field names and the dashboard-endpoint paths (`/status`, `/sessions/active`, `/transfers/summary` in `goanywhere_dashboard_summary` are representative, not guaranteed-exact) against your instance's built-in REST documentation (Admin > Documentation) before relying on this in production.

> **Licensed software — no zero-license live test.** GoAnywhere MFT publishes real Docker images (`fortrallc/goanywhere-mft` et al.), unlike some sibling schedulers in this family, but the application itself needs a trial or paid license key to do anything through the admin console or REST API. There's no documented license-free mode. The demo simulator is the only zero-license way to exercise the full surface; production wiring targets a real licensed GoAnywhere instance (trial or paid).

## The two-way trigger story

GoAnywhere and Dagster both need to be able to start work in the other. This component wires both directions:

**Dagster -> GoAnywhere** (assets):
Each declared GoAnywhere Project becomes a daily-partitioned Dagster asset. Materializing the asset runs the project via `POST .../gacmd/v1/projects`, polls `GET .../gacmd/v1/jobs/{jobId}` until terminal state, and records the job id + duration.

**GoAnywhere -> Dagster** (inbound trigger sensor):
Production wire-up: a GoAnywhere Project's post-step (or a file-arrival Trigger that fires a Project independent of Dagster) calls Dagster's GraphQL API with a `launchRun` mutation. The `goanywhere_inbound_trigger` sensor confirms the trigger was received and produces observable ticks in the Dagster UI.

## Operational ops — Dagster as the pane of glass

Three ops ship as Dagster jobs so you can rerun projects, pull GoAnywhere's own dashboard summary, and reconcile state directly from the Dagster UI:

| Job | Config | Purpose |
|---|---|---|
| `goanywhere_rerun_project` | `{project_name}` | Manually rerun a declared project (`POST .../gacmd/v1/projects`) |
| `goanywhere_dashboard` | — | Pull service status / active sessions / transfer summary (GoAnywhere's ~26 dashboard-style read-only endpoints) |
| `goanywhere_reconciliation` | — | Compare GoAnywhere job state vs Dagster state for jobs Dagster has triggered; report drift |

The `goanywhere_reconciliation_schedule` runs the reconciliation job every hour (STOPPED by default — toggle in the UI when ready).

Unlike this family's scheduler-style siblings (JAMS, Control-M, RunMyJobs), GoAnywhere's execution model is one-shot project runs, not long-lived schedulable entries you pause/resume mid-flight — so this component doesn't ship hold/release/cancel ops. If your GoAnywhere instance exposes those as REST actions, they're a straightforward addition to `component.py` following the existing op pattern.

## Hybrid deployment vs migration

Designed for the **hybrid** shape — GoAnywhere keeps owning the file-transfer-centric batch that only an MFT platform does well (SFTP/AS2/PGP, partner EDI, compliance-grade encryption and audit trails), Dagster owns the cloud/analytics/AI pipeline downstream of the landed files, both share a single Dagster UI with correct lineage.

Not a GoAnywhere killer. This component keeps GoAnywhere in the loop as the file-transfer system of record.

Sister components with a similar asset / op / sensor / schedule surface for other workload-automation and scheduler platforms:

- [`jams_integration`](../jams_integration/README.md) — Fortra JAMS Scheduler
- [`controlm_integration`](../controlm_integration/README.md) — BMC Control-M
- [`opcon_integration`](../opcon_integration/README.md) — SMA/Continuous OpCon

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `project_name` | `str` | GoAnywhere Project name (as registered in the GoAnywhere server). |
| `asset_name` | `str` | Dagster asset name that wraps this Project. |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `description` | `str` | — | Prose description shown in the Dagster UI. |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `domain` | `str` | — | GoAnywhere Domain the project belongs to (multi-tenant deployments). |
| `job_name` | `str` | — | Optional job name override; defaults to the project name. |
| `job_queue` | `str` | — | GoAnywhere job queue to run on. |
| `mode` | `str` | `"NORMAL"` | Run mode GACMD accepts for runParameters.mode (e.g. NORMAL, TEST). |
| `priority` | `int` | `5` | GACMD run priority (1-10, higher runs sooner). |
| `variables` | `dict` | `{}` | Project variables passed through runParameters.variables (e.g. source/destination paths). |

[//]: # (FIELDS:END)

## Example YAML

```yaml
type: dagster_community_components.GoAnywhereIntegrationComponent

attributes:
  demo_mode: true
  endpoint: "https://goanywhere.internal/goanywhere/rest"

  group_name: goanywhere_integration
  goanywhere_user_env: GOANYWHERE_USER
  goanywhere_password_env: GOANYWHERE_PASSWORD

  max_retries: 2
  retry_delay_seconds: 60

  projects:
    - project_name: PARTNER_EDI_INBOUND
      asset_name: goanywhere_partner_edi_inbound
      job_queue: SFTP_INBOUND
      priority: 5

    - project_name: REGULATORY_UPLOAD
      asset_name: goanywhere_regulatory_upload
      job_queue: SFTP_OUTBOUND
      priority: 8

  # Optional — bring GoAnywhere-landed files into Dagster's lineage
  source_files:
    - path: "/inbound/partners/processed"
      asset_name: goanywhere_partner_edi_landed
      produced_by: goanywhere_partner_edi_inbound
```

## REST API endpoints (GACMD)

```
POST .../gacmd/v1/projects                       — run a GoAnywhere Project (runParameters body) -> jobId
GET  .../gacmd/v1/jobs/{jobId}?jobStatusFormat=JSON  — poll job status
GET  .../status                                  — service status (dashboard-style, path representative)
GET  .../sessions/active                         — active transfer sessions (path representative)
GET  .../transfers/summary                       — transfer summary (path representative)
```

Status codes: `Q` (queued), `A` (active), `P` (paused), `S` (successful), `F` (failed), `C` (cancelled). Terminal: `S | F | C`.

Fortra doesn't host a single public GACMD REST reference page — confirm exact field names and the dashboard-endpoint paths against your instance's built-in REST documentation (Admin > Documentation) before going to production.

## Requirements

```
dagster
requests
```

## References

- Commands and APIs (GACMD overview): <https://www.goanywhere.com/products/goanywhere-mft/automation/commands-apis>
- API Integration with GoAnywhere MFT (auth + scope): <https://www.goanywhere.com/resources/guides/api-integration-goanywhere-mft>
- Docker images: <https://hub.docker.com/u/fortrallc>
