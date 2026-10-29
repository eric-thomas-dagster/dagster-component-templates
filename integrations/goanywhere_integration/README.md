# GoAnywhereIntegrationComponent

Fortra **GoAnywhere MFT** (Managed File Transfer) GACMD REST API integration. Each declared GoAnywhere Project becomes a **daily-partitioned Dagster asset** with a retry policy, `deps` set per-project (not one list shared by every project). Everything beyond the project assets themselves — rerun / dashboard / reconciliation ops+jobs, the inbound-trigger sensor — is **opt-in** via `generate_*` flags, off by default; only the external-execution monitor sensor is on by default.

`demo_mode: true` (default) swaps in a demo client that logs a simulated GACMD lifecycle on stdout instead of making real HTTP calls. The demo and real clients share one `submit`/`poll` interface (see "Client abstraction" below), so the asset body's submit-then-poll-to-terminal-state loop runs through the *same code* in both modes — demo mode isn't a parallel reimplementation that leaves the real polling logic unexercised. GoAnywhere MFT does ship official Docker images (`fortrallc/goanywhere-mft`, `fortrallc/goanywhere-agent`, `fortrallc/goanywhere-gateway` on Docker Hub), but the admin console and REST API don't do anything useful until activated with a trial or paid license key — Fortra doesn't publish a license-free mode — so the demo client is still the only zero-license way to exercise the calling logic. Flip `demo_mode` to `false` and set `GOANYWHERE_USER` / `GOANYWHERE_PASSWORD` to hit a real GoAnywhere instance.

## What you get

```
Dagster catalog after `dg dev` (defaults — no generate_* flags set):

Assets   ── goanywhere_partner_edi_inbound      [daily partitioned, kinds: python, goanywhere]
         ── goanywhere_regulatory_upload        [daily partitioned, kinds: python, goanywhere]
         ── goanywhere_partner_edi_landed       [source, kinds: goanywhere, file]  (optional)

Sensors  ── goanywhere_external_execution_monitor  (poll every 60s, on by default)
```

Turn on the rest explicitly:

```
generate_rerun_job: true                # Jobs    ── goanywhere_rerun_project       (run config: {project_name})
generate_dashboard_job: true            # Jobs    ── goanywhere_dashboard           (service status / sessions / transfer summary)
generate_reconciliation: true           # Jobs    ── goanywhere_reconciliation      (drift report)
                                         # Schedules ── goanywhere_reconciliation_schedule  (cron: 0 * * * *)
generate_inbound_trigger_sensor: true   # Sensors ── goanywhere_inbound_trigger     (stub -- logs only, see below)
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
       │  external-execution monitor sensor (on by default)       │
       │  op jobs + inbound trigger + reconciliation: opt-in       │
       │  (generate_* flags, off by default)                      │
       └──────────────────────────────────────────────────────────┘
```

## Try it in one command

```bash
bash setup_goanywhere_integration_demo.sh
```

Scaffolds a Dagster project, installs the component (with every `generate_*` flag turned on so the demo shows the full surface), materializes one partition end-to-end, and prints the full simulated GACMD REST API call trace (AUTH → RUN → POLL × 4 → DONE).

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

## Operational ops — opt-in, Dagster as the pane of glass

Three ops ship as Dagster jobs, **off by default** (`generate_rerun_job` / `generate_dashboard_job` / `generate_reconciliation`) so you can rerun projects, pull GoAnywhere's own dashboard summary, and reconcile state directly from the Dagster UI once you actually want them:

| Job | Flag | Config | Purpose |
|---|---|---|---|
| `goanywhere_rerun_project` | `generate_rerun_job` | `{project_name}` | Manually rerun a declared project (`POST .../gacmd/v1/projects`). Off by default — Dagster+'s own re-execution-from-failure covers most of what this would be used for. |
| `goanywhere_dashboard` | `generate_dashboard_job` | — | Pull service status / active sessions / transfer summary (GoAnywhere's ~26 dashboard-style read-only endpoints) |
| `goanywhere_reconciliation` | `generate_reconciliation` | — | Compare GoAnywhere job state vs Dagster state for jobs Dagster has triggered this process; report drift |

`generate_reconciliation: true` also builds `goanywhere_reconciliation_schedule` (hourly, STOPPED by default — toggle in the UI when ready).

Unlike this family's scheduler-style siblings (JAMS, Control-M, RunMyJobs), GoAnywhere's execution model is one-shot project runs, not long-lived schedulable entries you pause/resume mid-flight — so this component doesn't ship hold/release/cancel ops. If your GoAnywhere instance exposes those as REST actions, they're a straightforward addition to `component.py` following the existing op pattern.

The `goanywhere_inbound_trigger` sensor (`generate_inbound_trigger_sensor`, off by default) is a **stub**: in demo mode it logs a simulated message every third tick; in real mode it does nothing until you replace its body with a real event source. It exists to mark the wiring point, not as working functionality out of the box — that's why it defaults off instead of looking like a feature that's already live.

## Client abstraction — demo mode exercises the real calling logic

`_GoAnywhereClient` declares `submit`/`poll`/`dashboard_summary`; `_DemoGoAnywhereClient` and `_RealGoAnywhereClient` both implement it. The asset body, and every op that talks to GoAnywhere, call these methods and nothing else — the submit-then-poll-to-terminal-state loop, the timeout handling, and the terminal-status check all run through the *same code* whether `demo_mode` is `true` or `false`. Only what's inside `submit`/`poll`/`dashboard_summary` differs (print statements vs. real HTTP calls). A bug in the polling loop itself would show up in demo mode, not just in production.

Demo job ids are seeded off `(project_name, scheduled_date, call_count)` rather than random, so a given demo run is reproducible — the same inputs produce the same job id — rather than printing a different id on every run.

## Customizing tags/metadata — `translation:`

The component sets a handful of default tags (`tier`, `domain`, `integration_pattern`, `scheduler_owner`, `deployment_mode`) and metadata (`goanywhere_domain`, `goanywhere_queue`) on every project asset. To add, override, or (for everything except tags/metadata) fully replace what's generated, supply a `translation:` hook — the same native Dagster `TranslationFn` mechanism `hvr_hub_workspace` / `azure_data_factory` / `talend_cloud_workspace` already use in this repo:

```yaml
translation:
  tags:
    team: "{{ project.domain }}"
  group_name: finance_integrations
```

`group_name` / `kinds` / `deps` / `description` / `owners` are **replaced** by whatever you set. `tags` / `metadata` are **merged** onto the component's defaults — this is a Dagster framework behavior (`TRANSLATOR_MERGE_ATTRIBUTES`), not something this component can opt out of, so a default tag/metadata key can be overridden to a new value but not deleted outright by `translation:` alone.

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
| `deps` | `List[str]` | `list()` | Upstream asset keys (slash-separated for nested keys) that must complete before THIS project runs -- per-project, not shared across every project. |

### Sensor configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `job_name` | `str` | — | Optional job name override; defaults to the project name. |

### Source / target

| Field | Type | Default | Description |
|---|---|---|---|
| `mode` | `str` | `"NORMAL"` | Run mode GACMD accepts for runParameters.mode (e.g. NORMAL, TEST). |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `domain` | `str` | — | GoAnywhere Domain the project belongs to (multi-tenant deployments). |
| `job_queue` | `str` | — | GoAnywhere job queue to run on. |
| `priority` | `int` | `5` | GACMD run priority (1-10, higher runs sooner). |
| `variables` | `Dict[str, Any]` | `dict()` | Project variables passed through runParameters.variables (e.g. source/destination paths). |

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

  # Opt-in scaffold -- all off except the external monitor sensor
  generate_rerun_job: false
  generate_dashboard_job: false
  generate_reconciliation: false
  generate_inbound_trigger_sensor: false

  # deps is per-project now, not one list shared by every project
  projects:
    - project_name: PARTNER_EDI_INBOUND
      asset_name: goanywhere_partner_edi_inbound
      job_queue: SFTP_INBOUND
      priority: 5

    - project_name: REGULATORY_UPLOAD
      asset_name: goanywhere_regulatory_upload
      job_queue: SFTP_OUTBOUND
      priority: 8
      deps: ["goanywhere_partner_edi_inbound"]

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
