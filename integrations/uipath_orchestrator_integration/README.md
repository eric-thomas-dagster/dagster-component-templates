# UiPathOrchestratorIntegrationComponent

**UiPath Orchestrator** REST API integration — the RPA-domain sibling of [`runmyjobs_integration`](../runmyjobs_integration/README.md) and [`controlm_integration`](../controlm_integration/README.md). Each declared Release becomes a **daily-partitioned Dagster asset** with a retry policy. Ships with 5 operational ops + jobs (restart / soft-stop / kill / schedule-disable / reconcile), 2 sensors (external-execution monitor + inbound trigger), and an hourly reconciliation schedule.

`demo_mode: true` (default) simulates the whole REST API lifecycle on stdout — the component runs end-to-end with zero external dependencies and zero UiPath licensing. Flip to `false` and set `UIPATH_CLIENT_ID` / `UIPATH_CLIENT_SECRET` to hit a real Orchestrator instance.

## What you get

```
Dagster catalog after `dg dev`:

Assets   ── uipath_invoice_processing         [daily partitioned, kinds: python, uipath, rpa]
         ── uipath_kyc_screening              [daily partitioned, kinds: python, uipath, rpa]
         ── uipath_invoice_table              [source, kinds: uipath, database]  (optional)

Jobs     ── uipath_restart_job                (run config: {release_key, folder})
         ── uipath_stop_job_soft              (run config: {job_id})
         ── uipath_stop_job_kill              (run config: {job_id})
         ── uipath_disable_schedule           (run config: {schedule_id})
         ── uipath_reconciliation             (drift report)

Sensors  ── uipath_external_execution_monitor   (poll every 60s)
         ── uipath_inbound_trigger              (UiPath -> Dagster GraphQL)

Schedules ── uipath_reconciliation_schedule   (cron: 0 * * * *)
```

## Integration pattern

```
       ┌──────────────────────────────────────────────────────────┐
       │             UiPath Orchestrator                          │
       │                                                          │
       │  Folder: Finance (id 42)                                 │
       │   ├─ Release: invoice_processing (robots: finance_*)     │
       │   └─ Release: kyc_screening      (robots: compliance_*)  │
       │                                                          │
       │  REST API  (Authorization: Bearer <token>                │
       │             X-UIPATH-OrganizationUnitId: 42)             │
       └──────────────────────────────┬───────────────────────────┘
                                      │
        Dagster -> UiPath             │ UiPath -> Dagster
        POST /identity_/connect/token │ POST https://dagster.cloud/graphql
        POST /odata/Jobs/…/StartJobs  │ mutation { launchRun(...) }
        GET  /odata/Jobs({id})        │
        POST /odata/Jobs({id})/       │
             …/StopJob                │
                                      ▼
       ┌──────────────────────────────────────────────────────────┐
       │   UiPathOrchestratorIntegrationComponent (one YAML)      │
       │                                                          │
       │  daily-partitioned assets  +  RetryPolicy(max_retries=2) │
       │  op jobs (restart / soft-stop / kill /                   │
       │           schedule-disable / reconcile)                  │
       │  sensors (external-execution monitor + inbound trigger)  │
       │  hourly reconciliation schedule                          │
       └──────────────────────────────────────────────────────────┘
```

## Try it in one command

```bash
bash setup_uipath_orchestrator_integration_demo.sh
```

Scaffolds a Dagster project, installs the component in demo mode, materializes one partition end-to-end, and prints the full simulated REST API call trace (AUTH → FOLDER → START → POLL × 5 → OUTPUT → DONE).

Then open the Dagster UI:

```bash
cd uipath-orchestrator-demo
uv run dg dev
```

## Point at a real Orchestrator instance

1. Create an org + tenant at [cloud.uipath.com/signup](https://cloud.uipath.com/signup) (the "Community" tier is free) or use your existing Automation Cloud / on-prem Orchestrator.
2. In the Orchestrator UI go to **Admin -> External Applications** and register a Confidential application. Grant it the scopes `OR.Jobs`, `OR.Execution`, `OR.Folders`. Copy the generated `App ID` (client_id) and `App Secret` (client_secret).
3. Export the credentials:

```bash
export UIPATH_CLIENT_ID='<app-id>'
export UIPATH_CLIENT_SECRET='<app-secret>'
```

4. Set `demo_mode: false` and override `endpoint` to your Orchestrator base URL:

```yaml
attributes:
  demo_mode: false
  endpoint: "https://cloud.uipath.com/myorg/mytenant/orchestrator_"
  # ...on-prem:
  # endpoint: "https://orchestrator.internal"
```

> **API-surface caveat.** UiPath REST paths and payload shapes vary across Automation Cloud, standalone on-prem Orchestrator, and Orchestrator versions. The URIs in this component target the modern OData surface documented in the [UiPath Orchestrator API reference](https://docs.uipath.com/orchestrator/reference/api-references). If your instance uses a different prefix, either bake it into `endpoint` or fork `_execute_uipath` in `component.py` to match. Demo mode is unaffected — the whole simulator runs on stdout.

## No Docker image — Orchestrator is Windows-native

UiPath does **not** publish a public Orchestrator Docker image. Orchestrator is a Windows-native, enterprise-licensed server. That's why the `demo_mode` simulator ships end-to-end zero-license: you can validate the whole component wiring (assets / ops / sensors / schedules / retry policy / lineage) without touching an Orchestrator install. When you're ready for a real endpoint, the UiPath Cloud Community tier is free — see the setup steps above.

## The two-way trigger story

UiPath Orchestrator and Dagster both need to be able to start work in the other. This component wires both directions:

**Dagster -> UiPath** (assets):
Each declared Release becomes a daily-partitioned Dagster asset. Materializing the asset does the OAuth2 client-credentials exchange, calls `POST /odata/Jobs/UiPath.Server.Configuration.OData.StartJobs`, polls `GET /odata/Jobs({id})` until terminal state (`Successful` / `Faulted` / `Stopped`), and records `OutputArguments`.

**UiPath -> Dagster** (inbound trigger sensor):
Production wire-up: your UiPath process's post-step calls Dagster's GraphQL API with a `launchRun` mutation. The `uipath_inbound_trigger` sensor confirms the trigger was received and produces observable ticks in the Dagster UI.

## Operational ops — Dagster as the pane of glass

Five ops ship as Dagster jobs so you can restart / soft-stop / kill / schedule-disable / reconcile UiPath jobs directly from the Dagster UI:

| Job | Config | Purpose |
|---|---|---|
| `uipath_restart_job` | `{release_key, folder}` | Start a new job for a Release |
| `uipath_stop_job_soft` | `{job_id}` | SoftStop a running job |
| `uipath_stop_job_kill` | `{job_id}` | Kill a running job |
| `uipath_disable_schedule` | `{schedule_id}` | Disable a ProcessSchedule (`SetEnabled: false`) |
| `uipath_reconciliation` | — | Compare UiPath state vs Dagster state; report drift |

The `uipath_reconciliation_schedule` runs the reconciliation job every hour (STOPPED by default — toggle in the UI when ready).

## Terminology cheat-sheet — Control-M vs RunMyJobs vs UiPath

For teams running more than one of these (or migrating between them):

| Control-M | RunMyJobs | UiPath Orchestrator |
|---|---|---|
| Job | JobDefinition | Process (Release) |
| Folder | Application | Folder |
| Agent / Host | Queue | Robot / Machine |
| ODATE | scheduledTime | (InputArgument) |
| runId | processId | Job.Key |
| "Ended OK" | "Completed" | "Successful" |
| "Ended Not OK" | "Error" | "Faulted" |

The [`controlm_integration`](../controlm_integration/README.md) and [`runmyjobs_integration`](../runmyjobs_integration/README.md) sister components have the same asset / op / sensor / schedule surface, so a shop running any combination can present a single Dagster pane of glass over all of them.

## Hybrid deployment vs migration

Designed for the **hybrid** shape — UiPath keeps owning the RPA / attended-bot / unattended-bot work that only UiPath can own (SAP GUI screen-scraping, Excel macro chains, Citrix VDI automations, legacy desktop clients), Dagster owns the cloud / analytics / AI pipeline, both share a single Dagster UI with correct lineage.

Not a UiPath replacement. If you're migrating off UiPath, the destination is a different automation stack (e.g. proper APIs, message queues, Python workers) — this component keeps UiPath in the loop while that migration happens.

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Connection

| Field | Type | Default | Description |
|---|---|---|---|
| `endpoint` | `str` | `"https://cloud.uipath.com/organization/tenant/orchestrator_"` | UiPath Orchestrator base URL (used when demo_mode=false). Cloud: https://cloud.uipath.com/{org}/{tenant}/orchestrator_ — on-prem: https://<host> |

### Execution

| Field | Type | Default | Description |
|---|---|---|---|
| `poll_interval_seconds` | `int` | `10` | Seconds between UiPath job status polls. |
| `max_retries` | `int` | `2` | Dagster-side asset retries on failure. |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `group_name` | `str` | `"uipath_orchestrator_integration"` | Dagster asset group for the job assets. |

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
| `source_tables` | `List[UiPathSourceTableSpec]` | `list()` | Tables populated by UiPath processes that Dagster observes (adds to lineage). |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `demo_mode` | `bool` | `true` | Simulate the REST API on stdout (no external calls). |
| `jobs` | `List[UiPathProcessSpec]` | `list()` | UiPath Releases to wrap as daily-partitioned Dagster assets. |
| `upstream_deps` | `List[str]` | `list()` | Upstream asset keys (slash-separated for nested keys) that must complete before jobs run. |
| `uipath_client_id_env` | `str` | `"UIPATH_CLIENT_ID"` | Env var holding the UiPath External Application client_id. |
| `uipath_client_secret_env` | `str` | `"UIPATH_CLIENT_SECRET"` | Env var holding the UiPath External Application client_secret. |
| `poll_timeout_seconds` | `int` | `3600` | Total seconds to wait for terminal status before failing. |
| `reconciliation_cron` | `str` | `"0 * * * *"` | Cron for the UiPath vs Dagster state reconciliation job. |

[//]: # (FIELDS:END)

## Example YAML

```yaml
type: dagster_community_components.UiPathOrchestratorIntegrationComponent

attributes:
  demo_mode: true
  endpoint: "https://cloud.uipath.com/organization/tenant/orchestrator_"

  group_name: uipath_orchestrator_integration
  uipath_client_id_env: UIPATH_CLIENT_ID
  uipath_client_secret_env: UIPATH_CLIENT_SECRET

  max_retries: 2
  retry_delay_seconds: 60

  jobs:
    - release_key: "abc12345-6789-def0-1234-567890abcdef"
      asset_name: uipath_invoice_processing
      folder: Finance
      folder_id: 42
      input_arguments:
        RunDate: "{partition_key}"
        Mode: "batch"
      machine_group: finance_robots

    - release_key: "fedcba98-7654-3210-fedc-ba9876543210"
      asset_name: uipath_kyc_screening
      folder: Compliance
      folder_id: 57
      input_arguments:
        ScreeningDate: "{partition_key}"
      machine_group: compliance_robots

  # Optional — bring UiPath-populated tables into Dagster's lineage
  source_tables:
    - table_name: "FINANCE.INVOICE_STAGING"
      asset_name: uipath_invoice_table
      produced_by: uipath_invoice_processing
```

## REST API endpoints (modern OData surface)

```
POST /identity_/connect/token                                — OAuth2 client-credentials -> Bearer token
POST /odata/Jobs/UiPath.Server.Configuration.OData.StartJobs — start a Process (Release)
GET  /odata/Jobs({id})                                       — poll job status
GET  /odata/Jobs({id})?$expand=Robot,Release                 — job + related metadata
POST /odata/Jobs({id})/UiPath.Server.Configuration.OData.StopJob
                                                             — SoftStop or Kill
POST /odata/ProcessSchedules({id})/UiPath.Server.Configuration.OData.SetEnabled
                                                             — enable / disable a schedule
GET  /odata/Jobs?$filter=State eq 'Successful'&$top=200      — list recent jobs for reconciliation

Header: Authorization: Bearer <token>
Header: X-UIPATH-OrganizationUnitId: <folder_id>   (when targeting a folder)
```

Verify against your Orchestrator version's [API reference](https://docs.uipath.com/orchestrator/reference/api-references) — the OData surface has evolved across Orchestrator versions and cloud/on-prem builds.

## Requirements

```
dagster
requests
```

## References

- UiPath Orchestrator API reference: <https://docs.uipath.com/orchestrator/reference/api-references>
- UiPath Automation Cloud (free Community tier): <https://cloud.uipath.com/signup>
- External Applications guide: <https://docs.uipath.com/automation-cloud/automation-cloud/latest/admin-guide/managing-external-applications>
