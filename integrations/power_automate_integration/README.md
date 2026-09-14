# PowerAutomateIntegrationComponent

Microsoft **Power Automate** cloud flow REST API integration (Flow Management REST via `api.flow.microsoft.com`). Each declared cloud flow becomes a **daily-partitioned Dagster asset** with a retry policy. Ships with 5 operational ops + jobs (retrigger / cancel run / turn off / turn on / reconcile), 2 sensors (external-execution monitor + inbound trigger), and an hourly reconciliation schedule.

`demo_mode: true` (default) simulates the whole REST API lifecycle on stdout — the component runs end-to-end with **zero external dependencies (no M365 tenant needed)**. Flip to `false` and set `POWER_AUTOMATE_TENANT_ID` / `POWER_AUTOMATE_CLIENT_ID` / `POWER_AUTOMATE_CLIENT_SECRET` to hit a real Power Automate service.

> **Cloud-only, by design.** Power Automate does NOT run on-prem — it's a Microsoft cloud service exclusively. There is no Docker image, no self-hosted install. Customers get an Environment automatically with any M365 tenant; Power Automate cloud flows are included in most M365 licenses. On-prem RPA is a separate product (Power Automate for desktop, and the on-prem data gateway); this component targets **cloud flows**.

## What you get

```
Dagster catalog after `dg dev`:

Assets    ── pa_invoice_approval             [daily partitioned, kinds: python, power-automate, rpa]
          ── pa_onboarding_workflow          [daily partitioned, kinds: python, power-automate, rpa]
          ── pa_invoice_approvals_table      [source, kinds: power-automate, database]  (optional)

Jobs      ── power_automate_retrigger        (run config: {flow_id, environment_id})
          ── power_automate_cancel_run       (run config: {flow_id, environment_id, run_name})
          ── power_automate_turn_off         (run config: {flow_id, environment_id})
          ── power_automate_turn_on          (run config: {flow_id, environment_id})
          ── power_automate_reconciliation   (drift report)

Sensors   ── power_automate_external_execution_monitor   (poll every 60s)
          ── power_automate_inbound_trigger              (Power Automate -> Dagster GraphQL)

Schedules ── power_automate_reconciliation_schedule (cron: 0 * * * *)
```

## Integration pattern

```
       ┌──────────────────────────────────────────────────────────┐
       │       Microsoft Power Automate (cloud service)           │
       │                                                          │
       │  Environment: Default-<tenant-guid>                      │
       │   Solution: Finance                                      │
       │    ├─ pa_invoice_approval    (b1c2d3e4-...)              │
       │    └─ pa_onboarding_workflow (c2d3e4f5-...)              │
       │                                                          │
       │  REST API @ api.flow.microsoft.com                       │
       │  Auth: Azure AD (Entra ID) OAuth2 client-credentials     │
       │        Bearer <access_token>                             │
       └──────────────────────────────┬───────────────────────────┘
                                      │
       Dagster -> Power Automate      │ Power Automate -> Dagster
       POST /providers/               │ HTTP action:
         .../triggers/manual/run      │  POST https://dagster.cloud/graphql
       GET  /providers/               │  mutation { launchRun(...) }
         .../runs/{run_name}          │
       POST /providers/               │
         .../runs/{name}/cancel       │
                                      ▼
       ┌──────────────────────────────────────────────────────────┐
       │   PowerAutomateIntegrationComponent (one YAML)           │
       │                                                          │
       │  daily-partitioned assets  +  RetryPolicy(max_retries=2) │
       │  op jobs (retrigger / cancel / off / on / reconcile)     │
       │  sensors (external-execution monitor + inbound trigger)  │
       │  hourly reconciliation schedule                          │
       └──────────────────────────────────────────────────────────┘
```

## Try it in one command

```bash
bash setup_power_automate_integration_demo.sh
```

Scaffolds a Dagster project, installs the component in demo mode, materializes one partition end-to-end, and prints the full simulated REST API call trace (AUTH → TRIGGER → POLL × 5 → DONE).

Then open the Dagster UI:

```bash
cd power-automate-demo
uv run dg dev
```

## Point at a real Power Automate service

### 1. Register an Azure AD app

Create an app registration in the Azure portal (Entra ID → App registrations → New registration). Under **API permissions**, add application permissions for the Flow Service:

- `Flows.Read.All`   (list flows / read run history for the reconciliation and monitor)
- `Flows.Manage.All` (trigger / cancel / turn off / turn on)

Grant admin consent. Create a client secret under **Certificates & secrets**.

### 2. Export the credentials

```bash
export POWER_AUTOMATE_TENANT_ID='<your-tenant-guid>'
export POWER_AUTOMATE_CLIENT_ID='<your-app-client-id>'
export POWER_AUTOMATE_CLIENT_SECRET='<your-app-client-secret>'
```

### 3. Flip the switch

Set `demo_mode: false` and override `endpoint` if you're on a national cloud (GCC / GCC-High / DoD / China / Germany):

```yaml
attributes:
  demo_mode: false
  endpoint: "https://api.flow.microsoft.com"      # commercial cloud
  # endpoint: "https://gov.api.flow.microsoft.us" # GCC-High example
  ...
```

> **API-path caveat.** The Flow Management REST surface at `api.flow.microsoft.com` is the modern JSON API used by the Power Automate UI. National-cloud endpoints and the older Dataverse-hosted Power Platform Web API have different hostnames and prefixes; if your tenant lives on one of them, override `endpoint` accordingly. Demo mode is unaffected — the whole simulator runs on stdout.

## The two-way trigger story

Power Automate and Dagster both need to be able to start work in the other. This component wires both directions:

**Dagster -> Power Automate** (assets):
Each declared cloud flow becomes a daily-partitioned Dagster asset. Materializing the asset triggers the flow via `POST /providers/Microsoft.ProcessSimple/environments/{env}/flows/{flow_id}/triggers/manual/run`, polls the run URL until terminal state (`Succeeded` / `Failed` / `Cancelled` / `Skipped`), and records the `runName`.

**Power Automate -> Dagster** (inbound trigger sensor):
Production wire-up: your cloud flow uses an HTTP action to call Dagster's GraphQL API with a `launchRun` mutation. The `power_automate_inbound_trigger` sensor confirms the trigger was received and produces observable ticks in the Dagster UI.

## Operational ops — Dagster as the pane of glass

Five ops ship as Dagster jobs so you can retrigger / cancel / turn off / turn on / reconcile Power Automate cloud flows directly from the Dagster UI:

| Job | Config | Purpose |
|---|---|---|
| `power_automate_retrigger`        | `{flow_id, environment_id}`             | Re-trigger the manual trigger (`POST /triggers/manual/run`) |
| `power_automate_cancel_run`       | `{flow_id, environment_id, run_name}`   | Cancel a running run (`POST /runs/{name}/cancel`) |
| `power_automate_turn_off`         | `{flow_id, environment_id}`             | Disable the cloud flow (`POST /stop`) |
| `power_automate_turn_on`          | `{flow_id, environment_id}`             | Enable the cloud flow (`POST /start`) |
| `power_automate_reconciliation`   | —                                       | Compare Power Automate state vs Dagster state; report drift |

The `power_automate_reconciliation_schedule` runs the reconciliation job every hour (STOPPED by default — toggle in the UI when ready).

## Terminology cheat-sheet

For teams running Power Automate alongside Control-M / RunMyJobs / classic RPA:

| Control-M     | RunMyJobs        | Power Automate                    |
|---            |---               |---                                |
| Job           | JobDefinition    | Cloud Flow                        |
| Folder        | Application      | Environment                       |
| Agent / Host  | Queue            | (n/a — flows run in MS cloud)     |
| —             | —                | Solution (business tag)           |
| ODATE         | scheduledTime    | Trigger input variable            |
| runId         | processId        | runName                           |
| "Ended OK"    | "Completed"      | "Succeeded"                       |
| "Ended NOK"   | "Error"          | "Failed"                          |

**Environment IS the runtime context.** Unlike batch schedulers, Power Automate cloud flows have no per-run "host" concept — the Environment is where the flow lives and executes. The `run_as` field on `PowerAutomateFlowSpec` is a business tag for who owns the invocation, not an OS user.

## Sister RPA integrations

If you're building an RPA control plane in Dagster, the same asset / op / sensor / schedule surface ships for the other big RPA platforms — one YAML per platform, one Dagster pane of glass for all of them:

- [`uipath_orchestrator_integration`](../uipath_orchestrator_integration/README.md) — UiPath Orchestrator (unattended robots)
- [`automation_anywhere_integration`](../automation_anywhere_integration/README.md) — Automation Anywhere Control Room
- [`blue_prism_integration`](../blue_prism_integration/README.md) — Blue Prism (on-prem)

## Hybrid deployment vs migration

Designed for the **hybrid** shape — Power Automate keeps owning the RPA / SharePoint / Outlook / Teams orchestration only it can own, Dagster owns the data / analytics / AI pipeline, both share a single Dagster UI with correct lineage.

Not a Power Automate killer. This component keeps Power Automate in the loop as a first-class citizen of your Dagster catalog.

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Connection

| Field | Type | Default | Description |
|---|---|---|---|
| `endpoint` | `str` | `"https://api.flow.microsoft.com"` | Power Automate REST API base URL (used when demo_mode=false). |

### Execution

| Field | Type | Default | Description |
|---|---|---|---|
| `poll_interval_seconds` | `int` | `10` | Seconds between Power Automate run status polls. |
| `max_retries` | `int` | `2` | Dagster-side asset retries on failure. |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `group_name` | `str` | `"power_automate_integration"` | Dagster asset group for the flow assets. |

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
| `source_tables` | `List[PowerAutomateSourceTableSpec]` | `list()` | Tables loaded by Power Automate flows that Dagster observes (adds to lineage). |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `demo_mode` | `bool` | `true` | Simulate the REST API on stdout (no external calls). |
| `jobs` | `List[PowerAutomateFlowSpec]` | `list()` | Power Automate cloud flows to wrap as daily-partitioned Dagster assets. |
| `upstream_deps` | `List[str]` | `list()` | Upstream asset keys (slash-separated for nested keys) that must complete before flows run. |
| `power_automate_tenant_env` | `str` | `"POWER_AUTOMATE_TENANT_ID"` | Env var holding the Azure AD / Entra ID tenant GUID. |
| `power_automate_client_id_env` | `str` | `"POWER_AUTOMATE_CLIENT_ID"` | Env var holding the Azure AD app registration client ID. |
| `power_automate_client_secret_env` | `str` | `"POWER_AUTOMATE_CLIENT_SECRET"` | Env var holding the Azure AD app registration client secret. |
| `poll_timeout_seconds` | `int` | `3600` | Total seconds to wait for terminal status before failing. |
| `reconciliation_cron` | `str` | `"0 * * * *"` | Cron for the Power Automate vs Dagster state reconciliation job. |

[//]: # (FIELDS:END)

## Example YAML

```yaml
type: dagster_community_components.PowerAutomateIntegrationComponent

attributes:
  demo_mode: true
  endpoint: "https://api.flow.microsoft.com"

  group_name: power_automate_integration
  power_automate_tenant_env: POWER_AUTOMATE_TENANT_ID
  power_automate_client_id_env: POWER_AUTOMATE_CLIENT_ID
  power_automate_client_secret_env: POWER_AUTOMATE_CLIENT_SECRET

  max_retries: 2
  retry_delay_seconds: 60

  jobs:
    - flow_id: "b1c2d3e4-5f60-7a80-9b0c-1d2e3f405060"
      asset_name: pa_invoice_approval
      environment_id: "Default-a0b1c2d3-e4f5-6789-abcd-ef0123456789"
      solution: Finance
      application: INVOICE_OPS
      trigger_input:
        DATE: "{partition_key}"
        SOURCE_SYSTEM: SAP

    - flow_id: "c2d3e4f5-6071-8b90-ac1d-2e3f40506070"
      asset_name: pa_onboarding_workflow
      environment_id: "Default-a0b1c2d3-e4f5-6789-abcd-ef0123456789"
      solution: HR
      application: ONBOARDING
      trigger_input:
        DATE: "{partition_key}"

  # Optional — bring Power-Automate-managed tables into Dagster's lineage
  source_tables:
    - table_name: "FINANCE.INVOICE_APPROVALS"
      asset_name: pa_invoice_approvals_table
      produced_by: pa_invoice_approval
```

## REST API endpoints (Flow Management REST, api-version=2016-11-01)

```
POST /providers/Microsoft.ProcessSimple/environments/{env}/flows/{id}/triggers/manual/run
                                                                — trigger a cloud flow
GET  /providers/Microsoft.ProcessSimple/environments/{env}/flows/{id}/runs/{run_name}
                                                                — poll a run's status
GET  /providers/Microsoft.ProcessSimple/environments/{env}/flows/{id}/runs
                                                                — list run history
POST /providers/Microsoft.ProcessSimple/environments/{env}/flows/{id}/runs/{run_name}/cancel
                                                                — cancel a running run
POST /providers/Microsoft.ProcessSimple/environments/{env}/flows/{id}/stop
                                                                — turn flow off (disable)
POST /providers/Microsoft.ProcessSimple/environments/{env}/flows/{id}/start
                                                                — turn flow on (enable)

Auth:
POST https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token
     grant_type=client_credentials
     scope=https://service.flow.microsoft.com/.default
     -> Bearer <access_token>
```

Verify against the current Flow Management REST reference — Microsoft evolves the surface (national clouds, Dataverse-hosted variants).

## Requirements

```
dagster
requests
```

## References

- Power Automate Web API: <https://learn.microsoft.com/en-us/power-automate/web-api>
- Flow Management connector reference: <https://learn.microsoft.com/en-us/connectors/flowmanagement/>
- Azure AD app registration: <https://learn.microsoft.com/en-us/entra/identity-platform/quickstart-register-app>
- Application permissions for Power Automate: <https://learn.microsoft.com/en-us/power-automate/desktop-flows/desktop-flow-permissions>
