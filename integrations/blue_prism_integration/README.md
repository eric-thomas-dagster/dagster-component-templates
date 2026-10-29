# BluePrismIntegrationComponent

SS&C **Blue Prism** 7+ Web API integration. Each declared Blue Prism process becomes a **daily-partitioned Dagster asset** with a retry policy. Ships with 5 operational ops + jobs (restart / stop / terminate / hold / reconcile), 2 sensors (external-execution monitor + inbound trigger), and an hourly reconciliation schedule.

`demo_mode: true` (default) simulates the whole REST API lifecycle on stdout — the component runs end-to-end with zero external dependencies. Flip to `false` and set `BLUE_PRISM_USER` / `BLUE_PRISM_PASSWORD` (or `BLUE_PRISM_API_KEY`) to hit a real Blue Prism 7+ Web API instance.

## What you get

```
Dagster catalog after `dg dev`:

Assets   ── bp_invoice_extraction         [daily partitioned, kinds: python, blue-prism, rpa]
         ── bp_hr_onboarding              [daily partitioned, kinds: python, blue-prism, rpa]
         ── bp_invoice_staging_table      [source, kinds: blue-prism, database]  (optional)

Jobs     ── blue_prism_restart_session    (run config: {process_id, resource_id})
         ── blue_prism_stop_session       (run config: {session_id})
         ── blue_prism_terminate_session  (run config: {session_id})
         ── blue_prism_hold_process       (run config: {process_id})
         ── blue_prism_reconciliation     (drift report)

Sensors  ── blue_prism_external_execution_monitor   (poll every 60s)
         ── blue_prism_inbound_trigger              (Blue Prism -> Dagster GraphQL)

Schedules ── blue_prism_reconciliation_schedule (cron: 0 * * * *)
```

## Integration pattern

```
       +----------------------------------------------------------+
       |               SS&C Blue Prism 7 control room             |
       |                                                          |
       |  Application: Accounts_Payable                           |
       |   +- Invoice Extraction (resource: Finance runtime)      |
       |   +- HR Onboarding      (resource: HR runtime)           |
       |                                                          |
       |  Web API  (Authorization: Bearer <token>)                |
       |           (or X-API-Key: <blue-prism-7-web-api-key>)     |
       +--------------------------------+-------------------------+
                                        |
        Dagster -> Blue Prism           |   Blue Prism -> Dagster
        POST /api/v7/auth/authenticate  |   POST https://dagster.cloud/graphql
        POST /api/v7/sessions           |   mutation { launchRun(...) }
        GET  /api/v7/sessions/{id}      |
        GET  /api/v7/sessions/{id}/logs |
                                        v
       +----------------------------------------------------------+
       |      BluePrismIntegrationComponent (one YAML)            |
       |                                                          |
       |  daily-partitioned assets  +  RetryPolicy(max_retries=2) |
       |  op jobs (restart / stop / terminate / hold / reconcile) |
       |  sensors (external-execution monitor + inbound trigger)  |
       |  hourly reconciliation schedule                          |
       +----------------------------------------------------------+
```

## Try it in one command

```bash
bash setup_blue_prism_integration_demo.sh
```

Scaffolds a Dagster project, installs the component in demo mode, materializes one partition end-to-end, and prints the full simulated REST API call trace (AUTH -> START -> POLL x 5 -> LOGS -> DONE).

Then open the Dagster UI:

```bash
cd blue-prism-demo
uv run dg dev
```

## Point at a real Blue Prism instance

**Option A — Basic auth (bearer token exchange):**

```bash
export BLUE_PRISM_USER=svc_dagster
export BLUE_PRISM_PASSWORD='<your-password>'
```

**Option B — X-API-Key (Blue Prism 7 Web API):**

```bash
export BLUE_PRISM_API_KEY='<your-web-api-key>'
```

Set `demo_mode: false` and override `endpoint` to your Blue Prism Web API base URL:

```yaml
attributes:
  demo_mode: false
  endpoint: "https://blueprism.prod.internal/api/v7"
  ...
```

> **Blue Prism 6 vs 7 caveat.** This component targets the Blue Prism 7+ REST **Web API**. Older Blue Prism deployments (v6 and earlier) may only expose the legacy **SOAP** interface — the REST paths documented here will 404 against those environments. Verify against your Blue Prism version before flipping `demo_mode` off. Demo mode is unaffected — the whole simulator runs on stdout.

> **No public Docker image.** Blue Prism does not publish a public Docker image (Windows-native, enterprise-licensed to SS&C customers). The Blue Prism Cloud edition is SaaS-only for existing customers. `demo_mode: true` ships an end-to-end, zero-license simulator so you can evaluate the component shape without a Blue Prism environment.

## The two-way trigger story

Blue Prism and Dagster both need to be able to start work in the other. This component wires both directions:

**Dagster -> Blue Prism** (assets):
Each declared process becomes a daily-partitioned Dagster asset. Materializing the asset authenticates against `POST /api/v7/auth/authenticate`, starts a session with `POST /api/v7/sessions`, polls `GET /api/v7/sessions/{sessionId}` until terminal state, and retrieves the session logs.

**Blue Prism -> Dagster** (inbound trigger sensor):
Production wire-up: your Blue Prism process's finish stage calls Dagster's GraphQL API with a `launchRun` mutation. The `blue_prism_inbound_trigger` sensor confirms the trigger was received and produces observable ticks in the Dagster UI.

## Operational ops — Dagster as the pane of glass

Five ops ship as Dagster jobs so you can restart / stop / terminate / hold / reconcile Blue Prism sessions directly from the Dagster UI:

| Job | Config | Purpose |
|---|---|---|
| `blue_prism_restart_session` | `{process_id, resource_id}` | Start a fresh session for a process on a runtime resource |
| `blue_prism_stop_session` | `{session_id}` | Soft-stop a running session (`POST /sessions/{id}/stop`) |
| `blue_prism_terminate_session` | `{session_id}` | Hard-terminate a running session (`POST /sessions/{id}/terminate`) |
| `blue_prism_hold_process` | `{process_id}` | Disable the process (`POST /processes/{id}/setEnabled {enabled: false}`) |
| `blue_prism_reconciliation` | — | Compare Blue Prism state vs Dagster state; report drift |

The `blue_prism_reconciliation_schedule` runs the reconciliation job every hour (STOPPED by default — toggle in the UI when ready).

## Terminology cheat-sheet — Control-M / RunMyJobs vs Blue Prism

For teams running multiple schedulers / RPA platforms (or migrating between them):

| Control-M | RunMyJobs | Blue Prism |
|---|---|---|
| Job | JobDefinition | Process |
| Folder | Application | Environment / Application |
| Agent / Host | Queue | Runtime Resource |
| runId | processId | sessionId |
| ODATE | scheduledTime | startedAt |
| "Ended OK" / "Ended Not OK" | "Completed" / "Error" | "Completed" / "Failed" / "Terminated" / "Stopped" |

## RPA sister components

Same integration shape, different vendor:

- [`uipath_orchestrator_integration`](../uipath_orchestrator_integration/README.md) — UiPath Orchestrator REST API
- [`automation_anywhere_integration`](../automation_anywhere_integration/README.md) — Automation Anywhere Control Room
- [`power_automate_integration`](../power_automate_integration/README.md) — Microsoft Power Automate (Cloud + Desktop)

A shop running multiple RPA platforms can present a single Dagster pane of glass over all of them by declaring one component per vendor.

## Hybrid deployment vs migration

Designed for the **hybrid** shape — Blue Prism keeps owning the RPA work only it can own (UI automation, legacy Citrix / thick-client screen scraping, credential vaulting inside a controlled Windows fleet), Dagster owns the cloud / analytics / AI pipeline, both share a single Dagster UI with correct lineage.

Not a Blue Prism killer. If you're standardizing on Dagster and can pull the RPA workload apart, prefer native browser automation (`playwright_component` / `httpx` calls / SDK integrations) over screen scraping — this component keeps Blue Prism in the loop until you're ready.

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Connection

| Field | Type | Default | Description |
|---|---|---|---|
| `endpoint` | `str` | `"https://blueprism.internal/api/v7"` | Blue Prism Web API base URL (used when demo_mode=false). |

### Execution

| Field | Type | Default | Description |
|---|---|---|---|
| `poll_interval_seconds` | `int` | `10` | Seconds between Blue Prism session status polls. |
| `max_retries` | `int` | `2` | Dagster-side asset retries on failure. |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `group_name` | `str` | `"blue_prism_integration"` | Dagster asset group for the process assets. |

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
| `source_tables` | `List[BluePrismSourceTableSpec]` | `list()` | Tables loaded by Blue Prism that Dagster observes (adds to lineage). |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `demo_mode` | `bool` | `true` | Simulate the REST API on stdout (no external calls). |
| `jobs` | `List[BluePrismProcessSpec]` | `list()` | Blue Prism processes to wrap as daily-partitioned Dagster assets. |
| `upstream_deps` | `List[str]` | `list()` | Upstream asset keys (slash-separated for nested keys) that must complete before jobs run. |
| `blue_prism_user_env` | `str` | `"BLUE_PRISM_USER"` | Env var holding the Blue Prism REST username. |
| `blue_prism_password_env` | `str` | `"BLUE_PRISM_PASSWORD"` | Env var holding the Blue Prism REST password. |
| `blue_prism_api_key_env` | `str` | `"BLUE_PRISM_API_KEY"` | Env var holding the Blue Prism 7 Web API key (alternative to user+password). |
| `poll_timeout_seconds` | `int` | `3600` | Total seconds to wait for terminal status before failing. |
| `log_retrieval` | `bool` | `true` | Retrieve session logs on completion. |
| `reconciliation_cron` | `str` | `"0 * * * *"` | Cron for the Blue Prism vs Dagster state reconciliation job. |

[//]: # (FIELDS:END)

## Example YAML

```yaml
type: dagster_community_components.BluePrismIntegrationComponent

attributes:
  demo_mode: true
  endpoint: "https://blueprism.internal/api/v7"

  group_name: blue_prism_integration
  blue_prism_user_env: BLUE_PRISM_USER
  blue_prism_password_env: BLUE_PRISM_PASSWORD
  blue_prism_api_key_env: BLUE_PRISM_API_KEY

  max_retries: 2
  retry_delay_seconds: 60

  jobs:
    - process_id: "b1e2c3d4-5f6a-7b8c-9d0e-1f2a3b4c5d6e"
      asset_name: bp_invoice_extraction
      resource_id: "a0b1c2d3-4e5f-6a7b-8c9d-0e1f2a3b4c5d"
      resource_group: Finance
      application: Accounts_Payable
      inputs:
        invoice_date: "{partition_key}"
        batch_size: 500

    - process_id: "c3d4e5f6-7a8b-9c0d-1e2f-3a4b5c6d7e8f"
      asset_name: bp_hr_onboarding
      resource_id: "d4e5f6a7-8b9c-0d1e-2f3a-4b5c6d7e8f90"
      resource_group: HR
      application: Employee_Lifecycle

  # Optional — bring Blue Prism-managed tables into Dagster's lineage
  source_tables:
    - table_name: "FINANCE.INVOICE_STAGING"
      asset_name: bp_invoice_staging_table
      produced_by: bp_invoice_extraction
```

## REST API endpoints (Blue Prism 7 Web API)

```
POST /api/v7/auth/authenticate                  — Basic auth -> {accessToken}
POST /api/v7/sessions                           — start a session on a resource
GET  /api/v7/sessions/{sessionId}               — poll session status
GET  /api/v7/sessions/{sessionId}/logs          — session log entries
POST /api/v7/sessions/{sessionId}/stop          — soft stop
POST /api/v7/sessions/{sessionId}/terminate     — hard terminate
POST /api/v7/processes/{processId}/setEnabled   — hold / release ({enabled: bool})
GET  /api/v7/processes                          — list registered processes
GET  /api/v7/sessions?status=Completed,Failed&startedAfter=<iso>&limit=200
                                                — list sessions for reconciliation
```

Session status values: `Pending / Running / Terminated / Stopped / Completed / Failed`.
Terminal states: `Completed / Failed / Terminated / Stopped`.

Verify against your Blue Prism version's Web API reference — the exact surface has evolved and some deployments still front the SOAP API only.

## Requirements

```
dagster
requests
```

## References

- Blue Prism 7 Web API — Web API service published by the Blue Prism 7 platform
- SS&C Blue Prism product page: <https://www.blueprism.com/>
