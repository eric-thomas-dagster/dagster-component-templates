# AutomationAnywhereIntegrationComponent

**Automation Anywhere** (A360 on-prem / AAI Cloud) Control Room REST API integration. Each declared Bot (fileId) becomes a **daily-partitioned Dagster asset** with a retry policy. Ships with 5 operational ops + jobs (redeploy / pause / resume / stop / reconcile), 2 sensors (external-execution monitor + inbound trigger), and an hourly reconciliation schedule.

`demo_mode: true` (default) simulates the whole REST API lifecycle on stdout — the component runs end-to-end with zero external dependencies. Flip to `false` and set `AUTOMATION_ANYWHERE_USER` / `AUTOMATION_ANYWHERE_PASSWORD` to hit a real Control Room.

## What you get

```
Dagster catalog after `dg dev`:

Assets   ── aa_invoice_extract              [daily partitioned, kinds: python, automation-anywhere, rpa]
         ── aa_hr_onboarding                [daily partitioned, kinds: python, automation-anywhere, rpa]
         ── aa_invoice_staging_table        [source, kinds: automation-anywhere, database]  (optional)

Jobs     ── automation_anywhere_redeploy_bot         (run config: {file_id, device_pool_id})
         ── automation_anywhere_pause_execution      (run config: {execution_id})
         ── automation_anywhere_resume_execution     (run config: {execution_id})
         ── automation_anywhere_stop_execution       (run config: {execution_id})
         ── automation_anywhere_reconciliation       (drift report)

Sensors  ── automation_anywhere_external_execution_monitor   (poll every 60s)
         ── automation_anywhere_inbound_trigger              (AA callback -> Dagster GraphQL)

Schedules ── automation_anywhere_reconciliation_schedule (cron: 0 * * * *)
```

## Integration pattern

```
       ┌──────────────────────────────────────────────────────────┐
       │      Automation Anywhere Control Room (A360 / AAI)       │
       │                                                          │
       │  Workspace: Public                                       │
       │   ├─ fileId=12345 aa_invoice_extract (pool: 7)           │
       │   └─ fileId=12346 aa_hr_onboarding   (pool: 3)           │
       │                                                          │
       │  REST API  (X-Authorization: <JWT from /v1/auth>)        │
       └──────────────────────────────┬───────────────────────────┘
                                      │
       Dagster -> AA                  │ AA -> Dagster
       POST /v1/authentication        │ POST https://dagster.cloud/webhook
       POST /v3/automations/deploy    │ POST https://dagster.cloud/graphql
       GET  /v3/activity/             │ mutation { launchRun(...) }
            execution/{id}            │  (via callbackInfo on deploy)
       GET  /v3/activity/             │
            execution/{id}/logs       │
                                      ▼
       ┌──────────────────────────────────────────────────────────┐
       │  AutomationAnywhereIntegrationComponent (one YAML)       │
       │                                                          │
       │  daily-partitioned assets  +  RetryPolicy(max_retries=2) │
       │  op jobs (redeploy / pause / resume / stop / reconcile)  │
       │  sensors (external-execution monitor + inbound trigger)  │
       │  hourly reconciliation schedule                          │
       └──────────────────────────────────────────────────────────┘
```

## Try it in one command

```bash
bash setup_automation_anywhere_integration_demo.sh
```

Scaffolds a Dagster project, installs the component in demo mode, materializes one partition end-to-end, and prints the full simulated REST API call trace (AUTH → DEPLOY → POLL × 5 → LOGS → DONE).

Then open the Dagster UI:

```bash
cd automation-anywhere-demo
uv run dg dev
```

## Point at a real Control Room

```bash
export AUTOMATION_ANYWHERE_USER=svc_dagster
export AUTOMATION_ANYWHERE_PASSWORD='<your-password>'  # or API key
```

Set `demo_mode: false` and override `endpoint` to your Control Room base URL:

```yaml
attributes:
  demo_mode: false
  endpoint: "https://control-room.prod.internal"
  ...
```

> **No public Docker image.** Automation Anywhere Control Room does NOT publish a public Docker image — it's an enterprise-licensed, Windows-heavy install. Automation Anywhere offers a Cloud tier (AAI) at `aai.automationanywhere.com` but requires a business trial account. The `demo_mode: true` simulator ships end-to-end **zero-license, zero-network** so you can wire the pipeline shape into your Dagster project without any AA entitlement.

> **API-path caveat.** Control Room REST endpoints vary between A360 on-prem versions and AAI Cloud (v1 auth + v3 activity is the modern surface, but older on-prem installs still use different prefixes or payload shapes). The URIs in this component target the modern surface. If your instance differs (`/v2/…` vs `/v3/…` vs a fully-custom path), either bake the prefix into `endpoint` or fork `_execute_automation_anywhere` in `component.py` to match. Demo mode is unaffected — the whole simulator runs on stdout.

## The two-way trigger story

Automation Anywhere and Dagster both need to be able to start work in the other. This component wires both directions:

**Dagster -> Automation Anywhere** (assets):
Each declared Bot becomes a daily-partitioned Dagster asset. Materializing the asset authenticates to the Control Room via `POST /v1/authentication`, deploys the bot via `POST /v3/automations/deploy` with the partition date templated into `botInput`, polls `GET /v3/activity/execution/{id}` until terminal state, and retrieves execution logs.

**Automation Anywhere -> Dagster** (inbound trigger sensor):
Production wire-up: the `callbackInfo` field on the deploy payload points at a Dagster webhook / GraphQL `launchRun` mutation. The `automation_anywhere_inbound_trigger` sensor confirms the trigger was received and produces observable ticks in the Dagster UI.

## Operational ops — Dagster as the pane of glass

Five ops ship as Dagster jobs so you can redeploy / pause / resume / stop / reconcile AA executions directly from the Dagster UI:

| Job | Config | Purpose |
|---|---|---|
| `automation_anywhere_redeploy_bot` | `{file_id, device_pool_id}` | Redeploy a bot (`POST /v3/automations/deploy`) |
| `automation_anywhere_pause_execution` | `{execution_id}` | Pause a running execution (`POST /v3/activity/execution/{id}/pause`) |
| `automation_anywhere_resume_execution` | `{execution_id}` | Resume a paused execution (`POST /v3/activity/execution/{id}/resume`) |
| `automation_anywhere_stop_execution` | `{execution_id}` | Stop a running execution (`POST /v3/activity/execution/{id}/stop`) |
| `automation_anywhere_reconciliation` | — | Compare AA state vs Dagster state; report drift by status |

The `automation_anywhere_reconciliation_schedule` runs the reconciliation job every hour (STOPPED by default — toggle in the UI when ready).

## Terminology cheat-sheet — Control-M vs RunMyJobs vs Automation Anywhere

For teams running multiple orchestrators (or migrating between them):

| Control-M | RunMyJobs | Automation Anywhere |
|---|---|---|
| Job | JobDefinition | Bot (File) |
| Folder | Application | Workspace / Folder |
| Agent / Host | Queue | Device Pool |
| ODATE | scheduledTime | botInput.run_date |
| runId | processId | Deployment (executionId) |
| "Ended OK" / "Ended Not OK" | "Completed" / "Error" | "COMPLETED" / "FAILED" |

Sister components with the same asset / op / sensor / schedule surface so a shop running multiple platforms can present a single Dagster pane of glass over all of them:

- **RPA family**: [`uipath_orchestrator_integration`](../uipath_orchestrator_integration/README.md), [`blue_prism_integration`](../blue_prism_integration/README.md), [`power_automate_integration`](../power_automate_integration/README.md)
- **Batch-scheduler family**: [`controlm_integration`](../controlm_integration/README.md), [`runmyjobs_integration`](../runmyjobs_integration/README.md)

## Hybrid deployment vs migration

Designed for the **hybrid** shape — Automation Anywhere keeps owning the RPA workload that only AA can own (UI-driven screen scraping, PDF form filling, legacy-app automation), Dagster owns the cloud/analytics/AI pipeline, both share a single Dagster UI with correct lineage.

Not an Automation Anywhere killer. If you're doing a full migration off AA, headless / API-first replacements are the better fit — this component keeps AA in the loop.

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Connection

| Field | Type | Default | Description |
|---|---|---|---|
| `endpoint` | `str` | `"https://control-room.internal"` | Automation Anywhere Control Room REST API base URL (used when demo_mode=false). |

### Execution

| Field | Type | Default | Description |
|---|---|---|---|
| `poll_interval_seconds` | `int` | `10` | Seconds between AA execution status polls. |
| `max_retries` | `int` | `2` | Dagster-side asset retries on failure. |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `group_name` | `str` | `"automation_anywhere_integration"` | Dagster asset group for the bot assets. |

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
| `source_tables` | `List[AutomationAnywhereSourceTableSpec]` | `list()` | Tables populated by AA bots that Dagster observes (adds to lineage). |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `demo_mode` | `bool` | `true` | Simulate the REST API on stdout (no external calls). |
| `jobs` | `List[AutomationAnywhereBotSpec]` | `list()` | Automation Anywhere Bots (fileIds) to wrap as daily-partitioned Dagster assets. |
| `upstream_deps` | `List[str]` | `list()` | Upstream asset keys (slash-separated for nested keys) that must complete before bots run. |
| `automation_anywhere_user_env` | `str` | `"AUTOMATION_ANYWHERE_USER"` | Env var holding the Control Room REST username. |
| `automation_anywhere_password_env` | `str` | `"AUTOMATION_ANYWHERE_PASSWORD"` | Env var holding the Control Room REST password (or apiKey). |
| `poll_timeout_seconds` | `int` | `3600` | Total seconds to wait for terminal status before failing. |
| `log_retrieval` | `bool` | `true` | Retrieve execution logs on completion. |
| `reconciliation_cron` | `str` | `"0 * * * *"` | Cron for the Automation Anywhere vs Dagster state reconciliation job. |

[//]: # (FIELDS:END)

## Example YAML

```yaml
type: dagster_community_components.AutomationAnywhereIntegrationComponent

attributes:
  demo_mode: true
  endpoint: "https://control-room.internal"

  group_name: automation_anywhere_integration
  automation_anywhere_user_env: AUTOMATION_ANYWHERE_USER
  automation_anywhere_password_env: AUTOMATION_ANYWHERE_PASSWORD

  max_retries: 2
  retry_delay_seconds: 60

  jobs:
    - file_id: 12345
      asset_name: aa_invoice_extract
      workspace: Public
      application: FINANCE
      device_pool_id: 7
      run_as_user_ids: [42]
      bot_input:
        run_date: "{partition_key}"
        source_folder: "/mnt/invoices/inbound"

    - file_id: 12346
      asset_name: aa_hr_onboarding
      workspace: Public
      application: HR
      device_pool_id: 3
      run_as_user_ids: [42]
      bot_input:
        run_date: "{partition_key}"

  # Optional — bring AA-managed tables into Dagster's lineage
  source_tables:
    - table_name: "FINANCE.INVOICE_EXTRACT_STAGING"
      asset_name: aa_invoice_staging_table
      produced_by: aa_invoice_extract
```

## REST API endpoints (modern surface)

```
POST /v1/authentication                            — obtain JWT (returns {token})
                                                     use as X-Authorization: <token>
POST /v3/automations/deploy                        — deploy a bot
                                                     body: {fileId, runAsUserIds, poolIds,
                                                            overrideDefaultDevice, callbackInfo,
                                                            botInput}
                                                     returns: {deploymentId, executionId, ...}
GET  /v3/activity/execution/{id}                   — poll execution status
                                                     status ∈ DEPLOYED / SCHEDULED / QUEUED /
                                                              RUNNING / COMPLETED / FAILED /
                                                              CANCELLED / DEPLOY_FAILED /
                                                              TIMED_OUT
GET  /v3/activity/execution/{id}/logs              — execution logs
POST /v3/activity/execution/{id}/stop              — stop a running execution
POST /v3/activity/execution/{id}/pause             — pause a running execution
POST /v3/activity/execution/{id}/resume            — resume a paused execution
POST /v3/activity/list                             — list activity (reconciliation)
                                                     body: {filter, sort, page}
```

Terminal execution states: `COMPLETED`, `FAILED`, `CANCELLED`, `DEPLOY_FAILED`, `TIMED_OUT`.

Verify against your Control Room version's REST reference — the surface has evolved across A360 on-prem versions and Automation Anywhere Cloud (AAI) builds.

## Requirements

```
dagster
requests
```

## References

- Automation Anywhere docs: <https://docs.automationanywhere.com/>
- Automation Anywhere Cloud (AAI): <https://aai.automationanywhere.com/>
- Developer / API reference: <https://developer.automationanywhere.com/>
