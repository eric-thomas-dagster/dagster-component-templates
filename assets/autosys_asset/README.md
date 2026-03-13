# AutoSys Asset

Connects to a **CA/Broadcom AutoSys** enterprise job scheduler and creates one Dagster asset per discovered job. AutoSys condition expressions (`s()`, `d()`, `f()`) are parsed into visual Dagster dependency edges. Box jobs become navigable sub-DAGs. File Watcher jobs appear as external source assets.

Supports **REST API** (AutoSys v12+, recommended) with automatic fallback to the **CLI** (`autorep`/`sendevent`) for older deployments.

---

## Why Dagster beats AutoSys

| Capability | AutoSys | Dagster |
|---|---|---|
| **Dependency visualization** | Text-based JIL conditions | Interactive asset lineage graph |
| **Box jobs** | Opaque container; no cross-box visibility | Each box becomes a Dagster sub-DAG — full pipeline visible |
| **Alerting** | Email alerts from a 2002-era GUI | Dagster+ alert policies: Slack, PagerDuty, email, webhooks |
| **Job catalog** | Dated AutoSys GUI; no search | Searchable, filterable Dagster asset catalog with metadata |
| **Audit trail** | AutoSys job history (limited) | Full materialization history, run metadata, logs in Dagster UI |
| **Retry logic** | Re-run from AutoSys console | Dagster retry policies with backoff — no manual intervention |
| **Gradual migration** | Big-bang cutover | Start observing AutoSys jobs in Dagster; move scheduling incrementally |
| **CI/CD** | JIL files in version control (hopefully) | Component YAML in Git, reviewed like code, deployed via pipelines |

---

## Required packages

```
requests>=2.28.0
```

CLI mode (`use_cli: true`) uses `subprocess` — no additional Python packages required. The `autorep` and `sendevent` binaries must be on `PATH`.

---

## REST API requirements

AutoSys **r11.3.6 / v12+** is required for the REST API (`AEWS` — AutoSys Enterprise Workload Service). Older versions must use `use_cli: true`.

REST endpoint base: `https://{host}:{port}/AEWS/api`

---

## Fields

### Connection — REST API

| Field | Required | Default | Description |
|---|---|---|---|
| `host` | Yes | — | AutoSys server hostname |
| `port` | No | `9443` | REST API HTTPS port |
| `username_env_var` | No | `None` | Env var containing the Basic-auth username |
| `password_env_var` | No | `None` | Env var containing the Basic-auth password |
| `token_env_var` | No | `None` | Env var containing the `X-AUTH-TOKEN` header value |
| `use_ssl` | No | `true` | Use HTTPS for REST calls |
| `verify_ssl` | No | `true` | Verify SSL certificate (set `false` for self-signed certs in dev/staging) |

### Connection — CLI fallback

| Field | Required | Default | Description |
|---|---|---|---|
| `use_cli` | No | `false` | Force CLI mode even when REST is configured |
| `autosys_instance` | No | `None` | AutoSys instance name — sets the `AUTOSERV` env var for CLI calls |

### Discovery filters

| Field | Required | Default | Description |
|---|---|---|---|
| `job_filter` | No | `%` | AutoSys wildcard filter, e.g. `ETL_*` or `FINANCE_*` |
| `exclude_jobs` | No | `None` | Job names to exclude from the asset graph |
| `include_job_types` | No | `["CMD","BOX","FW"]` | AutoSys job types to include |
| `machine_filter` | No | `None` | Only include jobs that run on this machine |

### Box job behaviour

| Field | Required | Default | Description |
|---|---|---|---|
| `expand_box_jobs` | No | `true` | Create individual assets for each child inside a Box, forming a sub-DAG |
| `box_completion_asset` | No | `true` | Create a `{box}_complete` gate asset that depends on all children |
| `map_conditions` | No | `true` | Parse `condition` field into Dagster deps (`s()`/`d()` → dep; `f()` → metadata only) |

### Execution

| Field | Required | Default | Description |
|---|---|---|---|
| `group_name` | No | `autosys` | Dagster asset group name |
| `key_prefix` | No | `None` | Optional asset key prefix for all generated assets |
| `force_start` | No | `false` | Use `FORCE_STARTJOB` (ignores AutoSys conditions and calendar) instead of `STARTJOB` |
| `poll_interval_seconds` | No | `30` | Seconds between status polls while a job is running |
| `job_timeout_seconds` | No | `7200` | Maximum seconds to wait for a job to complete |

---

## Example YAML

### REST API — Finance ETL jobs

```yaml
type: dagster_component_templates.AutoSysAssetComponent
attributes:
  host: autosys.internal.company.com
  port: 9443
  username_env_var: AUTOSYS_USER
  password_env_var: AUTOSYS_PASSWORD
  job_filter: "FINANCE_ETL_*"
  expand_box_jobs: true
  box_completion_asset: true
  map_conditions: true
  group_name: autosys_finance
  poll_interval_seconds: 30
```

### Token auth

```yaml
type: dagster_component_templates.AutoSysAssetComponent
attributes:
  host: autosys.internal.company.com
  token_env_var: AUTOSYS_TOKEN
  job_filter: "FINANCE_ETL_*"
  group_name: autosys_finance
```

### CLI-only (older AutoSys / no REST API)

```yaml
type: dagster_component_templates.AutoSysAssetComponent
attributes:
  host: autosys-server
  use_cli: true
  autosys_instance: PROD
  job_filter: "ETL_*"
  group_name: autosys
```

---

## AutoSys condition language

AutoSys job dependencies are expressed in a condition mini-language in the `condition` JIL field:

| Predicate | Meaning | Dagster mapping |
|---|---|---|
| `s(JobName)` | Upstream job succeeded | Asset dependency |
| `d(JobName)` | Upstream job done (any status) | Asset dependency |
| `f(JobName)` | Upstream job failed | Metadata only — not a Dagster dep |
| `exitcode(JobName,0)` | Specific exit code | Ignored |
| `value(GlobalVar,x)` | Global variable condition | Ignored |

Operators `&` (`AND`) and `|` (`OR`) are both handled conservatively: all job names from both branches are collected as dependencies. This is the safe default — it creates more edges but never misses a required upstream job.

Examples:

```
s(ETL_EXTRACT)                          -> dep: ETL_EXTRACT
s(JobA) & s(JobB)                       -> deps: JobA, JobB
(s(JobA) & s(JobB)) | s(JobC)           -> deps: JobA, JobB, JobC (union)
f(CLEANUP_JOB)                          -> no dep (stored in metadata)
s(ETL_EXTRACT) & f(ERROR_HANDLER)       -> dep: ETL_EXTRACT only
```

---

## Box Job architecture

A Box job is an AutoSys container that groups child jobs and controls their execution. With `expand_box_jobs: true` (default) each Box becomes a Dagster sub-DAG:

```
AutoSys JIL                          Dagster Asset Graph
-----------                          -------------------

insert_job: ETL_DAILY_BOX            etl_daily_box_complete
  job_type: BOX                       /         |         \
                                      v         v          v
insert_job: ETL_EXTRACT          etl_extract  etl_transform  etl_load
  box_name: ETL_DAILY_BOX             ^              ^
                                      |              |
insert_job: ETL_TRANSFORM        (s(ETL_EXTRACT) condition mapped as dep)
  box_name: ETL_DAILY_BOX
  condition: s(ETL_EXTRACT)

insert_job: ETL_LOAD
  box_name: ETL_DAILY_BOX
  condition: s(ETL_TRANSFORM)
```

- **Child jobs** get individual assets with condition-derived deps.
- **`{box}_complete`** is a lightweight gate asset — it returns immediately once all children have materialized. It does not trigger anything in AutoSys; its only purpose is to give downstream assets a single dependency to declare.

With `expand_box_jobs: false` the entire Box is treated as a single opaque asset that triggers `STARTJOB` against the box name.

---

## Job status codes

AutoSys uses two-letter status codes. The component maps them as follows:

| Code | AutoSys meaning | Dagster action |
|---|---|---|
| `SU` | SUCCESS | Materialization succeeds |
| `FA` | FAILURE | Raises `Exception` |
| `TE` | TERMINATED | Raises `Exception` |
| `OH` | ON_HOLD | Raises `Exception` with `sendevent -E JOB_OFF_HOLD -J {name}` hint |
| `OI` | ON_ICE | Raises `Exception` with `sendevent -E JOB_OFF_ICE -J {name}` hint |
| `RU` | RUNNING | Continue polling |
| `ST` | STARTING | Continue polling |
| `AC` | ACTIVATED (box) | Continue polling |
| `QW` | QUE_WAIT | Continue polling |
| `WA` | WAITING | Continue polling |
| `RE` | RESTART | Continue polling |
| `IN` | INACTIVE | Continue polling |
| `SK` | SKIPPED | Continue polling (usually transitional) |

---

## IAM / Auth setup

### REST API — Basic auth

```bash
export AUTOSYS_USER=svc-dagster
export AUTOSYS_PASSWORD=<password>
```

The service account needs the **EXEC** privilege on target jobs in AutoSys. In CA PAM / CyberArk, map the secret to these env vars in the Dagster deployment.

### REST API — Token auth

```bash
export AUTOSYS_TOKEN=<X-AUTH-TOKEN value>
```

Obtain a token via the AutoSys REST login endpoint:
```
POST https://{host}:{port}/AEWS/api/login
Body: {"username": "...", "password": "..."}
Response header: X-AUTH-TOKEN
```

### CLI auth

The `autorep` and `sendevent` binaries must be installed on the Dagster agent machine (or container) and the `AUTOSERV` env var must point to the correct AutoSys instance:

```bash
export AUTOSERV=PROD   # or set autosys_instance: PROD in YAML
```

The OS user running the Dagster agent must have AutoSys privileges to submit and query jobs (`EXEC` privilege on target job definitions).

---

## Gradual migration strategy

1. **Phase 1 — Observe**: Deploy with `map_conditions: true`, `use_cli: true` (or REST). AutoSys continues scheduling; Dagster records materialization events by polling status.
2. **Phase 2 — Visibility**: Teams use the Dagster asset catalog and lineage graph to understand the AutoSys pipeline topology — often for the first time.
3. **Phase 3 — Alerting**: Replace AutoSys email alerts with Dagster+ alert policies (Slack, PagerDuty, email) by pointing alerts at the Dagster asset group.
4. **Phase 4 — Scheduling ownership**: Move job schedules from AutoSys calendars to Dagster schedules / sensors. Use `force_start: true` to bypass AutoSys calendar conditions.
5. **Phase 5 — Migration complete**: AutoSys remains as the execution layer (or is decommissioned). Dagster owns scheduling, observability, and retry logic.

---

## Caching and state

This component uses `StateBackedComponent`. The AutoSys job catalogue is fetched **once** at prepare time and cached to disk as JSON. Subsequent code-server reloads rebuild the asset graph from the cache — zero network calls.

Refresh the cache:
```bash
dagster dev                          # automatic in local development
dg utils refresh-defs-state          # CI/CD pipelines and image builds
```

The cache file contains:
```json
{
  "jobs": [...],
  "api_mode": "rest",
  "discovery_filter": "ETL_*"
}
```
