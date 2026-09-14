# JenkinsIntegrationComponent

Jenkins REST API integration for teams **migrating off Jenkins** (or living alongside it during the transition). Each declared Jenkins job becomes a **daily-partitioned Dagster asset** with a retry policy. Ships with 5 operational ops + jobs (rebuild / disable / enable / stop / reconcile), 2 sensors (external-execution monitor + inbound trigger), and an hourly reconciliation schedule.

`demo_mode: true` (default) simulates the whole Jenkins REST lifecycle on stdout — the component runs end-to-end with zero external dependencies. Flip to `false` and set `JENKINS_USER` / `JENKINS_API_TOKEN` to hit a real Jenkins controller.

## The transition-phase story

Most Jenkins-migration engagements aren't "kill Jenkins overnight" — they're multi-quarter. This component fits the **transition phase**: Dagster owns the new pipelines, Jenkins keeps owning what it already does, and the two are visible from a single Dagster UI with correct lineage. As you migrate individual jobs off Jenkins, you delete the corresponding YAML entry and the asset simply disappears from the graph.

## What you get

```
Dagster catalog after `dg dev`:

Assets   ── jenkins_eod_settlement          [daily partitioned, kinds: python, jenkins]
         ── jenkins_regulatory_extract      [daily partitioned, kinds: python, jenkins]
         ── jenkins_settlement_table        [source, kinds: jenkins, database]  (optional)

Jobs     ── jenkins_rebuild_job             (run config: {job_name})
         ── jenkins_disable_job             (run config: {job_name})       — analog to Control-M hold
         ── jenkins_enable_job              (run config: {job_name})       — release from disabled
         ── jenkins_stop_build              (run config: {job_name, build_number})
         ── jenkins_reconciliation          (drift report)

Sensors  ── jenkins_external_execution_monitor  (poll every 60s)
         ── jenkins_inbound_trigger             (Jenkins post-build -> Dagster GraphQL)

Schedules ── jenkins_reconciliation_schedule    (cron: 0 * * * *)
```

## Integration pattern

```
       ┌──────────────────────────────────────────────────────────┐
       │                Jenkins controller                        │
       │                                                          │
       │  Folder: banking                                         │
       │   ├─ eod-batch-settlement    (node: linux-heavy)         │
       │   └─ regulatory-extract      (node: linux-heavy)         │
       │                                                          │
       │  REST API  (HTTP Basic + API token; CSRF-crumb protected)│
       └──────────────────────────────┬───────────────────────────┘
                                      │
        Dagster -> Jenkins            │ Jenkins -> Dagster
        POST /crumbIssuer/api/json    │ POST https://dagster.cloud/graphql
        POST /job/<name>/build[WithP] │ mutation { launchRun(...) }
        GET  /queue/item/{id}/api/json│
        GET  /job/<n>/<b>/api/json    │
        GET  /job/<n>/<b>/consoleText │
                                      ▼
       ┌──────────────────────────────────────────────────────────┐
       │      JenkinsIntegrationComponent (one YAML)              │
       │                                                          │
       │  daily-partitioned assets  +  RetryPolicy(max_retries=2) │
       │  op jobs (rebuild / disable / enable / stop / reconcile) │
       │  sensors (external-execution monitor + inbound trigger)  │
       │  hourly reconciliation schedule                          │
       └──────────────────────────────────────────────────────────┘
```

## Try it in one command

```bash
bash setup_jenkins_integration_demo.sh
```

Scaffolds a Dagster project, installs the component in `demo_mode: true`, and materializes one partition end-to-end. Full simulated REST trace prints to run logs (`AUTH → CRUMB → TRIGGER → QUEUE × 2 → POLL × 4 → CONSOLE → DONE`). Zero external dependencies.

Then open the Dagster UI:

```bash
cd jenkins-demo
uv run dg dev
```

## Point at a real Jenkins

Jenkins credentials use an **API token**, not the user's password. Generate one from `http://<jenkins>/user/<username>/configure` -> "API Token" section.

```bash
export JENKINS_USER=svc-dagster
export JENKINS_API_TOKEN=11a2b3c4d5e6f7g8h9i0j1k2l3m4n5o6p7
```

Set `demo_mode: false` and override `endpoint` to your Jenkins base URL:

```yaml
attributes:
  demo_mode: false
  endpoint: "http://jenkins.prod.internal:8080"
  ...
```

Everything else stays exactly as the demo — same job list, same partition shape, same ops.

### Live-Docker option (jenkins/jenkins:lts)

Jenkins ships a first-party Docker image with no license restrictions — the fastest way to try the real-API path end-to-end:

```bash
docker run -d --name jenkins-demo -p 8080:8080 -p 50000:50000 \
  -v jenkins_home:/var/jenkins_home \
  jenkins/jenkins:lts
# Wait ~30s, then:
docker exec jenkins-demo cat /var/jenkins_home/secrets/initialAdminPassword
```

Open http://localhost:8080, complete the setup wizard (install suggested plugins, create admin user), generate an API token from your user's Configure page, then create a freestyle job named `eod-batch-settlement` with an "Execute shell" step (`echo "Settlement for $SETTLEMENT_DATE"; sleep 5`) and one string parameter `SETTLEMENT_DATE`. Point the component at `http://localhost:8080` with `demo_mode: false` and materialize.

## The two-way trigger story

**Dagster -> Jenkins** (assets):
Each declared job becomes a daily-partitioned Dagster asset. Materializing the asset acquires a CSRF crumb, triggers the job via `POST /job/<name>/build` (or `/buildWithParameters` if params are declared), tracks the queue item until a build number is assigned, polls the build to terminal result, and pulls consoleText.

**Jenkins -> Dagster** (inbound trigger sensor):
Production wire-up: your Jenkins post-build step is a `curl` (or `httpRequest` Groovy step) calling Dagster's GraphQL `launchRun` mutation. The `jenkins_inbound_trigger` sensor confirms the wire-up and produces observable ticks in the Dagster UI.

## Operational ops — Dagster as the pane of glass

Five ops ship as Dagster jobs so you can rebuild / disable / enable / stop / reconcile Jenkins jobs directly from the Dagster UI:

| Job | Config | Purpose |
|---|---|---|
| `jenkins_rebuild_job` | `{job_name}` | Rebuild a job (`POST /job/<name>/build`) |
| `jenkins_disable_job` | `{job_name}` | Disable a job (analog to Control-M hold) |
| `jenkins_enable_job` | `{job_name}` | Re-enable a disabled job |
| `jenkins_stop_build` | `{job_name, build_number}` | Stop a running build (`POST /job/<name>/<b>/stop`) |
| `jenkins_reconciliation` | — | Compare Jenkins state vs Dagster state; report drift by color (blue/red/yellow/disabled) |

The `jenkins_reconciliation_schedule` runs the reconciliation job every hour (STOPPED by default — toggle in the UI when ready).

## Terminology cheat-sheet — Jenkins vs Control-M / RunMyJobs

| Control-M | RunMyJobs | Jenkins |
|---|---|---|
| Job | JobDefinition | Job (freestyle or pipeline) |
| Folder | Application | Folder (Folders plugin) |
| Agent / Host | Queue | Node (agent/executor with a label) |
| ODATE | scheduledTime | Build parameter (typically) |
| runId | processId | Build number |
| "Ended OK" / "Ended Not OK" | "Completed" / "Error" | "SUCCESS" / "FAILURE" / "ABORTED" |

The [`controlm_integration`](../controlm_integration/README.md) and [`runmyjobs_integration`](../runmyjobs_integration/README.md) sister components use the same asset / op / sensor / schedule surface — a shop running Jenkins alongside Control-M or RMJ (common during migrations off multiple systems) can present a single Dagster pane of glass over all three.

## Hybrid deployment vs migration

Designed for the **transition-phase** shape — Jenkins keeps owning what it already runs (CI builds, legacy jobs), Dagster owns the new pipelines, and both are visible from the same Dagster UI. As you migrate individual jobs off Jenkins, just delete the corresponding YAML entry.

For a **full migration**, the destination shape is usually assets + Airflow-proxy + dbt / sql_transform / warehouse_workspace components — you don't keep a Jenkins wrapper around forever.

## Fields

### Connection

| Field | Type | Default | Description |
|---|---|---|---|
| `endpoint` | `str` | `"http://localhost:8080"` | Jenkins controller base URL. |
| `jenkins_user_env` | `str` | `"JENKINS_USER"` | Env var holding the Jenkins REST username. |
| `jenkins_token_env` | `str` | `"JENKINS_API_TOKEN"` | Env var holding the Jenkins user's API token. |

### Execution

| Field | Type | Default | Description |
|---|---|---|---|
| `demo_mode` | `bool` | `true` | Simulate the Jenkins REST API on stdout (no external calls). |
| `poll_interval_seconds` | `int` | `5` | Seconds between queue/build status polls. |
| `poll_timeout_seconds` | `int` | `3600` | Total seconds to wait for terminal result before failing. |
| `console_retrieval` | `bool` | `true` | Retrieve consoleText on completion. |

### Retry policy

| Field | Type | Default | Description |
|---|---|---|---|
| `max_retries` | `int` | `2` | Dagster-side asset retries on failure. |
| `retry_delay_seconds` | `int` | `60` | Delay between Dagster asset retries. |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `group_name` | `str` | `"jenkins_integration"` | Dagster asset group for the job assets. |

### Partitions

| Field | Type | Default | Description |
|---|---|---|---|
| `partition_start_date` | `str` | `"2024-01-01"` | Start date for the daily partitioned assets. |

### Schedules

| Field | Type | Default | Description |
|---|---|---|---|
| `reconciliation_cron` | `str` | `"0 * * * *"` | Cron for the Jenkins vs Dagster state reconciliation job. |

### Content

| Field | Type | Default | Description |
|---|---|---|---|
| `jobs` | `List[JenkinsJobSpec]` | `[]` | Jenkins jobs to wrap as daily-partitioned Dagster assets. |
| `source_tables` | `List[JenkinsSourceTableSpec]` | `[]` | Tables loaded by Jenkins that Dagster observes (adds to lineage). |
| `upstream_deps` | `List[str]` | `[]` | Upstream asset keys that must complete before jobs run. |

### JenkinsJobSpec

| Field | Type | Default | Description |
|---|---|---|---|
| `job_name` | `str` | required | Jenkins job name (use `folder/subfolder/job` for nested). |
| `asset_name` | `str` | required | Dagster asset name that wraps this job. |
| `description` | `str` | `""` | Prose description shown in the Dagster UI. |
| `folder` | `str` | `""` | Business tag for the containing folder. |
| `application` | `str` | `""` | Business tag (e.g. `CORE_BANKING`). |
| `node_label` | `str` | `""` | Jenkins node label the build runs on. |
| `parameters` | `dict` | `{}` | Parameters passed to `buildWithParameters`. Values templated with `{partition_key}`. |
| `run_as` | `str` | `"svc_dagster"` | Business tag for the OS user. |

### JenkinsSourceTableSpec

| Field | Type | Default | Description |
|---|---|---|---|
| `table_name` | `str` | required | Fully-qualified table name (`SCHEMA.TABLE`). |
| `asset_name` | `str` | required | Dagster asset name representing the table. |
| `description` | `str` | `""` | Prose description shown in the Dagster UI. |
| `group_name` | `str` | `"jenkins_managed_data"` | Dagster asset group for this table. |
| `produced_by` | `Optional[str]` | `None` | Asset name of the Jenkins job that loads this table. |

## Requirements

```
dagster
requests
```

## References

- Jenkins Remote Access API: <https://www.jenkins.io/doc/book/using/remote-access-api/>
- CSRF crumb protection: <https://www.jenkins.io/doc/book/security/csrf-protection/>
- API token setup: `http://<jenkins>/user/<name>/configure` -> "API Token" section
