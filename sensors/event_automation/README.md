# EventAutomationComponent

Prefect-Automations-style declarative event → action wiring in one YAML component. Each `when: … then: …` block becomes a real Dagster sensor under the covers; no Python needed for common trigger-action shapes.

**Surface: 35 trigger types + 27 action types + AND/OR compound composition (one level of nesting).**

## Triggers (35)

| Type | Fires on |
|---|---|
| `run_status` | Any run finishing with `SUCCESS/FAILURE/CANCELED/STARTED` |
| `asset_materialized` | Named assets get materialized |
| `schedule` | Cron (schedule → sensor with cron gating) |
| `http_poll` | Poll a URL; fires on response change, HTTP 2xx, or JSON path non-empty |
| `freshness_violation` | Asset stale beyond `max_age_minutes` (ongoing DQ) |
| `run_duration` | Run finished + duration > threshold (slow-run detector) |
| `run_stuck` | Active run running > threshold (once-per-run guard) |
| `asset_check_failed` | Named asset check evaluated FAILURE |
| `metric_threshold` | Numeric metadata crossed a threshold (gt/gte/lt/lte/eq/neq) |
| `absence` | Dead-man's switch: no materialization in `max_gap_minutes` |
| `log_pattern` | Regex match on run log lines (events / stdout / stderr — covers K8s / ECS container output) |
| `daemon_heartbeat` | Dagster daemon / Dagster+ agent stopped heartbeating |
| `code_location_status` | Code location failed to load / stuck loading / errored |
| `run_startup_slow` | Run took too long from creation to STARTED (compute spinup) |
| `asset_observation` | AssetObservation event emitted (distinct from materialization) |
| `step_error` | Op step raised an exception (step-level, not run-level; fires N times per multi-error run) |
| `metadata_match` | Materialization/observation carries specific metadata key=value (or key/regex) |
| `asset_value_change` | Numeric metadata Δ across two consecutive materializations |
| `backfill_status` | Partition backfill entered a state (COMPLETED/FAILED/CANCELED/REQUESTED) |
| `sensor_failing` | Target sensor failed N consecutive ticks (meta-observability) |
| `concurrency_hit` | Active-run count > threshold, optional tag filter |
| `hook_fired` | `@success_hook` / `@failure_hook` executed (per-op, distinct from step_error) |
| `asset_partition_materialized` | Specific asset **partition** materialized (partition_key or partition_key_pattern) |
| `run_reexecution` | Run was re-executed (retry audit trail) |
| `asset_wipe` | Materialization history wiped (destructive audit) |
| `config_override` | Run launched with non-default config (change-tracking) |
| `tag_set` | Run carries specific tag key/value (audit + routing) |
| `unhandled_exception` | Run-level unhandled exception (infra crash, distinct from step_error) |
| `asset_check_severity` | Asset check at WARN vs ERROR (separates severity handling) |
| `op_output` | Specific op yielded output (STEP_OUTPUT event) |
| `materialization_planned` | Pre-materialization event (warm caches, pre-provision downstream) |
| `asset_check_started` | Asset check evaluation started (pair with timer for "slow check" alerts) |
| `insights_metric` | **Dagster+ only.** Insights custom metric crossed threshold via GraphQL |
| `dagster_plus_audit` | **Dagster+ only.** Audit log event (RBAC, config, secrets) via GraphQL |
| `sqs_poll` | Poll an AWS SQS queue, fire per message |
| `all_of` | AND compound (all sub-triggers fire within `within_seconds`) |
| `any_of` | OR compound (nested inside `all_of` only) |

## Actions (27)

**Alerting / external** (11):

| Type | Effect |
|---|---|
| `webhook` | Arbitrary HTTP call with templated body |
| `slack` | Slack incoming-webhook alert |
| `pagerduty` | PagerDuty Events API v2 |
| `opsgenie` | OpsGenie Alerts API |
| `discord` | Discord webhook alert |
| `teams` | Microsoft Teams webhook |
| `mattermost` | Mattermost webhook |
| `email` | SMTP alert (stdlib smtplib) |
| `sns` | Publish to AWS SNS topic |
| `sqs` | Send to AWS SQS queue |
| `emit_event` | Log emission for downstream sensor chaining |

**Dagster runs / automation** (6):

| Type | Effect |
|---|---|
| `materialize` | Launch a materialization run |
| `launch_job` | Launch a job |
| `cancel_run` | Terminate a run (`instance.run_launcher.terminate`) |
| `retry_run` | Re-execute a failed run |
| `toggle_sensor` | Start/stop a sensor by name |
| `toggle_schedule` | Start/stop a schedule by name |

**Ops / self-healing** (10) — pair with triggers for auto-recovery loops:

| Type | Effect | Requires |
|---|---|---|
| `reload_code_location` | Reload a broken code location (or whole workspace) | Dagster+ token |
| `refresh_defs_state` | Refresh defs state for StateBackedComponent-shaped assets | Dagster+ token |
| `set_concurrency_limit` | Adjust a pool's slot count (scheduled scaling / reactive bump) | — (works on OSS) |
| `free_concurrency_slots` | Release slots stuck by a crashed run/step | — (works on OSS) |
| `set_auto_materialize_paused` | Globally pause/unpause Declarative Automation | Dagster+ token |
| `mute_alert_policy` | Temporarily mute a Dagster+ Alerts policy | Dagster+ token |
| `resume_backfill` | Resume a paused partition backfill | Dagster+ token |
| `cancel_backfill` | Cancel a partition backfill | Dagster+ token |
| `reexecute_backfill` | Re-execute a failed backfill (FROM_FAILURE or ALL_STEPS) | Dagster+ token |
| `add_dynamic_partition` | Register a new dynamic partition programmatically | — (works on OSS) |

**Template tokens available in every action:** `{event_type}`, `{run_id}`, `{job_name}`, `{asset_key}`, `{partition_key}`, `{status}`, `{timestamp}`, `{message}`, `{url}`. Partition-emitting triggers also expose per-dimension tokens like `{partition_date}` / `{partition_region}` when the event is on a `MultiPartitionsDefinition`.

## Dagster+ authentication

Several triggers and actions call the Dagster+ GraphQL API. The Dagster+ runtime injects two env vars for you automatically — **the only thing you have to provision is the API token as a code-location secret**:

| Env var | Purpose | Source |
|---|---|---|
| `DAGSTER_CLOUD_ORGANIZATION` | Your Dagster+ org slug (e.g. `acme-corp`). | **Auto-injected** by Dagster+ Serverless / Hybrid into every user-code container. |
| `DAGSTER_CLOUD_DEPLOYMENT_NAME` | Deployment name (`prod`, `staging`, a branch-deployment name, etc). | **Auto-injected** by Dagster+ — sensors running in a `dev` deployment target `dev` automatically. |
| `DAGSTER_CLOUD_API_TOKEN` | User / agent / service-user API token. | **You provision** this — Dagster+ Settings → Secrets, or via `dagster-cloud secret`. |

**Which triggers need it:** `insights_metric`, `dagster_plus_audit`.

**Which actions need it:** `reload_code_location`, `refresh_defs_state`, `set_auto_materialize_paused`, `mute_alert_policy`, `resume_backfill`, `cancel_backfill`, `reexecute_backfill`.

**Token permissions:** the minimum scope depends on the action — `metrics-read` for `insights_metric`, `audit-read` for `dagster_plus_audit`, and workspace-admin-equivalent for the recovery mutations (`reload_code_location`, `set_auto_materialize_paused`, backfill controls). For a dedicated alerts code location that fires all these actions, an agent token with full deployment access is the simplest pattern.

**Deployment scoping:** by default we auto-detect from `DAGSTER_CLOUD_DEPLOYMENT_NAME` — so a sensor running in `dev` targets `dev`, one running in `prod` targets `prod`, no config needed. Override per-trigger/action with `deployment: <name>` to force a specific target (useful if a prod-deployed sensor should observe a branch deployment).

**Failure mode:** if the token is missing at fire time, the trigger returns `SkipReason("... credentials missing")` and the action logs a warning and no-ops. Nothing crashes; nothing fires. Good for OSS-flavored deployments where you want the YAML to load cleanly even without Dagster+ configured.

You can override the env var names via `org_env_var` / `token_env_var` fields on every Dagster+ trigger and action if you need a different naming convention.

## Throttle / noise-reduction (11)

Every trigger takes an optional `throttle:` block. Rate-limit, suppress by context, or drive escalation ladders — full reference in **[Throttle / rate-limit / suppression](#throttle--rate-limit--suppression)** below.

| Field | Kind | Effect |
|---|---|---|
| `min_seconds_between_fires` | rate-limit | Cooldown between successful fires |
| `max_per_hour` | rate-limit | Rolling-window hard cap |
| `dedup_key_template` | scoping | Templated key scopes throttle state (per-job, per-asset, per-severity) |
| `strategy: silence` | strategy | Default — drop fires during cooldown or over cap |
| `strategy: summarize` | strategy | Buffer + fire ONE `[N× in window]` summary |
| `strategy: first_last` | strategy | Fire first + last of a burst, drop the middle |
| `strategy: llm` | strategy | LLM decides YES/NO per event (OpenAI / Anthropic), cached |
| `strategy: escalate` | strategy | Fire count → subset of `then:` actions (Slack → +PD → +execs) |
| `strategy: auto_resolve` | strategy | Pair every alert with a synthetic "resolved" event |
| `business_hours_only` | suppression | Only fire during a daily window (with tz + weekday filter) |
| `maintenance_windows` | suppression | Skip fires inside scheduled ISO8601 quiet periods |
| `correlation_suppress_sensors` | suppression | Suppress when a listed sensor fired recently (root-cause) |

Plus a retry-aware flag on `step_error` triggers: **`only_final_failures: true`** filters out attempts that will be retried by the op's `RetryPolicy`.

## Deploying as a dedicated alerts code location

The recommended pattern is a single `alerts_location` code location that observes every prod location — one team owns escalation logic, no changes to prod locations to add a new alert.

**What works from a dedicated location (the majority of triggers):** anything that scans `instance.event_log_storage` reads events from every code location, so these all work: `asset_materialized`, `asset_observation`, `freshness_violation`, `absence`, `materialization_planned`, `asset_partition_materialized`, `asset_wipe`, `metadata_match`, `metric_threshold`, `asset_value_change`, `log_pattern`, `step_error`, `op_output`, `hook_fired`, `unhandled_exception`, `asset_check_failed` / `_severity` / `_started`, `insights_metric`, `dagster_plus_audit`, `daemon_heartbeat`, `code_location_status`, `sensor_failure`, `concurrency_hit`, `backfill_status`, `run_reexecution`, `config_override`, `tag_set`, `run_stuck`, `run_startup_slow`, plus the external ones (`http_poll`, `sqs_poll`, `schedule`).

**Three caveats:**

1. **`run_status` and `run_duration` use `@run_status_sensor`**, which by default watches only jobs in its own repository. Set `monitored_locations` or `monitored_jobs` to observe other locations:

   ```yaml
   when:
     - type: run_status
       status: FAILURE
       monitored_locations: [prod_ingest, prod_analytics]   # watch every job in these locations
   ```

   Or pick specific jobs:

   ```yaml
   when:
     - type: run_status
       status: FAILURE
       monitored_jobs:
         - location: prod_ingest
           job: hourly_ingest
         - location: prod_analytics
           job: dbt_run
           repository: __repository__     # optional; defaults to __repository__ (create-dagster convention)
   ```

   The two fields compose as a union. Same shape on `run_duration`.

2. **Asset-selection *strings* only resolve against sibling assets** in the same defs folder. In a dedicated alerts location with no sibling assets, `asset_keys: "group:marts"` won't find anything. Options:
   - Use explicit lists: `asset_keys: [marts/orders, marts/customers]` — works cross-location because the event-log lookup is by raw key.
   - Or drop `external_asset` stubs in the alerts location so selectors have something to resolve against.

3. **Actions must be addressable from the alerts location.** Alerting actions (`slack`, `pagerduty`, `webhook`, `email`, `teams`, `opsgenie`, `discord`, `mattermost`, `sns`, `sqs`) have no cross-location concerns. `cancel_run` / `retry_run` work by run_id (always cross-location OK). `materialize` / `launch_job` need the target job/asset to be visible to the launcher — on Dagster+ this Just Works since all locations share one instance; on OSS it depends on your workspace configuration.

**Layout for a dedicated alerts location:**

```
alerts_location/
├── pyproject.toml
├── src/alerts_location/
│   └── defs/
│       ├── prod_run_failures/defs.yaml    # EventAutomationComponent
│       ├── freshness_slas/defs.yaml       # EventAutomationComponent
│       └── platform_health/defs.yaml      # EventAutomationComponent
```

Each `defs.yaml` uses `monitored_locations` on `run_status` / `run_duration` triggers; other trigger types work without configuration.

## Targeting: selection syntax + run filters

Both asset-based and run-based triggers accept richer targeting than a bare literal — use the same shape Dagster's UI uses.

**Asset-based triggers** (`asset_materialized`, `asset_observation`, `freshness_violation`, `absence`, `asset_check_failed`, `asset_check_severity`, `asset_check_started`, `asset_partition_materialized`, `materialization_planned`, `asset_wipe`): `asset_keys` accepts either an explicit list OR a single Dagster asset-selection string. Selectors are resolved at `build_defs` time against sibling assets in the same defs folder.

| Form | Example |
|---|---|
| Explicit list | `asset_keys: [marts/orders, marts/customers]` |
| Group | `asset_keys: "group:marts"` |
| Tag | `asset_keys: "tag:tier=gold"` |
| Kind | `asset_keys: "kind:dbt"` |
| Type filter | `asset_keys: "is:external"` |
| Boolean composition | `asset_keys: "group:marts and tag:critical"` |
| Fnmatch glob | `asset_keys: "marts/*"` |
| Single literal (backward compat) | `asset_keys: "hourly_summary"` |

**Run-based triggers** (`run_status`, `run_duration`, `run_stuck`, `run_startup_slow`, `step_error`, `hook_fired`): three complementary filters that AND together.

| Field | Semantics | Example |
|---|---|---|
| `job_name` | exact match | `job_name: hourly_ingest` |
| `job_name_pattern` | fnmatch glob | `job_name_pattern: "prod_*"` |
| `run_tags` | every listed key=value must be present in the run's tags | `run_tags: {priority: P0, team: data-platform}` |

```yaml
- type: run_status
  status: FAILURE
  job_name_pattern: "prod_*"
  run_tags:
    priority: P0
```

## Why

Prefect's Automations UI is a first-class surface where any trigger → any action is composable, no code required. Dagster has all the underlying primitives — sensors, `AutomationCondition`, run-status sensors, asset checks, freshness policies — but they're Python-first. This component collapses the common trigger-action patterns into a single YAML shape that renders in the components UI, so ops-heavy teams can wire glue without dropping to Python.

## Shape

```yaml
type: dagster_community_components.EventAutomationComponent
attributes:
  name: <automation_name>
  description: "Free-form description shown in the UI"
  default_status: STOPPED   # or RUNNING
  when:
    - type: <trigger_type>
      # ... trigger-specific fields
  then:
    - type: <action_type>
      # ... action-specific fields
```

**Composition semantics:**

- Multiple `when:` triggers → **OR** (any fires the automation)
- Multiple `then:` actions → all run when triggered, sequential, best-effort
- Compound triggers `all_of` / `any_of` → **AND** / explicit OR with one level of nesting
- Each trigger emits its own Dagster sensor; all share the same action bundle

## Trigger reference

### State-based

**`run_status`** — job / asset run finished with specific status.

```yaml
- type: run_status
  status: FAILURE          # SUCCESS | FAILURE | CANCELED | STARTED
  job_name: hourly_ingest  # optional; omit = any job
```

**`asset_materialized`** — any of the named assets get materialized.

```yaml
- type: asset_materialized
  asset_keys: [raw_data, staging_data]
```

**`run_duration`** — a run finished, and duration exceeded a threshold.

```yaml
- type: run_duration
  max_duration_seconds: 1800     # 30 minutes
  job_name: nightly_etl          # optional
  on_status: SUCCESS             # ANY | SUCCESS | FAILURE
```

**`run_stuck`** — an active run has been running too long. Polls active runs each tick; fires once per stuck run.

```yaml
- type: run_stuck
  max_running_seconds: 3600      # 1 hour
  job_name: hourly_ingest        # optional
```

### Time-based

**`schedule`** — cron trigger. Also gives you a purely cron-driven "kick something off" shape in YAML without writing a schedule in Python.

```yaml
- type: schedule
  cron: "0 * * * *"
  execution_timezone: America/New_York   # default: UTC
```

### External

**`http_poll`** — poll a URL and fire on one of three conditions:

- `response_changed` (default) — fire when the response body differs from prior poll (hashed cursor)
- `status_ok` — fire on any HTTP 2xx (every tick)
- `json_path_present` — fire when a JSON path resolves non-empty

```yaml
- type: http_poll
  url: "https://api.example.com/pending-jobs"
  method: GET
  minimum_interval_seconds: 60
  condition: json_path_present
  json_path: "pending"    # required for json_path_present
```

**`sqs_poll`** — poll an AWS SQS queue. Each message becomes one automation firing; `{message}` template token is the raw body. Messages deleted after successful action execution by default.

```yaml
- type: sqs_poll
  queue_url: "https://sqs.us-east-1.amazonaws.com/123456789012/my-queue"
  region: us-east-1
  max_messages: 10          # SQS API max 10 per receive
  minimum_interval_seconds: 30
  delete_after: true
```

### Data quality

**`freshness_violation`** — asset stale beyond `max_age_minutes` (ongoing DQ signal).

```yaml
- type: freshness_violation
  asset_keys: [hourly_summary]
  max_age_minutes: 60
```

**`absence`** — dead-man's switch: named asset has NOT materialized in the last `max_gap_minutes` (was expected but didn't happen). Fires once per gap.

```yaml
- type: absence
  asset_keys: [hourly_ingest_run]
  max_gap_minutes: 90
```

**`asset_check_failed`** — named asset check(s) evaluated to FAILURE. Watches the event log.

```yaml
- type: asset_check_failed
  check_names: [row_count_positive, revenue_non_negative]  # omit = any check
  asset_keys: [transactions]                                # optional
```

**`metric_threshold`** — numeric metadata on a materialization crossed a threshold.

```yaml
- type: metric_threshold
  asset_key: hourly_summary
  metadata_key: row_count
  comparison: lt              # gt | gte | lt | lte | eq | neq
  threshold: 100
```

**`asset_value_change`** — numeric metadata Δ between two consecutive materializations. Direction filter (`increase` / `decrease` / `any`), absolute delta OR percentage delta.

```yaml
- type: asset_value_change
  asset_key: hourly_summary
  metadata_key: row_count
  direction: decrease            # any | increase | decrease
  min_delta_pct: 25              # fires when |Δ|/prev > 25%
  # min_delta: 500               # absolute alternative
```

**`log_pattern`** — regex match on run log lines. Catches issues that a raw run_status doesn't (runs that "succeed" with warnings, OOMs, or specific stack traces). `sources` controls which log streams get scanned:

- `events` (default) — dagster event log entries (context.log.info/warning/error calls + framework messages + tracebacks)
- `stdout` / `stderr` — raw compute_log_manager output (K8s / ECS / Docker container stdout+stderr). Catches OOMKilled + kernel panics + oomkill traces that never made it to the dagster logger.

```yaml
- type: log_pattern
  pattern: "OOMKilled|OutOfMemoryError|MemoryError"
  sources: [events, stderr]       # default: [events]
  job_name: prod_ingest           # optional
```

**`asset_observation`** — an `AssetObservation` event was emitted. Distinct from `asset_materialized` — observations record signals about an asset without producing a new materialization (freshness updates, external system state, DQ checks).

```yaml
- type: asset_observation
  asset_keys: [external_status_asset, upstream_health]
```

**`step_error`** — an op step raised an exception (STEP_FAILURE event). Fires at the step level even when the run overall succeeds (retries, downstream steps that recover). Fires multiple times per run if multiple steps fail.

```yaml
- type: step_error
  job_name: prod_ingest                   # optional
  step_key_pattern: ".*etl.*"             # optional regex
  exception_pattern: "OOMKilled|Timeout"  # optional regex
  only_final_failures: true               # skip attempts that will be retried
```

Set `only_final_failures: true` when the op has a `RetryPolicy` — otherwise every intermediate attempt fires a pager. Under the hood, the sensor checks `instance.get_run_step_stats()` at fire time and only pages when the step's current status is definitively FAILURE (not `RETRY_REQUESTED` / `IN_PROGRESS` / `SUCCESS`). `run_status: FAILURE` already fires only on terminal runs, so this flag is specific to step-level failures.

**`metadata_match`** — materialization/observation carries specific metadata. Three shapes:

- `metadata_key` alone → fires when the key is present (any value)
- `metadata_key` + `equals` → fires when key == equals
- `metadata_key` + `regex` → fires when str(value) matches regex

```yaml
- type: metadata_match
  asset_key: hourly_summary
  metadata_key: quality_grade
  regex: "poor|failed"                    # or `equals: "poor"`
  include_observations: true              # default: true
```

### Reliability / meta

**`backfill_status`** — a partition backfill entered a state.

```yaml
- type: backfill_status
  status: FAILED                  # COMPLETED | FAILED | CANCELED | REQUESTED
  job_name: daily_ingest          # optional
```

**`sensor_failing`** — a target sensor has been failing N consecutive ticks (meta-observability for broken sensors).

```yaml
- type: sensor_failing
  target_sensor_name: kafka_ingest_sensor
  consecutive_failures: 5
```

**`concurrency_hit`** — queued+running run count exceeded a threshold. Optional tag filter.

```yaml
- type: concurrency_hit
  max_queued: 50
  tag_key: dagster/job            # optional
  tag_value: heavy_batch          # optional
```

### Platform / infra

**`daemon_heartbeat`** — a Dagster daemon / Dagster+ agent stopped heartbeating. Covers OSS daemons (SENSOR / SCHEDULER / QUEUED_RUN_COORDINATOR / BACKFILL / ASSET / FRESHNESS) and Dagster+ Hybrid agents (Docker / K8s / ECS / ACR — they report through the same daemon interface). Once-per-outage semantics.

```yaml
- type: daemon_heartbeat
  daemon_type: SENSOR              # optional — filter to specific daemon type
  max_seconds_since_heartbeat: 120
```

**`code_location_status`** — a code location entered an unhealthy state. Catches `dg plus deploy` failures, dependency drift, long-tail load times.

```yaml
- type: code_location_status
  on_status: UNHEALTHY             # ERROR | LOADING | TIMED_OUT | UNHEALTHY
  max_seconds_loading: 300         # only used with on_status=LOADING
  location_name_pattern: "prod-.*" # optional
```

**`run_startup_slow`** — captures "compute took too long to spin up" (pex load on Serverless, docker pull + container start on Hybrid, K8s pod scheduling delays, ECS task placement waits, resource-init hangs). Distinct from `run_stuck` — this measures QUEUED/STARTING → STARTED latency.

```yaml
- type: run_startup_slow
  max_startup_seconds: 120         # fire when creation → STARTED > 2min
  job_name: heavy_batch            # optional
```

### Audit + compliance

**`run_reexecution`** — a run was re-executed (has `parent_run_id` set). Audit trail for retry actions. Optional `job_name` + `strategy` (from_failure / all_steps) filters.

```yaml
- type: run_reexecution
  job_name: prod_pipeline
  strategy: from_failure           # optional
```

**`config_override`** — a run was launched with non-default `run_config`. Change-tracking + audit workflows.

```yaml
- type: config_override
  job_name: prod_pipeline           # optional
```

**`tag_set`** — a run carries a specific tag key (with optional value or regex). Useful for audit + routing: `env=prod-hotfix`, `priority=P0`, `owner=user123`.

```yaml
- type: tag_set
  tag_key: priority
  tag_value: P0                    # optional exact
  # tag_value_pattern: "P[0-1]"    # or regex
```

**`unhandled_exception`** — run-level unhandled exception distinct from step-level errors. Catches infrastructure crashes / OOM kills that terminate the run process before any step-level error is emitted.

```yaml
- type: unhandled_exception
  job_name: prod_pipeline           # optional
```

**`asset_wipe`** — someone deleted materialization history for an asset. Rare + important audit signal.

```yaml
- type: asset_wipe
  asset_keys: [customer_revenue]    # optional; omit = any wipe
```

### Fine-grained event

**`hook_fired`** — a `@success_hook` / `@failure_hook` executed. Distinct from `step_error` — hooks capture success paths too, and are per-op not per-step.

```yaml
- type: hook_fired
  hook_name_pattern: ".*prod.*"     # optional regex
  on_status: FAILURE                # ANY | SUCCESS | FAILURE
```

**`asset_partition_materialized`** — a specific asset PARTITION was materialized. Distinct from `asset_materialized` — filters to an exact partition key or partition family regex.

```yaml
# Single-dim: exact match or regex
- type: asset_partition_materialized
  asset_keys: [daily_revenue]
  partition_key: "2024-01-15"       # exact match
  # partition_key_pattern: "2024-Q1-.*"   # or regex

# Multi-dim (MultiPartitionsDefinition): dict; unspecified dims wildcard
- type: asset_partition_materialized
  asset_keys: [regional_revenue]
  partition_key:
    region: "us"                    # matches every date for region=us
```

Emits `{partition_key}` (the whole key) plus `{partition_<dim>}` tokens (e.g. `{partition_date}`, `{partition_region}`) so downstream actions can template dynamic per-partition values.

**`op_output`** — a specific op yielded output (STEP_OUTPUT event). Fine-grained; useful for non-asset op-based workflows.

```yaml
- type: op_output
  step_key_pattern: ".*etl.*"
  output_name: cleaned_data         # optional
```

**`materialization_planned`** — an `ASSET_MATERIALIZATION_PLANNED` event was emitted (before materialization). Useful for pre-materialization side-effects (warm caches, pre-provision downstream resources).

```yaml
- type: materialization_planned
  asset_keys: [expensive_ml_model]
```

**`asset_check_started`** — an asset check evaluation started. Mirror of `asset_check_failed` but on start. Pair with a timer to alert on "slow check".

```yaml
- type: asset_check_started
  check_names: [row_count_positive]
  asset_keys: [customer_events]     # optional
```

**`asset_check_severity`** — asset check evaluations at a specific severity level. Variant of `asset_check_failed` with severity filter (WARN vs ERROR) so you can separate handling.

```yaml
- type: asset_check_severity
  severity: WARN                    # WARN | ERROR
  check_names: [null_rate_check]
```

### Dagster+ integrations

These EXTEND Dagster+ (programmatic reaction via GraphQL) rather than replacing its native alerting UI. Require `DAGSTER_CLOUD_ORGANIZATION` + `DAGSTER_CLOUD_API_TOKEN` env vars.

**`insights_metric`** — a Dagster+ Insights **time-window aggregate** crossed a threshold. Distinct from `metric_threshold`, which fires on a single materialization's raw metadata. This queries Dagster+ Insights (Victoria Metrics under the hood) for aggregations over a configurable time window — alert on trend shape ("weekly average", "daily p95") rather than single-event crossings. Also the entry point for platform-computed metrics that don't exist as raw materialization metadata (credit spend, run duration aggregates, freshness pass %).

```yaml
- type: insights_metric
  metric_name: hourly_summary.row_count   # promoted via Insights UI, or Dagster+ built-in
  comparison: lt                          # gt | gte | lt | lte | eq | neq
  threshold: 100
  granularity: DAILY                      # HOURLY | DAILY | WEEKLY | MONTHLY (Victoria Metrics bucket)
  aggregation: AVERAGE                    # SUM | AVERAGE | MIN | MAX (how bucket values combine)
  lookback_hours: 168                     # how many hours of history (168 = 7 days)
  # Optional scoping via Dagster asset-selection syntax (server-side resolved
  # via reportingMetricsByAssetSelection). Unset = deployment-wide.
  asset_selection: "group:marts and tag:tier=gold"
  deployment: prod
  org_env_var: DAGSTER_CLOUD_ORGANIZATION
  token_env_var: DAGSTER_CLOUD_API_TOKEN
```

The `asset_selection` field is a Dagster asset-selection string resolved server-side by the `reportingMetricsByAssetSelection` GraphQL query — the same selection surface Dagster's UI uses. Covers group / tag / kind / asset-key targeting via one field (`group:marts and tag:tier=gold`, `kind:dbt`, `is:external`, `key:"marts/orders"`). Verified against live prod GraphQL.

**`dagster_plus_audit`** — a Dagster+ audit log event matched a filter. Dagster+ Alerts doesn't cover audit-log events, so this is the programmatic hook: SOC2 / SIEM feeds, security-team Slack on prod RBAC changes, secret-rotation notifications, config-change tracking.

```yaml
- type: dagster_plus_audit
  # Server-side push-down filters (verified live) — prefer these over regex
  event_types:                                           # exact enum match
    - CREATE_SECRET
    - UPDATE_SECRET
    - DELETE_SECRET
    - CHANGE_USER_PERMISSIONS
  user_emails: ["alice@company.com", "bob@company.com"]  # actor filter
  deployment_names: ["prod"]
  is_branch_deployment: false                            # main deployments only
  # Client-side regex (applied after push-down)
  event_type_pattern: ".*SECRET.*"                       # optional
  actor_pattern: ".*@company.com"                        # optional
  deployment: prod
```

**Real audit event types (42 total, verified via live GraphQL introspection):**

- **RBAC / users** — `CHANGE_USER_PERMISSIONS`, `CREATE_SERVICE_USER`, `UPDATE_SERVICE_USER`, `DELETE_SERVICE_USER`, `CHANGE_SERVICE_USER_PERMISSIONS`
- **Tokens** — `CREATE_USER_TOKEN`, `REVOKE_USER_TOKEN`, `CREATE_AGENT_TOKEN`, `REVOKE_AGENT_TOKEN`, `UPDATE_AGENT_TOKEN_PERMISSIONS`, `CREATE_SERVICE_TOKEN`, `REVOKE_SERVICE_TOKEN`, `PUT_REVOKE_TOKEN`
- **Secrets** — `CREATE_SECRET`, `UPDATE_SECRET`, `DELETE_SECRET`
- **Deployments** — `CREATE_DEPLOYMENT`, `DELETE_DEPLOYMENT`, `UPDATE_DEPLOYMENT_SETTINGS`
- **Code locations** — `CREATE_CODE_LOCATION`, `UPDATE_CODE_LOCATION`, `DELETE_CODE_LOCATION`, `REDEPLOY_SERVERLESS_AGENT`
- **Automation** — `UPDATE_SCHEDULE`, `UPDATE_SENSOR`, `SET_AUTO_MATERIALIZE_PAUSED`, `LAUNCH_RUN`, `LAUNCH_BACKFILL`
- **Alerts (meta!)** — `MODIFY_ALERT_POLICIES`, `SET_ALERT_POLICY_MUTE_UNTIL`
- **Org** — `CREATE_ORGANIZATION_SUBDOMAIN`, `DELETE_ORGANIZATION_SUBDOMAIN`, `UPDATE_SUBSCRIPTION_PLAN`, `UPDATE_SUBSCRIPTION_TYPE`
- **Auth** — `LOG_IN`, `IFRAME_LOG_IN`

Emits tokens: `{audit_event_type}` (e.g. `CREATE_SECRET`), `{actor}` (the user email or agent token id that took the action), `{deployment}` (which deployment) — plus the standard token surface. Handy for SIEM webhooks that need structured fields.

### Composite (AND / OR)

**`all_of`** — AND-composition. Fires only when ALL sub-triggers have fired within `within_seconds`. Sub-triggers can be any leaf trigger OR an `any_of` (giving you `all_of([leaf, any_of(...)])` — two levels of nesting).

```yaml
- type: all_of
  within_seconds: 3600
  triggers:
    - type: any_of                  # nested OR
      triggers:
        - type: run_status
          status: FAILURE
          job_name: job_a
        - type: run_status
          status: FAILURE
          job_name: job_b
    - type: freshness_violation
      asset_keys: [hourly_summary]
      max_age_minutes: 120
```

Reads as: **(job_a failed OR job_b failed) AND freshness violated within 1 hour**.

**`any_of`** — OR inside a compound. At the top of `when:`, multiple triggers are already OR; use `any_of` only when nested inside `all_of`.

## Throttle / rate-limit / suppression

Every trigger supports an optional `throttle:` block. Keeps sensors from spamming Slack/PagerDuty/webhooks when the same event fires repeatedly.

```yaml
- type: run_status
  status: FAILURE
  throttle:
    min_seconds_between_fires: 300     # cooldown between successful fires
    max_per_hour: 4                    # rolling-window cap
    dedup_key_template: "{job_name}"   # scope per key (default: whole trigger)
    strategy: silence                  # silence | summarize | first_last | llm | escalate | auto_resolve
```

**Fields:**

| Field | Meaning |
|---|---|
| `min_seconds_between_fires` | Drop fires that happen within this cooldown of the last fire |
| `max_per_hour` | Rolling-window hard cap — drop fires that would exceed this in the last hour |
| `dedup_key_template` | Rendered template becomes the state key. Default: whole trigger (`*`). Use `"{job_name}"`, `"{asset_key}"`, `"{event_type}:{status}"`, etc. |
| `strategy` | See below |
| `flush_after_seconds` | For `summarize` / `first_last`: buffer window |

### Strategies

**`silence`** (default) — Simplest: drop fires during cooldown or over the hourly cap.

```yaml
throttle:
  min_seconds_between_fires: 300
  strategy: silence
```

**`summarize`** — Buffer events during the flush window, then fire ONE summary: `"[N× in the last window] First: <msg> @ ts=X. Last: <msg> @ ts=Y."`

```yaml
throttle:
  strategy: summarize
  flush_after_seconds: 600
  dedup_key_template: "{job_name}"
```

**`first_last`** — Fire the first + last of a burst, drop the middle. Good for "started/resolved" pairs.

```yaml
throttle:
  strategy: first_last
  flush_after_seconds: 300
```

**`llm`** — Ask an LLM to decide YES/NO based on the current alert + recent history for the same dedup key. Falls back to YES (fire) on any API error.

```yaml
throttle:
  strategy: llm
  llm_provider: openai              # openai | anthropic
  llm_model: gpt-4o-mini
  llm_api_key_env_var: OPENAI_API_KEY
  llm_prompt_template: |
    You are the on-call paging engineer.
    Alert: {message}
    Recent alerts: {recent}
    Should I page? Consider: is it business hours? Is this recurring?
    Answer 'YES: <reason>' or 'NO: <reason>'.
  llm_decision_cache_seconds: 60    # cache the YES/NO for this key
```

The LLM decision is cached briefly per dedup key so a flood of the same event doesn't spam the LLM.

**`escalate`** — Fire count drives which subset of your `then:` list runs. Tier 0 might be Slack; tier 1 adds PagerDuty; tier 2 pages the whole team.

```yaml
- type: run_status
  status: FAILURE
  throttle:
    strategy: escalate
    escalation_ladder:
      - after_fires: 0     # first fire → slack only
        action_indices: [0]
      - after_fires: 3     # after 3 fires → slack + pagerduty
        action_indices: [0, 1]
      - after_fires: 10    # after 10 fires → slack + pagerduty + email exec
        action_indices: [0, 1, 2]
then:
  - type: slack
    webhook_url_env_var: SLACK_WEBHOOK
  - type: pagerduty
    routing_key_env_var: PD_KEY
  - type: email
    smtp_host_env_var: SMTP_HOST
    to: [execs@company.com]
```

`action_indices` refer to positions in the `then:` list (0-based). Fire count persists across ticks per dedup key.

**`auto_resolve`** — Emits a paired "resolved" event once the underlying condition clears. Great for pager alerts where you want the "up again" notification without a second sensor.

```yaml
- type: http_poll
  url: https://api.example.com/health
  status_matches: 5xx
  throttle:
    strategy: auto_resolve
    min_seconds_between_fires: 60
    auto_resolve_message: "✅ health OK — was down {duration_seconds}s ({fire_count} fires)"
then:
  - type: slack
    webhook_url_env_var: SLACK_WEBHOOK
    message: "{event_type} — {message}"
```

On a subsequent tick where the condition is no longer firing (nothing has fired for ~2× the cooldown), a synthetic event with `event_type=auto_resolved` runs the same actions. Downstream Slack templates get `{duration_seconds}` and `{fire_count}` tokens.

### Suppression: business hours / maintenance / correlation

Three gates that run BEFORE cooldown/rate/strategy checks. Any gate matching = fire dropped.

**`business_hours_only`** — Only fire during the given daily window (optionally scoped to weekdays).

```yaml
throttle:
  business_hours_only: "09:00-17:00 America/New_York mon,tue,wed,thu,fri"
```

Format: `HH:MM-HH:MM tz [days]`. Timezone is any IANA name (`America/New_York`, `UTC`, etc.). Days are optional; if omitted, all 7 days match. Overnight windows work (`22:00-06:00`).

**`maintenance_windows`** — Scheduled quiet periods. Fires during any listed ISO8601 window are suppressed with a log line naming the reason.

```yaml
throttle:
  maintenance_windows:
    - from_ts: "2024-01-15T02:00:00Z"
      to_ts: "2024-01-15T06:00:00Z"
      reason: "quarterly warehouse rebuild"
    - from_ts: "2024-01-20T00:00:00Z"
      to_ts: "2024-01-20T04:00:00Z"
      reason: "planned deploy"
```

**`correlation_suppress_sensors`** — Root-cause suppression. If any of the listed sensor names fired in the correlation window, drop this fire. Useful when a daemon-down sensor should silence 20 downstream "X is broken" alerts.

```yaml
throttle:
  correlation_suppress_sensors:
    - "daemon_heartbeat"        # substring match on sensor name
    - "code_location_status"
  correlation_within_seconds: 300
```

Substring match, so `daemon_heartbeat` matches the auto-generated name `ops_alerts__daemon_heartbeat_0`. Cross-sensor state lives in a bounded module-level fire log (500 most recent fires across all sensors).

### State semantics

- **State scope**: module-level dict, keyed by `sensor_name:dedup_key`
- **Persistence**: survives across ticks in the same daemon process, **resets on process restart** — planned + acceptable ("throttle resets after deploy" is defensible semantics)
- **No cursor conflicts**: throttle state is separate from each sensor's own cursor state
- **Cross-sensor state**: correlation uses a bounded (500 entries) global fire log

## Action reference

Every action gets template tokens: `{event_type}`, `{run_id}`, `{job_name}`, `{asset_key}`, `{status}`, `{timestamp}`, `{message}`, `{url}`.

### Dagster runs

**`materialize`** — launch a materialization run.

```yaml
# Single-dim partition — usually you want the partition FROM the event, not a hardcode
- type: materialize
  asset_keys: [derived_data]
  partition_key: "{partition_key}"        # pulled from triggering event's tokens

# Multi-dim (MultiPartitionsDefinition) — dict form, each dim optionally templated
- type: materialize
  asset_keys: [derived_data]
  partition_key:
    date: "{partition_date}"
    region: "{partition_region}"

# Literal — rare; only when you want the SAME partition every time the trigger fires
- type: materialize
  asset_keys: [derived_data]
  partition_key: "2024-01-15"
```

`partition_key` values run through template rendering. The trigger tokens include `{partition_key}` (the whole key as a string) plus per-dimension tokens like `{partition_date}` / `{partition_region}` when the source event was on a `MultiPartitionsDefinition`.

**`launch_job`** — launch a job.

```yaml
- type: launch_job
  job_name: cleanup
  tags: {priority: high}
```

**`cancel_run`** — terminate a run via `instance.run_launcher.terminate(run_id)`.

```yaml
- type: cancel_run
  which: triggering             # triggering | all_matching
  job_name_filter: long_job     # only used when which=all_matching
```

**`retry_run`** — re-execute a failed run. Best-effort — needs workspace context, works better in Dagster+ than raw dg-core.

```yaml
- type: retry_run
  strategy: from_failure        # from_failure | all_steps
```

**`toggle_sensor`** / **`toggle_schedule`** — flip InstigatorStatus for a sensor / schedule by name.

```yaml
- type: toggle_sensor
  sensor_name: ingest_sensor
  action: stop                  # start | stop
```

### Alerts

**`slack`** — Slack incoming-webhook.

```yaml
- type: slack
  webhook_url_env_var: SLACK_WEBHOOK_URL
  message: "🚨 {job_name} failed (run_id={run_id})"
  channel: "#alerts"
  username: "Dagster"
  icon_emoji: ":robot_face:"
```

**`pagerduty`** — PagerDuty Events API v2 with dedup + severity.

```yaml
- type: pagerduty
  routing_key_env_var: PAGERDUTY_ROUTING_KEY
  severity: error       # critical | error | warning | info
  summary_template: "Prod {job_name} failed"
  dedup_key_template: "prod-failure:{job_name}"
  event_action: trigger # trigger | acknowledge | resolve
```

**`opsgenie`** — OpsGenie Alerts API.

```yaml
- type: opsgenie
  api_key_env_var: OPSGENIE_KEY
  priority: P1              # P1 | P2 | P3 | P4 | P5
  message_template: "Dagster: {event_type} on {job_name}"
  dedup_key_template: "prod-failure:{job_name}"
```

**`discord`** / **`teams`** / **`mattermost`** — webhook-driven chat alerts.

```yaml
- type: teams
  webhook_url_env_var: TEAMS_WEBHOOK_URL
  message: "Dagster: {event_type} on {job_name}"
  title: "Prod alert"

- type: discord
  webhook_url_env_var: DISCORD_WEBHOOK_URL
  message: "Dagster: {event_type} on {job_name}"

- type: mattermost
  webhook_url_env_var: MATTERMOST_URL
  message: "Dagster: {event_type} on {job_name}"
  channel: "#alerts"
```

**`email`** — SMTP alert (stdlib `smtplib`, no extra deps).

```yaml
- type: email
  smtp_host_env_var: SMTP_HOST
  smtp_port_env_var: SMTP_PORT      # optional, default 587
  smtp_user_env_var: SMTP_USER
  smtp_password_env_var: SMTP_PASSWORD
  from_addr: "alerts@example.com"
  to: ["oncall@example.com", "team@example.com"]
  subject_template: "Dagster: {event_type} {job_name}"
  body_template: "Run {run_id} — {message}"
  use_tls: true
```

### External integrations

**`webhook`** — arbitrary HTTP call.

```yaml
- type: webhook
  url: "https://hooks.example.com/dagster"
  method: POST
  headers: {Content-Type: application/json}
  body_template: '{"event": "{event_type}", "at": "{timestamp}"}'
  timeout_seconds: 15
```

**`sns`** — publish to AWS SNS topic. Optional deps: `boto3`.

```yaml
- type: sns
  topic_arn: "arn:aws:sns:us-east-1:123456789012:dagster-alerts"
  region: us-east-1
  subject_template: "Dagster: {event_type}"
  message_template: "{job_name} → {status}"
```

**`sqs`** — send message to AWS SQS queue. Optional deps: `boto3`.

```yaml
- type: sqs
  queue_url: "https://sqs.us-east-1.amazonaws.com/123456789012/dagster-out"
  region: us-east-1
  body_template: '{"event":"{event_type}","job":"{job_name}"}'
  message_group_id: "dagster-events"        # FIFO only
  message_deduplication_id_template: "{event_type}:{run_id}"
```

**`emit_event`** — logs an emission for downstream sensor chaining.

```yaml
- type: emit_event
  asset_key: automation_fired
  metadata_template:
    fired_by: "{event_type}"
    ts: "{timestamp}"
```

## Common recipes

### Alert-on-failure

```yaml
name: alert_on_prod_failure
when:
  - type: run_status
    status: FAILURE
    job_name: hourly_ingest
then:
  - type: slack
    webhook_url_env_var: SLACK_WEBHOOK_URL
    message: "🚨 {job_name} failed: {run_id}"
  - type: pagerduty
    routing_key_env_var: PAGERDUTY_ROUTING_KEY
    severity: error
```

### Kill long-running runs

```yaml
name: kill_stuck_runs
when:
  - type: run_stuck
    max_running_seconds: 1800   # 30 minutes
then:
  - type: cancel_run
    which: triggering
  - type: pagerduty
    routing_key_env_var: PD_KEY
    severity: warning
    summary_template: "Killed stuck run {run_id[:8]} of {job_name}"
```

### Reprocess on upstream change

```yaml
name: reprocess_on_raw_update
when:
  - type: asset_materialized
    asset_keys: [raw_data]
then:
  - type: materialize
    asset_keys: [derived_data, aggregated_data]
```

### Freshness → auto-heal + escalate

```yaml
name: stale_summary_heal
when:
  - type: freshness_violation
    asset_keys: [hourly_summary]
    max_age_minutes: 90
then:
  - type: materialize
    asset_keys: [hourly_summary]     # self-heal
  - type: pagerduty
    routing_key_env_var: PD_KEY
    severity: warning
    summary_template: "{asset_key} stale — {message}"
```

### Multi-signal outage (nested compound)

```yaml
name: outage_correlation
when:
  - type: all_of
    within_seconds: 3600
    triggers:
      - type: any_of
        triggers:
          - type: run_status
            status: FAILURE
            job_name: job_a
          - type: run_status
            status: FAILURE
            job_name: job_b
      - type: freshness_violation
        asset_keys: [hourly_summary]
        max_age_minutes: 120
then:
  - type: opsgenie
    api_key_env_var: OPSGENIE_KEY
    priority: P1
    message_template: "Multi-signal outage: {message}"
```

### OOM detection via log_pattern

```yaml
name: oom_alert
when:
  - type: log_pattern
    pattern: "OOMKilled|OutOfMemoryError|Killed process"
then:
  - type: pagerduty
    routing_key_env_var: PD_KEY
    severity: error
    summary_template: "OOM in {job_name} (run {run_id})"
```

### Traffic drop detection via asset_value_change

```yaml
name: revenue_drop_alert
when:
  - type: asset_value_change
    asset_key: daily_revenue
    metadata_key: total
    direction: decrease
    min_delta_pct: 20                # fires on 20%+ drop
then:
  - type: slack
    webhook_url_env_var: SLACK_URL
    message: "📉 Revenue dropped: {message}"
```

### Failed backfill → OpsGenie

```yaml
name: backfill_failure_alert
when:
  - type: backfill_status
    status: FAILED
then:
  - type: opsgenie
    api_key_env_var: OPSGENIE_KEY
    priority: P2
    message_template: "Backfill {run_id} failed: {message}"
```

### Broken-sensor detection (meta-observability)

```yaml
name: watch_ingest_sensor
when:
  - type: sensor_failing
    target_sensor_name: kafka_ingest_sensor
    consecutive_failures: 5
then:
  - type: pagerduty
    routing_key_env_var: PD_KEY
    severity: warning
    summary_template: "kafka_ingest_sensor failing 5 ticks — check daemon logs"
```

### Concurrency-hit → cancel + alert

```yaml
name: overload_guardrail
when:
  - type: concurrency_hit
    max_queued: 100
    tag_key: dagster/job
    tag_value: heavy_batch
then:
  - type: cancel_run
    which: all_matching
    job_name_filter: heavy_batch
  - type: opsgenie
    api_key_env_var: OPSGENIE_KEY
    priority: P1
    message_template: "Queue overloaded: {message}"
```

### External queue → Dagster runs (AWS)

```yaml
name: aws_bridge
when:
  - type: sqs_poll
    queue_url: "https://sqs.us-east-1.amazonaws.com/12345/ingest-jobs"
    max_messages: 10
then:
  - type: launch_job
    job_name: process_batch
  - type: sns
    topic_arn: "arn:aws:sns:us-east-1:12345:dagster-events"
    message_template: "Processed SQS: {message}"
```

### Cron heartbeat

```yaml
name: hourly_heartbeat
when:
  - type: schedule
    cron: "0 * * * *"
then:
  - type: webhook
    url: "https://uptime.example.com/hourly-heartbeat"
    method: GET
```

### DQ escalation ladder

```yaml
name: dq_escalation
when:
  - type: asset_check_failed
    check_names: [row_count_positive]
then:
  - type: slack
    webhook_url_env_var: SLACK_URL
    message: "DQ failure: {message}"
  - type: emit_event
    asset_key: dq_incident_marker

# Then a second automation reacts to sustained DQ failures via compound:
---
name: dq_escalation_p1
when:
  - type: all_of
    within_seconds: 900
    triggers:
      - type: asset_check_failed
        check_names: [row_count_positive]
      - type: asset_check_failed
        check_names: [revenue_non_negative]
then:
  - type: opsgenie
    api_key_env_var: OPSGENIE_KEY
    priority: P1
```

## Existing sensor components as trigger sources

The registry has ~50 dedicated sensor components (`kafka_monitor`, `s3_monitor`, `eventhubs_monitor`, `redis_streams_sensor`, `mqtt_monitor`, `gcs_monitor`, `adls_monitor`, `pagerduty_incident_sensor`, `airbyte_sync_sensor`, `dbt_cloud_job_sensor`, `zendesk_ticket_sensor`, ...). They emit RunRequests when their target fires.

**Pattern:** point the dedicated sensor at an asset. Then use EventAutomation's `asset_materialized` trigger to react to that asset. This gives you the specialized event source + EventAutomation's alerting fanout in the same YAML surface.

```yaml
# The dedicated sensor materializes an asset when new S3 objects appear.
type: dagster_community_components.S3MonitorSensorComponent
attributes:
  bucket: my-bucket
  prefix: incoming/
  materialize_assets: [new_s3_object]
---
# EventAutomation reacts to the materialization + fans out to alerts + jobs.
type: dagster_community_components.EventAutomationComponent
attributes:
  name: on_new_s3_upload
  when:
    - type: asset_materialized
      asset_keys: [new_s3_object]
  then:
    - type: launch_job
      job_name: process_new_object
    - type: slack
      webhook_url_env_var: SLACK_URL
      message: "New S3 object at {asset_key}"
```

Not every event source needs a wrapper trigger inside EventAutomation — the existing sensor components ARE the right primitives. EventAutomation is the "declarative event fanout" layer on top.

## Overlap with Dagster+ paid features

Dagster+ Pro ships native Slack/PagerDuty/email alerting as a paid feature. This component intentionally duplicates the alerting surface so:

- **OSS Dagster** users get alert-on-failure without a Dagster+ Pro seat
- **Dagster+ Serverless** entry-tier users get the same
- **Prefect migrators** port their Automations 1:1 without changing plans

If you're already on Dagster+ Pro, the native notifications UI is more polished — one less moving piece in code. This component's value is the "same shape as Prefect Automations, works everywhere Dagster runs" story.

## Non-Prefect wins

- **Every automation appears in the Dagster+ UI** — sensors tab, run history, materialization events. Not a separate mini-app.
- **Materialize / launch_job actions emit real `RunRequest`s** — same execution path as any Dagster run, same lineage.
- **Cancel + retry actions call the SDK directly** — `instance.run_launcher.terminate()` + `instance.create_reexecuted_run()`.
- **Composable with the rest of your project** — dropped into `defs.yaml` alongside anything else, participates in the same UI.
