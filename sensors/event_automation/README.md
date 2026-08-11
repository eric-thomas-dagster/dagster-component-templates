# EventAutomationComponent

Prefect-Automations-style declarative event → action wiring in one YAML component. Each `when: … then: …` block becomes a real Dagster sensor under the covers; no Python needed for common trigger-action shapes.

**Surface: 16 trigger types + 17 action types + AND/OR compound composition (one level of nesting).**

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

## Triggers (11)

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

**`log_pattern`** — regex match on run log lines. Catches issues that a raw run_status doesn't (runs that "succeed" with warnings, OOMs, or specific stack traces).

```yaml
- type: log_pattern
  pattern: "OOMKilled|OutOfMemoryError|MemoryError"
  job_name: prod_ingest           # optional
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

## Actions (17)

Every action gets template tokens: `{event_type}`, `{run_id}`, `{job_name}`, `{asset_key}`, `{status}`, `{timestamp}`, `{message}`, `{url}`.

### Dagster runs

**`materialize`** — launch a materialization run.

```yaml
- type: materialize
  asset_keys: [derived_data]
  partition_key: "2024-01-15"
```

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
