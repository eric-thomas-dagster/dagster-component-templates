# EventAutomationComponent

Prefect-Automations-style declarative event → action wiring in one YAML component. Each `when: … then: …` block becomes a real Dagster sensor / schedule under the covers; no Python needed for common trigger-action shapes.

## Why

Prefect's Automations page (`/v3/concepts/automations`) is a UI-first surface where any trigger → any action is composable, no code required. Dagster has all the underlying primitives — sensors, `AutomationCondition`, run-status sensors, freshness policies, asset checks — but they're Python-first. This component collapses the common trigger-action patterns into a single YAML shape that renders in the components UI, so ops-heavy teams can wire glue without dropping to Python.

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

- Multiple `when:` triggers → OR (any fires the automation)
- Multiple `then:` actions → all run when triggered (sequential, best-effort)
- Each trigger emits its own Dagster primitive (sensor / schedule); all share the same action bundle

## Triggers

### `run_status`

Fires when a Dagster run finishes with the given status. Optionally filter to a specific job.

```yaml
- type: run_status
  status: FAILURE          # SUCCESS | FAILURE | CANCELED | STARTED
  job_name: hourly_ingest  # optional; omit = any job
```

### `asset_materialized`

Fires when any of the named assets are materialized.

```yaml
- type: asset_materialized
  asset_keys: [raw_data, staging_data]
```

### `schedule`

Cron trigger. Also gives you a purely cron-driven "kick something off" shape in YAML without writing a schedule in Python.

```yaml
- type: schedule
  cron: "0 * * * *"
  execution_timezone: America/New_York   # default: UTC
```

### `http_poll`

Poll a URL and fire on one of three conditions:

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

### `freshness_violation`

Fire when any of the named assets haven't been materialized recently enough.

```yaml
- type: freshness_violation
  asset_keys: [hourly_summary]
  max_age_minutes: 60
  minimum_interval_seconds: 300
```

## Actions

Every action gets access to template tokens: `{event_type}`, `{run_id}`, `{job_name}`, `{asset_key}`, `{status}`, `{timestamp}`, `{message}`, `{url}`.

### `materialize`

Launch a materialization run.

```yaml
- type: materialize
  asset_keys: [derived_data]
  partition_key: "2024-01-15"    # optional
```

### `launch_job`

Launch a job.

```yaml
- type: launch_job
  job_name: cleanup
  tags: {priority: high}         # optional
```

### `webhook`

Arbitrary HTTP call with a templated body.

```yaml
- type: webhook
  url: "https://hooks.example.com/dagster"
  method: POST
  headers: {Content-Type: application/json}
  body_template: '{"event": "{event_type}", "at": "{timestamp}"}'
  timeout_seconds: 15
```

### `slack`

Slack incoming-webhook alert. The webhook URL is read from an env var so it stays out of your repo.

```yaml
- type: slack
  webhook_url_env_var: SLACK_WEBHOOK_URL
  message: "🚨 Prod failure on {job_name} (run_id={run_id})"
  channel: "#alerts"    # advanced Slack webhooks only
  username: "Dagster"   # optional
  icon_emoji: ":robot_face:"   # optional
```

### `pagerduty`

PagerDuty Events API v2. Dedup key coalesces repeat firings.

```yaml
- type: pagerduty
  routing_key_env_var: PAGERDUTY_ROUTING_KEY
  severity: error       # critical | error | warning | info
  summary_template: "Dagster: prod {job_name} failed"
  dedup_key_template: "prod-failure:{job_name}"
  event_action: trigger # trigger | acknowledge | resolve
```

### `discord`

Discord webhook alert.

```yaml
- type: discord
  webhook_url_env_var: DISCORD_WEBHOOK_URL
  message: "Dagster: {event_type} on {job_name}"
```

### `emit_event`

Emit a Dagster asset observation for downstream sensors to react to.

```yaml
- type: emit_event
  asset_key: automation_fired
  metadata_template:
    fired_by: "{event_type}"
    ts: "{timestamp}"
```

## Common shapes

### Alert-on-failure

```yaml
type: dagster_community_components.EventAutomationComponent
attributes:
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
      summary_template: "Prod {job_name} failed"
```

### Reprocess-on-upstream-change

```yaml
type: dagster_community_components.EventAutomationComponent
attributes:
  name: reprocess_on_raw_update
  when:
    - type: asset_materialized
      asset_keys: [raw_data]
  then:
    - type: materialize
      asset_keys: [derived_data, aggregated_data]
```

### Cron + webhook (heartbeat pattern)

```yaml
type: dagster_community_components.EventAutomationComponent
attributes:
  name: hourly_heartbeat
  when:
    - type: schedule
      cron: "0 * * * *"
  then:
    - type: webhook
      url: "https://uptime.example.com/hourly-heartbeat"
      method: GET
```

### Freshness → escalate

```yaml
type: dagster_community_components.EventAutomationComponent
attributes:
  name: stale_data_escalation
  when:
    - type: freshness_violation
      asset_keys: [hourly_summary]
      max_age_minutes: 90
  then:
    - type: pagerduty
      routing_key_env_var: PD_ROUTING_KEY
      severity: warning
      summary_template: "{asset_key} is stale — {message}"
    - type: materialize
      asset_keys: [hourly_summary]     # attempt self-heal
```

### External queue → job launch

```yaml
type: dagster_community_components.EventAutomationComponent
attributes:
  name: react_to_external_queue
  when:
    - type: http_poll
      url: "https://api.example.com/pending-batches"
      condition: json_path_present
      json_path: "pending"
      minimum_interval_seconds: 30
  then:
    - type: launch_job
      job_name: process_batch
      tags: {source: external_queue}
```

## Overlap with Dagster+ paid features

Dagster+ has native notifications / alerting as a paid feature. This component intentionally duplicates the alerting surface (Slack / PagerDuty / Discord / webhook) so teams on the OSS Dagster path can still get alert-on-failure without a Dagster+ Pro seat. If you're already on Dagster+ Pro, the native notifications UI is better — one less moving piece to keep in code.

## Non-Prefect wins

Under the hood every trigger is a real Dagster sensor / schedule, so:

- **Every automation appears in the Dagster+ UI** — sensors tab, schedules tab, run history, materialization events. Not a separate "automations" mini-app.
- **Materialize / launch_job actions emit real `RunRequest`s** — same execution path as any Dagster run, same lineage, same run history.
- **Composable with the rest of your project** — the emitted sensors and schedules can be referenced by name from other components / assets / selectors.
