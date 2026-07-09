# sentry_monitor_checkin_job

Register a Dagster scheduled job as a Sentry Cron Monitor. On each run, fire an `in_progress` check-in at start and an `ok` check-in at end. If Sentry doesn't see the expected check-in within the schedule window + `checkin_margin`, it auto-alerts on-call.

Docs: <https://docs.sentry.io/product/crons/getting-started/http/>

## Why

Two orchestration failure modes cause silent outages:
1. The schedule doesn't fire (worker down, code server unhealthy, ADI paused).
2. The schedule fires but the job takes forever (external API stalls).

Sentry Cron Monitoring catches both. Configure once, get paged when the cadence breaks — no per-alert wiring in Dagster+.

## Fields

- `job_name` / `schedule` / `default_status` / `tags` — standard Dagster job knobs.
- `dsn_env_var` — env var holding the Sentry DSN (default `SENTRY_DSN`).
- `monitor_slug` — the stable identifier for this monitor in Sentry (e.g. `nightly-etl`).
- `environment` / `release` — Sentry tags applied to the check-in.
- `monitor_config` — optional dict that auto-creates the monitor on first check-in:
  ```yaml
  monitor_config:
    schedule: { type: crontab, value: "0 3 * * *" }
    checkin_margin: 5    # minutes late before alerting
    max_runtime: 15      # minutes runtime before considered failed
    timezone: America/New_York
  ```
- `fail_on_error` — default `False`. Sentry outages shouldn't make the scheduler flap.

## Behavior

On each run:
1. Init the SDK from `SENTRY_DSN` + environment + release.
2. `capture_checkin(monitor_slug, status='in_progress', monitor_config=...)` — Sentry auto-registers/updates the monitor if `monitor_config` is set.
3. `capture_checkin(monitor_slug, check_in_id, status='ok')`.
4. Flush the SDK.

If the Sentry SDK raises and `fail_on_error=False`, the job logs a warning but does NOT fail — this is intentional so a Sentry-side outage doesn't break your scheduler.

## Related

- `sentry_alert_job` — fire a Sentry EVENT (message) on schedule; different pattern (alerts inbox, not cron monitor).
- `dataframe_to_sentry` — pipe DataFrame rows into Sentry as events.
- `sentry_issues_ingestion` — read issues FROM Sentry for warehouse analytics.
- `http_webhook_job` — generic webhook pattern; use when you don't need Sentry's cron-monitor semantics.
