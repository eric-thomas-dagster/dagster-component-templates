# sentry_alert_job

Fire a Sentry event on a schedule (or trigger manually) — no asset materialized. Matches the pattern of `http_webhook_job`, `observability_heartbeat_job`.

## Fields

- `job_name` — Dagster job name.
- `schedule` — cron string. `None` = no schedule; still triggerable manually.
- `default_status` — `STOPPED` or `RUNNING`.
- `tags` — Dagster job tags (not Sentry event tags — see `sentry_tags`).

**Sentry event:**
- `dsn_env_var` — env var holding the DSN (default `SENTRY_DSN`).
- `message` — event message. `{now}` is replaced with the UTC ISO timestamp before send.
- `level` — `debug|info|warning|error|fatal` (default `info`).
- `environment` / `release` — standard Sentry tags applied to the event.
- `sentry_tags` — Sentry-searchable tag pairs (e.g. `{system: dagster, job: nightly_etl_heartbeat}`).
- `sentry_extras` — non-indexed context (visible on event detail page).
- `fingerprint` — controls Sentry grouping.
- `fail_on_error` — when `False`, Sentry-send failures log a warning rather than failing the job. Good for heartbeats that must not break the schedule if Sentry is temporarily down.

## Common patterns

**Nightly ETL heartbeat** — a job that fires an event every 3 AM proving the scheduler is alive. If Sentry sees the event go missing, on-call gets paged.

**Release notification** — trigger on demand (or on deploy hooks) with `level: info`, `sentry_tags.deploy: {sha}`, `release: {version}`.

**Job-completion signal** — chain from a data pipeline's terminal asset to a follow-up job that reports success into the on-call Sentry inbox.

## Related

- `dataframe_to_sentry` — send events from a DataFrame (asset sink, per-row)
- `sentry_issues_ingestion` — read issues FROM Sentry
- `http_webhook_job` — same shape, but generic webhook (Slack / PagerDuty / anywhere)
- `observability_heartbeat_job` — heartbeat pings for arbitrary observability endpoints
