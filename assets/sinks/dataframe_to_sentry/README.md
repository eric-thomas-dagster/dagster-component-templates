# dataframe_to_sentry

Send each row of an upstream DataFrame to Sentry as an event. Uses the official `sentry-sdk` — supports levels, searchable tags, extras (non-indexed context), and fingerprints (grouping key).

## Killer pattern: Dagster run failures → Sentry

Chain `dagster_plus_run_events_ingestion` (or `dagster_plus_asset_events_ingestion`) through a `filter` (status == 'FAILURE'), then into `dataframe_to_sentry`. Your on-call now sees Dagster ETL failures alongside application errors in one Sentry inbox — with the run_id / asset_key as tags for triage.

```yaml
# Ingest failure events from Dagster+
type: dagster_community_components.DagsterPlusRunEventsIngestionComponent
attributes:
  asset_name: dagster_run_events
  since_hours: 24

# Filter to failures only
---
type: dagster_community_components.FilterComponent
attributes:
  asset_name: dagster_failures
  upstream_asset_key: dagster_run_events
  condition: "status == 'FAILURE'"

# Forward to Sentry
---
type: dagster_community_components.DataframeToSentryComponent
attributes:
  asset_name: sentry_dagster_failures
  upstream_asset_key: dagster_failures
  dsn_env_var: SENTRY_DSN
  environment: prod
  release: dagster-etl
  message_column: failure_reason
  default_level: error
  tag_columns: [run_id, asset_key, job_name, code_location]
  extra_columns: [stack_trace, started_at, ended_at]
  fingerprint_columns: [asset_key, job_name]
```

## Fields

- `asset_name` — output asset name (this asset is a sink; returns `None`).
- `upstream_asset_key` — the DataFrame to forward.
- `dsn_env_var` — env var holding the Sentry DSN. Default `SENTRY_DSN`.
- `environment` / `release` — standard Sentry tags applied to every event.
- `message_column` — column whose value becomes the event message.
- `level_column` — column supplying per-row level (`debug|info|warning|error|fatal`); rows with an unknown value fall back to `default_level`.
- `default_level` — default event level.
- `tag_columns` — columns attached as searchable tags in Sentry.
- `extra_columns` — columns attached as extra context (not indexed, but visible on the event).
- `fingerprint_columns` — combined into the event fingerprint to control grouping (default is Sentry's auto-grouping).

## Output

Sink returns `None`. Metadata includes rows sent, rows skipped (missing message), environment/release, and DSN prefix.

## Related

- `sentry_issues_ingestion` — read issues FROM Sentry
- `dagster_plus_run_events_ingestion` / `dagster_plus_asset_events_ingestion` — the natural upstreams
- `audit_logs_to_splunk` / `audit_logs_to_datadog_logs` — same pattern for other observability platforms
