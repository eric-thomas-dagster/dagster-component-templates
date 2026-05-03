# DagsterPlusToSiemJobComponent

Compound op job: pull Dagster+ events (audit-log/runs/asset-events) → optionally normalize → ship to SIEM (Splunk/Sentinel/Datadog/Sumo/S3) on cron.

## What this builds
A Dagster `dg.job` (no asset materialized) that runs three ops on cron:

1. **fetch** — calls Dagster+ GraphQL with the chosen `event_type`
2. **normalize** — maps to OCSF or ECS schema (or pass-through)
3. **ship** — POSTs to the chosen SIEM

All in one YAML — no glue Python.

## Validate the GraphQL query
Default queries are best-guess. Inspect your Dagster+ GraphQL playground and
override via `query` + `result_path` if field names differ.

## Sinks supported
splunk | sentinel | datadog_logs | sumo_logic | s3

For Splunk: `sink_config.hec_url`, `hec_token_env`, optional `index`/`sourcetype`.
For Sentinel: `sink_config.workspace_id`, `workspace_key_env`, optional `log_type`.
For Datadog: `sink_config.api_key_env`, optional `site`/`service`.
For Sumo: `sink_config.collector_url_env`.
For S3: `sink_config.bucket`, optional `prefix`/`region_name`.
