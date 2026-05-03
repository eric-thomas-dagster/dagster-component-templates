# ObservabilityHeartbeatJobComponent

Compound op job: send a heartbeat to multiple destinations (Slack + PagerDuty + HTTP) on cron — single YAML config.

## Dependencies
- `requests`

## What this is
A **Dagster job** (no asset materialized) — the component installs a `dg.job`
plus an optional schedule. Lineage graph stays clean; the work happens
behind the scenes.

## See also
- [Schema](schema.json) · [Example config](example.yaml)
