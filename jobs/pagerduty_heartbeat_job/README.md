# PagerdutyHeartbeatJobComponent

Send a PagerDuty heartbeat (resolve event) — confirms a job is alive on a cadence.

## Dependencies
- `requests`

## What this is
A **Dagster job** (no asset materialized) — the component installs a `dg.job`
plus an optional schedule. Lineage graph stays clean; the work happens
behind the scenes.

## See also
- [Schema](schema.json) · [Example config](example.yaml)
