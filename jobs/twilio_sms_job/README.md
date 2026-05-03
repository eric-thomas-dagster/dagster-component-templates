# TwilioSmsJobComponent

Send an SMS via Twilio as a job — useful for on-call paging without a full PagerDuty stack.

## Dependencies
- `twilio`

## What this is
A **Dagster job** (no asset materialized) — the component installs a `dg.job`
plus an optional schedule. Lineage graph stays clean; the work happens
behind the scenes.

## See also
- [Schema](schema.json) · [Example config](example.yaml)
