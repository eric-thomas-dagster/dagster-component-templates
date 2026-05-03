# SiemAuditExportJobComponent

Compound op job: pull audit logs from a SaaS source, normalize to OCSF/ECS, ship to a SIEM — all in one YAML config.

## Dependencies
- `pandas`
- `requests`

## What this is
A **Dagster job** (no asset materialized) — the component installs a `dg.job`
plus an optional schedule. Lineage graph stays clean; the work happens
behind the scenes.

## See also
- [Schema](schema.json) · [Example config](example.yaml)
