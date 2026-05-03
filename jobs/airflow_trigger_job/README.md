# AirflowTriggerJobComponent

Trigger an Airflow DAG run via the Airflow REST API as a Dagster job.

## Dependencies
- `requests`

## What this is
A **Dagster job** (no asset materialized) — the component installs a `dg.job`
plus an optional schedule. Lineage graph stays clean; the work happens
behind the scenes.

## See also
- [Schema](schema.json) · [Example config](example.yaml)
