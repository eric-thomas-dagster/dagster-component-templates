# PythonCallableJobComponent

Run an arbitrary Python callable as a Dagster job — Airflow PythonOperator equivalent.

## Dependencies
(none)

## What this is
A **Dagster job** (no asset materialized) — the component installs a `dg.job`
plus an optional schedule. Lineage graph stays clean; the work happens
behind the scenes.

## See also
- [Schema](schema.json) · [Example config](example.yaml)
