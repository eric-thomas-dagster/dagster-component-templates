# PostgresBackupJobComponent

Run pg_dump → upload to S3/GCS/local — backup as a job, no asset materialized.

## Dependencies
- `boto3`

## What this is
A **Dagster job** (no asset materialized) — the component installs a `dg.job`
plus an optional schedule. Lineage graph stays clean; the work happens
behind the scenes.

## See also
- [Schema](schema.json) · [Example config](example.yaml)
