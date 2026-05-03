# S3CleanupJobComponent

Delete S3 objects older than N days under a prefix — cost control + compliance retention.

## Dependencies
- `boto3`

## What this is
A **Dagster job** (no asset materialized) — the component installs a `dg.job`
plus an optional schedule. Lineage graph stays clean; the work happens
behind the scenes.

## See also
- [Schema](schema.json) · [Example config](example.yaml)
