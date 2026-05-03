# AdlsCleanupJobComponent

Delete Azure ADLS Gen2 / Blob objects older than N days under a prefix.

## Dependencies
- `azure-storage-blob`
- `azure-identity`

## What this is
A **Dagster job** (no asset materialized) — the component installs a `dg.job`
plus an optional schedule. Lineage graph stays clean; the work happens
behind the scenes.

## See also
- [Schema](schema.json) · [Example config](example.yaml)
