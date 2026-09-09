# DagsterStalePartitionCleanupJobComponent

Op-shaped job that detects orphaned partition materialization events for
assets whose `PartitionsDefinition` has been re-shaped (renamed keys,
removed keys, migrated from static to dynamic, etc.). Reports counts by
default; optionally wipes the entire asset's history when a threshold is
exceeded.

## Motivation

When you change an asset's partition scheme, old materialization events
in the event log remain tied to defunct partition keys. They clutter
the partition-status heat map and inflate storage. This job detects
them.

## Behavior

1. For each partitioned asset (all, or scoped via `asset_keys`):
   - Enumerates the current valid partition keys via `partitions_def.get_partition_keys()`.
   - Fetches the historical set from `instance.get_materialized_partitions(key)` (falls back to full event scan if not available).
   - Computes `stale = historical - valid`.
2. If `stale` is nonempty:
   - Always reports counts + sample.
   - If `wipe_on_detection=True` AND `len(stale) >= max_stale_before_wipe`, wipes the entire asset via `instance.wipe_assets([key])`.

## Cleanup caveat

Dagster doesn't expose per-partition-event deletion. The only cleanup
option is wiping the ENTIRE asset's history and letting future runs
re-materialize. This is destructive; default is report-only. Enable
`wipe_on_detection` only when you understand the tradeoff.

## YAML example

```yaml
type: dagster_component_templates.DagsterStalePartitionCleanupJobComponent
attributes:
  job_name: detect_stale_partitions
  schedule: "0 6 * * 0"
  default_status: STOPPED
  asset_keys: []                   # [] = all partitioned assets
  wipe_on_detection: false         # report-only by default
  max_stale_before_wipe: 100
```
