# DagsterComputeLogsArchiveJobComponent

Op-shaped job that gzips + archives old Dagster compute log files
(stdout/stderr per step) from primary storage to cheap cold storage
(S3, GCS, or local), then optionally deletes the originals.

Compute logs are typically the largest single storage bill in a
long-running Dagster deployment. Archiving old ones frees primary
storage without losing the audit trail.

## Behavior

1. Walks `source_dir/compute_logs/*` (default: `$DAGSTER_HOME/storage/compute_logs/*`).
2. Filters to files older than `min_age_days`.
3. For each candidate: gzips → uploads to configured target (local dir / S3 bucket / GCS bucket) → optionally deletes the original.

## YAML example

```yaml
type: dagster_component_templates.DagsterComputeLogsArchiveJobComponent
attributes:
  job_name: archive_dagster_compute_logs
  schedule: "0 6 * * *"                # daily at 6am
  default_status: STOPPED
  source_dir: ""                       # empty = $DAGSTER_HOME/storage
  min_age_days: 7
  archive_target: s3                   # local | s3 | gcs
  archive_bucket: acme-dagster-logs-cold
  archive_prefix: dagster-compute-logs-archive/
  delete_after_archive: false          # safer default
  max_files_per_run: 1000
  dry_run: true
```

## Options

- `archive_target` — `local` (needs `archive_local_dir`), `s3` (needs `archive_bucket`, uses boto3), or `gcs` (needs `archive_bucket`, uses google-cloud-storage).
- `min_age_days` — only archive files older than this.
- `delete_after_archive` — default False (safer). Flip on once you trust the flow.
- `max_files_per_run` — per-tick cap.
- `dry_run` — default True.

## Required deps

- `boto3` if `archive_target=s3`
- `google-cloud-storage` if `archive_target=gcs`
