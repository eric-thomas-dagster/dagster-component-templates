# EventLogToS3JobComponent

Op-shaped job that streams new Dagster event log entries to S3 as
gzipped JSONL batches, partitioned by hour. Useful for cheap event
archival, cold-storage compliance, or downstream ingestion into a
data lake.

## Behavior

1. Reads the last processed `storage_id` from a run tag set on the
   prior successful run. First run pulls `initial_lookback_hours` of history.
2. Queries new events for the target types (`PIPELINE_START/SUCCESS/FAILURE`,
   `STEP_FAILURE`, `ASSET_MATERIALIZATION`, `ASSET_OBSERVATION`, `ASSET_CHECK_EVALUATION`).
3. Groups events by hour → one gzipped JSONL object per hour bucket
   under `s3://<bucket>/<key_prefix>/YYYY-MM-DD/HH/events-<ts>-<runid>.jsonl.gz`.
4. Tags the run with the new max storage_id as cursor.

## Schema per event (JSONL row)

```json
{"storage_id": 12345, "timestamp": 1725888000.123, "run_id": "abc...",
 "job_name": "my_job", "event_type": "ASSET_MATERIALIZATION",
 "step_key": "my_asset", "asset_key": "analytics/orders",
 "message": "Materialized value analytics/orders."}
```

## YAML example

```yaml
type: dagster_component_templates.EventLogToS3JobComponent
attributes:
  job_name: dagster_events_to_s3
  schedule: "*/15 * * * *"
  default_status: RUNNING
  bucket: acme-dagster-events
  key_prefix: dagster-events/
  aws_region: us-east-1
  initial_lookback_hours: 24
```

## Required env / IAM

- AWS credentials via the standard boto3 chain (env, `~/.aws/`, IAM role, etc.)
- The chosen S3 bucket must allow `PutObject` for your credentials
