# Dataplex Data-Quality Scan Results Asset

Pull the latest (or historical) results from a [Dataplex Data-Quality scan](https://cloud.google.com/dataplex/docs/auto-data-quality-overview) into a pandas DataFrame.

Dataplex DQ scans are managed, BigQuery-native rule evaluations — uniqueness, null-rate, regex matches, custom SQL, range stats. This component **observes** their results in Dagster so:

- DQ failures show up in your catalog alongside the data
- Downstream pipelines can gate on a check wrapping this asset
- Historical DQ trends are queryable from a single source

```yaml
type: dagster_component_templates.DataplexDqScanResultsAssetComponent
attributes:
  asset_name: orders_dq_results
  project_id: my-project
  location: us-central1
  scan_id: orders-table-dq-scan
  mode: latest
```

## Observe, don't trigger

This component **does not trigger** new scans — it reads results from whatever scans Dataplex has already run on its own schedule. To trigger:

```bash
gcloud dataplex datascans run <scan_id> --location=<region>
```

Or invoke the [Data Scans REST API](https://cloud.google.com/dataplex/docs/reference/rest/v1/projects.locations.dataScans/run) from a Cloud Function / Cloud Tasks / Dagster sensor.

## Two modes

| Mode | What it returns |
|---|---|
| `latest` (default) | One scan_job — the most recent. One row per (job × rule). |
| `history` | All jobs since `since_iso`, up to `max_jobs`. One row per (job × rule). |

## Output shape

| Column | Meaning |
|---|---|
| `job_name` | Full Dataplex job resource id |
| `job_state` | `SUCCEEDED`, `FAILED`, `CANCELLED`, etc. |
| `start_time` / `end_time` | Job timing |
| `passed` | Whole-scan pass/fail |
| `row_count` | Rows scanned |
| `rule_passed` | Per-rule pass/fail |
| `rule_name` / `rule_dimension` / `rule_column` | Rule metadata |
| `evaluated_count` / `passing_count` | Rule pass-rate inputs |
| `failing_rows_query` | BQ SQL you can run to see the actual failing rows |

## Gating downstream pipelines

Wrap this asset in a `pandas_dataframe_check` (or write a small custom `@asset_check`) that fails when `failed_rules > 0`. Set `blocking: true` to halt the pipeline.

## Auth

Service account needs `roles/dataplex.dataScanViewer` on the scan resource.

## Sister components

- `pandas_dataframe_check` — generic shape check (post-materialize, in-Dagster)
- `great_expectations_check` — full GE suites in-Dagster (vs. Dataplex's managed approach)
- `bigquery_query_asset` — pull DQ-style queries you wrote yourself
- `cloud_logging_query_asset` — pull DQ-job audit logs for cross-team reporting
