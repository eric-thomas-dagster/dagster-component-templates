# QlikReplicateTaskMetricsIngestionComponent

Materialize per-task metrics from **Qlik Replicate** (via Enterprise Manager REST API) as a DataFrame asset. Each materialization polls the API and emits one row per task with its current state, latency, throughput, and error count.

Pairs with [`qlik_replicate_resource`](../../../resources/qlik_replicate_resource/) for auth.

## When to use

- **CDC observability dashboards** — feed this into your BI tool to visualize replication latency and throughput per source system.
- **Alert-input assets** — chain this to `dataframe_to_pagerduty` or `dataframe_to_slack_message` to alert when any task's `error_count > 0` or `cdc_latency_seconds > threshold`.
- **SLO tracking** — compare `cdc_latency_seconds` against source-of-truth commit rates to enforce CDC latency SLOs.

## Emitted schema

One row per task per materialization. Columns:

| Column | Type | Notes |
|---|---|---|
| `server` | string | Server name as registered in Enterprise Manager |
| `task` | string | Replicate task name |
| `state` | string | RUNNING / STOPPED / ERROR / STARTING / STOPPING |
| `stage` | string | current stage (FULL_LOAD / CDC / ...) |
| `full_load_progress_percent` | float | 0–100 during full-load; null once in CDC |
| `cdc_latency_seconds` | float | latency between source commit and target apply |
| `source_transactions_committed` | int | count |
| `target_transactions_applied` | int | count |
| `source_records_processed` | int | count |
| `target_records_applied` | int | count |
| `error_count` | int | non-null even at zero |
| `last_state_change_time` | string | ISO-8601 |
| `polled_at` | float | unix epoch seconds when we polled |

## Example — poll every task on two Replicate servers

```yaml
type: dagster_community_components.QlikReplicateTaskMetricsIngestionComponent
attributes:
  asset_key: qlik_task_metrics
  servers: [prod-replicate-01, prod-replicate-02]
  group_name: qlik_observability
  resource_key: qlik_replicate_resource
```

## Example — narrow the whitelist to specific tasks

```yaml
attributes:
  asset_key: order_pipeline_metrics
  servers: [prod-replicate-01]
  tasks:
    - orders_sqlserver_to_snowflake
    - customers_db2_to_snowflake
  resource_key: qlik_replicate_resource
```

## Pairing with automation

Combine with an `automation_condition` on the asset (e.g. every 5 minutes) so the metrics table updates on a fixed cadence:

```yaml
# In a separate defs.yaml or overlay
schedules:
  - job_name: refresh_qlik_metrics
    cron: "*/5 * * * *"
```

Or use a schedule-based partitioned asset if you want to keep historical rows in the asset itself. For simple current-state monitoring, non-partitioned is fine.

## Related

- [`qlik_replicate_resource`](../../../resources/qlik_replicate_resource/) — shared auth
- [`qlik_replicate_task_trigger_job`](../../../jobs/qlik_replicate_task_trigger_job/) — imperative task control (start/stop/reload)
- [`qlik_replicate_task_status_sensor`](../../../sensors/qlik_replicate_task_status_sensor/) — event-drive on state transitions
- [`dataframe_to_pagerduty`](../../sinks/dataframe_to_pagerduty/) — chain metrics → alerts on `error_count > 0` rows
