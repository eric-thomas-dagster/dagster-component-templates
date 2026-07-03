# temporal_workflow_sensor

Poll Temporal for terminal workflow status via the `temporalio` SDK. On terminal success, fire a Dagster job and (optionally) emit an `AssetMaterialization` or `AssetObservation` on a paired external asset.

## Pairing modes

- **Case A — Temporal owns the schedule.** Pair with [`external_temporal_workflow`](../../external_assets/external_temporal_workflow). Sensor watches for the workflow's terminal state and materializes the external asset each time.
- **Case B — Dagster owns the schedule.** Pair with [`temporal_workflow_trigger`](../../assets/infrastructure/temporal_workflow_trigger) using `wait_for_result: false`. Set `asset_event_type: observation` on the sensor to avoid double-counting materializations on the same asset.

Do not use both `external_temporal_workflow` and `temporal_workflow_trigger` on the same `asset_key` — they represent different schedule ownership.

## Filter modes

Exactly one of:
- `workflow_id` — a specific workflow ID (Dagster started it or the ID is fixed per Schedule).
- `list_filter` — a Temporal Visibility SQL-ish filter; sensor picks the most-recently-closed match.

```yaml
type: dagster_community_components.TemporalWorkflowSensorComponent
attributes:
  sensor_name: temporal_nightly_done
  list_filter: "WorkflowType='NightlyAggregationWorkflow' AND ExecutionStatus!='Running'"
  job_name: downstream_processing_job
  asset_key: temporal/etl/nightly_aggregation
```

## Fields

| Field | Type | Description |
|---|---|---|
| `sensor_name` | string | Unique sensor name. |
| `workflow_id` | string | Specific workflow ID. Mutually exclusive with `list_filter`. |
| `list_filter` | string | Temporal Visibility filter. Mutually exclusive with `workflow_id`. |
| `job_name` | string | Dagster job fired on terminal success. |
| `asset_key` | string | Optional. Fires an AssetMaterialization or AssetObservation on this key. |
| `asset_event_type` | string | `materialization` or `observation`. |
| `target_host` | string | Temporal frontend host:port. Default `localhost:7233`. |
| `namespace` | string | Temporal namespace. Default `default`. |
| `api_key_env_var` / `tls_cert_env_var` / `tls_key_env_var` / `tls_server_root_ca_env_var` | string | Auth env vars for Temporal Cloud / self-hosted mTLS. |
| `minimum_interval_seconds` | int | Poll cadence. Default 60. |
| `default_status` | string | `running` or `stopped`. |

## Terminal status handling

- `COMPLETED`, `CONTINUED_AS_NEW` → success (fire job + optional asset event, advance cursor).
- `FAILED`, `CANCELED`, `TERMINATED`, `TIMED_OUT` → surfaced as skip_reason with the status (no job fire).
- `RUNNING` → skip_reason (poll again next tick).

The cursor is `<workflow_id>:<run_id>`, so re-runs of the same workflow_id (or new runs matching the same list filter) are not double-fired.

## Requirements

```
temporalio>=1.7.0
```
