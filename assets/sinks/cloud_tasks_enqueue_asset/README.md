# Cloud Tasks Enqueue Asset

Push one [Cloud Tasks](https://cloud.google.com/tasks) HTTP task per row of an upstream DataFrame onto a queue. The queue's worker (Cloud Run, Cloud Function, etc.) then processes them asynchronously — Dagster doesn't wait.

```yaml
type: dagster_component_templates.CloudTasksEnqueueAssetComponent
attributes:
  asset_name: tasks_enqueued
  upstream_asset_key: pending_jobs
  location: us-central1
  queue_id: my-worker-queue
  target_url: https://my-worker-abc-uc.a.run.app/process
  body_columns: [job_id, payload]
  oidc_service_account_email: worker-invoker@my-project.iam.gserviceaccount.com
```

## Why use it

| Pattern | Win |
|---|---|
| Fan-out N work items without blocking the asset on N workers | Dagster materializes fast; workers run async |
| Deferred work ("send reminder at T+24h") | Use `schedule_time_column` |
| Throttle outbound API calls | Queue-level rate limiting handles it |
| Built-in retries | Cloud Tasks retries on 4xx/5xx per queue config |

## URL templating

`target_url` supports `{column}` placeholders. Example:
```yaml
target_url: https://worker.example.com/orders/{order_id}/process
```
Each row's `order_id` value is substituted in.

## Auth to the worker

For private Cloud Run / Cloud Function endpoints, set `oidc_service_account_email` — Cloud Tasks mints an OIDC token bearer header automatically. The configured SA needs `roles/run.invoker` (or equivalent) on the worker.

## Auth for this component

Service account needs `roles/cloudtasks.enqueuer` on the queue (or `roles/cloudtasks.taskRunner` for broader access).

## Sister components

- `pubsub_publish_asset` — fan-out via Pub/Sub instead (no built-in retries, but lower latency and higher throughput)
- `cloud_run_job_trigger_asset` — kick off a Cloud Run **Job** (not a service) once
- `cloud_functions_invoke_asset` — synchronous Cloud Function invocation
