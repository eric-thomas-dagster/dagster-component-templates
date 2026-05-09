# Cloud Run Job Trigger

Trigger a single deployed Cloud Run job, wait for it to finish, surface the execution result + log link as Dagster metadata. Same shape as `matillion_run_asset` / `rivery_run_asset` / `precisely_run_asset`.

```yaml
type: dagster_component_templates.CloudRunJobTriggerAssetComponent
attributes:
  asset_name: nightly_etl_run
  credentials_path: ${GOOGLE_APPLICATION_CREDENTIALS}
  job_name: nightly-etl
  location: us-central1
  container_overrides:
    - name: main
      env:
        - { name: RUN_DATE, value: "2026-05-08" }
        - { name: TARGET,   value: prod }
  task_count: 1
  poll_interval_seconds: 5
  timeout_seconds: 3600
```

## When to use this vs `google_cloud_run_jobs`

- **`cloud_run_job_trigger_asset`** (this) — trigger ONE specific job as ONE Dagster asset, with optional per-execution overrides. Use when the job has a clear orchestration role in your pipeline.
- **`google_cloud_run_jobs`** (integration) — batch IMPORT every Cloud Run job in a project as Dagster assets, no overrides. Use for catalog discovery / "everything as an asset."

## Required SA roles

`roles/run.invoker` on the job + `roles/iam.serviceAccountUser` on the runtime SA the job uses + Cloud Run Admin API enabled.

## Behavior on failure

If the resulting Execution has `failed_count > 0` and `succeeded_count == 0`, the asset raises `RuntimeError` with the log URI in the message. Mixed results (some tasks failed, some succeeded) materialize the asset and surface counts in metadata for downstream alerting.

## Sister components

- `google_cloud_run_jobs` — multi-asset import.
- `cloud_functions_invoke_asset` — invoke a Cloud Function.
- `http_external_asset` — generic HTTP-driven external job runner.
