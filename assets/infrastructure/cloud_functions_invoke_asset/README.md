# Cloud Functions Invoke

Invoke a deployed Cloud Function (gen 1 / gen 2) or any Cloud Run-deployed function over HTTPS with automatic OIDC authentication. Two modes:

**Static call** — single request, returns the function's response:

```yaml
type: dagster_component_templates.CloudFunctionsInvokeAssetComponent
attributes:
  asset_name: notify_pipeline_done
  credentials_path: ${GOOGLE_APPLICATION_CREDENTIALS}
  function_url: https://us-central1-my-project.cloudfunctions.net/notify
  method: POST
  payload:
    event: pipeline_done
    severity: info
```

**Per-row** — one call per upstream DataFrame row, returns upstream rows + response columns:

```yaml
attributes:
  asset_name: enriched_via_function
  upstream_asset_key: events_to_enrich
  function_url: https://us-central1-my-project.cloudfunctions.net/enrich
  payload_template:
    event_id: "{id}"
    severity: "{level}"
```

`{column}` placeholders in `payload_template` values get replaced per-row before the call. For one-column-per-row payloads, use `payload_column` instead.

## Cloud Run services (HTTPS-invoked)

This component works unchanged against any Cloud Run **service** URL — Cloud Functions Gen 2 *is* Cloud Run under the hood, and the OIDC auth flow is identical. Just use the Cloud Run service URL as `function_url`:

```yaml
function_url: https://my-service-abc123-uc.a.run.app/process
```

Use `cloud_run_job_trigger_asset` for Cloud Run **jobs** (long-running batch), not HTTPS services.

## OIDC auth (private functions)

The component automatically mints an ID token with the function URL as the `target_audience` and sends it in the `Authorization` header. For this to work the SA needs:

- `roles/cloudfunctions.invoker` (gen 1) or `roles/run.invoker` (gen 2 / Cloud Run-deployed)
- The function set to **require authentication** (the default for new deployments)

Public / `--allow-unauthenticated` functions work too — the component still sends the OIDC header, the function just ignores it.

## Sister components

- `cloud_run_job_trigger_asset` — trigger long-running Cloud Run jobs (similar pattern, different runtime).
- `http_external_asset` — generic HTTP-driven external job runner (not auto-OIDC; uses any auth resource).
