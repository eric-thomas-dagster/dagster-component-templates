# HttpWebhookJobComponent

POST/GET to an HTTP endpoint as a job — heartbeats, status pings, fire-and-forget triggers.

## Dependencies
- `requests`

## What this is
A **Dagster job** (no asset materialized) — the component installs a `dg.job`
plus an optional schedule. Lineage graph stays clean; the work happens
behind the scenes.

## See also
- [Schema](schema.json) · [Example config](example.yaml)

## Reuse patterns

`http_webhook_job` is the right tool for any HTTP-triggered serverless
function or workflow — no need for cloud-specific trigger components:

### Azure Functions (HTTP trigger)

```yaml
attributes:
  job_name: invoke_my_func
  url: https://my-funcapp.azurewebsites.net/api/my-func
  method: POST
  headers:
    x-functions-key: "${AZURE_FUNC_KEY}"
  body:
    customer_id: "C100"
  schedule: "*/15 * * * *"
```

### Azure Logic Apps (HTTP trigger workflow)

```yaml
attributes:
  job_name: kick_off_logic_workflow
  url: https://prod-XX.eastus.logic.azure.com/workflows/...?api-version=2016-10-01&sp=...&sig=...
  method: POST
  body:
    event_type: order_placed
```

### AWS Lambda (Function URL)

```yaml
attributes:
  job_name: invoke_lambda
  url: https://abc123.lambda-url.us-east-1.on.aws/
  method: POST
  headers:
    Authorization: "Bearer ${LAMBDA_TOKEN}"
```

### GCP Cloud Functions (HTTP trigger)

```yaml
attributes:
  job_name: invoke_gcf
  url: https://us-central1-myproj.cloudfunctions.net/my-func
  method: POST
  headers:
    Authorization: "Bearer ${GCF_TOKEN}"
```

For more complex cloud-specific patterns (Lambda async invoke,
Functions durable functions, Logic Apps with managed identity), use the
respective cloud SDKs in a custom op instead of this generic job.
