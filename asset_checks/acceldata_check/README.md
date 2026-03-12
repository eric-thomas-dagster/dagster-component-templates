# Acceldata Check

Triggers an Acceldata data quality rule execution on demand and surfaces the result as a Dagster asset check.

Acceldata owns the rule logic — Dagster triggers execution and surfaces pass/fail/warn, exactly like the Soda, Great Expectations, Monte Carlo, and Sifflet integrations.

## How It Works

1. `POST /api/v1/rules/{rule_id}/execute` — triggers the rule run, returns an `executionId`
2. Polls `GET /api/v1/rules/{rule_id}/executions/{executionId}` until `status` is terminal
3. Returns `AssetCheckResult(passed=True/False, metadata={...})`

## Configuration

| Field | Required | Description |
|-------|----------|-------------|
| `asset_key` | Yes | Dagster asset key to attach this check to (e.g. `warehouse/orders`) |
| `api_url_env_var` | Yes | Env var with Acceldata API base URL (e.g. `https://mycompany.acceldata.io`) |
| `api_token_env_var` | Yes | Env var with Acceldata API token |
| `rule_id` | Yes | ID of the Acceldata DQ rule to execute |
| `poll_interval_seconds` | No | Seconds between status polls (default: 5.0) |
| `timeout_seconds` | No | Max seconds to wait for execution (default: 300) |
| `severity` | No | `ERROR` or `WARN` (default: `ERROR`) |

## Finding Your Rule ID

In the Acceldata UI: **Rules → Select Rule → Rule Details → ID**

## Example

```yaml
type: dagster_component_templates.AcceldataCheckComponent
attributes:
  asset_key: warehouse/orders
  api_url_env_var: ACCELDATA_API_URL
  api_token_env_var: ACCELDATA_API_TOKEN
  rule_id: "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
  severity: ERROR
```

## Environment Variables

```bash
export ACCELDATA_API_URL=https://mycompany.acceldata.io
export ACCELDATA_API_TOKEN=your-api-token
```

## Dependencies

```
requests>=2.28.0
```
