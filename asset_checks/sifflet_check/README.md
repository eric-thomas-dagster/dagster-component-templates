# Sifflet Check

Triggers a Sifflet monitor run on demand via the Sifflet REST API (`https://{tenant}.siffletdata.com/api/v1`), polls for completion, and surfaces the result as a Dagster asset check.

Sifflet owns the monitor logic; Dagster triggers execution and surfaces pass/fail results — the same pattern as Monte Carlo, Soda, and similar tools.

## Required packages

```
requests>=2.28.0
```

## Fields

| Field | Required | Default | Description |
|---|---|---|---|
| `asset_key` | Yes | — | Asset key to attach this check to (e.g. `warehouse/orders`) |
| `sifflet_tenant_env_var` | Yes | — | Env var containing the Sifflet tenant name (e.g. `mycompany` from `mycompany.siffletdata.com`) |
| `api_token_env_var` | Yes | — | Env var containing the Sifflet API token |
| `monitor_id` | Yes | — | UUID of the Sifflet monitor to run |
| `severity` | No | `ERROR` | Asset check severity: `ERROR` or `WARN` |
| `poll_interval_seconds` | No | `5.0` | Seconds between status polls |
| `timeout_seconds` | No | `300` | Maximum seconds to wait for monitor run |

## Example YAML

```yaml
type: dagster_component_templates.SiffletCheckComponent
attributes:
  asset_key: warehouse/orders
  sifflet_tenant_env_var: SIFFLET_TENANT
  api_token_env_var: SIFFLET_API_TOKEN
  monitor_id: "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
  severity: ERROR
  timeout_seconds: 300
```

## Finding the monitor ID

In the Sifflet UI: **Monitor → Details → ID**.
