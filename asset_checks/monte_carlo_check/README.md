# Monte Carlo Check

Triggers a Monte Carlo Custom SQL Rule or Field Health monitor on demand via the [pycarlo](https://github.com/monte-carlo-data/pycarlo) SDK, polls for completion, and surfaces the result as a Dagster asset check.

Monte Carlo owns the rule logic; Dagster triggers execution and surfaces pass/fail results — the same pattern as Soda, Great Expectations, and similar tools.

## Required packages

```
pycarlo
```

## Fields

| Field | Required | Default | Description |
|---|---|---|---|
| `asset_key` | Yes | — | Asset key to attach this check to (e.g. `warehouse/orders`) |
| `mcd_id_env_var` | Yes | — | Env var containing the Monte Carlo API key ID |
| `mcd_token_env_var` | Yes | — | Env var containing the Monte Carlo API token |
| `rule_uuid` | Yes | — | UUID of the Monte Carlo Custom SQL Rule to execute |
| `severity` | No | `ERROR` | Asset check severity: `ERROR` or `WARN` |
| `poll_interval_seconds` | No | `5.0` | Seconds between status polls |
| `timeout_seconds` | No | `300` | Maximum seconds to wait for rule execution |

## Example YAML

```yaml
type: dagster_component_templates.MonteCarloCheckComponent
attributes:
  asset_key: warehouse/orders
  mcd_id_env_var: MCD_ID
  mcd_token_env_var: MCD_TOKEN
  rule_uuid: "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
  severity: ERROR
  timeout_seconds: 300
```

## Finding the rule UUID

In the Monte Carlo UI: **Monitors → Custom SQL Rule → Settings → Rule UUID**.
