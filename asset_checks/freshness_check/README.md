# Freshness Policy

Attaches a Dagster-native [`FreshnessPolicy`](https://docs.dagster.io/guides/observe/asset-freshness-policies) to an asset for continuous SLA monitoring.

Unlike a manual asset check, Dagster evaluates freshness policies automatically and raises alerts when an SLA is missed — no sensor or manual trigger required. Enable in `dagster.yaml`:

```yaml
freshness:
  enabled: True
```

## Policy Types

### `time_window` — rolling window SLA

Asset must be materialized within the last N hours. A warning can be raised before the hard fail.

```yaml
type: dagster_component_templates.FreshnessPolicyComponent
attributes:
  asset_key: warehouse/orders
  policy_type: time_window
  fail_window_hours: 25
  warn_window_hours: 13
```

### `cron` — deadline-based SLA

Asset must be materialized before each cron deadline, within a lookback window.

```yaml
type: dagster_component_templates.FreshnessPolicyComponent
attributes:
  asset_key: warehouse/daily_summary
  policy_type: cron
  deadline_cron: "0 10 * * *"
  lower_bound_delta_hours: 1
  timezone: America/New_York
```

## Configuration

| Field | Required | Description |
|-------|----------|-------------|
| `asset_key` | Yes | Asset key to apply the policy to (e.g. `warehouse/orders`) |
| `policy_type` | No | `time_window` (default) or `cron` |
| `fail_window_hours` | `time_window` | Hours until the asset is considered stale and fails |
| `warn_window_hours` | No | Hours until a warning is raised (must be < `fail_window_hours`) |
| `deadline_cron` | `cron` | Cron expression for the materialization deadline |
| `lower_bound_delta_hours` | `cron` | Hours before the deadline when the window opens |
| `timezone` | No | Timezone for cron evaluation (default: UTC) |

## How It Works

This component creates an `AssetSpec` with the `freshness_policy` attribute set. Dagster's freshness evaluation system continuously monitors materialization events and surfaces violations in the UI and alerting integrations — no explicit check execution needed.

## Dependencies

```
dagster
```
