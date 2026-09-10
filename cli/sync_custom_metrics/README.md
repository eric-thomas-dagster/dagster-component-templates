# `sync_custom_metrics.py`

GitOps sync of **custom Insights metrics** to a Dagster+ deployment.
Takes a YAML manifest, upserts each metric via the Dagster+ GraphQL API
(`createCustomMetric` / `updateCustomMetric`). Idempotent — matches by
`metadata_key` (the natural key) and updates in place; creates new
metrics when unmatched.

Once defined, a custom Insights metric is aggregated automatically
across every asset that emits a matching `MetadataValue.<type>(...)` — no
per-asset opt-in required. This CLI is the one-time (or GitOps-managed)
step that promotes a numeric metadata key into a first-class Insights
chart.

- **Script:** [`../sync_custom_metrics.py`](../sync_custom_metrics.py)
- **Requires:** Python 3.8+, PyYAML, a Dagster+ user API token

## Install

```bash
pip install pyyaml
curl -fsSL https://raw.githubusercontent.com/eric-thomas-dagster/dagster-component-templates/main/cli/sync_custom_metrics.py \
    -o sync_custom_metrics.py
chmod +x sync_custom_metrics.py

export DAGSTER_CLOUD_API_TOKEN=user:xxxxxxxx
```

## Two subcommands

### `sync` — apply a manifest to a deployment

```bash
# Preview (no API writes)
./sync_custom_metrics.py sync metrics.yaml \
    --deployment-url https://acme.dagster.cloud/prod \
    --dry-run

# Apply
./sync_custom_metrics.py sync metrics.yaml \
    --deployment-url https://acme.dagster.cloud/prod

# Apply + delete metrics not in the manifest
./sync_custom_metrics.py sync metrics.yaml \
    --deployment-url https://acme.dagster.cloud/prod \
    --prune
```

### `list` — show current custom metrics in the deployment

```bash
./sync_custom_metrics.py list \
    --deployment-url https://acme.dagster.cloud/prod
# rows_ingested   metric_12345    INTEGER    Rows Ingested
# cost_usd        metric_12346    FLOAT      Compute Cost (USD)
# p99_latency_ms  metric_12347    TIME_MS    P99 Latency
```

## Options

### `sync` subcommand

| Flag | Required | Default | Description |
|---|---|---|---|
| `manifest` | yes | — | Path to the YAML manifest (positional). |
| `--deployment-url` | yes | — | Full deployment URL, e.g. `https://acme.dagster.cloud/prod` |
| `--token-env` | | `DAGSTER_CLOUD_API_TOKEN` | Env var name holding the user API token |
| `--dry-run` | | off | Print what would be upserted/deleted without touching the deployment |
| `--prune` | | off | Delete deployment-side metrics that aren't in the manifest |

### `list` subcommand

| Flag | Required | Default | Description |
|---|---|---|---|
| `--deployment-url` | yes | — | Full deployment URL |
| `--token-env` | | `DAGSTER_CLOUD_API_TOKEN` | Env var name holding the user API token |

## Manifest shape (YAML)

```yaml
metrics:
  - metadata_key: rows_ingested       # the asset metadata key to promote
    display_name: Rows Ingested       # optional; shown in Insights UI
    description: Rows ingested per materialization
    unit_type: INTEGER                # INTEGER | TIME_MS | TIME_SECONDS | FLOAT | BYTES
  - metadata_key: cost_usd
    display_name: Compute Cost (USD)
    unit_type: FLOAT
  - metadata_key: p99_latency_ms
    display_name: P99 Latency
    unit_type: TIME_MS
```

## `unit_type` values

| Value | Renders as | Use for |
|---|---|---|
| `INTEGER` | `1,234` | Row counts, event counts, credits |
| `FLOAT` | `1.234` | Cost, ratios, computed floats |
| `TIME_MS` | `1.2s` (auto-converted from ms) | Latencies, durations |
| `TIME_SECONDS` | `1.2s` | Longer durations (queries, batch jobs) |
| `BYTES` | `1.2 KB` (auto-converted) | Payload sizes, blob sizes |

Bad values return a validation error at CLI-load time before any API
call is made.

## `--prune` — remove metrics not in the manifest

Default behavior is upsert-only. `--prune` also **deletes** any
deployment-side metric that isn't in the manifest. Always combine with
`--dry-run` first to preview:

```bash
./sync_custom_metrics.py sync metrics.yaml \
    --deployment-url https://acme.dagster.cloud/prod \
    --prune --dry-run
```

Warning: `--prune` will delete UI-created metrics too. If your team
manages metrics partially by UI, don't run `--prune`.

## What the CLI configures (and what stays in the UI)

The metric **definition** (name, key, unit, description) is what this
CLI manages. Aggregation function (sum vs. avg vs. p99, etc.) and asset
selection filter (which assets contribute to a given chart) are
per-chart UI choices in Insights — one metric can drive multiple charts
with different aggregations and selections.

## Common failure modes

| Symptom | Cause | Fix |
|---|---|---|
| `HTTP 401` | Bad token or wrong deployment URL | Verify `DAGSTER_CLOUD_API_TOKEN` and `--deployment-url` |
| Validation error on `unit_type` | Typo or unsupported value | Use one of `INTEGER`, `TIME_MS`, `TIME_SECONDS`, `FLOAT`, `BYTES` |
| Metric appears in `list` but no data in Insights UI | No asset has materialized with that `metadata_key` yet, or ingestion lag (~5–15 min on VictoriaMetrics) | Materialize an asset that emits `MetadataValue.<type>(...)` under the matching key, then wait |
| `--prune` deleted a metric you wanted to keep | UI-created metric not in manifest | Add it to the manifest first, or drop `--prune` |
