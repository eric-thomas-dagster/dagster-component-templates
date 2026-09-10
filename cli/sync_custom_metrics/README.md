# `sync_custom_metrics.py`

GitOps sync of **custom Insights metrics** to a Dagster+ deployment via
the real GraphQL API.

Mirrors the shape of `dagster-cloud deployment alert-policies sync`:
takes a YAML manifest, upserts each metric via the Dagster+ GraphQL API
(`createCustomMetric` / `updateCustomMetric`). Idempotent — matches
existing metrics by `metadata_key` (the natural key) and updates them;
creates new ones when unmatched.

- **Script:** [`../sync_custom_metrics.py`](../sync_custom_metrics.py)
- **Requires:** Python 3.8+, PyYAML, a Dagster+ user API token
- **Verified end-to-end** against a live Dagster+ deployment on 2026-09

## Install + run

```bash
pip install pyyaml
curl -fsSL https://raw.githubusercontent.com/eric-thomas-dagster/dagster-component-templates/main/cli/sync_custom_metrics.py \
    -o sync_custom_metrics.py
chmod +x sync_custom_metrics.py
```

## Usage

```bash
export DAGSTER_CLOUD_API_TOKEN=user:xxxxxxxx

# Preview what would be upserted (no API call)
./sync_custom_metrics.py sync metrics.yaml \
    --deployment-url https://acme.dagster.cloud/prod \
    --dry-run

# Apply
./sync_custom_metrics.py sync metrics.yaml \
    --deployment-url https://acme.dagster.cloud/prod

# List current custom metrics
./sync_custom_metrics.py list \
    --deployment-url https://acme.dagster.cloud/prod

# Apply + delete metrics not in the manifest
./sync_custom_metrics.py sync metrics.yaml \
    --deployment-url https://acme.dagster.cloud/prod \
    --prune
```

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

## What Dagster+ handles automatically (and what it doesn't)

Once a custom metric is defined via this CLI:

- **Automatic aggregation** across every asset that emits a
  `MetadataValue.<type>(...)` with the matching `metadata_key`. No
  per-asset opt-in required.
- **Time-series storage** in the Insights backend (VictoriaMetrics on
  most SaaS tenants) with ~5-15 min ingestion lag.
- **UI chart panel** on the Insights page — auto-appears after first
  materialization emits the key.

The CLI does NOT configure:

- **Aggregation function** (sum vs. avg vs. p99, etc.) — that's a
  per-chart UI choice in Insights.
- **Asset selection filter** — which assets contribute is also a
  per-chart UI choice; the metric definition itself is global.

Both of those choices are UI-side because Insights lets one metric
support multiple charts with different aggregations / selections.

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

Default: upsert-only. `--prune` also deletes deployment-side metrics
absent from the manifest. Same warning as `sync_catalog_views` — this
will delete UI-created metrics too. Combine with `--dry-run` first.

## Discovery: verify a metric is defined

After a sync, run `list` to confirm:

```bash
./sync_custom_metrics.py list \
    --deployment-url https://acme.dagster.cloud/prod
# rows_ingested   metric_12345    INTEGER    Rows Ingested
# cost_usd        metric_12346    FLOAT      Compute Cost (USD)
# p99_latency_ms  metric_12347    TIME_MS    P99 Latency
```

Or fetch via `pull_credit_usage.py metric-types` — the sibling CLI
enumerates every metric visible to your Dagster+, custom + built-in.

## Sharing with customers

Self-contained, stdlib + PyYAML only. Safe to copy directly to a
customer environment.

## See also

- **[../README.md](../README.md)** — overview of all three CLIs in this repo.
- **[../sync_catalog_views/](../sync_catalog_views/)** — sibling CLI for named asset selections.
- **[../pull_credit_usage/](../pull_credit_usage/)** — the "pull, don't push" companion for Insights data extraction.
