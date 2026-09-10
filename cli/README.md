# Dagster+ config-sync CLIs

Two standalone Python scripts for syncing GitOps-managed configuration
into a Dagster+ deployment via the real GraphQL API. Both mirror the
shape of the built-in `dagster-cloud deployment alert-policies sync`:
YAML manifest in → idempotent upsert-by-name out.

**Verified against a live Dagster+ deployment (2026-09).** GraphQL
mutation names + input types match the real Dagster+ schema (not
inferred).

| Script | Manages / pulls | Dagster+ concept |
|---|---|---|
| [`sync_catalog_views.py`](sync_catalog_views.py) | Push named asset selections | Catalog Views |
| [`sync_custom_metrics.py`](sync_custom_metrics.py) | Push custom Insights metrics | Custom Metrics |
| [`pull_credit_usage.py`](pull_credit_usage.py) | Pull credit usage rollup | Insights (usage) |

## Pull — credit usage across deployments × code locations × assets × days

The Dagster+ UI shows credit usage under Insights but doesn't expose a
cross-deployment / per-code-location / per-asset download. This CLI hits
the same GraphQL endpoints the UI does and merges the results into one
table.

```bash
# Last 30 days, per deployment × code location × asset — CSV
./pull_credit_usage.py \
    --org ericthomas-dagster \
    --token-env DAGSTER_CLOUD_API_TOKEN \
    --deployments prod,staging \
    credits --start 2026-08-10 --end 2026-09-10 \
    --group-by asset --output-csv credits.csv

# Daily breakdown per deployment — one row per (deployment, day)
./pull_credit_usage.py --org ericthomas-dagster \
    --token-env DAGSTER_CLOUD_API_TOKEN \
    credits --start 2026-08-10 --end 2026-09-10 \
    --group-by deployment,day --output-csv credits_daily.csv

# Group-by axes are composable — e.g. deployment × code_location × day
./pull_credit_usage.py --org ericthomas-dagster \
    --token-env DAGSTER_CLOUD_API_TOKEN \
    credits --start 2026-08-10 --end 2026-09-10 \
    --group-by deployment,code_location,day

# Verify the Insights schema shape against YOUR org's Dagster+ version
./pull_credit_usage.py --org ericthomas-dagster \
    --token-env DAGSTER_CLOUD_API_TOKEN \
    --deployments prod introspect
```

Group-by axes: `deployment`, `code_location`, `asset`, `day` — pick any
combination, comma-separated. Every axis you name becomes a column in
the output; the remaining `credits` + `compute_seconds` columns are
rolled-up sums.

If a query returns no data, run `introspect` — Dagster+ Insights'
GraphQL surface evolves across releases, so the exact field names may
have shifted since 2026-09. The introspect output shows the current
field names + arg types so you can edit `Q_ASSET_CREDITS` /
`Q_INSIGHTS_METRICS_FALLBACK` to match.

## Zero deps except PyYAML

Both scripts use Python 3.8+ stdlib + PyYAML only. No SDK install, no
build step. Copy one file, `chmod +x`, run.

```bash
pip install pyyaml
chmod +x sync_catalog_views.py sync_custom_metrics.py
```

## Usage — Catalog Views (asset selections)

```bash
# Preview what would be upserted (no API call)
./sync_catalog_views.py sync catalog_views.yaml \
    --deployment-url https://acme.dagster.cloud/prod \
    --token-env DAGSTER_CLOUD_API_TOKEN \
    --dry-run

# Actually apply
./sync_catalog_views.py sync catalog_views.yaml \
    --deployment-url https://acme.dagster.cloud/prod \
    --token-env DAGSTER_CLOUD_API_TOKEN

# Apply + prune: delete any view in the deployment not in the manifest
./sync_catalog_views.py sync catalog_views.yaml \
    --deployment-url https://acme.dagster.cloud/prod \
    --token-env DAGSTER_CLOUD_API_TOKEN \
    --prune

# List current state
./sync_catalog_views.py list \
    --deployment-url https://acme.dagster.cloud/prod \
    --token-env DAGSTER_CLOUD_API_TOKEN
```

Manifest shape ([examples/catalog_views.example.yaml](examples/catalog_views.example.yaml)):

```yaml
catalog_views:
  - name: high_priority_assets
    description: Assets tagged priority=high
    icon: globe                            # icon names must match Dagster+'s catalog
    is_private: false
    query_selection: 'tag:"priority"="high"'
  - name: analytics_downstream
    description: Everything downstream of analytics
    icon: globe
    is_private: false
    query_selection: '+group:"analytics"'
```

**Selection sources.** Each view uses `query_selection` (a raw
asset-selection string using Dagster's syntax). Structured filters
(`groups`, `kinds`, `tags`, `owners`, `code_locations`, `columns`,
`column_tags`, `table_names`) are also supported and can be combined
with `query_selection` — Dagster+ intersects them.

## Usage — Custom Insights metrics

```bash
./sync_custom_metrics.py sync metrics.yaml \
    --deployment-url https://acme.dagster.cloud/prod \
    --token-env DAGSTER_CLOUD_API_TOKEN \
    --dry-run

./sync_custom_metrics.py sync metrics.yaml \
    --deployment-url https://acme.dagster.cloud/prod \
    --token-env DAGSTER_CLOUD_API_TOKEN

./sync_custom_metrics.py sync metrics.yaml \
    --deployment-url https://acme.dagster.cloud/prod \
    --token-env DAGSTER_CLOUD_API_TOKEN \
    --prune

./sync_custom_metrics.py list \
    --deployment-url https://acme.dagster.cloud/prod \
    --token-env DAGSTER_CLOUD_API_TOKEN
```

Manifest shape ([examples/metrics.example.yaml](examples/metrics.example.yaml)):

```yaml
metrics:
  - metadata_key: rows_ingested              # the natural key — matches asset metadata
    display_name: Rows Ingested
    description: Rows ingested per materialization
    unit_type: INTEGER                        # INTEGER | TIME_MS | TIME_SECONDS | FLOAT | BYTES
  - metadata_key: cost_usd
    display_name: Compute Cost (USD)
    unit_type: FLOAT
```

Note: Dagster+ Insights aggregates automatically — you don't specify an
aggregation or asset-selection per metric here. Those are UI-side
choices when you build a chart.

## GitOps flow

```yaml
# .github/workflows/sync-dagster-plus-config.yml
on:
  push:
    branches: [main]
    paths:
      - "dagster-plus/catalog_views.yaml"
      - "dagster-plus/metrics.yaml"

jobs:
  sync:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: pip install pyyaml
      - name: Sync catalog views
        run: ./cli/sync_catalog_views.py sync dagster-plus/catalog_views.yaml \
               --deployment-url ${{ secrets.DAGSTER_PLUS_URL }} \
               --token-env DAGSTER_CLOUD_API_TOKEN --prune
        env:
          DAGSTER_CLOUD_API_TOKEN: ${{ secrets.DAGSTER_CLOUD_API_TOKEN }}
      - name: Sync custom metrics
        run: ./cli/sync_custom_metrics.py sync dagster-plus/metrics.yaml \
               --deployment-url ${{ secrets.DAGSTER_PLUS_URL }} \
               --token-env DAGSTER_CLOUD_API_TOKEN --prune
        env:
          DAGSTER_CLOUD_API_TOKEN: ${{ secrets.DAGSTER_CLOUD_API_TOKEN }}
```

Pair with the built-in **`dg api alert-policy sync <file>`** (from the
`dagster-dg-cli` package, [documented here](https://docs.dagster.io/api/clis/dg-cli/dg-api#dg-api))
for the third leg — alert policies. Together those three cover the
config surface most ops teams manage out-of-band from their code
deployment.

## Why these live in `cli/` and not in `dg api`

As of dg 1.13.20, `dg api` only ships `alert-policy list/sync` — no
`catalog-view` or `custom-metric` subcommands (verified via source at
`dagster_dg_cli/cli/api/`). These scripts fill the gap in the same
shape (`sync <manifest>` + `list`) so ops teams have a uniform GitOps
flow for all three today. When Dagster+ ships the missing subcommands
upstream (see [`docs/FEEDBACK_dg_api_catalog_views_metrics.md`](../docs/FEEDBACK_dg_api_catalog_views_metrics.md)),
these scripts become removable — the YAML manifests will drop in
unchanged (same field names).

## `--prune` safety note

`--prune` deletes anything in the deployment that isn't in the
manifest. If your manifest is the SINGLE SOURCE OF TRUTH for these
config items, prune is what you want. If other people manually create
Catalog Views or Custom Metrics via the UI, `--prune` will delete them
— so leave it off (or scope to specific naming prefixes by convention).

## Getting a Dagster+ API token

1. In Dagster+: **Cloud Settings → Tokens**.
2. Create a **User** token (personal) or a **Service** token (CI/CD).
3. Export under whatever env var name you pass to `--token-env`
   (default: `DAGSTER_CLOUD_API_TOKEN`).

## Debugging: introspect your deployment's schema

The mutations used here were verified against a live Dagster+
deployment in 2026-09. If your deployment's schema differs, introspect:

```bash
curl -X POST https://acme.dagster.cloud/prod/graphql \
    -H "Dagster-Cloud-Api-Token: $DAGSTER_CLOUD_API_TOKEN" \
    -H "Content-Type: application/json" \
    -d '{"query":"{ __schema { mutationType { fields { name } } } }"}' \
    | python -m json.tool
```

Look for mutations matching `catalogView` or `customMetric`. If names
differ, edit the `Q_*` constants at the top of each script.

## Sharing with customers

Both scripts are self-contained — safe to copy directly to a customer
environment. They import only Python stdlib + PyYAML; no dependency on
`dagster_community_components` or any other internal tooling. Rename
them or the `argparse` `prog=` string if desired.
