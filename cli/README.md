# Dagster+ CLIs

Standalone Python scripts that talk to a Dagster+ deployment's real GraphQL (and, for
`migrate_asset_metadata.py`, REST) API directly — no SDK, no build step. Two sync
GitOps-managed configuration in (mirroring the shape of the built-in `dagster-cloud deployment
alert-policies sync`: YAML manifest in → idempotent upsert-by-name out), two pull reporting data
out, and one performs a one-time, non-idempotent migration action.

**Verified against a live Dagster+ deployment (2026-09).** GraphQL
mutation/query names and input types match the real Dagster+ schema where stated (not
inferred) — see each script's own section below for exactly what was verified when.

| Script | Manages / pulls | Dagster+ concept |
|---|---|---|
| [`sync_catalog_views.py`](sync_catalog_views/sync_catalog_views.py) | Push named asset selections | Catalog Views |
| [`sync_custom_metrics.py`](sync_custom_metrics/sync_custom_metrics.py) | Push custom Insights metrics | Custom Metrics |
| [`pull_credit_usage.py`](pull_credit_usage/pull_credit_usage.py) | Pull credit usage rollup | Insights (usage) |
| [`pull_audit_logs.py`](pull_audit_logs/pull_audit_logs.py) | Pull audit log entries | Cloud Settings → Audit Log |
| [`migrate_asset_metadata.py`](migrate_asset_metadata/migrate_asset_metadata.py) | Carry metadata from one asset key to another | Asset catalog / materialization history |

## Migrate — carry an asset's metadata over after a rename

Renaming an asset in code doesn't move its materialization history: the catalog, metadata,
and history are all keyed by asset key, and Dagster has no "rename" operation (the event log
is append-only). The renamed asset shows up as brand new, with none of the old one's history.
Dagster support's standard answer to this is "report backfilled materializations under the new
key" (via `dg.AssetMaterialization` — see [external assets
docs](https://dagster.io/docs/guides/build/assets/external-assets#recording-materializations-and-metadata)),
and Dagster's own [`examples/oss-metadata-to-plus`](https://github.com/dagster-io/dagster/tree/master/examples/oss-metadata-to-plus)
example does exactly this from an OSS instance into Dagster+. This script does the same thing,
but entirely within Dagster+ — asset-to-asset, no OSS instance involved — and supports a
genuine key *rename* (the upstream example assumes the read key and write key are the same).

It reads the source asset's materialization history (every metadata entry Dagster+ recorded)
via GraphQL, and reports new events carrying that metadata under the destination key via
Dagster+'s real `report_asset_materialization/` REST endpoint (the same one the upstream
example uses). **Read the caveats in the script's own docstring before running this for real** —
in short: the new events get TODAY's timestamp, not the original historical dates (there's no
API to backdate them); this is not reversible (no "delete a materialization" API); and the
write endpoint needs an **agent token**, not a regular user/service token.

```bash
export DAGSTER_CLOUD_API_TOKEN=agent:xxxxxxxx   # must be an agent token

# Preview first (default -- never writes anything):
./migrate_asset_metadata.py migrate \
    --deployment-url https://acme.dagster.cloud/prod \
    --token-env DAGSTER_CLOUD_API_TOKEN \
    --source-asset-key "old/asset/key" \
    --dest-asset-key "new/asset/key"

# Then actually report the events:
./migrate_asset_metadata.py migrate \
    --deployment-url https://acme.dagster.cloud/prod \
    --token-env DAGSTER_CLOUD_API_TOKEN \
    --source-asset-key "old/asset/key" \
    --dest-asset-key "new/asset/key" \
    --yes

# Batch rename via manifest (examples/migrate_asset_metadata.example.yaml):
./migrate_asset_metadata.py migrate manifest.yaml \
    --deployment-url https://acme.dagster.cloud/prod \
    --token-env DAGSTER_CLOUD_API_TOKEN --yes

# Inspect a source asset's history first, read-only:
./migrate_asset_metadata.py list-materializations \
    --deployment-url https://acme.dagster.cloud/prod \
    --token-env DAGSTER_CLOUD_API_TOKEN \
    --asset-key "old/asset/key"
```

By default, one event is reported per partition (the latest metadata for that partition) —
enough to repopulate the new asset's catalog page and partition status grid. Pass `--all-events`
to instead replay every historical event per partition (usually not more useful: every event
still lands with today's timestamp either way, so this just produces several near-identical
events instead of one).

GraphQL field names for the ~19 `MetadataEntry` subtypes (`TextMetadataEntry.text`,
`JsonMetadataEntry.jsonString`, etc.) are never hardcoded — the script introspects your own
deployment's schema at runtime and only ever requests fields that actually exist on it, so it
self-corrects across Dagster+ schema versions instead of drifting. A metadata entry whose type
has no plain scalar value (table/table-schema-shaped entries) is skipped and reported as
skipped, never silently dropped or guessed at.

## Pull — credit usage across deployments × code locations × assets × days

The Dagster+ UI shows credit usage under Insights but doesn't expose a
cross-deployment / per-code-location / per-asset download. This CLI
hits the same real GraphQL endpoints the UI does
(`reportingMetricsByDeployment`, `reportingMetricsByAsset`) and merges
into one table. **Query shape verified against a live Dagster+
deployment (2026-09-10).**

Three subcommands:

```bash
export DAGSTER_CLOUD_API_TOKEN=user:xxxxxxxx

# 1. List deployments in the org (also verifies your token works):
./pull_credit_usage.py --org ericthomas-dagster deployments

# 2. List metric types visible to your Dagster+ (both built-ins and
#    custom Insights metrics you've synced via sync_custom_metrics.py).
#    Confirms __dagster_dagster_credits + __dagster_execution_time_ms
#    exist on your version.
./pull_credit_usage.py --org ericthomas-dagster metric-types

# 3. Pull the actual usage report:
./pull_credit_usage.py --org ericthomas-dagster \
    credits --start 2026-08-10 --end 2026-09-10 \
    --group-by deployment,code_location,asset --output-csv usage.csv
```

Group-by axes: `deployment`, `code_location`, `asset`, `day` — pick any
combination, comma-separated. Every axis you name becomes a column in
the output; `credits` + `compute_seconds` columns are rolled-up sums.

The script picks the right GraphQL path based on the axes you request:

| Axes | Path | Endpoints hit |
|---|---|---|
| `deployment` or `deployment,day` | `reportingMetricsByDeployment` | 1 org endpoint |
| includes `code_location` or `asset` | `reportingMetricsByAsset` | 1 per deployment |

Day bucketing uses `granularity: DAILY` under the hood — no manual
window-splitting. The Dagster+ API returns
`timestamps: [epoch, epoch, …]` + `values: [n, n, …]` per entity, and
the script explodes those into one row per (…, day).

If `credits` returns no rows, the script prints a hint listing the
usual causes (no runs in window, custom-metric ingestion lag,
metric name not visible). Run `metric-types` to sanity-check the
metric names on your specific Dagster+ version.

### Dagster+ Insights internals (learned the hard way)

Behaviors baked into the script — worth knowing when reading the
output or debugging:

| Constraint | Consequence in the CLI |
|---|---|
| Single-query window capped at 120 days | Auto-chunked. `--start` / `--end` can span any range; the script fans out. |
| Two metric stores: `VICTORIA_METRICS` (recent, ≈6 months) + `POSTGRES` (long-tail history) | Default `--store BOTH` queries both and unions. `--store VICTORIA_METRICS` alone will silently drop everything > ~6 months old. |
| `codeLocations` filter supported by POSTGRES but returns 500 on VM | Script never uses the filter. Code-location is joined client-side via `assetNodes { assetKey repository { location { name } } }` on the same deployment endpoint. |
| `reportingMetricsByDeployment` returns `ReportingInputError: Branch deployment metrics are not yet supported in VictoriaMetrics` on VM tenants | Script uses `reportingMetricsByAsset` throughout and rolls up deployment totals client-side. |
| Default `metricsFilter.limit` is 10 | `--limit 5000` default. Raise for very large orgs or split the window. |
| VM 500s with `PythonError: Internal Server Error (Trace ID: …)` are the "no data" signal | Not retried. Falls through to POSTGRES cleanly. |

Verified end-to-end against a live Dagster+ org (2026-09-10) — 9-month
history query pulled 1,624,267 credits across VM + POSTGRES with a
correct per-day breakdown (top daily bucket: `prod, 2026-04-19,
31,506 credits, 24,797s compute`).

## Zero deps except PyYAML

Every script in this directory uses Python 3.8+ stdlib only (PyYAML only where a script reads
a YAML manifest). No SDK install, no build step. Copy one file, `chmod +x`, run.

```bash
pip install pyyaml
chmod +x sync_catalog_views.py sync_custom_metrics.py migrate_asset_metadata.py
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

`migrate_asset_metadata.py` is the exception: it never hardcodes `MetadataEntry` subtype field
names at all — it introspects your deployment's own schema for those at runtime, every run (see
its own docstring). The `assetNodeOrError`/`assetMaterializations` query shape and the
`report_asset_materialization/` REST endpoint itself are NOT introspected, only verified against
Dagster's own `examples/oss-metadata-to-plus` example — if either has changed shape on your
deployment, this is the first place to look.

## Sharing with customers

Every script here is self-contained — safe to copy directly to a customer
environment. They import only Python stdlib + PyYAML; no dependency on
`dagster_community_components` or any other internal tooling. Rename
them or the `argparse` `prog=` string if desired.
