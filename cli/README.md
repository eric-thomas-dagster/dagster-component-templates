# Dagster+ config-sync CLIs

Two standalone Python scripts for syncing GitOps-managed configuration
into a Dagster+ deployment via the GraphQL API. Both mirror the
shape of the built-in `dagster-cloud deployment alert-policies sync`:
YAML manifest in → idempotent upsert-by-name out.

| Script | Manages |
|---|---|
| [`sync_asset_selections.py`](sync_asset_selections.py) | Named asset selections (saved selectors used across the UI + alert policies + Insights) |
| [`sync_custom_metrics.py`](sync_custom_metrics.py) | Custom Insights metrics (roll-ups of asset metadata by SUM / AVG / MIN / MAX) |

## Zero non-stdlib deps except PyYAML

Both scripts use only Python 3.8+ stdlib and PyYAML. No SDK install,
no build step, no venv gymnastics — copy one file, `chmod +x`, run.

```bash
pip install pyyaml
chmod +x sync_asset_selections.py sync_custom_metrics.py
```

## Usage — asset selections

```bash
# Preview what would be upserted
./sync_asset_selections.py sync selections.yaml \
    --deployment-url https://acme.dagster.cloud/prod \
    --token-env DAGSTER_CLOUD_API_TOKEN \
    --dry-run

# Actually apply
./sync_asset_selections.py sync selections.yaml \
    --deployment-url https://acme.dagster.cloud/prod \
    --token-env DAGSTER_CLOUD_API_TOKEN

# Apply + prune: delete anything in the deployment that isn't in the manifest
./sync_asset_selections.py sync selections.yaml \
    --deployment-url https://acme.dagster.cloud/prod \
    --token-env DAGSTER_CLOUD_API_TOKEN \
    --prune

# List current state
./sync_asset_selections.py list \
    --deployment-url https://acme.dagster.cloud/prod \
    --token-env DAGSTER_CLOUD_API_TOKEN
```

Manifest shape ([examples/selections.example.yaml](examples/selections.example.yaml)):

```yaml
selections:
  - name: high_priority_assets
    description: assets tagged priority=high
    selection: "tag:priority=high"
  - name: analytics_downstream
    selection: "+group:analytics"
```

## Usage — custom Insights metrics

```bash
./sync_custom_metrics.py sync metrics.yaml \
    --deployment-url https://acme.dagster.cloud/prod \
    --token-env DAGSTER_CLOUD_API_TOKEN \
    --dry-run

./sync_custom_metrics.py sync metrics.yaml \
    --deployment-url https://acme.dagster.cloud/prod \
    --token-env DAGSTER_CLOUD_API_TOKEN \
    --prune
```

Manifest shape ([examples/metrics.example.yaml](examples/metrics.example.yaml)):

```yaml
metrics:
  - name: rows_ingested
    description: sum of rows_ingested metadata across ingestion assets
    metadata_key: rows_ingested
    aggregation: SUM         # SUM | AVG | MIN | MAX
    unit: rows
    asset_selection: "group:ingestion"
```

## GitOps flow

Typical CI shape (GitHub Actions or equivalent):

```yaml
# .github/workflows/sync-dagster-plus-config.yml
on:
  push:
    branches: [main]
    paths:
      - "dagster-plus/selections.yaml"
      - "dagster-plus/metrics.yaml"

jobs:
  sync:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: pip install pyyaml
      - name: Sync selections
        run: ./cli/sync_asset_selections.py sync dagster-plus/selections.yaml \
               --deployment-url ${{ secrets.DAGSTER_PLUS_URL }} \
               --token-env DAGSTER_CLOUD_API_TOKEN --prune
        env:
          DAGSTER_CLOUD_API_TOKEN: ${{ secrets.DAGSTER_CLOUD_API_TOKEN }}
      - name: Sync metrics
        run: ./cli/sync_custom_metrics.py sync dagster-plus/metrics.yaml \
               --deployment-url ${{ secrets.DAGSTER_PLUS_URL }} \
               --token-env DAGSTER_CLOUD_API_TOKEN --prune
        env:
          DAGSTER_CLOUD_API_TOKEN: ${{ secrets.DAGSTER_CLOUD_API_TOKEN }}
```

Pair with the built-in `dagster-cloud deployment alert-policies sync`
(from the `dagster-cloud` package) for the third leg — alert policies.
Together those three cover the config surface most ops teams manage
out-of-band from their code deployment.

## Getting a Dagster+ API token

1. In Dagster+: **Cloud Settings → Tokens**.
2. Create a **User** token (personal) or a **Service** token (CI/CD).
3. Export it under whatever env var name you pass to `--token-env`.

## Schema caveat

The GraphQL mutation names used in these scripts
(`saveAssetSelection`, `saveInsightsCustomMetric`, etc.) reflect the
Dagster+ schema as of the time these scripts were written. If your
deployment's schema uses different names, adjust the `Q_UPSERT_*`
constants at the top of each script. To introspect your deployment's
schema:

```bash
curl -X POST https://acme.dagster.cloud/prod/graphql \
    -H "Dagster-Cloud-Api-Token: $DAGSTER_CLOUD_API_TOKEN" \
    -H "Content-Type: application/json" \
    -d '{"query":"{ __schema { mutationType { fields { name } } } }"}' \
    | python -m json.tool
```

## Sharing with customers

Both scripts are self-contained — safe to copy directly to a customer
environment. They import only Python stdlib + PyYAML; no dependency on
the `dagster_community_components` package or any other internal
tooling. Rename them or the CLI `prog=` string if desired.
