# `sync_catalog_views.py`

GitOps sync of **Catalog Views** (a.k.a. named asset selections) to a
Dagster+ deployment via the real GraphQL API.

Mirrors the shape of `dagster-cloud deployment alert-policies sync`: takes
a YAML manifest, upserts each view by name. Idempotent — re-running with
the same manifest is a no-op. Uses the real Dagster+ mutation
`createOrUpdateCatalogView`.

- **Script:** [`../sync_catalog_views.py`](../sync_catalog_views.py)
- **Requires:** Python 3.8+, PyYAML, a Dagster+ user API token
- **Verified end-to-end** against a live Dagster+ deployment on 2026-09

## Install + run

```bash
pip install pyyaml
curl -fsSL https://raw.githubusercontent.com/eric-thomas-dagster/dagster-component-templates/main/cli/sync_catalog_views.py \
    -o sync_catalog_views.py
chmod +x sync_catalog_views.py
```

## Usage

```bash
export DAGSTER_CLOUD_API_TOKEN=user:xxxxxxxx

# Preview what would be upserted (no API call to the deployment)
./sync_catalog_views.py sync catalog_views.yaml \
    --deployment-url https://acme.dagster.cloud/prod \
    --dry-run

# Apply
./sync_catalog_views.py sync catalog_views.yaml \
    --deployment-url https://acme.dagster.cloud/prod

# List current catalog views in the deployment
./sync_catalog_views.py list \
    --deployment-url https://acme.dagster.cloud/prod

# Apply + remove views not in the manifest
./sync_catalog_views.py sync catalog_views.yaml \
    --deployment-url https://acme.dagster.cloud/prod \
    --prune
```

## Manifest shape (YAML)

```yaml
catalog_views:
  - name: high_priority_assets
    description: Assets tagged priority=high
    icon: star                       # any icon name Dagster+ accepts
    is_private: false
    # Provide EITHER a raw asset-selection query (recommended) …
    query_selection: "tag:priority=high"
    # … OR structured filters (empty lists ok):
    groups: []
    kinds: []
    tags: []
    owners: []
    code_locations: []
    columns: []
    column_tags: []
    table_names: []
```

Two known constraints on the API side that the script defends against:

1. **`tableNames` must be `[]` not `null`** — Dagster+ 500s on `null` even
   though the schema says nullable. The script always sends `[]` when
   unset.
2. **`icon` must be a real Dagster+ icon** — unknown icons return an
   opaque 500. Safe default: `globe` (matches existing customer views).
   The script defaults to `globe` when unset.

## `--prune` — remove views not in the manifest

Default behavior: the script only **upserts**. Views that exist in the
deployment but are absent from the manifest are left alone.

Pass `--prune` to also **delete** any deployment-side view that isn't in
the manifest. Combine with `--dry-run` first to preview the deletions:

```bash
./sync_catalog_views.py sync catalog_views.yaml \
    --deployment-url https://acme.dagster.cloud/prod \
    --prune --dry-run
```

Warning: `--prune` will delete UI-created views (any view added via the
Dagster+ web UI that isn't tracked in your manifest). If your team
manages catalog views partially by UI, don't run `--prune` — the CLI
should be an additive tool for the GitOps subset.

## Debugging: introspect your deployment's schema

The mutations here were verified against a live Dagster+ deployment. If
your Dagster+ version has a schema drift, introspect:

```bash
curl -X POST https://acme.dagster.cloud/prod/graphql \
    -H "Dagster-Cloud-Api-Token: $DAGSTER_CLOUD_API_TOKEN" \
    -H "Content-Type: application/json" \
    -d '{"query":"{ __schema { mutationType { fields { name } } } }"}' \
    | python -m json.tool
```

Look for mutations matching `catalogView`. If names differ, edit the
`Q_*` constants at the top of the script.

## Sharing with customers

Self-contained, stdlib + PyYAML only. Safe to copy directly to a
customer environment. Rename or change the `argparse` `prog=` string if
you want it to identify differently in `--help`.

## See also

- **[../README.md](../README.md)** — overview of all three CLIs in this repo.
- **[../sync_custom_metrics/](../sync_custom_metrics/)** — sibling CLI for custom Insights metrics.
