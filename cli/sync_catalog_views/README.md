# `sync_catalog_views.py`

GitOps sync of **Catalog Views** (named asset selections) to a Dagster+
deployment. Takes a YAML manifest, upserts each view by name via the
Dagster+ GraphQL API (`createOrUpdateCatalogView`). Idempotent — matches
by name and updates in place; creates new views when unmatched.

The Dagster+ UI lets you create and edit views manually, but there's no
built-in way to track them in git or promote a set of views across
deployments. This CLI closes that gap.

- **Script:** [`./sync_catalog_views.py`](./sync_catalog_views.py)
- **Requires:** Python 3.8+, PyYAML, a Dagster+ user API token

## Install

```bash
pip install pyyaml
curl -fsSL https://raw.githubusercontent.com/eric-thomas-dagster/dagster-component-templates/main/cli/sync_catalog_views/sync_catalog_views.py \
    -o sync_catalog_views.py
chmod +x sync_catalog_views.py

export DAGSTER_CLOUD_API_TOKEN=user:xxxxxxxx
```

## Two subcommands

### `sync` — apply a manifest to a deployment

```bash
# Preview (no API writes)
./sync_catalog_views.py sync catalog_views.yaml \
    --deployment-url https://acme.dagster.cloud/prod \
    --dry-run

# Apply
./sync_catalog_views.py sync catalog_views.yaml \
    --deployment-url https://acme.dagster.cloud/prod

# Apply + delete views not in the manifest
./sync_catalog_views.py sync catalog_views.yaml \
    --deployment-url https://acme.dagster.cloud/prod \
    --prune
```

### `list` — show current catalog views in the deployment

```bash
./sync_catalog_views.py list \
    --deployment-url https://acme.dagster.cloud/prod
```

## Options

### `sync` subcommand

| Flag | Required | Default | Description |
|---|---|---|---|
| `manifest` | yes | — | Path to the YAML manifest (positional). |
| `--deployment-url` | yes | — | Full deployment URL, e.g. `https://acme.dagster.cloud/prod` |
| `--token-env` | | `DAGSTER_CLOUD_API_TOKEN` | Env var name holding the user API token |
| `--dry-run` | | off | Print what would be upserted/deleted without touching the deployment |
| `--prune` | | off | Delete deployment-side views that aren't in the manifest |

### `list` subcommand

| Flag | Required | Default | Description |
|---|---|---|---|
| `--deployment-url` | yes | — | Full deployment URL |
| `--token-env` | | `DAGSTER_CLOUD_API_TOKEN` | Env var name holding the user API token |

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

## `--prune` — remove views not in the manifest

Default behavior is upsert-only. `--prune` also **deletes** any
deployment-side view that isn't in the manifest. Always combine with
`--dry-run` first to preview:

```bash
./sync_catalog_views.py sync catalog_views.yaml \
    --deployment-url https://acme.dagster.cloud/prod \
    --prune --dry-run
```

Warning: `--prune` will delete UI-created views (any view added via the
Dagster+ web UI that isn't tracked in your manifest). If your team
manages catalog views partially by UI, don't run `--prune` — the CLI
should be an additive tool for the GitOps subset.

## Common failure modes

| Symptom | Cause | Fix |
|---|---|---|
| `HTTP 401` | Bad token or wrong deployment URL | Verify `DAGSTER_CLOUD_API_TOKEN` and `--deployment-url` |
| `HTTP 500` on sync with `tableNames: null` | Dagster+ rejects `null` for this field | Use `[]` in the manifest (the script sends `[]` by default when unset) |
| `HTTP 500` on sync with a custom `icon:` | Unknown icon name | Use a valid Dagster+ icon; the script defaults to `globe` when unset |
| `--prune` deleted a view you wanted to keep | UI-created view not in manifest | Add it to the manifest first, or drop `--prune` |
