# `migrate_asset_metadata.py`

Carry an asset's historical metadata over to a **new asset key** within the same Dagster+
deployment — the "I renamed an asset in code and it now shows up as brand new, with no
history" situation. Dagster has no rename operation (the event log is append-only and keyed),
so the old key's history stays exactly where it is forever; the new key starts with none. This
CLI closes that gap by reporting new materialization events under the destination key, carrying
the source's metadata forward.

- **Script:** [`./migrate_asset_metadata.py`](./migrate_asset_metadata.py)
- **Requires:** Python 3.8+, PyYAML (only for the manifest form of `migrate`), a Dagster+
  **AGENT** API token (not a user/service token — see Caveats)

## What this does (and does NOT do)

- Reads the SOURCE asset key's materialization history via Dagster+'s real GraphQL API
  (`assetNodeOrError(assetKey).assetMaterializations`), including every metadata entry
  Dagster+ recorded for each event.
- Reports NEW materialization events under the DESTINATION asset key via Dagster+'s real
  `report_asset_materialization/` REST endpoint — the same endpoint Dagster's own
  [`examples/oss-metadata-to-plus`](https://github.com/dagster-io/dagster/tree/master/examples/oss-metadata-to-plus)
  example uses — carrying metadata, `data_version` (read off the real `dagster/data_version`
  event tag), and `description` forward. Confirmed directly against the
  [endpoint's own docs](https://docs.dagster.io/api/python-api/external-assets-rest-api): it
  accepts exactly 5 fields (`asset_key`, `metadata`, `data_version`, `description`,
  `partition`) — there is no 6th field for a timestamp.
- By default, reports one event per PARTITION (or a single event for an unpartitioned asset),
  carrying that partition's most recent metadata — enough to populate the new asset's catalog
  page and partition status grid the way the old one looked. Pass `--all-events` to instead
  replay every historical event for every partition.
- Does **NOT** preserve original timestamps — every reported event lands at "now," not the
  original historical date. "The new asset looks like it has always existed with this
  history" is not achievable via any public API today; only "the new asset has this metadata
  and shows as materialized" is.
- Is **NOT reversible** — the event log is append-only; there's no "delete a materialization"
  API. `--dry-run` (the default) never writes anything; `--yes` is required to actually report
  events.
- Skips (never guesses at) structured metadata entries (`TableMetadataEntry`,
  `TableSchemaMetadataEntry`, and similar non-scalar types) — only plain scalar metadata entry
  types are forwarded. A skipped entry is reported, never silently dropped.
- GraphQL field names for metadata entry types are **not hardcoded** — the script introspects
  your own deployment's schema at runtime (one extra GraphQL call) so it only ever requests
  fields your deployment's schema actually has, making it self-correcting across Dagster+
  schema versions.

## Caveats

- **Requires an AGENT token**, not a regular user/service API token, per Dagster's own example
  (unverified beyond that example's own README claim) — an auth error with a user/service
  token is almost certainly this.
- Only the destination asset needs to exist as a real asset definition in your code; the
  source key does not need to still be defined anywhere.

## Install

```bash
pip install pyyaml  # only needed for the manifest form of `migrate`
curl -fsSL https://raw.githubusercontent.com/eric-thomas-dagster/dagster-component-templates/main/cli/migrate_asset_metadata/migrate_asset_metadata.py \
    -o migrate_asset_metadata.py
chmod +x migrate_asset_metadata.py

export DAGSTER_CLOUD_API_TOKEN=agent:xxxxxx   # must be an AGENT token
```

## Two subcommands

### `migrate` — carry one (or many) asset's metadata to a new key

```bash
# Preview (default, never writes anything):
./migrate_asset_metadata.py migrate \
    --deployment-url https://acme.dagster.cloud/prod \
    --token-env DAGSTER_CLOUD_API_TOKEN \
    --source-asset-key "old/asset/key" \
    --dest-asset-key "new/asset/key"

# Actually report the events:
./migrate_asset_metadata.py migrate \
    --deployment-url https://acme.dagster.cloud/prod \
    --token-env DAGSTER_CLOUD_API_TOKEN \
    --source-asset-key "old/asset/key" \
    --dest-asset-key "new/asset/key" \
    --yes

# Batch, via a YAML manifest of {source_asset_key, dest_asset_key} pairs:
./migrate_asset_metadata.py migrate manifest.yaml \
    --deployment-url https://acme.dagster.cloud/prod \
    --token-env DAGSTER_CLOUD_API_TOKEN --yes
```

Manifest shape (YAML):

```yaml
migrations:
  - source_asset_key: old/path/to/asset
    dest_asset_key: new/path/to/asset
  - source_asset_key: another_old_key
    dest_asset_key: another_new_key
```

Other `migrate` flags: `--all-events` (replay every historical event per partition, instead of
just the latest per partition) and `--limit N` (cap source events read per asset, for debugging
or chunked runs).

### `list-materializations` — inspect a source asset's history first (read-only)

```bash
./migrate_asset_metadata.py list-materializations \
    --deployment-url https://acme.dagster.cloud/prod \
    --token-env DAGSTER_CLOUD_API_TOKEN \
    --asset-key "old/asset/key"
```

Dumps the asset's materialization history and metadata so you can decide what a migration would
carry over before running one.

---

Shared overview of all CLIs in this directory: [`../README.md`](../README.md)
