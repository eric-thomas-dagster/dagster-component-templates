# Ask: add `dg api catalog-view` + `dg api custom-metric`

**tl;dr:** `dg api alert-policy sync` is great. Ops teams need the same
thing for Catalog Views and Custom Metrics. Server mutations already
exist — just the CLI wrappers are missing.

## What exists today

`dg api` (v1.13.20) ships `list`/`sync` for **alert-policy only**. No
`catalog-view`, no `custom-metric`. Verified in the source at
`dagster_dg_cli/cli/api/`.

## What's needed

- `dg api catalog-view {list, sync}` — YAML manifest → upsert Catalog Views
- `dg api custom-metric {list, sync}` — YAML manifest → upsert Custom Insights Metrics

Same shape as existing `alert-policy sync`. Same flags. Same YAML
round-trip (`list` outputs a manifest that `sync` can consume).

## Backend is already there

I verified against a live deployment. Mutations:

- **Catalog Views**: `createOrUpdateCatalogView` / `deleteCatalogView` +
  query `catalogViews`. Input: `CatalogViewSelectionInput`.
- **Custom Metrics**: `createCustomMetric` / `updateCustomMetric` /
  `deleteCustomMetric` + query `customMetrics`.

No new GraphQL work needed — only the Python CLI/SDK wrappers.

## Two live-discovered gotchas worth fixing regardless

Both cause opaque `500 InternalServerError` (no useful message client-side):

1. `CatalogViewSelectionInput.tableNames: null` crashes — must be `[]`.
   The type says nullable but the server doesn't handle it.
2. `icon` accepts any string but silently 500s on invalid icon names.
   Should validate against the known icon set (or accept anything and
   just render the default).

Worth adding server-side validation regardless of whether the CLI ships.

## Reference implementation

I built + verified working scripts against `ericthomas-dagster/prod`:

- `cli/sync_catalog_views.py` (~290 LOC)
- `cli/sync_custom_metrics.py` (~280 LOC)

In the DCC repo. Feel free to lift the mutations / input shapes /
prune logic directly — everything was schema-introspected against
live prod, not guessed.

## Once shipped

Our two scripts become removable. YAML manifests we defined use field
names that should map 1:1 to what a `dg api sync` would want.

— eric.thomas@dagsterlabs.com
