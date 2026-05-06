# Lineage → HTTP Webhook

Sink asset that pushes the upstream `lineage_graph` (from
`lineage_graph_extractor`) to **webhook**. Compares the
incoming payload hash against the last successfully pushed hash and skips
the network call when nothing changed.

## Pipeline shape

```
lineage_graph_extractor (source asset)
        │
        ▼
Lineage → HTTP Webhook (sink asset)
```

Add multiple sinks for fan-out (lineage_to_purview + lineage_to_datahub
side by side share the same upstream — both stay in lock-step).

## Required env vars

| Var | Value |
|---|---|
| `LINEAGE_WEBHOOK_TOKEN` | API token / OAuth bearer for webhook |

## Change detection

`only_push_on_change: true` (default) — looks up the last
materialization's `pushed_hash` metadata; if it matches the upstream
`payload_hash`, the catalog POST is skipped and the asset is marked
materialized with `skipped: true`. Set to `false` if you want every
materialization to push.

## Companion components

- `lineage_graph_extractor` — required upstream
- Other lineage sinks: `lineage_to_alation`, `lineage_to_collibra`,
  `lineage_to_datahub`, `lineage_to_openlineage`, `lineage_to_purview`,
  `lineage_to_webhook`, `lineage_to_file`
