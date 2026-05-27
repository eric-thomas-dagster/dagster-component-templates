# Vendor pages

One-stop landing pages aggregating every community component that targets a given vendor (or vendor *family* — e.g. "IBM" covers Db2 LUW, Db2 on Cloud, AS/400, etc.).

Each vendor page has the same shape:

1. **Overview** — what the vendor does, why Dagster customers integrate with it
2. **Components** — table of every component in this repo that targets the vendor, grouped by category (resource / asset / sensor / etc.)
3. **Validation status** — `live` / `code` / `infra` per component (linked to evidence)
4. **Walkthroughs** — links to `examples/<topic>.md` for any end-to-end demos
5. **Connection / auth** — common patterns, env vars, paid-vs-free tiers
6. **Gotchas** — vendor-specific quirks (port defaults, dialect oddities, version-dependent endpoints)

## Index

| Vendor | Page | Components |
|---|---|---|
| Precisely | [precisely.md](precisely.md) | 6 — Connect ETL (sensor, trigger, external asset), DIS DQ check, Address Verify, Data360 lineage sink |

More vendor pages land here as the registry grows. Suggested next:

- `ibm.md` — Db2 LUW + Db2 on Cloud + Db2 for i / AS/400
- `snowflake.md` — workspace + Snowpark + DT/MV/iceberg/cortex
- `databricks.md` — workspace + lakehouse + Unity Catalog
- `microsoft.md` — Synapse + Fabric + Power BI + MSGraph + Dynamics
- `aws.md` — S3 + SQS + Glue + Kinesis + DynamoDB

## Why these exist

The registry has ~800 components — flat lists scale poorly. Customers who already use a specific vendor want a single page listing "what can I do with X in Dagster?" rather than searching tag-by-tag. Vendor pages collapse a search-driven discovery flow into a single overview that maps cleanly onto how customers actually think about their stack.

These pages are also the source of truth for the UI (https://dagster-component-ui.vercel.app/) — the registry can build per-vendor landing pages by reading the `vendor` field on each manifest entry and pulling the matching markdown from here.

## Manifest field

Each component's `manifest.json` entry can carry an optional `vendor:` field (or a `vendors:` list for components that target multiple vendors). When present, the registry UI groups by it:

```json
{
  "id": "precisely_dis_dq_check",
  "vendor": "Precisely",
  ...
}
```

For components without a `vendor:` field, the UI falls back to the existing `tags:` for grouping.
