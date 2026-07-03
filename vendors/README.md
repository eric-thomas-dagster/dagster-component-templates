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

| Vendor | Components |
|---|---|
| [Apache Doris](doris.md) | 6 — resource, dataframe sink (Stream Load), external table, query asset, Routine Load sensor, schema_inventory dialect |
| [Argo](argo.md) | 3 — external workflow + status sensor + trigger asset (submit→watch pattern) |
| [AWS](aws.md) | ~32 — S3 + Kinesis + SQS + Redshift + Athena + DynamoDB + Glue + DMS + SageMaker + CDK + CloudWatch + CloudTrail |
| [ClickHouse](clickhouse.md) | 6 — resource + dataframe sink + IO managers + external table + observation sensor (live-validated demo) |
| [Databricks](databricks.md) | 11 — workspace + Delta Lake + Asset Bundles + lakehouse |
| [Google Cloud](google.md) | ~32 — BigQuery + GCS + Pub/Sub + Vertex AI + Deployment Manager |
| [IBM](ibm.md) | Db2 family — LUW + Cloud + Warehouse + Db2 for i / AS/400 (auto-fixes: RDB discovery, CCSID=1208, QSYS2 auto-routing) |
| [InfluxDB](influxdb.md) | 2 — resource + dataframe sink (line protocol). Covers InfluxDB 2.x + 3.x |
| [Microsoft](microsoft.md) | ~25 — SQL Server + Synapse + Fabric + Azure Data Factory + Power BI + MSGraph + Dynamics + SharePoint + Azure OpenAI + Sentinel |
| [Precisely](precisely.md) | 6 — Connect ETL (sensor, trigger, external asset), DIS DQ check, Address Verify, Data360 lineage sink |
| [Snowflake](snowflake.md) | 29 — workspace + Snowpark + DT/MV/Iceberg/Cortex + tasks + procs + alerts + OpenFlow + Snowpipe |
| [StarRocks](starrocks.md) | 3 — resource + dataframe sink (Stream Load) + external table (Doris fork; same wire protocol) |
| [Temporal](temporal.md) | 5 — full four-mode Dagster ↔ Temporal integration: trigger, sensor, external, signal_asset (push), query_asset (pull) |
| [TimescaleDB](timescaledb.md) | 1 — Postgres-extension resource with hypertable / compression / retention helpers |
| [Trino + Starburst](trino_starburst.md) | 3 — Trino resource + IO manager + Starburst alias (subclass; same engine) |
| [Vercel](vercel.md) | 3 — deployment sensor + external deployment asset + AI Gateway agent (OpenAI-compatible multi-provider LLM) |
| [VictoriaMetrics](victoriametrics.md) | 3 — resource + dataframe sink (Prometheus text format) + PromQL query asset |

More vendor pages land here as the registry grows.

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
