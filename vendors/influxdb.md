# InfluxDB

[InfluxDB](https://www.influxdata.com/) is a time-series database from InfluxData. Two-product story today: **InfluxDB OSS 2.x** (Flux query language; line-protocol writes) and **InfluxDB 3.x** (open-source `influxdb_iox` engine speaking SQL via Apache Arrow Flight). InfluxDB 1.x is end-of-life and not covered by these components.

The community registry covers InfluxDB with **2 components** today:

## Components

| Component | Category | What it does | Validation |
|---|---|---|---|
| [`influxdb_resource`](https://dagster-component-ui.vercel.app/c/influxdb_resource) | resource | Connection via the official `influxdb-client` Python SDK. URL + token + org + bucket auth model. Works against InfluxDB 2.x AND 3.x (Cloud Serverless / Dedicated). | `code` |
| [`dataframe_to_influxdb`](https://dagster-community-components-cli/c/dataframe_to_influxdb) | sink | Bulk-write a Pandas DataFrame as line-protocol points. Auto-classifies columns: numeric → field, others → tag. Supports explicit `tag_columns:` / `field_columns:` overrides. Batched writes via `SYNCHRONOUS` write_api. | `code` |

## Connection / auth — quick reference

| InfluxDB version | URL pattern | Auth | Read language |
|---|---|---|---|
| 2.x OSS / Docker | `http://localhost:8086` | API token + org + bucket | Flux |
| 2.x Cloud | `https://us-west-2-1.aws.cloud2.influxdata.com` | API token + org + bucket | Flux |
| 3.x Cloud Serverless / Dedicated | `https://us-east-1-1.aws.cloud2.influxdata.com` | API token + org + bucket | SQL (via Arrow Flight) |
| **AWS Timestream for InfluxDB** | `https://<id>-<region>.timestream-influxdb.<region>.on.aws:8086` | API token + org + bucket | Flux |

For 3.x SQL queries, prefer the `influxdb_client.flight.FlightClient` path or `sql_transform` against the connection URL — a dedicated `influxdb_query_asset` is on the roadmap.

### AWS Timestream coverage

[AWS Timestream](https://aws.amazon.com/timestream/) has two flavors:

| Flavor | Status | Dagster coverage |
|---|---|---|
| **Timestream for InfluxDB** | Active (AWS's go-forward TSDB) | ✅ **Use `influxdb_resource` + `dataframe_to_influxdb`** — point the `url:` at your Timestream-for-InfluxDB endpoint. Same auth model, same SDK. |
| **Timestream for LiveAnalytics** (original) | On a "no new customers" path as of mid-2025; AWS migrating customers off | ❌ **Skipped intentionally** — building net-new components for a deprecating product is low-ROI. AWS's migration guide points to Timestream for InfluxDB. |

## Tags vs fields — Pandas mapping

InfluxDB's data model:
- **Tag** = low-cardinality, indexed, string-only (e.g. `region`, `service`)
- **Field** = the actual metric value, numeric or string, NOT indexed (e.g. `throughput`, `errors`)
- **Timestamp** = the index column

`dataframe_to_influxdb` defaults:
- Column named `timestamp` (or `timestamp_column:` override) → index
- Numeric dtypes → fields
- Other dtypes → tags

Override explicitly with `tag_columns: [...]` + `field_columns: [...]` when the auto-classification doesn't match your schema.

## Gotchas

- **InfluxDB 1.x is NOT covered**. v1's InfluxQL + database-per-user model is end-of-life; if you're on 1.x and need Dagster, upgrade or wrap the v1 HTTP API in a custom component.
- **High cardinality** — same as VictoriaMetrics, putting `user_id` / `request_id` as a TAG explodes series count. As a field it's fine. The `dataframe_to_influxdb` auto-classifier helps (numeric → field), but watch tag columns.
- **Token scope** — InfluxDB tokens scope to specific buckets + ops. The `dataframe_to_influxdb` sink needs `write` on the destination bucket; reads need `read`.
- **3.x SQL via Flight** — Apache Arrow Flight runs on a separate port. The `influxdb-client` v1.45+ handles both; older versions only do v2 Flux.

## See also

- [InfluxDB docs (2.x / 3.x)](https://docs.influxdata.com/)
- [`vendors/victoriametrics.md`](victoriametrics.md) — closest peer (Prometheus-compatible)
- [`vendors/timescaledb.md`](timescaledb.md) — Postgres-extension alternative for time-series
