# VictoriaMetrics

[VictoriaMetrics](https://victoriametrics.com/) is a high-performance Prometheus-compatible time-series database. Lower memory + disk footprint than vanilla Prometheus at the same write rate, supports PromQL + MetricsQL (a superset), pluggable storage tiers. Dagster uses VictoriaMetrics internally.

The community registry covers VictoriaMetrics with **3 components** today:

## Components

| Component | Category | What it does | Validation |
|---|---|---|---|
| [`victoriametrics_resource`](https://dagster-component-ui.vercel.app/c/victoriametrics_resource) | resource | Base URL + auth (bearer or HTTP basic) + helpers to construct read/write endpoint URLs | `code` (live demo planned) |
| [`dataframe_to_victoriametrics`](https://dagster-component-ui.vercel.app/c/dataframe_to_victoriametrics) | sink | Bulk-ingest a Pandas DataFrame via `/api/v1/import/prometheus` (text format). Auto-classifies columns: numeric → field, others → label, plus configurable `timestamp_column` + `value_column`. | `code` |
| [`victoriametrics_query_asset`](https://dagster-component-ui.vercel.app/c/victoriametrics_query_asset) | source | Run a PromQL query against `/api/v1/query` or `/api/v1/query_range`; flatten matrix/vector result into a Pandas DataFrame (timestamp, value, + one column per label). | `code` |

## Connection / auth — quick reference

| Surface | URL | Notes |
|---|---|---|
| Read | `{base}/api/v1/query` (instant) `+/api/v1/query_range` (matrix) | Prometheus-compatible API |
| Ingest (text format) | `POST {base}/api/v1/import/prometheus` | `dataframe_to_victoriametrics` uses this |
| Ingest (remote-write) | `POST {base}/api/v1/write` | snappy + protobuf — not the default path; file an issue if needed |
| Labels | `{base}/api/v1/labels`, `/api/v1/label/{name}/values` | label discovery |

Auth: optional bearer token (vmauth-fronted clusters) or HTTP basic. Local single-binary VM runs without auth.

## Roadmap (convenience adds)

- `victoriametrics_snapshot_job` — create snapshots via `/snapshot/create` + optional S3 upload (Eric's "export" pattern).
- `victoriametrics_cardinality_sensor` — watch label / series cardinality; fire alert when over threshold. Production-ops gold.
- `victoriametrics_recording_rule_asset` — declarative recording-rule deployment.
- `external_grafana_dashboard` — declare a Grafana dashboard backed by VM as a catalog asset (cross-product).

## Gotchas

- **Prometheus text format vs remote-write** — the text-format endpoint is what `dataframe_to_victoriametrics` uses (simpler, curl-able, no protobuf). Remote-write is faster at scale (>1M samples/sec sustained); file an issue if needed.
- **Label cardinality** — every unique combination of label values creates a new time series. Avoid putting high-cardinality data (user_id, request_id) as labels — that's what fields / `_value` are for in InfluxDB; VictoriaMetrics doesn't have a "field" concept, everything in `{}` is a label.
- **Timestamps** — VictoriaMetrics expects milliseconds-since-epoch in the text format. `dataframe_to_victoriametrics` handles the `int(ts.timestamp() * 1000)` conversion.

## See also

- [VictoriaMetrics docs](https://docs.victoriametrics.com/)
- [`vendors/influxdb.md`](influxdb.md) — closest peer (InfluxDB 2.x/3.x)
- [`vendors/timescaledb.md`](timescaledb.md) — Postgres-extension alternative for time-series
