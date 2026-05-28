# StarRocks

[StarRocks](https://www.starrocks.io/) is the open-source real-time OLAP MPP database — literal fork of Apache Doris (the projects diverged in 2021–2022). Same MySQL wire protocol, same Stream Load HTTP ingestion path, same architecture (FE + BE). Operationally for Dagster, StarRocks and Doris are interchangeable; pick based on community / vendor preference, not Dagster integration capability.

The community registry covers StarRocks with **3 components** mirroring the Doris core. Doris-specific shape (Routine Load sensor, query asset) ports cleanly to StarRocks — file an issue if you need any of those mirrored explicitly.

## Components

| Component | Category | What it does | Validation |
|---|---|---|---|
| [`starrocks_resource`](https://dagster-component-ui.vercel.app/c/starrocks_resource) | resource | SQLAlchemy URL (MySQL wire protocol on port 9030) + raw PyMySQL + HTTP frontend endpoint (port 8030) for Stream Load. | `code` |
| [`dataframe_to_starrocks`](https://dagster-component-ui.vercel.app/c/dataframe_to_starrocks) | sink | Bulk-ingest via **Stream Load** (HTTP PUT). Same contract as Doris. | `code` |
| [`external_starrocks_table`](https://dagster-component-ui.vercel.app/c/external_starrocks_table) | external | Declare-only `AssetSpec`. Surfaces `table_model` (duplicate/unique/aggregate/primary/mv) as metadata. | `code` |

### Discovery + migration

`database_schema_inventory` supports `database_type: starrocks` — uses `information_schema.tables` + `information_schema.views` (StarRocks's MySQL-compatible info_schema, excluding `_statistics_` + `sys` system schemas).

## Walkthroughs

Live walkthrough planned for v0.10.1 (Docker Compose, same constraint as Doris — FE+BE separation needs multi-container).

## Connection / auth — quick reference

Identical to Doris:

| Surface | Port | Protocol |
|---|---|---|
| Query | 9030 | MySQL wire |
| Stream Load + admin REST | 8030 | HTTP / HTTPS |

## Doris vs StarRocks — Dagster perspective

| Aspect | Doris | StarRocks |
|---|---|---|
| Wire protocol | MySQL | MySQL |
| Stream Load | ✅ | ✅ |
| Components in registry | 6 (resource, sink, external, query, routine_load_sensor, schema_inventory dialect) | 3 + schema_inventory dialect |
| Commercial offering | (various IBM partners, Alibaba SelectDB) | CelerData (StarRocks-managed SaaS) |
| Apache governance | ✅ Apache project | Linux Foundation project |

If you're choosing for a new install, the differences are community velocity, commercial support, and specific enterprise features (CelerData, materialized-view freshness, etc.) — all outside Dagster's integration concern. Either works.

## Roadmap

- **`dataframe_from_starrocks`** (query asset) — mirror of `doris_query_asset`. Not yet shipped.
- **`starrocks_routine_load_sensor`** — same shape as Doris's. Not yet shipped.
- **Live demo via Docker Compose** — v0.10.1.

## See also

- [StarRocks docs](https://docs.starrocks.io/)
- [`vendors/doris.md`](doris.md) — the Doris counterpart
- [`vendors/clickhouse.md`](clickhouse.md)
