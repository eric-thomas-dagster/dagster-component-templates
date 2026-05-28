# ClickHouse

[ClickHouse](https://clickhouse.com/) is the dominant open-source columnar OLAP database — wide install base, blazing-fast aggregations on TB-PB scale, HTTP + native binary protocols + MySQL wire protocol for interop. The community registry covers ClickHouse with **6 components** spanning resource, IO managers, sinks, external-asset declaration, and observation.

## Components

### Connection + execution

| Component | Category | What it does | Validation |
|---|---|---|---|
| [`clickhouse_resource`](https://dagster-component-ui.vercel.app/c/clickhouse_resource) | resource | Uses the official `clickhouse-connect` Python client (HTTP-based, port 8123 plain / 8443 TLS) + a SQLAlchemy URL via `clickhouse-sqlalchemy` for generic SQL components. | `code` (live walkthrough v0.10.0) |

### IO managers

| Component | Category | What it does | Validation |
|---|---|---|---|
| [`clickhouse_io_manager`](https://dagster-component-ui.vercel.app/c/clickhouse_io_manager) | io_manager | Pandas → ClickHouse via clickhouse-connect | `code` |
| [`clickhouse_polars_io_manager`](https://dagster-component-ui.vercel.app/c/clickhouse_polars_io_manager) | io_manager | Polars → ClickHouse | `code` |

### Sinks + external

| Component | Category | What it does | Validation |
|---|---|---|---|
| [`dataframe_to_clickhouse`](https://dagster-component-ui.vercel.app/c/dataframe_to_clickhouse) | sink | Bulk-insert via clickhouse-connect's `client.insert_df()` — most efficient Pandas → ClickHouse path (RowBinary over HTTP; ~1M rows/sec per client). | `live` (Docker walkthrough below) |
| [`external_clickhouse_table`](https://dagster-component-ui.vercel.app/c/external_clickhouse_table) | external | Declare-only `AssetSpec` for tables ClickHouse owns (Materialized Views, Distributed engines, Kafka engine tables, etc.). | `live` |

### Observation

| Component | Category | What it does | Validation |
|---|---|---|---|
| [`clickhouse_table_observation_sensor`](https://dagster-component-ui.vercel.app/c/clickhouse_table_observation_sensor) | observation | Periodic table-row-count + size observation via `system.tables`. | `code` |

### Discovery + migration

`database_schema_inventory` supports `database_type: clickhouse` — uses `system.tables` (NOT `INFORMATION_SCHEMA.*`) with engine info on each table (MergeTree / ReplicatedMergeTree / Distributed / View / MaterializedView / Kafka / etc.). The engine column is critical for migration planning — re-creating a `Kafka` engine table on a different cluster is wildly different from re-creating a plain `MergeTree`.

## Walkthroughs

**[`examples/clickhouse.md`](https://github.com/eric-thomas-dagster/dagster-community-components-cli/blob/main/examples/clickhouse.md)** — single-container Docker walkthrough. `setup_clickhouse_demo.sh` brings up `clickhouse/clickhouse-server` locally, scaffolds a Dagster project with `clickhouse_resource` + `dataframe_to_clickhouse` + `external_clickhouse_table`, and materializes assets end-to-end against the running container.

## Connection / auth — quick reference

| Port | Protocol | Used by |
|---|---|---|
| 8123 | HTTP plain | `clickhouse-connect` (default) |
| 8443 | HTTPS | `clickhouse-connect` with `secure=true` |
| 9000 | Native TCP | `clickhouse-driver` (not our default) |
| 9004 | MySQL wire | interop only — not the production path |

Default user is `default` with empty password on fresh installs. Production usually creates per-service users via `CREATE USER`.

## Gotchas

- **`INFORMATION_SCHEMA.*` exists but is a thin compatibility layer**. The native catalog is `system.tables` / `system.columns` / `system.databases` — that's what our `database_schema_inventory` queries.
- **Table engines drive everything**. Picking the right engine (MergeTree vs ReplacingMergeTree vs AggregatingMergeTree vs SummingMergeTree) determines whether deduplication, aggregations, and TTL "just work" or fall through silently.
- **Pandas datetime columns** round-trip cleanly via `clickhouse-connect` (uses Python `datetime` objects, not raw integers). No `use_logical_type` flag needed.
- **Distributed engines** are query routers, not storage. If you write to a Distributed table, the rows physically land on the shard whose key matches. If you `external_clickhouse_table` declares a Distributed engine, the catalog metadata reflects that.
- **The MySQL wire protocol (port 9004)** is interop-only — fine for BI tools, NOT recommended for production write paths. Use clickhouse-connect's HTTP path.

## Roadmap

- **Kafka engine table sensor** — ClickHouse's Kafka engine consumes Kafka topics directly into MergeTree tables. A Routine-Load-style sensor would monitor the kafka_xxx system tables for lag + consumption rate.
- **MV refresh observation** — ClickHouse MVs are incremental; observation sensor for refresh lag.
- **Query asset (read source)** — `clickhouse_query_asset` for `SELECT ... → DataFrame`. Today you'd use `sql_transform` + the resource's connection_string.

## See also

- [ClickHouse docs](https://clickhouse.com/docs/)
- [clickhouse-connect Python client](https://clickhouse.com/docs/en/integrations/python)
- [`vendors/doris.md`](doris.md) / [`vendors/starrocks.md`](starrocks.md) — the other major open-source OLAP MPP databases
