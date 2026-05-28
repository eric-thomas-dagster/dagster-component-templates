# Apache Doris

[Apache Doris](https://doris.apache.org/) is the real-time OLAP MPP database (alongside ClickHouse / StarRocks / Druid / Pinot in the same category). Sub-second analytical queries on TB-scale, MySQL wire protocol, high-throughput Stream Load ingestion. Customers running Doris want catalog presence in Dagster + observability of their real-time ingestion pipelines.

**Not an "open-source Snowflake"** — Doris is real-time OLAP, not a cloud warehouse. Closest peers: ClickHouse / StarRocks / Druid / Pinot. The community registry covers Doris with **6 components**.

## Components

### Connection + execution

| Component | Category | What it does | Validation |
|---|---|---|---|
| [`doris_resource`](https://dagster-component-ui.vercel.app/c/doris_resource) | resource | SQLAlchemy URL (via MySQL wire protocol on port 9030) + raw PyMySQL + HTTP frontend endpoint (port 8030) for Stream Load. Pairs with generic SQL components (`sql_transform`, `dataframe_to_table`). | `code` |

### Reads + writes

| Component | Category | What it does | Validation |
|---|---|---|---|
| [`doris_query_asset`](https://dagster-component-ui.vercel.app/c/doris_query_asset) | source | Run a SQL query against Doris → emit the result as a Pandas DataFrame asset. The symmetric counterpart to `dataframe_to_doris`. | `code` |
| [`dataframe_to_doris`](https://dagster-component-ui.vercel.app/c/dataframe_to_doris) | sink | Bulk-ingest a DataFrame via **Stream Load** (HTTP PUT). Production high-throughput path — 100k–1M rows/sec per FE per table. Supports CSV (default) and JSON. `mode: append|replace` with predicate-based DELETE. | `code` |

### Catalog presence + monitoring

| Component | Category | What it does | Validation |
|---|---|---|---|
| [`external_doris_table`](https://dagster-component-ui.vercel.app/c/external_doris_table) | external | Declare-only `AssetSpec` for tables Doris owns the lifecycle of (Flink CDC, Routine Load from Kafka, external Stream Load consumers). | `code` |
| [`doris_routine_load_sensor`](https://dagster-component-ui.vercel.app/c/doris_routine_load_sensor) | sensor | Watch Doris **Routine Load** jobs (continuous Kafka→Doris ingestion — Doris's analog to Snowpipe AUTO_INGEST). Polls `SHOW ROUTINE LOAD`; emits AssetObservation on healthy state + AssetMaterialization + RunRequest on PAUSED / CANCELLED / STOPPED. | `code` |

### Discovery + migration

Doris is also a supported source dialect for `database_schema_inventory`:

```yaml
type: dagster_community_components.DatabaseSchemaInventoryComponent
attributes:
  asset_name: doris_inventory
  connection_env_var: DORIS_CONN
  database_type: doris
```

The `doris` dialect queries `information_schema.tables` + `information_schema.views` (Doris's MySQL-compatible INFORMATION_SCHEMA). Excludes Doris system schemas (`__internal_schema`, `information_schema`, `mysql`, `sys`).

## Walkthroughs

Live walkthrough planned for v0.10.1 (multi-container Docker Compose; Doris's FE + BE separation needs at least 2 containers).

## Connection / auth — quick reference

| Surface | Port | Protocol | Notes |
|---|---|---|---|
| Query | 9030 | MySQL wire | SQLAlchemy via PyMySQL — same as connecting to MySQL |
| Stream Load + admin REST | 8030 | HTTP / HTTPS | Used by `dataframe_to_doris` |
| Edit log (between FEs) | 9010 | internal | not exposed externally |
| Heartbeat | 9050 | internal | not exposed externally |

Auth: Doris's own user model (`CREATE USER 'dagster_runner'@'%' IDENTIFIED BY '<pw>'`); no external IdP today. Modern installs front this with a sidecar OIDC proxy. TLS available on 2.0+.

## Gotchas

- **Doris ≠ open-source Snowflake.** Closer to ClickHouse/StarRocks. No time-travel, no data sharing, no marketplace, no separation of storage/compute (FE+BE are tightly coupled).
- **Stream Load `mode: replace`** requires a Unique Key or Aggregate table model with delete-sign support. Duplicate Key tables fail this — pre-DELETE via SQL + use `mode: append`.
- **Table model matters for writes.** `dataframe_to_doris` doesn't try to CREATE TABLE — the table must exist in Doris first. Choose the right model: Duplicate (default; append-only), Unique (upsert by primary key), Aggregate (pre-aggregated), Primary (Unique + queryable column updates).
- **`information_schema.tables.row_count` is NULL on Doris** in many versions. The `database_schema_inventory` component returns NULL for row_count rather than running expensive COUNT(*) queries.

## Roadmap

- **Live demo via Docker Compose** — Doris's FE+BE separation needs at least 2 containers; deferred to v0.10.1 follow-up.
- **`doris_workspace`** — analog of `snowflake_workspace`: auto-discover every database/table/MV as an external asset. Bigger lift; not yet shipped.
- **Compaction backlog sensor** — `SHOW PROC '/compactions'` exposes tablet compaction score. Production-ops value. Not yet shipped.
- **MV refresh trigger asset** — Doris MVs are auto-refreshed but customers sometimes want a manual trigger. Not yet shipped.

## See also

- [Apache Doris docs](https://doris.apache.org/docs/)
- [`vendors/starrocks.md`](starrocks.md) — Doris's open-source sibling fork
- [`vendors/clickhouse.md`](clickhouse.md) — the other major open-source OLAP MPP
