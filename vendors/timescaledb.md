# TimescaleDB

[TimescaleDB](https://www.timescale.com/) is a time-series database built as a **PostgreSQL extension** — same wire protocol, same SQLAlchemy dialect (`psycopg2`), with **hypertables** + **compression** + **continuous aggregates** layered on top of regular Postgres tables. Customers running Postgres-native shops who want time-series at scale without leaving the ecosystem.

The community registry covers TimescaleDB with **1 dedicated component** + integration with the generic SQL components:

## Components

| Component | Category | What it does | Validation |
|---|---|---|---|
| [`timescaledb_resource`](https://dagster-component-ui.vercel.app/c/timescaledb_resource) | resource | Postgres connection (`postgresql+psycopg2://`) + helpers for TimescaleDB-specific operations: `create_hypertable`, `add_compression_policy`, `add_retention_policy`. | `code` |

### Pairs with generic SQL components

Because TimescaleDB is Postgres-on-the-wire, **all generic SQL components in this registry work against it**:

| Generic component | Use against TimescaleDB |
|---|---|
| `dataframe_to_table` | Pandas → hypertable (table must be pre-converted via `create_hypertable`) |
| `sql_transform` | Continuous aggregates, refresh policies, custom DDL |
| `database_schema_inventory` (`database_type: postgres`) | Inventory the database — includes hypertables alongside regular tables |
| `database_replication` / `database_tables_migration` | Migrate from another source TO TimescaleDB or the reverse |
| `dataframe_to_csv` / `dataframe_to_parquet` | Export hypertable data |

## TimescaleDB-specific helpers

The `timescaledb_resource` exposes Python methods callable from any asset:

```python
@asset(required_resource_keys={"timescaledb_resource"})
def setup_metrics_hypertable(context):
    ts = context.resources.timescaledb_resource
    ts.create_hypertable("events", "timestamp")              # convert events → hypertable
    ts.add_compression_policy("events", "7 days")            # compress chunks older than 7 days
    ts.add_retention_policy("events", "90 days")             # drop chunks older than 90 days
```

These run idempotent SQL — calling them on an already-set-up table is a no-op.

## Connection / auth — quick reference

Same as Postgres:

| Surface | Detail |
|---|---|
| Port | 5432 |
| Auth | username + password (literal or env var) |
| TLS | `ssl: true` adds `?sslmode=require` |
| SQLAlchemy dialect | `postgresql+psycopg2://` |

## Gotchas

- **The extension lives server-side** — Postgres must have `CREATE EXTENSION timescaledb` run on the database. Docker image `timescale/timescaledb` does this automatically. Existing Postgres clusters need the timescaledb package installed at the OS level + `CREATE EXTENSION` per database.
- **Hypertables don't have a partition column** in `INFORMATION_SCHEMA`; you'd query Timescale's own `_timescaledb_catalog.hypertable` view to discover them. `database_schema_inventory(database_type=postgres)` returns them as regular tables.
- **Compression caveat**: once a chunk is compressed, you can't UPDATE / DELETE rows in it (only INSERT new ones). Plan accordingly when setting compression policies.
- **Continuous aggregates** are materialized views with refresh policies; the `timescaledb_resource` helpers don't yet expose them (use `sql_transform` for now).

## Roadmap

- `create_continuous_aggregate` helper on the resource.
- `dataframe_to_timescaledb` thin wrapper that auto-calls `create_hypertable` on first materialization (today: pre-create the hypertable manually + use `dataframe_to_table`).
- Hypertable-aware schema-inventory dialect (`database_type: timescaledb`) that surfaces chunk count + compressed-chunk count + size.

## See also

- [TimescaleDB docs](https://docs.timescale.com/)
- [`vendors/victoriametrics.md`](victoriametrics.md) / [`vendors/influxdb.md`](influxdb.md) — Postgres-extension-free alternatives
- [Postgres warehouse migration playbook](https://github.com/eric-thomas-dagster/dagster-community-components-cli/blob/main/examples/warehouse_migration.md) — covers Postgres → DuckDB end-to-end; same flow + hypertable awareness applies for Postgres ↔ TimescaleDB
