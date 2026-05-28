# IBM

Covers the IBM Db2 family — Db2 LUW (Linux/UNIX/Windows), Db2 Community Edition, Db2 on Cloud, Db2 Warehouse, and **Db2 for i / AS/400 / iSeries**. One resource component covers all five system types via a `system_type:` switch; downstream SQL components (replication, schema inventory, migration, table sinks) consume the same SQLAlchemy URL.

## Components

| Component | Category | What it does | Validation |
|---|---|---|---|
| [`db2_resource`](https://dagster-component-ui.vercel.app/c/db2_resource) | resource | SQLAlchemy + raw `ibm_db` connection for all Db2 variants. `system_type: luw|cloud|iseries` toggles port defaults (50000 vs 446) + AS/400 library-list parameters. | `live` (LUW/Cloud); `code` (AS/400 — no Dockerable i system available) |

Pairs with the generic SQL components (none of which are IBM-specific but all work against Db2 via the resource's connection string):

| Component | Role |
|---|---|
| `dataframe_to_table` | DataFrame → Db2 (or any SQLAlchemy target) |
| `sql_transform` | Templated SQL CTAS / read |
| `database_schema_inventory` | Catalog discovery — `database_type: db2` for LUW/Cloud; `database_type: db2_iseries` for AS/400 (uses QSYS2.* instead of SYSCAT.*) |
| `database_tables_migration` / `database_replication` / `database_constraints_migration` / `database_views_migration` | Migrate Db2 → modern warehouse (Snowflake / BigQuery / Databricks / DuckDB). See [`warehouse_migration.md`](https://github.com/eric-thomas-dagster/dagster-community-components-cli/blob/main/examples/warehouse_migration.md) |
| `sling_sync` / official `dagster-sling` | Recurring replication (Sling supports Db2 natively, including the i variant) |

## Walkthroughs

| Path | Walkthrough |
|---|---|
| **Db2 LUW** (Docker Community Edition) | [`examples/db2.md`](https://github.com/eric-thomas-dagster/dagster-community-components-cli/blob/main/examples/db2.md) — end-to-end against a Docker container |
| **Db2 for i / AS/400** | [`examples/db2_iseries.md`](https://github.com/eric-thomas-dagster/dagster-community-components-cli/blob/main/examples/db2_iseries.md) — config-only (IBM i can't be Docker'd); covers connection differences, library-list semantics, EBCDIC, alternative pyodbc driver path |
| **Db2 → modern warehouse migration** | [`examples/warehouse_migration.md`](https://github.com/eric-thomas-dagster/dagster-community-components-cli/blob/main/examples/warehouse_migration.md) — DDL-first + data-first patterns; same flow applies to Db2-i sources |

## Connection / auth — quick reference

| System type | Default port | Auth | TLS | Catalog |
|---|---|---|---|---|
| `luw` (default) | 50000 | username + password | Optional on-prem | `SYSCAT.*` |
| `cloud` | 50000 (overridden to 31xxx for Db2 on Cloud) | username + password | **Required** | `SYSCAT.*` |
| `iseries` (AS/400) | 446 (DRDA listener) | IBM i user profile + password | **Almost always required** on i 7.x+ | `QSYS2.*` |

The `ibm_db` Python wheel bundles the `clidriver` libraries — no external Db2 client install required. The same `ibm_db_sa` SQLAlchemy dialect handles every variant; only connection params and catalog SQL differ.

## Gotchas

- **AS/400 `database:` is the RDB name**, not the LUW-style database name. Find it via `WRKRDBDIRE` on the i — the entry marked `*LOCAL`.
- **AS/400 library list** maps to `CURRENT SCHEMA` (first entry) + `CURRENT PATH` (rest). Set via the `library_list:` field on `db2_resource`. Equivalent to `CHGLIBL` on the i.
- **AS/400 catalog uses `QSYS2.*`, NOT `SYSCAT.*`**. The `database_schema_inventory` component has a separate `database_type: db2_iseries` dialect — using `database_type: db2` on an i system returns empty results.
- **EBCDIC encoding**: if string columns come back as mojibake on i, the user profile is on a non-UTF-8 CCSID. Fix with `CHGUSRPRF USRPRF(YOURUSER) CCSID(1208)` or cast per-query.
- **Alternative driver**: if you have IBM i Access Client Solutions installed (common on legacy enterprise), `pyodbc` + the i Access ODBC driver also works. Not natively exposed by `db2_resource` today; file an issue if you need it.

## Roadmap

- **CL / RPG programs as Dagster assets** — IBM i ships CL (Control Language) + RPG II/III/IV programs. No component wraps `CALL QSYS/QCMDEXC(...)` or the SQL stored-procedure alias path today. Modelable; not yet shipped.
- **i job-scheduler integration** — `WRKJOBSCDE` on the i; would model as a `precisely_job_sensor`-style sensor that polls i job-scheduler state.
- **Watson + Cloud Pak for Data** — IBM's AI / data fabric stack. No components yet.

## See also

- [`db2_resource` README](https://dagster-component-ui.vercel.app/c/db2_resource)
- [Sling Db2 connector](https://docs.slingdata.io/connections/database-connections/db2) — pair with `dagster-sling` for recurring replication
- IBM Db2 docs: [LUW](https://www.ibm.com/docs/en/db2) · [Cloud](https://cloud.ibm.com/docs/Db2onCloud) · [i 7.5](https://www.ibm.com/docs/en/i/7.5)
