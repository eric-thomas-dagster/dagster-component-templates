# Databricks

The community Databricks surface is **11 components** spanning workspace auto-discovery, Delta Lake reads + writes (Delta is the foundational table format), external-table declaration, and Asset Bundles. Designed to live alongside the official `dagster-databricks` integration.

## Official Dagster integration (`dagster-databricks`)

For Databricks job execution + cluster control + dbt-via-Databricks, **prefer the official `dagster-databricks` package**. It's the supported path with first-class support for `submit_run`, cluster reuse, and the standard Databricks REST API.

The community components below are **complements, not replacements**. They cover:
- Workspace auto-discovery — `databricks_workspace` imports Unity Catalog tables / views / jobs as Dagster assets
- Pandas-friendly Delta Lake reads + writes (no Spark required; uses `delta-rs`)
- External-asset declarations for tables Databricks owns the lifecycle of
- Asset Bundles registration

## Components

### Workspace + observation

| Component | Category | What it does | Validation |
|---|---|---|---|
| [`databricks_workspace`](https://dagster-component-ui.vercel.app/c/databricks_workspace) | integration | Auto-import Unity Catalog tables / views / jobs as Dagster assets. Per-entity `assets_by_name` overrides like the Snowflake workspace. | `code` |
| [`databricks_table_observation_sensor`](https://dagster-component-ui.vercel.app/c/databricks_table_observation_sensor) | observation | Periodic Unity Catalog table observation (row counts, last-modified). | `code` |
| [`databricks_asset_bundle`](https://dagster-component-ui.vercel.app/c/databricks_asset_bundle) | integration | Register a Databricks Asset Bundle's tables as Dagster assets via the Unity Catalog API. | `code` |

### Resource + IO manager

| Component | Category | What it does | Validation |
|---|---|---|---|
| [`databricks_resource`](https://dagster-component-ui.vercel.app/c/databricks_resource) | resource | Connection resource (host + token); used by sinks + IO manager. | `code` |
| [`databricks_io_manager`](https://dagster-component-ui.vercel.app/c/databricks_io_manager) | io_manager | Pandas / Polars / PySpark → Delta on Unity Catalog. | `code` |

### Delta Lake (no Spark required)

| Component | Category | What it does | Validation |
|---|---|---|---|
| [`dataframe_to_delta_table`](https://dagster-component-ui.vercel.app/c/dataframe_to_delta_table) | sink | Pandas → Delta. Uses `delta-rs` (Rust); no JVM. | `live` |
| [`delta_ingestion`](https://dagster-component-ui.vercel.app/c/delta_ingestion) | ingestion | Read Delta table → Pandas. | `live` |
| [`delta_lake_io_manager`](https://dagster-component-ui.vercel.app/c/delta_lake_io_manager) | io_manager | Asset persistence in Delta format. | `live` |
| [`external_delta_table`](https://dagster-component-ui.vercel.app/c/external_delta_table) | external | Declare-only external asset for a Delta table Databricks (or anything else) maintains. | `live` |

### Sinks + external

| Component | Category | What it does | Validation |
|---|---|---|---|
| [`dataframe_to_databricks`](https://dagster-component-ui.vercel.app/c/dataframe_to_databricks) | sink | Pandas DataFrame → Databricks SQL warehouse via `databricks-sql-connector`. v0.8.5 emits real per-column DDL types (int / float / bool / timestamp / string) instead of the previous all-STRING DDL. | `live` |
| [`external_databricks_table`](https://dagster-component-ui.vercel.app/c/external_databricks_table) | external | Declare-only external asset (Databricks owns the data lifecycle). | `live` |

## Walkthroughs

| Path | Walkthrough |
|---|---|
| **Iceberg + Delta lakehouse (local FS)** | [`examples/lakehouse_local.md`](https://github.com/eric-thomas-dagster/dagster-community-components-cli/blob/main/examples/lakehouse_local.md) — exercises `dataframe_to_delta_table` + `delta_ingestion` + `delta_lake_io_manager` end-to-end without Databricks |
| **Pandas → Delta sink** | Covered in `examples/local_transforms.md` |

## Connection / auth — quick reference

| Surface | Auth |
|---|---|
| `databricks_resource` / `dataframe_to_databricks` / `databricks_workspace` | Personal Access Token (PAT) or service principal OAuth. `DATABRICKS_HOST` + `DATABRICKS_TOKEN` env vars. |
| Delta-rs sinks / sources (no warehouse needed) | Storage credentials only — S3 / ADLS / GCS keys for the underlying object store. |

The Delta-rs path is **fully local** — no Databricks workspace required. Useful for lakehouse demos that don't depend on a paid account.

## Gotchas

- **Pandas all-STRING DDL** — fixed in v0.8.5. Previously `dataframe_to_databricks`'s CREATE TABLE emitted every column as STRING, regardless of pandas dtype. Now maps pandas → Databricks types (BIGINT / DOUBLE / BOOLEAN / TIMESTAMP / STRING) properly.
- **Unity Catalog vs Hive metastore** — `databricks_workspace` queries Unity Catalog. If your workspace is still on the legacy Hive metastore, the discovery queries return empty.
- **delta-rs is the engine** — community Delta components don't require Spark / JVM. If you're already paying for Databricks compute and want to push transforms onto the cluster instead of local Python, use `dagster-databricks`'s `submit_run` path.
- **Databricks SQL connector quirks** — long-running queries can timeout silently; set a higher `query_timeout` on the `databricks_resource` for warehouse-scan-heavy workloads.

## Roadmap

- **Databricks Workflows job trigger asset** — fire a Workflows job from Dagster (today, customers use `dagster-databricks`'s `databricks_job_op`); a community asset-shaped wrapper would close the gap for declarative YAML.
- **MLflow tracking integration** — log Dagster runs to MLflow when Databricks ML is in use. Out of scope today.

## See also

- [Official `dagster-databricks` docs](https://docs.dagster.io/integrations/databricks)
- [Databricks REST API](https://docs.databricks.com/api/)
- [delta-rs docs](https://delta-io.github.io/delta-rs/) — the Rust-based Delta engine underneath the community sinks
