# Snowflake

The community Snowflake surface is **29 components** covering the entire Snowflake account — warehouse + Snowpark + dynamic tables + Cortex (LLM + Search) + Iceberg + OpenFlow + Snowpipe + alerts + time travel + tasks + stored procedures + streams. Designed to live alongside the official `dagster-snowflake` integration: official handles the resource + IO-manager + dbt-compatible SQL flow; community fills the long tail (workspace auto-discovery, every native Snowflake object as a real Dagster asset, observation sensors, single-entity wrappers for click-to-materialize demos).

## Official Dagster integration (`dagster-snowflake`)

For the resource + IO-manager + standard SQL execution path, prefer the **official `dagster-snowflake`** package. It's the maintained, packaged path with first-class support.

The community components below are **complements, not replacements** — they cover:
- Auto-discovery (`snowflake_workspace` imports tasks / procs / DTs / streams / pipes / alerts / OpenFlow flows / stages / MVs / external tables as Dagster assets)
- Single-entity Dagster-asset wrappers (`snowflake_task_execute_asset`, `snowflake_dynamic_table_refresh_asset`, `snowflake_stored_procedure_call_asset`) — click-to-materialize-one-thing UX
- Native Snowflake objects (DT, MV, Iceberg, Cortex Search) as first-class Dagster assets
- Observation sensors (task completion, DT refresh, Snowpipe load, table activity)

## Components — by sub-area

### Workspace auto-discovery (the big one)

| Component | Category | What it does | Validation |
|---|---|---|---|
| [`snowflake_workspace`](https://dagster-component-ui.vercel.app/c/snowflake_workspace) | integration | Imports every entity in a database/schema as Dagster assets — tasks, dynamic tables, stored procedures, streams, snowpipes, stages, alerts, materialized views, external tables, OpenFlow flows. Pairs with a built-in observation sensor + a dedicated DT-refresh sensor. Per-entity `assets_by_name` overrides, multi-instance dispatch, `config_schema:` for launchpad-overridable task config. | `code` (eager-creds path — needs real account to fully L1) |

### Resources + IO managers

| Component | Category | What it does | Validation |
|---|---|---|---|
| [`snowflake_resource`](https://dagster-component-ui.vercel.app/c/snowflake_resource) | resource | Connection resource | `code` |
| [`snowflake_io_manager`](https://dagster-component-ui.vercel.app/c/snowflake_io_manager) | io_manager | Pandas → Snowflake | `code` |
| [`snowflake_polars_io_manager`](https://dagster-component-ui.vercel.app/c/snowflake_polars_io_manager) | io_manager | Polars → Snowflake | `code` |
| [`snowflake_pyspark_io_manager`](https://dagster-component-ui.vercel.app/c/snowflake_pyspark_io_manager) | io_manager | PySpark → Snowflake | `code` |

### Single-entity asset wrappers (click-to-materialize one specific entity)

| Component | What | Validation |
|---|---|---|
| [`snowflake_task_execute_asset`](https://dagster-component-ui.vercel.app/c/snowflake_task_execute_asset) | EXECUTE TASK \<task\> on materialize | `code` |
| [`snowflake_stored_procedure_call_asset`](https://dagster-component-ui.vercel.app/c/snowflake_stored_procedure_call_asset) | CALL \<proc\>(args...) on materialize | `code` |
| [`snowflake_dynamic_table_refresh_asset`](https://dagster-component-ui.vercel.app/c/snowflake_dynamic_table_refresh_asset) | ALTER DYNAMIC TABLE \<dt\> REFRESH on materialize | `code` |
| [`snowflake_time_travel_asset`](https://dagster-component-ui.vercel.app/c/snowflake_time_travel_asset) | Query a table AT(OFFSET => -N\*60) | `code` |

### Native Snowflake objects as Dagster assets

| Component | What | Validation |
|---|---|---|
| [`snowflake_task`](https://dagster-component-ui.vercel.app/c/snowflake_task) | One specific task | `code` |
| [`snowflake_stored_procedure`](https://dagster-component-ui.vercel.app/c/snowflake_stored_procedure) | One specific proc | `code` |
| [`snowflake_dynamic_table`](https://dagster-component-ui.vercel.app/c/snowflake_dynamic_table) | One specific dynamic table | `code` |
| [`snowflake_materialized_view`](https://dagster-component-ui.vercel.app/c/snowflake_materialized_view) | MV | `code` |
| [`snowflake_iceberg_table`](https://dagster-component-ui.vercel.app/c/snowflake_iceberg_table) | Iceberg-backed Snowflake table | `code` |
| [`snowflake_stream`](https://dagster-component-ui.vercel.app/c/snowflake_stream) | CDC stream | `code` |
| [`snowflake_snowpipe`](https://dagster-component-ui.vercel.app/c/snowflake_snowpipe) | Continuous-ingestion pipe | `code` |
| [`snowflake_alert`](https://dagster-component-ui.vercel.app/c/snowflake_alert) | Alert | `code` |

### AI / Cortex

| Component | What | Validation |
|---|---|---|
| [`snowflake_cortex_asset`](https://dagster-component-ui.vercel.app/c/snowflake_cortex_asset) | SNOWFLAKE.CORTEX.COMPLETE LLM call | `code` |
| [`snowflake_cortex_search`](https://dagster-component-ui.vercel.app/c/snowflake_cortex_search) | Cortex Search Service backed asset | `code` |

### Snowpark

| Component | What | Validation |
|---|---|---|
| [`snowpark_pipeline`](https://dagster-component-ui.vercel.app/c/snowpark_pipeline) | Run a Snowpark DataFrame pipeline as an asset | `code` |

### Sensors + observation

| Component | What | Validation |
|---|---|---|
| [`snowflake_task_completion_sensor`](https://dagster-component-ui.vercel.app/c/snowflake_task_completion_sensor) | TASK_HISTORY-based completion sensor | `code` |
| [`snowflake_snowpipe_load_sensor`](https://dagster-component-ui.vercel.app/c/snowflake_snowpipe_load_sensor) | COPY_HISTORY-based load sensor | `code` |
| [`snowflake_table_observation_sensor`](https://dagster-component-ui.vercel.app/c/snowflake_table_observation_sensor) | Periodic table-row-count observation | `code` |
| [`snowflake_openflow_status_sensor`](https://dagster-component-ui.vercel.app/c/snowflake_openflow_status_sensor) | OpenFlow telemetry events | `code` |

### Sinks + external + ingestion

| Component | What | Validation |
|---|---|---|
| [`dataframe_to_snowflake`](https://dagster-component-ui.vercel.app/c/dataframe_to_snowflake) | Pandas DataFrame → Snowflake table (write_pandas) | `live` |
| [`dataframe_to_snowflake_bulk`](https://dagster-component-ui.vercel.app/c/dataframe_to_snowflake_bulk) | Memory-efficient bulk: parquet → PUT to internal stage → COPY INTO | `code` |
| [`external_snowflake_table`](https://dagster-component-ui.vercel.app/c/external_snowflake_table) | Declare-only external asset | `live` |
| [`external_snowflake_openflow_flow`](https://dagster-component-ui.vercel.app/c/external_snowflake_openflow_flow) | Declare-only OpenFlow flow as external asset | `code` |
| [`snowflake_access_history_ingestion`](https://dagster-component-ui.vercel.app/c/snowflake_access_history_ingestion) | Ingest ACCOUNT_USAGE.ACCESS_HISTORY for governance / audit | `code` |

## Walkthroughs

| Path | Walkthrough |
|---|---|
| **Snowflake workspace end-to-end** | [`examples/snowflake_workspace.md`](https://github.com/eric-thomas-dagster/dagster-community-components-cli/blob/main/examples/snowflake_workspace.md) |
| **Sandboxed-account capability probes + auto-skip** | [`examples/snowflake_demo_account_requirements.md`](https://github.com/eric-thomas-dagster/dagster-community-components-cli/blob/main/examples/snowflake_demo_account_requirements.md) |

## Connection / auth — quick reference

Three auth modes supported across the workspace + single-entity components:

| Mode | When | Setup |
|---|---|---|
| **Password** | Dev / sandbox | `password_env_var: SNOWFLAKE_PASSWORD` |
| **PAT (Programmatic Access Token)** | Service accounts, CI | `pat_env_var: SNOWFLAKE_PAT` — token attaches to a network policy |
| **Keypair JWT** | Headless production | `private_key_path_env_var: SF_PRIVATE_KEY_PATH` + `ALTER USER ... SET RSA_PUBLIC_KEY=...` once |

See `examples/snowflake_demo_account_requirements.md` for the **partnership-ask paragraph** to give your Snowflake AE if your sandbox account is missing the privileges + tier features the full demo exercises.

## Gotchas

- **`SHOW ...` vs `INFORMATION_SCHEMA.*`** — most workspace queries use `SHOW <object>` because `INFORMATION_SCHEMA` is invisible to least-privilege roles. SHOW only needs USAGE on the schema + any privilege on the object. The few `INFORMATION_SCHEMA` function calls (TASK_HISTORY, COPY_HISTORY) are wrapped in try/except so the action succeeds even if the role can't read history.
- **Column-name version drift** — `SHOW DYNAMIC TABLES` columns changed in 2024 (`last_completed_refresh_*` is now canonical; older accounts return `last_refresh_*`). The DT-refresh sensor probes both shapes; `data_timestamp` is the universal fallback when status columns are absent entirely.
- **COPY_HISTORY** — `TABLE_NAME` parameter takes the COPY *target* table, NOT the pipe name. The workspace parses target-table from the pipe DEFINITION at import time. Status returns `'Loaded'` (mixed case), and pipe_name can be qualified or unqualified depending on the executing role's schema context — sensor filters use `UPPER(...)` + accept both.
- **Tier features** — Materialized Views need Enterprise; Hybrid Tables need `ENABLE_UNISTORE_FEATURES=TRUE`; Cortex needs the right region (or `CORTEX_ENABLED_CROSS_REGION='AWS_US'`); replication needs Business Critical. The demo gracefully skips capabilities the account doesn't have.

## Roadmap (deferred)

- **DT-refresh sensor refactor to INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY** — stable cross-version columns; replaces the current SHOW DYNAMIC TABLES probe-and-fallback approach.
- **Resource Monitor as a Dagster asset** — surface account credit consumption.
- **Data Sharing (outbound)** — declare Snowflake shares as Dagster-orchestrated assets.

## See also

- [Official `dagster-snowflake` docs](https://docs.dagster.io/integrations/snowflake)
- [Snowflake REST + SQL docs](https://docs.snowflake.com/)
- [`examples/snowflake_workspace.md`](https://github.com/eric-thomas-dagster/dagster-community-components-cli/blob/main/examples/snowflake_workspace.md)
