# `SsisWorkspaceComponent`

Wrap **SQL Server Integration Services (SSIS)** behind a Dagster workspace-shape component. Discovers every package deployed to SSISDB and emits one Dagster asset per package. On materialize, optionally invokes `SSISDB.catalog.create_execution` + `set_execution_parameter_value` + `start_execution` and polls to completion.

The missing "orchestrate SSIS from Dagster" primitive for every SQL-Server enterprise still running legacy ETL packages.

## When to use this

- You have SSIS packages deployed to SSISDB, currently kicked off by SQL Server Agent jobs, and want Dagster to own their orchestration + observability.
- You're mid-migration off SSIS and want Dagster to run the legacy packages during parallel-run periods while newer transforms move to dbt / Snowpark / etc.
- You want SSIS packages to appear in the same asset graph as downstream Snowflake mart tables, dbt models, and Power BI reports.

## Full workspace shape (parity with other workspace components)

- **`@public`** class + `StateBackedComponent` caching (discovery cached to disk via `write_state_to_path`; refresh with `dg utils refresh-defs-state`).
- **`translation:` callable** for per-asset customization (rename, tag by folder, add owners, per-project group_name).
- **`assets_by_package:`** — per-package overrides keyed by qualified name (`<folder>/<project>/<package>`), including real run-order `deps:` between packages. Mirrors the official `dagster-databricks` `DatabricksWorkspaceComponent.assets_by_task_key` shape (same convention `snowflake_workspace.assets_by_name` already uses in this registry).
- **`package_selector:`** — filter by folder / project / include / exclude fnmatch patterns.
- **`package_overrides:`** — runtime parameters + environment references applied at execute time.
- **`action: execute` is subsettable** (`can_subset=true`) — materializing one package runs only that package (plus any `assets_by_package` dependency ahead of it, in dependency order), not every discovered package.
- **`polling_sensor:`** — poll `SSISDB.catalog.executions` for completions and emit `AssetObservation`.
- **`freshness_lag_threshold_seconds:`** — attach an asset check per package that fails when the last successful execution is older than N seconds (staleness).
- **`max_execution_minutes:`** — attach a `ssis_completed_within_window` asset check per package that fails when the latest successful execution took longer than N minutes to run (a runtime/SLA check -- a package can be perfectly fresh and still have blown its runtime budget; neither check catches the other's failure mode).
- **`demo_mode:`** — skip the real SQL Server connection entirely; discovery, execute, the sensor, and both checks all run a synthetic/simulated path instead. Use to smoke-test the whole shape (including `can_subset` ordering and `assets_by_package`) with no SSISDB available.

## Demo mode — no SSISDB required

`demo_mode: true` replaces every real SQL query with a synthetic/simulated equivalent:

- Discovery returns a fixed 2-package catalog (`Demo/DemoETL/ExtractCustomers`, `Demo/DemoETL/LoadOrders`) instead of querying `SSISDB.catalog.packages`.
- `action: execute` logs a simulated create_execution/start_execution lifecycle and always succeeds (deterministic, seeded on the package's own qualified name).
- The observation sensor emits one simulated `AssetObservation` per tick, cycling through packages, instead of querying `SSISDB.catalog.executions`.
- Freshness checks always pass with a simulated zero-lag result.

Use it to exercise the whole component shape — `can_subset` ordering, `assets_by_package` overrides, the sensor, the checks — with no real SQL Server available. `_build_engine()` itself raises loudly if ever reached while `demo_mode: true`, as a backstop against a future code path silently falling through to a real connection attempt.

## Backing SQL

All queries hit SSISDB system views + stored procs (bypassed entirely by `demo_mode: true`):

| Query | Purpose |
|---|---|
| `SELECT * FROM SSISDB.catalog.packages JOIN projects JOIN folders` | Package enumeration for discovery. |
| `EXEC SSISDB.catalog.create_execution + start_execution` | Trigger a package. |
| `EXEC SSISDB.catalog.set_execution_parameter_value` | Runtime parameter override (per parameter). |
| `SELECT reference_id FROM SSISDB.catalog.environment_references` | Environment binding lookup. |
| `SELECT status FROM SSISDB.catalog.executions WHERE execution_id = ?` | Poll for completion. |
| `SELECT TOP 5 message FROM SSISDB.catalog.operation_messages WHERE operation_id = ? AND message_type = 120` | Surface error messages to Dagster metadata on failure. |
| `SELECT TOP 1 end_time FROM executions WHERE ... AND status = 7 ORDER BY end_time DESC` | Freshness check per package. |

## Required SQL permissions

The connecting login needs:
- **`VIEW ANY DATABASE`** or the **`ssis_admin`** role — for enumeration + polling.
- **`ssis_admin`** (or DBO on SSISDB) — if you set `action: execute`.

## Connection options

Either supply a full SQLAlchemy URL via `connection_string_env_var`:

```yaml
workspace:
  connection_string_env_var: SSISDB_MSSQL_URL   # value e.g. mssql+pyodbc://svc:pw@sql-01/SSISDB?driver=ODBC+Driver+18+for+SQL+Server
```

...or the flat fields (component builds the pyodbc URL):

```yaml
workspace:
  server:   "{{ env.SSIS_SERVER }}"
  database: SSISDB
  user:     "{{ env.SSIS_USER }}"
  password: "{{ env.SSIS_PASSWORD }}"
  driver:   "ODBC Driver 18 for SQL Server"
  trust_server_certificate: true                 # only for lab servers
```

## Runtime parameters + environments

Real SSIS packages take parameters — config file paths, target schemas, batch dates, extract cutoffs. `package_overrides:` applies them per-package (or per-project) via `SSISDB.catalog.set_execution_parameter_value` at execute time. If an SSISDB **environment** is bound to the project, reference it by name and SSIS substitutes the environment's variable bundle over matching parameters.

```yaml
package_overrides:
  # per-package parameters
  - match: "Sales/*/LoadCustomers"
    parameters:
      Extract.TargetSchema: "raw_sales"
      Extract.BatchDate:    "{{ '{partition_key}' }}"    # substituted at run time
      Extract.RowLimit:     50000
    scope: package                    # (default) — object_type=30
    environment_reference: "PROD"     # optional; environment must exist in the project's folder

  # project-scoped connection override
  - match: "Sales/*/*"
    parameters:
      CM.DEST_DB.ConnectionString: "Data Source=snowflake-01;..."
    scope: project                    # object_type=20
```

The **`{partition_key}`** token in a string value substitutes at run time (via `str.format`) — pair with `PartitionsDefinition` on the asset (via `translation:`) for per-partition SSIS runs.

## Actions

- **`action: noop` (default)** — external asset shape. Every SSIS package becomes a Dagster asset, but materialize does nothing. Use this when Dagster observes SSIS (via `polling_sensor: true` + `freshness_lag_threshold_seconds:`) without triggering it.
- **`action: execute`** — materialize actually runs the package via `SSISDB.catalog.create_execution + set_execution_parameter_value + start_execution`. With `wait_for_completion: true`, the op blocks until the execution reaches a terminal SSIS status (Succeeded / Failed / Canceled / Ended unexpectedly / Completed). On failure, the op raises `dg.Failure` with the top 5 error messages from `SSISDB.catalog.operation_messages`. **Subsettable** (`can_subset=true`): materializing one package in the UI (or via `dg launch --select`) executes only that package — if an `assets_by_package` dependency of it is also selected, that dependency runs first; packages outside the current selection never run.

## assets_by_package — per-package overrides and dependencies

SSISDB has no native concept of inter-package dependency, so `deps:` here is the only way to declare one:

```yaml
assets_by_package:
  Sales/SalesETL/LoadOrders:
    deps: [ssis/sql-01/Sales/SalesETL/ExtractCustomers]
    group_name: sales_nightly
    tags: {tier: critical}
```

Keyed by the package's qualified name (`<folder>/<project>/<package>`, matching the asset key's own folder/project/package suffix). `key` / `group_name` / `description` / `kinds` / `owners` replace; `deps` / `metadata` / `tags` merge onto whatever the base translator (or `translation:`) already set. Mirrors the official `dagster-databricks` `DatabricksWorkspaceComponent.assets_by_task_key` shape.

## SSIS status codes

Values in `SSISDB.catalog.executions.status`:

| Code | Meaning |
|---|---|
| 1 | Created |
| 2 | Running |
| 3 | Canceled |
| 4 | Failed |
| 5 | Pending |
| 6 | Ended unexpectedly |
| 7 | **Succeeded** |
| 8 | Stopping |
| 9 | Completed |

`{3, 4, 6, 7, 9}` are terminal — the poll loop stops on any of them. Only `7` means success.

## Companion components

- **`snowflake_workspace`** — the natural downstream (SSIS lands data in a table, Snowflake mart reads it).
- **`hvr_hub_workspace`** — for replication that SSIS doesn't cover well (CDC).
- **`dbt_cloud_trigger_job`** — the "then trigger dbt Cloud" step after SSIS lands its data.
- **`talend_cloud_workspace`, `informatica_workspace`** — companion components for the other big legacy-ETL vendors.

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `workspace` | `SsisWorkspaceConfig` | Connection to the SQL Server hosting SSISDB. |

### Execution

| Field | Type | Default | Description |
|---|---|---|---|
| `wait_for_completion` | `bool` | `true` | Only used when `action: execute`. If true, poll executions until a terminal status; if false, return once SSIS accepts the trigger. |
| `poll_interval_seconds` | `int` | `30` | Seconds between polls of `SSISDB.catalog.executions.status`. |
| `timeout_seconds` | `int` | `3600` | Overall timeout for a synchronous execution. Set 0 for no timeout. |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `group_name` | `str` | `"ssis"` | Dagster asset group. |
| `kinds` | `List[str]` | — | Asset kinds badge (Dagster catalog UI). Default: `['ssis', 'mssql']`. |
| `tags` | `Union[Dict[str, Any], List[str]]` | — | Additional asset tags. |
| `owners` | `List[str]` | — | Asset owners (team names / email addresses). |

### Freshness

| Field | Type | Default | Description |
|---|---|---|---|
| `freshness_lag_threshold_seconds` | `int` | — | If set, attach an asset check to every package that fails when the last successful execution is older than this many seconds. |

### Partitions

| Field | Type | Default | Description |
|---|---|---|---|
| `partition_type` | `str` | — | Applies the SAME partitions_def to every imported package: 'daily'\|'weekly'\|'monthly'\|'hourly'\|'static'\|'dynamic', or None for unpartitioned (default). Real per-package execution history (`SSISDB.catalog.executions`… _(full docs in schema.json + component README)_ |
| `partition_cron_schedule` | `str` | — | Required when partition_type='cron'. A cron expression restricting the date axis to a custom cadence, e.g. '0 0 * * 1-5' for weekdays only (business-day cadence). Built on Dagster's TimeWindowPartitionsDefinition(cron_schedule=...). |
| `partition_cron_fmt` | `str` | `"%Y-%m-%d"` | strftime format for partition keys produced by partition_type='cron'. Only change this for a sub-daily cron_schedule. |
| `partition_start` | `str` | — | ISO start date for time-based partition types (e.g. '2024-01-01'). |
| `partition_values` | `Union[str, List[str]]` | — | Comma-separated values for partition_type='static'. |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `demo_mode` | `bool` | `false` | Skip the real SQL Server connection entirely. Discovery returns a small synthetic package catalog (2 packages across 1 folder/project); execute/poll, the observation sensor, and freshness checks all log a simulated SSIS… _(full docs in schema.json + component README)_ |
| `package_selector` | `SsisPackageSelector` | — | Filter which packages become assets. Default = every package in SSISDB. |
| `package_overrides` | `List[SsisPackageOverride]` | — | Runtime parameters + environment references applied at execute time via `SSISDB.catalog.set_execution_parameter_value` (and, when set, `create_environment_reference`). Only used when `action: execute`. Multiple overrides… _(full docs in schema.json + component README)_ |
| `assets_by_package` | `Dict[str, Dict[str, Any]]` | — | Override the asset spec for specific packages, keyed by qualified name '<folder>/<project>/<package>'. Mirrors the official dagster-databricks `DatabricksWorkspaceComponent.assets_by_task_key` shape (same convention `sno… _(full docs in schema.json + component README)_ |
| `translation` | `TranslationFn[SsisPackageProps]` | — | Optional per-asset translation callable. Receives a SsisPackageProps and returns a dict of AssetSpec kwargs (key / group_name / tags / owners / kinds / metadata / description). Use for per-folder / per-project customizat… _(full docs in schema.json + component README)_ |
| `action` | `str` | `"noop"` | What materialize() does. `noop` = external asset (declare-only). `execute` = call `SSISDB.catalog.create_execution` + `start_execution`. |
| `polling_sensor` | `bool` | `false` | If true, emit a sensor that polls SSISDB.catalog.executions for completions since watermark and emits AssetObservation per package. |
| `observation_interval_seconds` | `int` | `300` | Sensor tick interval. Only used when `polling_sensor: true`. |
| `max_execution_minutes` | `int` | — | If set, attach a `ssis_completed_within_window` asset check to every package that fails when the package's LATEST successful execution took longer than this many minutes (DATEDIFF(MINUTE, start_time, end_time) from SSISD… _(full docs in schema.json + component README)_ |
| `asset_key_prefix` | `List[str]` | — | Prefix for every emitted asset key. Default: `['ssis', <ssis_server_hostname>]`. |
| `defs_state` | `ResolvedDefsStateConfig` | `DefsStateConfigArgs.local_filesystem()` | StateBackedComponent state config. Default: local filesystem cache keyed on server+database hash so multiple SSIS servers don't collide. |
| `dynamic_partition_name` | `str` | — | Name for DynamicPartitionsDefinition (when partition_type='dynamic'). |

[//]: # (FIELDS:END)
