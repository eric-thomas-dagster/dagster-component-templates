# `SqlServerStoredProcedureWorkspaceComponent`

Wrap plain **T-SQL stored procedures** on an on-prem SQL Server estate as Dagster assets — the T-SQL equivalent of `ssis_workspace` (SSISDB packages), for shops that run ETL directly out of stored procedures instead of (or alongside) SSIS. Many SQL Server estates have both; this component and `ssis_workspace` are siblings, not alternatives.

## When to use this

- You have T-SQL stored procedures doing ETL on SQL Server, currently kicked off by SQL Server Agent jobs, and want Dagster to own their orchestration + observability.
- You're mid-migration off a legacy scheduler and want Dagster to run the stored procedures during parallel-run periods.
- You want stored procedures to appear in the same asset graph as downstream Snowflake mart tables, dbt models, and BI reports.

## Full workspace shape (parity with other workspace components)

- **`@public`** class + `StateBackedComponent` caching (discovery cached to disk via `write_state_to_path`; refresh with `dg utils refresh-defs-state`).
- **`assets_by_procedure:`** — per-procedure overrides keyed by qualified name (`<schema>.<procedure>`), including real run-order `deps:` between procedures (SQL Server has no native concept of inter-procedure dependency). Mirrors the official `dagster-databricks` `DatabricksWorkspaceComponent.assets_by_task_key` shape.
- **`procedure_selector:`** — filter by schema / include / exclude fnmatch patterns.
- **`action: execute` is subsettable** (`can_subset=true`) — materializing one procedure runs only that procedure (plus any `assets_by_procedure` dependency ahead of it, in dependency order), not every discovered procedure.
- **`polling_sensor:`** — poll `msdb.dbo.sysjobhistory` for SQL-Agent-triggered runs and emit `AssetObservation`.
- **`demo_mode:`** — skip the real SQL Server connection entirely; discovery, execute, and the sensor all run a synthetic/simulated path instead. Use to smoke-test the whole shape (including `can_subset` ordering and `assets_by_procedure`) with no SQL Server available.

## Connection options

Either supply a full SQLAlchemy URL via `connection_string_env_var`:

```yaml
connection:
  connection_string_env_var: SQL_SERVER_URL   # value e.g. mssql+pyodbc://svc:pw@sql-01/Canvas?driver=ODBC+Driver+18+for+SQL+Server
```

...or the flat fields (component builds the pyodbc URL):

```yaml
connection:
  server:   "{{ env.SQL_SERVER }}"
  database: Canvas
  user:     "{{ env.SQL_USER }}"
  password: "{{ env.SQL_PASSWORD }}"
  driver:   "ODBC Driver 18 for SQL Server"
  trust_server_certificate: true                 # only for lab servers
```

## assets_by_procedure — per-procedure overrides, dependencies, and parameters

```yaml
assets_by_procedure:
  dbo.LoadSilverClaims:
    key: silver/claims
    group_name: silver
    params:
      BusinessDate: "{partition_key}"
    sql_agent_job_name: "Nightly - Load Silver Claims"
  dbo.LoadGoldClaims:
    key: gold/claims
    deps: [silver/claims]
```

Keyed by the procedure's qualified name (`<schema>.<procedure>`). `key` / `group_name` / `description` / `kinds` / `owners` replace; `deps` / `metadata` / `tags` merge onto whatever the default spec already set. `params:` is a dict of `EXEC` parameter name → value, bound as real SQL parameters (never string-interpolated) — `{partition_key}` and `{run_id}` are substituted at execute time. `sql_agent_job_name` names the SQL Agent job wrapping this procedure (if any); it's required for `polling_sensor` coverage of this specific procedure — a procedure without it is simply skipped by the sensor, not an error (most stored procedures aren't wrapped in a SQL Agent job at all).

## Actions

- **`action: noop` (default)** — external asset shape. Every discovered procedure becomes a Dagster asset, but materialize does nothing. Use this when Dagster only observes the procedures (via `polling_sensor: true`) without triggering them.
- **`action: execute`** — materialize runs `EXEC [schema].[procedure] @Param = :value` (parameters from `assets_by_procedure.params`, bound, not interpolated). **Subsettable** (`can_subset=true`): materializing one procedure in the UI (or via `dg launch --select`) executes only that procedure — if an `assets_by_procedure` dependency of it is also selected, that dependency runs first; procedures outside the current selection never run.

## Observing SQL-Agent-triggered runs

A stored procedure has no catalog of its own run history the way SSISDB does for SSIS packages — the only way to see a run SQL Agent (not Dagster) triggered is via the job that wraps it:

```yaml
assets_by_procedure:
  dbo.LoadSilverClaims:
    sql_agent_job_name: "Nightly - Load Silver Claims"
polling_sensor: true
observation_interval_seconds: 300
```

The sensor queries `msdb.dbo.sysjobhistory` joined to `msdb.dbo.sysjobs`, filtered to each mapped procedure's job name, and emits `AssetObservation` for job-history rows (`step_id = 0`, the job outcome row) it hasn't seen before (cursor-based dedup by `instance_id`), with `run_status = 1` (succeeded). Procedures without a `sql_agent_job_name` set are not watched at all.

## Demo mode — no SQL Server required

`demo_mode: true` replaces every real SQL query with a synthetic/simulated equivalent:

- Discovery returns a fixed 2-procedure catalog (`dbo.LoadSilverClaims`, `dbo.LoadGoldClaims`) instead of querying `sys.procedures`.
- `action: execute` logs a simulated `EXEC` and always succeeds (deterministic, seeded on the procedure's own qualified name).
- The polling sensor emits a simulated job-history row per mapped procedure per tick instead of querying `msdb.dbo.sysjobhistory`.

Use it to exercise the whole component shape — `can_subset` ordering, `assets_by_procedure` overrides, the sensor — with no real SQL Server available. `_build_engine()` itself raises loudly if ever reached while `demo_mode: true`, as a backstop against a future code path silently falling through to a real connection attempt.

## Companion components

- **`ssis_workspace`** — the SSIS-package sibling for the same SQL Server estate; many shops run both stored procedures and SSIS packages side by side.
- **`snowflake_workspace` / `sql_transform`** — the natural downstream (a stored procedure lands data on-prem, a warehouse transform picks it up).
- **`mssql_io_manager`, `mssql_resource`** — lower-level MSSQL building blocks for custom assets this component doesn't cover.

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `connection` | `SqlServerConnectionConfig` | Connection to the SQL Server instance. |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `group_name` | `str` | `"sql_server"` | Dagster asset group. |
| `kinds` | `List[str]` | — | Asset kinds badge. Default: ['sqlserver', 'tsql']. |
| `tags` | `Union[Dict[str, Any], List[str]]` | — | Additional asset tags. |
| `owners` | `List[str]` | — | Asset owners (team names / email addresses). |

### Partitions

| Field | Type | Default | Description |
|---|---|---|---|
| `partition_type` | `str` | — | Partition type: 'daily'\|'weekly'\|'monthly'\|'hourly'\|'cron'\|'static'\|'dynamic', or None for unpartitioned. Applies the SAME partitions_def to every imported procedure. |
| `partition_start` | `str` | — | ISO start date for time-based partition types (e.g. '2024-01-01'). |
| `partition_cron_schedule` | `str` | — | Required when partition_type='cron'. A cron expression restricting the date axis to a custom cadence. |
| `partition_cron_fmt` | `str` | `"%Y-%m-%d"` | strftime format for partition keys produced by partition_type='cron'. |
| `partition_values` | `Union[str, List[str]]` | — | Comma-separated values for partition_type='static'. |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `demo_mode` | `bool` | `false` | Skip the real SQL Server connection entirely. Discovery returns a small synthetic procedure catalog (2 procedures); execute, the polling sensor, all log a simulated lifecycle instead of running real T-SQL. Use to smoke-t… _(full docs in schema.json + component README)_ |
| `procedure_selector` | `SqlServerProcedureSelector` | — | Filter which procedures become assets. Omit for every procedure. |
| `assets_by_procedure` | `Dict[str, Dict[str, Any]]` | — | Per-procedure overrides keyed by qualified name ('<schema>.<procedure>'). Mirrors the official dagster-databricks DatabricksWorkspaceComponent.assets_by_task_key shape. Per-procedure keys (all optional): key, group_name… _(full docs in schema.json + component README)_ |
| `action` | `str` | `"noop"` | noop (default) = external asset, declare-only, materialize does nothing. execute = materialize runs EXEC [schema].[procedure] (with assets_by_procedure.params bound as parameters, never string-interpolated). Subsettable… _(full docs in schema.json + component README)_ |
| `polling_sensor` | `bool` | `false` | If true, adds ONE sensor (not one per procedure) that polls msdb.dbo.sysjobhistory for completions of each procedure's sql_agent_job_name (set via assets_by_procedure) and emits AssetObservation for runs Dagster did not… _(full docs in schema.json + component README)_ |
| `observation_interval_seconds` | `int` | `300` | Seconds between sensor polls. Only used when polling_sensor=True. |
| `asset_key_prefix` | `List[str]` | — | Prefix for every emitted asset key. Default: ['sqlserver', <server-host>]. |
| `dynamic_partition_name` | `str` | — | Name for DynamicPartitionsDefinition (when partition_type='dynamic'). |
| `defs_state` | `ResolvedDefsStateConfig` | `DefsStateConfigArgs.local_filesystem()` | — |

[//]: # (FIELDS:END)
