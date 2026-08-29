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
- **`package_selector:`** — filter by folder / project / include / exclude fnmatch patterns.
- **`package_overrides:`** — runtime parameters + environment references applied at execute time.
- **`polling_sensor:`** — poll `SSISDB.catalog.executions` for completions and emit `AssetObservation`.
- **`freshness_lag_threshold_seconds:`** — attach an asset check per package that fails when the last successful execution is older than N seconds.

## Backing SQL

All queries hit SSISDB system views + stored procs:

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
- **`action: execute`** — materialize actually runs the package via `SSISDB.catalog.create_execution + set_execution_parameter_value + start_execution`. With `wait_for_completion: true`, the op blocks until the execution reaches a terminal SSIS status (Succeeded / Failed / Canceled / Ended unexpectedly / Completed). On failure, the op raises `dg.Failure` with the top 5 error messages from `SSISDB.catalog.operation_messages`.

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

### Connection

| Field | Type | Default | Description |
|---|---|---|---|
| `connection_string_env_var` | `str` | — | Env var containing a full SQLAlchemy URL. Takes precedence over the flat fields. Example value: `mssql+pyodbc://svc:pw@sql-01/SSISDB?driver=ODBC+Driver+18+for+SQL+Server`. |
| `server` | `str` | — | SQL Server host / instance (e.g. `sql-01.corp` or `sql-01\INST1`). |
| `database` | `str` | `"SSISDB"` | SSIS catalog database. Almost always `SSISDB`. |
| `user` | `str` | — | SQL login username (basic auth). |
| `password` | `str` | — | SQL login password (basic auth). |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `driver` | `str` | `"ODBC Driver 18 for SQL Server"` | ODBC driver name for the mssql+pyodbc dialect. |
| `trust_server_certificate` | `bool` | `false` | Passes `TrustServerCertificate=yes` on the connection — needed for lab SQL Servers with self-signed TLS. |

[//]: # (FIELDS:END)
