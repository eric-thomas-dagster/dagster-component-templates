# SQL Asset

Runs one or more SQL statements against **any already-registered database resource that exposes
`.get_connection()`** — one component instead of a dialect-specific `<dialect>_sql_asset` per
resource type.

## Which resources this works with

Any resource in this repo whose component already provides `.get_connection()` (returning a
DBAPI connection, directly or via its own `@contextmanager`):

`postgres_resource`, `mysql_resource`, `duckdb_resource`, `databricks_sql_resource`,
`mssql_resource`, `oracle_resource`, `db2_resource`, `doris_resource`, `starburst_resource`,
`starrocks_resource`, `timescaledb_resource`, `trino_resource`.

**Snowflake is intentionally not included.** [`snowflake_sql_asset`](https://dagster-component-ui.vercel.app/c/snowflake_sql_asset)
covers it separately, with Snowflake-specific niceties (real `num_statements` auto-multi-statement
splitting, `%(name)s` pyformat parameters, `USE WAREHOUSE/DATABASE/SCHEMA/ROLE` session fields)
that only make sense for that one driver — generalizing them here would mean faking a
cross-driver guarantee that doesn't actually exist.

## What's intentionally driver-dependent, not invented

- **`parameters`** bind values are passed straight to `cursor.execute(stmt, parameters)`. The
  placeholder style in your SQL (`%(name)s`, `:name`, `?`, …) is whatever your driver's DBAPI
  paramstyle is — this component doesn't translate between them.
- **Multi-statement text in one `sql` string** works for drivers that happen to support it
  (e.g. psycopg2, duckdb) and silently runs only the first statement on drivers that don't. Use
  `sql_statements` (a list, one `execute()` call per entry) when you need multiple statements to
  reliably all run, regardless of driver.
- **Session setup** (`USE ...` / `SET ...`) is written by you via `pre_sql`, in your dialect's own
  syntax — there's no structured warehouse/database/schema/role field set, because that shape is
  Snowflake-specific, not universal.

## `pool` / retry policy / `timeout_seconds`

Same shape as `snowflake_sql_asset`:

```yaml
pool: warehouse_concurrency      # Dagster's native named concurrency pool
retry_policy_max_retries: 3      # retries transient connection failures, warehouse resume delays, etc.
retry_policy_delay_seconds: 2
retry_policy_backoff: exponential  # or 'linear'
timeout_seconds: 300             # kills the step's wait (not the query itself) past this many seconds
```

`timeout_seconds` is enforced by a background-thread wrapper, not a driver-level query timeout:
the underlying query may keep running server-side past the deadline (Python threads can't be
forcibly killed, and this doesn't cancel the query), but the Dagster step fails and moves on.

## `connection_env` / `dry_run` — load/preview before the connection is wired up

`resource_key` needs an already-registered Dagster resource under that key -- and because
`context.resources` access requires declaring that key in `required_resource_keys`, Dagster
itself validates it's registered when the REPOSITORY loads (`dg check defs`, `dg dev`, any code
location load), not when the asset runs. A project that hasn't wired up that resource yet can't
load at all. Two ways around this, for exactly that situation:

```yaml
# Option 1: connect directly via a SQLAlchemy URL, no Dagster resource needed at all
connection_env: ANALYTICS_DB_URL   # env var holding e.g. 'postgresql://user:pass@host/db'

# Option 2: preview the SQL with no connection attempted at all
dry_run: true
```

`connection_env` never touches Dagster's resource system -- this asset's definitions always load
cleanly regardless of whether the env var is set yet; a missing/empty env var only fails at
materialization time, with a message naming the env var. `dry_run` logs the fully-resolved SQL
(`pre_sql` + the main statement(s)) and returns without opening any connection at all -- useful
to sanity-check a chain before any resource or `connection_env` is configured. Set exactly one of
`resource_key` / `connection_env` (unless `dry_run` is `true`, which needs neither).

**The constraint `resource_key` itself can't avoid** (confirmed against Dagster's own execution
model, not assumed): an op/asset can only read `context.resources.<key>` for a key it declared in
`required_resource_keys`, and Dagster never binds an unrequired resource into `context.resources`
at run time, even if one was passed to `resources=`. A fallback resource this component might
register under the same key, to paper over a not-yet-configured project, would instead COLLIDE
with the project's real one the moment it's configured (`Definitions.merge` raises on two
different resource defs sharing a key) -- worse than today's behavior, not better. So using
`resource_key` genuinely requires that resource registered for this asset's definitions to load,
full stop; `connection_env` or `dry_run` are the ways to defer that.

## `run_window` — skip cleanly, don't fail, outside a time window

```yaml
run_window: "09:00-17:00 America/New_York mon,tue,wed,thu,fri"
```

Checked once per materialization, before any statement runs. Outside the window, the asset
**skips cleanly** — no SQL runs, the run succeeds, and real downstream dependents skip too,
rather than failing or blocking the DAG. A malformed spec fails open (always runs) rather than
silently blocking the SQL forever. Unset (default): always runs.

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `asset_key` | `str` | Dagster asset key (e.g. 'warehouse/transforms/rollup'). |

### Connection

| Field | Type | Default | Description |
|---|---|---|---|
| `resource_key` | `str` | — | Key of a registered resource that exposes .get_connection() returning a DBAPI connection (directly, or via its own @contextmanager). Matches postgres_resource, mysql_resource, duckdb_resource, databricks_sql_resource, ms… _(full docs in schema.json + component README)_ |

### Execution

| Field | Type | Default | Description |
|---|---|---|---|
| `parameters` | `Dict[str, Any]` | — | Bind parameters applied to every statement. Placeholder style is whatever the underlying driver's DBAPI paramstyle is (e.g. pyformat %(name)s for psycopg2/mysql-connector, qmark ? or named :name for pyodbc/oracledb) -- p… _(full docs in schema.json + component README)_ |
| `timeout_seconds` | `float` | — | Kill the wait and fail the run if the SQL hasn't finished after this many seconds. Enforced via a background-thread wrapper -- the underlying query may continue running past the deadline (Python threads can't be forcibly… _(full docs in schema.json + component README)_ |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `group_name` | `str` | `"data_warehouse"` | Dagster asset group name. |
| `description` | `str` | — | Asset description. |
| `deps` | `List[str]` | — | Upstream Dagster asset keys for lineage. |
| `owners` | `List[str]` | — | Asset owners -- team names ('team:analytics') or email addresses. |
| `asset_tags` | `Union[Dict[str, Any], List[str]]` | — | Additional key-value tags applied to the asset in the Dagster catalog. |
| `kinds` | `List[str]` | — | Asset kinds for the catalog (e.g. ['postgres']). Auto-includes 'sql' if empty. |

### Retry policy

| Field | Type | Default | Description |
|---|---|---|---|
| `retry_policy_max_retries` | `int` | — | Max retries on asset failure. Defines a RetryPolicy. Useful for transient connection failures, warehouse resume delays, etc. |
| `retry_policy_delay_seconds` | `int` | — | Seconds between retries (default 1). |
| `retry_policy_backoff` | `str` | `"exponential"` | Backoff strategy: 'linear' or 'exponential'. |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `connection_env` | `str` | — | Env var holding a SQLAlchemy connection URL (e.g. 'postgresql://user:pass@host/db'), read at RUN time and connected via sqlalchemy.create_engine(url).raw_connection() -- a plain DBAPI connection, same shape resource_key'… _(full docs in schema.json + component README)_ |
| `dry_run` | `bool` | `false` | Log the fully-resolved SQL (pre_sql + the main statement(s)) and skip actually running it -- no resource is looked up, no SQLAlchemy engine is created, no connection is opened at all. Output metadata always includes dry_… _(full docs in schema.json + component README)_ |
| `sql` | `str` | — | SQL text, run in a single cursor.execute() call. Whether multiple ;-separated statements run in one call is driver-dependent (works for psycopg2/duckdb; most other drivers run only the first). Set exactly one of sql / sq… _(full docs in schema.json + component README)_ |
| `sql_file` | `str` | — | Path (relative to the Dagster process working directory) to a .sql file, read and treated the same as `sql`. Set exactly one of sql / sql_file / sql_statements. |
| `sql_statements` | `List[str]` | — | Explicit list of statements, each run as its own cursor.execute() call -- the one way to run multiple statements that works regardless of driver. Set exactly one of sql / sql_file / sql_statements. |
| `pre_sql` | `List[str]` | — | Dialect-specific statements run before the main SQL, e.g. ['SET search_path TO analytics'] (Postgres) or ['USE mydb'] (MySQL) -- write whatever your dialect needs; this component doesn't guess it for you. |
| `pool` | `str` | — | Dagster's native named concurrency pool (e.g. Airflow's `pool` -- a fixed number of slots shared across every asset/op using the same pool name, e.g. warehouse concurrency limits). The pool's own slot count is configured… _(full docs in schema.json + component README)_ |
| `run_window` | `str` | — | 'HH:MM-HH:MM tz [day,day,...]' (e.g. '09:00-17:00 America/New_York mon,tue,wed,thu,fri'). Outside this window, the asset SKIPS cleanly instead of running or failing -- no SQL runs, and real downstream dependents skip too… _(full docs in schema.json + component README)_ |

[//]: # (FIELDS:END)

## Example

```yaml
type: dagster_community_components.SqlAssetComponent
attributes:
  asset_key: warehouse/transforms/customer_rollup
  resource_key: analytics_postgres
  sql_statements:
    - "DELETE FROM analytics.customer_rollup WHERE updated_at < NOW() - INTERVAL '1 day'"
    - "INSERT INTO analytics.customer_rollup SELECT * FROM raw.customers WHERE updated_at >= NOW() - INTERVAL '1 day'"
  pre_sql:
    - "SET search_path TO analytics"
  group_name: data_warehouse
```

`resource_key` must point to an already-registered resource component instance, e.g. a
`postgres_resource` elsewhere in the same project.

## Requirements

```
dagster
sqlalchemy
```

`sqlalchemy` is only imported when `connection_env` is actually used. Using `resource_key`
instead needs whatever driver that resource already requires (e.g. `psycopg2-binary` for
`postgres_resource`) — not an extra dependency of this component itself.

## See also

- [`snowflake_sql_asset`](https://dagster-component-ui.vercel.app/c/snowflake_sql_asset) — Snowflake-specific equivalent with multi-statement/session-override support
- [`database_query`](https://dagster-component-ui.vercel.app/c/database_query) — read-only SELECT-to-DataFrame source asset (SQLAlchemy URL based)
