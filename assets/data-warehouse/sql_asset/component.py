"""SqlAssetComponent.

Run one or more SQL statements against ANY already-registered database resource that exposes
`.get_connection()` -- the DBAPI-connection pattern already shared by postgres_resource,
mysql_resource, duckdb_resource, databricks_sql_resource, mssql_resource, oracle_resource,
db2_resource, doris_resource, starburst_resource, starrocks_resource, timescaledb_resource, and
trino_resource in this repo. One component instead of twelve near-identical
`<dialect>_sql_asset`s.

Snowflake is intentionally NOT included here: `snowflake_sql_asset` already covers it with
Snowflake-specific niceties this component doesn't attempt to generalize -- the real
`cursor.execute(sql, num_statements=0)` auto-multi-statement-split and `%(name)s` pyformat
parameters are Snowflake-connector-specific, not a cross-driver guarantee. Use
`snowflake_sql_asset` for Snowflake; use this component for everything else.

Because the underlying drivers differ, two things are intentionally NOT invented into a fake
universal contract here, and are called out in the README instead:
  - Parameter placeholder style (`parameters`) is whatever the underlying driver's DBAPI
    paramstyle is (pyformat for psycopg2/mysql, named/qmark for pyodbc, etc.) -- passed through
    verbatim, not translated.
  - Session setup (`USE`/`SET ...`) is dialect-specific SQL the caller writes themselves via
    `pre_sql`, not a set of structured warehouse/database/schema/role fields (which would only
    make sense for Snowflake-shaped dialects).
"""
import time
from contextlib import nullcontext
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Union

import dagster as dg
from pydantic import Field, model_validator


# BEGIN field-shape normalization (inlined per component; keep copies identical — see tests/test_field_shapes.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def coerce_field_shapes(data, *, lists=(), csv=(), tags=()):
    """Accept the shapes people (and LLMs) naturally write for common boilerplate fields."""
    if not isinstance(data, dict):
        return data
    out = dict(data)
    for k in lists:  # List[str]: "a" / "a, b" -> ["a", "b"]; strip + drop empties
        v = out.get(k)
        if isinstance(v, str):
            v = v.split(",")
        if isinstance(v, (list, tuple)):
            out[k] = [x.strip() if isinstance(x, str) else x for x in v if not (isinstance(x, str) and not x.strip())]
    for k in csv:  # comma-separated str given as a list -> "a,b"
        v = out.get(k)
        if isinstance(v, (list, tuple, set)):
            out[k] = ",".join(str(x).strip() for x in v)
    for k in tags:  # Dict[str, str]: scalars -> str, ["k=v"] / ["k:v"] -> {k: v}
        v = out.get(k)
        if isinstance(v, (list, tuple)) and v and all(isinstance(x, str) and ("=" in x or ":" in x) for x in v):
            v = dict((p.strip() for p in (x.split("=", 1) if "=" in x else x.split(":", 1))) for x in v)
        if isinstance(v, dict):
            out[k] = {str(a).strip(): (str(b).lower() if isinstance(b, bool) else str(b)) for a, b in v.items() if b is not None}
    return out
# END field-shape normalization


# BEGIN generic timeout wrapper (inlined per component; keep copies identical — see tests/test_generic_timeout_drift.py)
def _run_with_timeout(fn, timeout_seconds, *args, **kwargs):
    """Bounds how long this step waits for `fn`, raising dg.Failure past timeout_seconds.
    Python threads can't be forcibly killed -- fn keeps running in the background past the
    deadline (same documented trade-off as this repo's own @timeout decorator); this bounds
    the STEP's wait, not the underlying call itself. Deliberately NOT `with ThreadPoolExecutor()`
    -- that form's __exit__ calls shutdown(wait=True), which blocks until the background call
    finishes anyway, silently defeating the timeout for a call that hangs indefinitely."""
    if not timeout_seconds:
        return fn(*args, **kwargs)
    import concurrent.futures
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    future = executor.submit(fn, *args, **kwargs)
    try:
        result = future.result(timeout=timeout_seconds)
    except concurrent.futures.TimeoutError:
        executor.shutdown(wait=False)
        raise dg.Failure(description=f"Exceeded timeout_seconds={timeout_seconds}.") from None
    executor.shutdown(wait=False)
    return result
# END generic timeout wrapper


# BEGIN run_window (inlined per component; keep copies identical — ported verbatim from
# sensors/event_automation's own business_hours_only parser/checker, same format and same
# fail-open-on-malformed-input behavior, just renamed for this field's own name)
_WEEKDAY_MAP = {"mon": 0, "tue": 1, "wed": 2, "thu": 3, "fri": 4, "sat": 5, "sun": 6}


def _parse_run_window(spec: str) -> Tuple[int, int, str, Optional[List[int]]]:
    """Parse 'HH:MM-HH:MM tz [day,day,...]' into (start_min, end_min, tz, weekdays). Raises
    ValueError on malformed input."""
    parts = spec.strip().split()
    if len(parts) < 2:
        raise ValueError(f"run_window needs 'HH:MM-HH:MM tz [days]', got '{spec}'")
    window, tz_name = parts[0], parts[1]
    start_str, _, end_str = window.partition("-")
    sh, _, sm = start_str.partition(":")
    eh, _, em = end_str.partition(":")
    start_min = int(sh) * 60 + int(sm or 0)
    end_min = int(eh) * 60 + int(em or 0)
    weekdays: Optional[List[int]] = None
    if len(parts) >= 3:
        weekdays = []
        for day in parts[2].split(","):
            key = day.strip().lower()[:3]
            if key in _WEEKDAY_MAP:
                weekdays.append(_WEEKDAY_MAP[key])
    return start_min, end_min, tz_name, weekdays


def _is_in_run_window(now_ts: float, spec: str) -> bool:
    """True if now_ts falls in the daily window (and optional day-of-week list). Fails open
    (returns True -- never blocks) on a malformed spec or any unexpected error."""
    try:
        start_min, end_min, tz_name, weekdays = _parse_run_window(spec)
    except Exception:
        return True
    try:
        try:
            from zoneinfo import ZoneInfo
            tz = ZoneInfo(tz_name) if tz_name.upper() != "UTC" else timezone.utc
        except Exception:
            tz = timezone.utc
        local = datetime.fromtimestamp(now_ts, tz=tz)
        cur_min = local.hour * 60 + local.minute
        if weekdays is not None and local.weekday() not in weekdays:
            return False
        if start_min <= end_min:
            return start_min <= cur_min < end_min
        return cur_min >= start_min or cur_min < end_min  # overnight window, e.g. 22:00-06:00
    except Exception:
        return True
# END run_window


class SqlAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Run one or more SQL statements against any registered resource exposing
    `.get_connection()` -- one component serving every DBAPI-style dialect in this repo
    instead of a dialect-specific asset per resource type."""

    asset_key: str = Field(description="Dagster asset key (e.g. 'warehouse/transforms/rollup').")
    resource_key: Optional[str] = Field(
        default=None,
        description=(
            "Key of a registered resource that exposes .get_connection() returning a DBAPI "
            "connection (directly, or via its own @contextmanager). Matches postgres_resource, "
            "mysql_resource, duckdb_resource, databricks_sql_resource, mssql_resource, "
            "oracle_resource, db2_resource, doris_resource, starburst_resource, "
            "starrocks_resource, timescaledb_resource, trino_resource. For Snowflake, use "
            "snowflake_sql_asset instead. Set exactly one of resource_key / connection_env "
            "(unless dry_run is True, which needs neither). NOTE: using resource_key means this "
            "asset's required_resource_keys names it, which is Dagster's own hard requirement "
            "for reading a resource dynamically by name -- a project that hasn't registered this "
            "resource yet can't load (dg check defs / dg dev fail) until it is. There's no way "
            "around that specific constraint while using resource_key (confirmed against "
            "Dagster's own execution model: an unrequired resource is never bound into "
            "context.resources at run time, and a fallback resource registered under the same "
            "key here would instead conflict with the project's real one once configured). Use "
            "connection_env (bypasses Dagster's resource system entirely) or dry_run (never "
            "connects at all) if you need this asset's definitions to load before the resource "
            "is wired up."
        ),
    )
    connection_env: Optional[str] = Field(
        default=None,
        description=(
            "Env var holding a SQLAlchemy connection URL (e.g. "
            "'postgresql://user:pass@host/db'), read at RUN time and connected via "
            "sqlalchemy.create_engine(url).raw_connection() -- a plain DBAPI connection, same "
            "shape resource_key's own .get_connection() returns. Unlike resource_key, this "
            "never touches Dagster's resource system at all, so this asset's definitions always "
            "load cleanly regardless of whether the env var is set yet; a missing/empty env var "
            "only fails at materialization time, with a clear message. Set exactly one of "
            "resource_key / connection_env (unless dry_run is True, which needs neither)."
        ),
    )
    dry_run: bool = Field(
        default=False,
        description=(
            "Log the fully-resolved SQL (pre_sql + the main statement(s)) and skip actually "
            "running it -- no resource is looked up, no SQLAlchemy engine is created, no "
            "connection is opened at all. Output metadata always includes dry_run so a "
            "materialization history shows which runs were real."
        ),
    )

    sql: Optional[str] = Field(default=None, description="SQL text, run in a single cursor.execute() call. Whether multiple ;-separated statements run in one call is driver-dependent (works for psycopg2/duckdb; most other drivers run only the first). Set exactly one of sql / sql_file / sql_statements.")
    sql_file: Optional[str] = Field(default=None, description="Path (relative to the Dagster process working directory) to a .sql file, read and treated the same as `sql`. Set exactly one of sql / sql_file / sql_statements.")
    sql_statements: Optional[List[str]] = Field(default=None, description="Explicit list of statements, each run as its own cursor.execute() call -- the one way to run multiple statements that works regardless of driver. Set exactly one of sql / sql_file / sql_statements.")
    parameters: Optional[Dict[str, Any]] = Field(default=None, description="Bind parameters applied to every statement. Placeholder style is whatever the underlying driver's DBAPI paramstyle is (e.g. pyformat %(name)s for psycopg2/mysql-connector, qmark ? or named :name for pyodbc/oracledb) -- passed through as-is, not translated.")
    pre_sql: Optional[List[str]] = Field(default=None, description="Dialect-specific statements run before the main SQL, e.g. ['SET search_path TO analytics'] (Postgres) or ['USE mydb'] (MySQL) -- write whatever your dialect needs; this component doesn't guess it for you.")

    group_name: str = Field(default="data_warehouse", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    deps: Optional[List[str]] = Field(default=None, description="Upstream Dagster asset keys for lineage.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners -- team names ('team:analytics') or email addresses.")
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Additional key-value tags applied to the asset in the Dagster catalog.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds for the catalog (e.g. ['postgres']). Auto-includes 'sql' if empty.")

    retry_policy_max_retries: Optional[int] = Field(default=None, description="Max retries on asset failure. Defines a RetryPolicy. Useful for transient connection failures, warehouse resume delays, etc.")
    retry_policy_delay_seconds: Optional[int] = Field(default=None, description="Seconds between retries (default 1).")
    retry_policy_backoff: str = Field(default="exponential", description="Backoff strategy: 'linear' or 'exponential'.")
    pool: Optional[str] = Field(
        default=None,
        description=(
            "Dagster's native named concurrency pool (e.g. Airflow's `pool` -- a fixed "
            "number of slots shared across every asset/op using the same pool name, e.g. "
            "warehouse concurrency limits). The pool's own slot count is configured at the "
            "deployment level (dagster.yaml's concurrency: pools:, or the Dagster+ UI), not here."
        ),
    )
    timeout_seconds: Optional[float] = Field(
        default=None,
        description="Kill the wait and fail the run if the SQL hasn't finished after this many seconds. Enforced via a background-thread wrapper -- the underlying query may continue running past the deadline (Python threads can't be forcibly killed, and this does not cancel the query server-side); this bounds the step's wait, not the call itself.",
    )
    run_window: Optional[str] = Field(
        default=None,
        description=(
            "'HH:MM-HH:MM tz [day,day,...]' (e.g. '09:00-17:00 America/New_York "
            "mon,tue,wed,thu,fri'). Outside this window, the asset SKIPS cleanly instead of "
            "running or failing -- no SQL runs, and real downstream dependents skip too (not "
            "fail). Checked once per materialization, before any statement runs. A malformed "
            "spec fails open (never blocks). Unset (default): always runs."
        ),
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('deps', 'owners', 'kinds', 'sql_statements', 'pre_sql'), csv=(), tags=('asset_tags',))

    @model_validator(mode="after")
    def _validate_sql_source(self):
        sources = [s for s in (self.sql, self.sql_file, self.sql_statements) if s]
        if len(sources) != 1:
            raise ValueError("SqlAssetComponent: set exactly one of `sql`, `sql_file`, `sql_statements`.")
        return self

    @model_validator(mode="after")
    def _validate_connection_source(self):
        if self.dry_run:
            if self.resource_key and self.connection_env:
                raise ValueError("SqlAssetComponent: set at most one of `resource_key` or `connection_env` -- not both.")
            return self  # dry_run never opens a connection -- neither is required
        if bool(self.resource_key) == bool(self.connection_env):
            raise ValueError("SqlAssetComponent: set exactly one of `resource_key` or `connection_env` -- not both, not neither.")
        return self

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="SQL Asset", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        component = self
        kinds = set(component.kinds) if component.kinds else {"sql"}
        resource_key = component.resource_key
        connection_env = component.connection_env
        dry_run = component.dry_run
        deps_keys = [dg.AssetKey.from_user_string(k) for k in (component.deps or [])]
        timeout_seconds = component.timeout_seconds
        run_window = component.run_window

        _retry_policy = None
        if component.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            _retry_policy = RetryPolicy(
                max_retries=component.retry_policy_max_retries,
                delay=component.retry_policy_delay_seconds or 1,
                backoff=Backoff[component.retry_policy_backoff.upper()],
            )

        outs = {
            "result": dg.AssetOut(
                key=dg.AssetKey(component.asset_key.split("/")),
                description=component.description or "SQL statement(s) against a registered database resource",
                is_required=False,
                kinds=kinds,
                owners=component.owners or None,
                tags=component.asset_tags or None,
            ),
        }

        # required_resource_keys is Dagster's own hard constraint for context.resources access --
        # declaring it forces the WHOLE repository/code location to fail loading (confirmed live:
        # DagsterInvalidDefinitionError from get_repository_def(), the same check `dg check defs`
        # and `dg dev` run) until something is registered under that key. There is no supported
        # way to defer that specific check to run time while still using context.resources
        # (Dagster never binds an unrequired resource into context.resources at all, and a
        # fallback resource registered here under the same key would instead COLLIDE with the
        # project's real one once configured -- both confirmed live, not assumed). So this is
        # only ever declared when resource_key is actually the active path (not dry_run, which
        # never touches a connection -- connection_env also never needs it, since it builds a
        # SQLAlchemy connection directly, bypassing Dagster's resource system entirely). Prefer
        # dry_run or connection_env over resource_key if you need this asset's definitions to
        # load before a shared resource is wired up project-wide.
        @dg.multi_asset(
            name=dg.AssetKey(component.asset_key.split("/")).to_python_identifier(),
            outs=outs,
            group_name=component.group_name,
            deps=deps_keys,
            required_resource_keys={resource_key} if (resource_key and not dry_run) else None,
            retry_policy=_retry_policy,
            pool=component.pool,
        )
        def _sql_asset(context: dg.AssetExecutionContext):
            if run_window and not _is_in_run_window(time.time(), run_window):
                context.log.info(f"run_window={run_window!r}: outside the configured window -- skipping (not failing).")
                return
            if dry_run:
                yield _dry_run_preview(context)
                return
            yield _run_with_timeout(_execute, timeout_seconds, context)

        def _dry_run_preview(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            for stmt in (component.pre_sql or []):
                context.log.info(f"[dry_run] would run: {stmt}")
            if component.sql_statements:
                statements = list(component.sql_statements)
            else:
                statements = [component.sql if component.sql else open(component.sql_file).read()]
            for stmt in statements:
                context.log.info(f"[dry_run] would run: {stmt}")
            return dg.MaterializeResult(metadata={
                "dry_run": dg.MetadataValue.bool(True),
                "statement_count": dg.MetadataValue.int(len(statements)),
                "statements": dg.MetadataValue.json(statements),
            })

        def _resolve_connection(context: dg.AssetExecutionContext):
            """Returns a context manager yielding a raw DBAPI connection (.cursor()/.commit()/
            .close()) from whichever of resource_key / connection_env is configured.
            connection_env is resolved here, at RUN time, with a clear dg.Failure if the env var
            turns out to be unset -- it never touches Dagster's resource system at all.
            resource_key, when active, is guaranteed bound by this point: the decorator above
            only declares required_resource_keys for this path, so a missing resource is caught
            at definition-load time instead (see build_defs's own comment on why that can't be
            deferred further for resource_key specifically)."""
            if connection_env:
                from sqlalchemy import create_engine

                url = dg.EnvVar(connection_env).get_value()
                if not url:
                    raise dg.Failure(
                        description=(
                            f"SqlAssetComponent: connection_env={connection_env!r} is set but that "
                            "environment variable is empty/unset. Set it to a SQLAlchemy connection "
                            "URL (e.g. 'postgresql://user:pass@host/db')."
                        ),
                    )
                return nullcontext(create_engine(url).raw_connection())

            resource = getattr(context.resources, resource_key)
            if not hasattr(resource, "get_connection"):
                raise ValueError(
                    f"resource {resource_key!r} must expose .get_connection() "
                    f"(returning a DBAPI connection, directly or via its own @contextmanager)."
                )
            # Same acquisition idiom as database_query's sink path: some resources' own
            # get_connection() is already a context manager (e.g. trino/starburst), others
            # return a raw driver connection object directly -- normalize both to `with ... as conn`.
            raw_or_cm = resource.get_connection()
            return raw_or_cm if hasattr(raw_or_cm, "__enter__") else nullcontext(raw_or_cm)

        def _execute(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            with _resolve_connection(context) as conn:
                cursor = conn.cursor()
                try:
                    for stmt in (component.pre_sql or []):
                        context.log.info(stmt)
                        cursor.execute(stmt)

                    results: List[Dict[str, Any]] = []
                    if component.sql_statements:
                        for stmt in component.sql_statements:
                            context.log.info(stmt)
                            cursor.execute(stmt, component.parameters) if component.parameters else cursor.execute(stmt)
                            results.append({"statement": stmt, "rowcount": getattr(cursor, "rowcount", None)})
                    else:
                        sql_text = component.sql if component.sql else open(component.sql_file).read()
                        context.log.info(sql_text)
                        cursor.execute(sql_text, component.parameters) if component.parameters else cursor.execute(sql_text)
                        results.append({"statement": sql_text, "rowcount": getattr(cursor, "rowcount", None)})

                    try:
                        conn.commit()
                    except Exception as e:  # not every dialect supports/needs commit (e.g. autocommit engines)
                        context.log.debug(f"conn.commit() raised (tolerated): {e}")
                finally:
                    try:
                        cursor.close()
                    except Exception:
                        pass
                    try:
                        conn.close()  # tolerated even if the resource's own CM closes it again on exit
                    except Exception:
                        pass

            return dg.MaterializeResult(metadata={
                "resource_key": dg.MetadataValue.text(resource_key) if resource_key else dg.MetadataValue.null(),
                "connection_env": dg.MetadataValue.text(connection_env) if connection_env else dg.MetadataValue.null(),
                "dry_run": dg.MetadataValue.bool(False),
                "statement_count": dg.MetadataValue.int(len(results)),
                "results": dg.MetadataValue.json(results),
            })

        return dg.Definitions(assets=[_sql_asset])
