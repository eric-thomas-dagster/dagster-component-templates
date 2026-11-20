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
    resource_key: str = Field(
        description=(
            "Key of a registered resource that exposes .get_connection() returning a DBAPI "
            "connection (directly, or via its own @contextmanager). Matches postgres_resource, "
            "mysql_resource, duckdb_resource, databricks_sql_resource, mssql_resource, "
            "oracle_resource, db2_resource, doris_resource, starburst_resource, "
            "starrocks_resource, timescaledb_resource, trino_resource. For Snowflake, use "
            "snowflake_sql_asset instead."
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

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="SQL Asset", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        component = self
        kinds = set(component.kinds) if component.kinds else {"sql"}
        resource_key = component.resource_key
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

        @dg.multi_asset(
            name=dg.AssetKey(component.asset_key.split("/")).to_python_identifier(),
            outs=outs,
            group_name=component.group_name,
            deps=deps_keys,
            required_resource_keys={resource_key},
            retry_policy=_retry_policy,
            pool=component.pool,
        )
        def _sql_asset(context: dg.AssetExecutionContext):
            if run_window and not _is_in_run_window(time.time(), run_window):
                context.log.info(f"run_window={run_window!r}: outside the configured window -- skipping (not failing).")
                return
            yield _run_with_timeout(_execute, timeout_seconds, context)

        def _execute(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
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
            acquire = raw_or_cm if hasattr(raw_or_cm, "__enter__") else nullcontext(raw_or_cm)

            with acquire as conn:
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
                "resource_key": dg.MetadataValue.text(resource_key),
                "statement_count": dg.MetadataValue.int(len(results)),
                "results": dg.MetadataValue.json(results),
            })

        return dg.Definitions(assets=[_sql_asset])
