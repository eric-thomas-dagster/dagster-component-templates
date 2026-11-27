"""SnowflakeSqlAssetComponent.

Run one or more SQL statements against Snowflake as a Dagster asset -- mirrors Airflow's
`SnowflakeOperator` / `SQLExecuteQueryOperator`.

Checked for existing coverage before building: `dagster.TemplatedSqlComponent` (core Dagster,
marked `@preview`) + `dagster_snowflake.SnowflakeResource.connect_and_execute()` already cover
"render one Jinja-templated SQL string and run it once against Snowflake" -- but
`connect_and_execute`'s real implementation is just
`with self.get_connection() as conn: conn.cursor().execute(sql)` -- one string, no
`num_statements` (so real multi-statement text isn't handled), no bind parameters, no session
overrides, no per-statement result metadata. This component covers exactly what that doesn't:
statements from text/list/file (verified `cursor.execute(..., num_statements=...)` is Snowflake's
real, documented mechanism for multiple ;-separated statements in one call -- not naive string
splitting, which breaks on quoted literals/comments), session-level USE overrides, bind
parameters, and rowcount metadata per statement.
"""
from typing import Any, Dict, List, Optional, Union

import dagster as dg
from pydantic import Field, model_validator


# BEGIN field-shape normalization (inlined per component; keep copies identical — see tests/test_field_shapes.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def coerce_field_shapes(data, *, lists=(), csv=(), tags=(), strs=()):
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
    for k in strs:  # str (or Dict[str, str]) fields: a numeric-looking YAML scalar resolves to int/float/bool
        v = out.get(k)
        if isinstance(v, bool):
            out[k] = "true" if v else "false"
        elif isinstance(v, (int, float)):
            out[k] = str(v)
        elif isinstance(v, dict):
            out[k] = {str(a): (("true" if b is True else "false") if isinstance(b, bool) else (str(b) if isinstance(b, (int, float)) else b)) for a, b in v.items()}
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


def _require_env(name: str) -> str:
    """Read an env var via dagster.EnvVar, with an actionable message when it is unset. Same
    helper as snowflake_stored_procedure_call_asset (duplicated, not imported -- components are
    self-contained)."""
    value = dg.EnvVar(name).get_value()
    if value is None:
        raise KeyError(
            f"Environment variable {name!r} is not set. Set it in the environment of the "
            "Dagster code location (or point the *_env_var field at one that is)."
        )
    return value


class SnowflakeSqlAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Run one or more SQL statements against Snowflake as a Dagster asset -- mirrors Airflow's
    `SnowflakeOperator` / `SQLExecuteQueryOperator`."""

    asset_key: str = Field(description="Dagster asset key (e.g. 'snowflake/transforms/rollup').")

    sql: Optional[str] = Field(default=None, description="SQL text. May contain multiple ;-separated statements -- executed via Snowflake's real num_statements mechanism, not naive string splitting. Set exactly one of sql / sql_file / sql_statements.")
    sql_file: Optional[str] = Field(default=None, description="Path (relative to the Dagster process working directory) to a .sql file, read and treated the same as `sql`. Set exactly one of sql / sql_file / sql_statements.")
    sql_statements: Optional[List[str]] = Field(default=None, description="Explicit list of statements, each run as its own cursor.execute() call -- no splitting ambiguity. Set exactly one of sql / sql_file / sql_statements.")
    parameters: Optional[Dict[str, Any]] = Field(default=None, description="Bind parameters applied to every statement (cursor.execute(stmt, parameters)), pyformat style (e.g. a statement containing %(my_param)s).")

    session_warehouse: Optional[str] = Field(default=None, description="If set, runs 'USE WAREHOUSE <value>' before the main SQL -- a per-run override, distinct from warehouse_env_var (used when first connecting).")
    session_database: Optional[str] = Field(default=None, description="If set, runs 'USE DATABASE <value>' before the main SQL.")
    session_schema: Optional[str] = Field(default=None, description="If set, runs 'USE SCHEMA <value>' before the main SQL.")
    session_role: Optional[str] = Field(default=None, description="If set, runs 'USE ROLE <value>' before the main SQL.")

    account_env_var: Optional[str] = Field(default=None, description="Env var with Snowflake account. Required unless resource_key is set.")
    user_env_var: Optional[str] = Field(default=None, description="Env var with Snowflake username. Required unless resource_key is set.")
    password_env_var: Optional[str] = Field(default=None, description="Env var with Snowflake password.")
    pat_env_var: Optional[str] = Field(default=None, description="Env var with Snowflake PAT.")
    private_key_path_env_var: Optional[str] = Field(default=None, description="Env var with RSA private key path.")
    private_key_passphrase_env_var: Optional[str] = Field(default=None, description="Env var with private-key passphrase.")
    warehouse_env_var: Optional[str] = Field(default=None, description="Env var with Snowflake warehouse (used when first connecting).")
    role_env_var: Optional[str] = Field(default=None, description="Env var with Snowflake role (used when first connecting).")
    resource_key: Optional[str] = Field(
        default=None,
        description=(
            "Optional key of a registered SnowflakeResourceComponent (resources/snowflake_resource). When set, connection "
            "credentials are read from that resource at run time instead of the inline "
            "account_env_var/user_env_var/password_env_var/pat_env_var/private_key_path_env_var/warehouse_env_var/role_env_var "
            "fields -- lets one credential serve this component and other components without configuring it twice."
        ),
    )

    group_name: str = Field(default="snowflake", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    deps: Optional[List[str]] = Field(default=None, description="Upstream Dagster asset keys for lineage.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners -- team names ('team:analytics') or email addresses.")
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Additional key-value tags applied to the asset in the Dagster catalog.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds (auto-includes 'snowflake').")

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
        description="Kill the wait and fail the run if the SQL hasn't finished after this many seconds. Enforced via a background-thread wrapper -- the underlying Snowflake call may continue running past the deadline (Python threads can't be forcibly killed, and this does not cancel the query server-side); this bounds the step's wait, not the call itself.",
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('deps', 'owners', 'kinds', 'sql_statements'), csv=(), tags=('asset_tags',))

    @model_validator(mode="after")
    def _validate_sql_source_and_connection(self):
        sources = [s for s in (self.sql, self.sql_file, self.sql_statements) if s]
        if len(sources) != 1:
            raise ValueError("SnowflakeSqlAssetComponent: set exactly one of `sql`, `sql_file`, `sql_statements`.")
        if not self.resource_key and not (self.account_env_var and self.user_env_var):
            raise ValueError("SnowflakeSqlAssetComponent: set `resource_key`, or both `account_env_var` and `user_env_var`.")
        return self

    def _connection_kwargs(self) -> Dict[str, Any]:
        import os
        kwargs: Dict[str, Any] = {
            "account": _require_env(self.account_env_var),
            "user": _require_env(self.user_env_var),
        }
        if self.warehouse_env_var:
            kwargs["warehouse"] = os.environ.get(self.warehouse_env_var, "")
        if self.role_env_var:
            kwargs["role"] = os.environ.get(self.role_env_var, "")
        if self.pat_env_var and os.environ.get(self.pat_env_var):
            kwargs["password"] = _require_env(self.pat_env_var)
        elif self.private_key_path_env_var and os.environ.get(self.private_key_path_env_var):
            kwargs["private_key_file"] = _require_env(self.private_key_path_env_var)
            if self.private_key_passphrase_env_var:
                kwargs["private_key_file_pwd"] = os.environ.get(self.private_key_passphrase_env_var, "")
            kwargs["authenticator"] = "snowflake_jwt"
        elif self.password_env_var:
            kwargs["password"] = os.environ.get(self.password_env_var, "")
        return kwargs

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Snowflake SQL Asset", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        component = self
        kinds = set(component.kinds) if component.kinds else set()
        kinds.add("snowflake")
        resource_key = component.resource_key
        deps_keys = [dg.AssetKey.from_user_string(k) for k in (component.deps or [])]

        _retry_policy = None
        if component.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            _retry_policy = RetryPolicy(
                max_retries=component.retry_policy_max_retries,
                delay=component.retry_policy_delay_seconds or 1,
                backoff=Backoff[component.retry_policy_backoff.upper()],
            )

        @dg.asset(
            key=dg.AssetKey(component.asset_key.split("/")),
            description=component.description or "Snowflake SQL statement(s)",
            group_name=component.group_name,
            kinds=kinds,
            owners=component.owners,
            tags=component.asset_tags,
            deps=deps_keys,
            required_resource_keys=({resource_key} if resource_key else None),
            retry_policy=_retry_policy,
            pool=component.pool,
        )
        def _snowflake_sql(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            def _do_execute():
                import contextlib
                import snowflake.connector

                stack = contextlib.ExitStack()
                if resource_key:
                    conn = stack.enter_context(getattr(context.resources, resource_key).get_connection())
                else:
                    conn = snowflake.connector.connect(**component._connection_kwargs())
                cursor = conn.cursor()
                try:
                    for override_sql in (
                        (f"USE WAREHOUSE {component.session_warehouse}" if component.session_warehouse else None),
                        (f"USE DATABASE {component.session_database}" if component.session_database else None),
                        (f"USE SCHEMA {component.session_schema}" if component.session_schema else None),
                        (f"USE ROLE {component.session_role}" if component.session_role else None),
                    ):
                        if override_sql:
                            context.log.info(override_sql)
                            cursor.execute(override_sql)

                    results: List[Dict[str, Any]] = []
                    if component.sql_statements:
                        for stmt in component.sql_statements:
                            context.log.info(stmt)
                            cursor.execute(stmt, component.parameters) if component.parameters else cursor.execute(stmt)
                            results.append({"statement": stmt, "rowcount": cursor.rowcount})
                    else:
                        sql_text = component.sql if component.sql else open(component.sql_file).read()
                        context.log.info(sql_text)
                        # num_statements=0: let the Snowflake connector itself detect and run every
                        # ;-separated statement in sql_text -- the real, documented Snowflake
                        # mechanism for multi-statement text, not naive string splitting (which
                        # breaks on semicolons inside quoted literals or comments).
                        cursor.execute(sql_text, component.parameters, num_statements=0) if component.parameters else cursor.execute(sql_text, num_statements=0)
                        results.append({"statement": sql_text, "rowcount": cursor.rowcount})

                    conn.commit()
                finally:
                    cursor.close()
                    if not resource_key:
                        conn.close()
                    stack.close()
                return results

            results = _run_with_timeout(_do_execute, component.timeout_seconds)
            return dg.MaterializeResult(metadata={
                "statement_count": dg.MetadataValue.int(len(results)),
                "results": dg.MetadataValue.json(results),
            })

        return dg.Definitions(assets=[_snowflake_sql])
