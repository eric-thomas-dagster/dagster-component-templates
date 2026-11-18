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
from contextlib import nullcontext
from typing import Any, Dict, List, Optional, Union

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

        @dg.asset(
            key=dg.AssetKey(component.asset_key.split("/")),
            description=component.description or "SQL statement(s) against a registered database resource",
            group_name=component.group_name,
            kinds=kinds,
            owners=component.owners,
            tags=component.asset_tags,
            deps=deps_keys,
            required_resource_keys={resource_key},
        )
        def _sql_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
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
