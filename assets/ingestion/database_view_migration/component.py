"""Database View Replication.

Replicate a SQL view from a source database to a target. Reads the view
definition from the source's INFORMATION_SCHEMA / ALL_VIEWS / SYSCAT.VIEWS,
applies table-reference substitutions (so `app.orders` → `raw.orders` after
migration), then executes CREATE OR REPLACE VIEW on the target.

Works for the 80% of views that are plain SELECT-aggregate-join queries.
Dialect-specific syntax (Oracle `(+)`, `CONNECT BY`, `ROWNUM`; MSSQL
`CROSS APPLY`; etc.) fails loudly at CREATE VIEW time so the migration team
can rewrite those by hand.
"""
from typing import Dict, List, Optional, Tuple
import dagster as dg
from pydantic import Field
import os
import re


VALID_DIALECTS = {
    "postgres", "mysql", "mssql", "oracle", "db2",
    "snowflake", "redshift", "duckdb",
}


_SCHEME_ALIASES = {
    "postgres://": "postgresql+psycopg2://",
    "postgresql://": "postgresql+psycopg2://",
    "mysql://": "mysql+pymysql://",
    "mssql://": "mssql+pyodbc://",
    "sqlserver://": "mssql+pyodbc://",
    "oracle://": "oracle+oracledb://",
    "db2://": "ibm_db_sa://",
    "redshift://": "redshift+redshift_connector://",
    "duckdb://": "duckdb:///",
}


def validate_dialect(dialect: str, role: str = "source") -> None:
    if dialect not in VALID_DIALECTS:
        raise ValueError(
            f"{role}_type={dialect!r} not supported. Use one of {sorted(VALID_DIALECTS)}."
        )


def resolve_connection(literal, env_var, name: str) -> str:
    if literal:
        return literal
    if env_var:
        v = os.environ.get(env_var)
        if not v:
            raise EnvironmentError(f"Env var {env_var!r} (for {name}) is not set")
        return v
    raise ValueError(f"Set either {name!r} or {name + '_env_var'!r}")


def engine_for_url(url: str, dialect: str):
    """Build a SQLAlchemy engine from a connection URL."""
    from sqlalchemy import create_engine
    for prefix, sa_prefix in _SCHEME_ALIASES.items():
        if url.startswith(prefix) and not url.startswith(sa_prefix):
            url = sa_prefix + url[len(prefix):]
            break
    return create_engine(url)


def engine_for(env_var: str, dialect: str):
    """Backwards-compatible wrapper — prefer engine_for_url + resolve_connection."""
    return engine_for_url(resolve_connection(None, env_var, "connection"), dialect)


def split_qualified(qualified: str) -> Tuple[Optional[str], str]:
    """'schema.name' → ('schema', 'name'); 'name' → (None, 'name')."""
    if "." in qualified:
        schema, name = qualified.split(".", 1)
        return schema, name
    return None, qualified


def fetch_view_definition(conn, dialect: str, schema: Optional[str], name: str) -> Optional[str]:
    """Return the view DDL (the SELECT body) from the source DB, or None if not found."""
    from sqlalchemy import text

    if dialect in ("postgres", "mysql", "snowflake", "redshift", "duckdb"):
        if schema:
            sql = (
                "SELECT view_definition FROM information_schema.views "
                "WHERE table_schema = :s AND table_name = :n"
            )
            row = conn.execute(text(sql), {"s": schema, "n": name}).fetchone()
        else:
            sql = "SELECT view_definition FROM information_schema.views WHERE table_name = :n"
            row = conn.execute(text(sql), {"n": name}).fetchone()
        return row[0] if row else None

    if dialect == "oracle":
        if schema:
            row = conn.execute(
                text("SELECT text FROM all_views WHERE owner = :o AND view_name = :n"),
                {"o": schema.upper(), "n": name.upper()},
            ).fetchone()
        else:
            row = conn.execute(
                text("SELECT text FROM all_views WHERE view_name = :n"), {"n": name.upper()}
            ).fetchone()
        return row[0] if row else None

    if dialect == "mssql":
        if schema:
            row = conn.execute(
                text(
                    "SELECT m.definition FROM sys.sql_modules m "
                    "JOIN sys.views v ON m.object_id = v.object_id "
                    "JOIN sys.schemas s ON s.schema_id = v.schema_id "
                    "WHERE s.name = :s AND v.name = :n"
                ),
                {"s": schema, "n": name},
            ).fetchone()
        else:
            row = conn.execute(
                text(
                    "SELECT m.definition FROM sys.sql_modules m "
                    "JOIN sys.views v ON m.object_id = v.object_id "
                    "WHERE v.name = :n"
                ),
                {"n": name},
            ).fetchone()
        # MSSQL returns the full CREATE VIEW statement, not just the SELECT body.
        # Strip the CREATE VIEW ... AS prefix so we can wrap it ourselves on the target.
        if row:
            ddl = row[0]
            match = re.search(r"\bAS\b", ddl, re.IGNORECASE)
            return ddl[match.end():].strip() if match else ddl
        return None

    if dialect == "db2":
        if schema:
            row = conn.execute(
                text("SELECT text FROM syscat.views WHERE viewschema = :s AND viewname = :n"),
                {"s": schema.upper(), "n": name.upper()},
            ).fetchone()
        else:
            row = conn.execute(
                text("SELECT text FROM syscat.views WHERE viewname = :n"), {"n": name.upper()}
            ).fetchone()
        if row:
            ddl = row[0]
            match = re.search(r"\bAS\b", ddl, re.IGNORECASE)
            return ddl[match.end():].strip() if match else ddl
        return None

    raise ValueError(f"Unsupported dialect for view fetch: {dialect}")


def apply_patterns(
    discovered: List[Tuple[str, str]],
    include_patterns: Optional[List[str]],
    exclude_patterns: Optional[List[str]],
) -> List[Tuple[str, str]]:
    """Filter (schema, name) tuples by fnmatch glob patterns against qualified names.

    Patterns match case-insensitively against 'schema.name'. Include applied first
    (any pattern matches → keep), then exclude (any pattern matches → drop).
    Exact strings are valid globs.
    """
    import fnmatch
    out = discovered
    if include_patterns:
        inc = [p.lower() for p in include_patterns]
        out = [
            (s, n) for s, n in out
            if any(fnmatch.fnmatchcase(f"{s}.{n}".lower(), p) for p in inc)
        ]
    if exclude_patterns:
        exc = [p.lower() for p in exclude_patterns]
        out = [
            (s, n) for s, n in out
            if not any(fnmatch.fnmatchcase(f"{s}.{n}".lower(), p) for p in exc)
        ]
    return out


def list_views(conn, dialect: str, schemas: Optional[List[str]]) -> List[Tuple[str, str]]:
    """Return [(schema_name, view_name), ...] from the source DB, optionally filtered."""
    from sqlalchemy import text

    if dialect in ("postgres", "mysql", "snowflake", "redshift", "duckdb"):
        sql = "SELECT table_schema, table_name FROM information_schema.views"
        if dialect in ("postgres", "redshift"):
            sql += " WHERE table_schema NOT IN ('pg_catalog', 'information_schema')"
        elif dialect == "mysql":
            sql += " WHERE table_schema NOT IN ('mysql','sys','information_schema','performance_schema')"
        elif dialect == "snowflake":
            sql += " WHERE table_schema <> 'INFORMATION_SCHEMA'"
        rows = conn.execute(text(sql)).fetchall()
    elif dialect == "oracle":
        rows = conn.execute(
            text(
                "SELECT owner, view_name FROM all_views WHERE owner NOT IN "
                "('SYS','SYSTEM','XDB','MDSYS','CTXSYS','DBSNMP','OUTLN','APPQOSSYS',"
                "'GSMADMIN_INTERNAL','AUDSYS','OJVMSYS','ORDDATA','ORDSYS','LBACSYS',"
                "'DVSYS','DVF','REMOTE_SCHEDULER_AGENT','GGSYS')"
            )
        ).fetchall()
    elif dialect == "mssql":
        rows = conn.execute(
            text(
                "SELECT s.name, v.name FROM sys.views v "
                "JOIN sys.schemas s ON s.schema_id = v.schema_id"
            )
        ).fetchall()
    elif dialect == "db2":
        rows = conn.execute(
            text("SELECT viewschema, viewname FROM syscat.views WHERE viewschema NOT LIKE 'SYS%'")
        ).fetchall()
    else:
        raise ValueError(f"Unsupported dialect for view list: {dialect}")

    out = [(str(r[0]), str(r[1])) for r in rows]
    if schemas:
        wanted = {s.lower() for s in schemas}
        out = [(s, n) for s, n in out if s.lower() in wanted]
    return out


def apply_substitutions(
    sql: str,
    table_replacements: Dict[str, str],
    function_replacements: Dict[str, str],
) -> str:
    """Apply table-ref + function-name substitutions to a view's SQL body.

    Table refs: exact string substitution (handles `app.orders` → `raw.orders`).
    Function names: case-insensitive whole-word substitution to avoid eating
    column names that happen to contain the function name as a substring.
    """
    out = sql
    for old, new in table_replacements.items():
        out = out.replace(old, new)
    for old, new in function_replacements.items():
        out = re.sub(rf"\b{re.escape(old)}\b", new, out, flags=re.IGNORECASE)
    return out


def replace_view(conn, dialect: str, target_view: str, view_body: str) -> None:
    """Drop-then-create a view on the target. Dialect-aware because MSSQL and
    Oracle don't both support `CREATE OR REPLACE VIEW`."""
    from sqlalchemy import text

    if dialect in ("postgres", "mysql", "snowflake", "redshift", "duckdb", "db2"):
        # CREATE OR REPLACE VIEW is supported on all of these
        conn.execute(text(f"CREATE OR REPLACE VIEW {target_view} AS {view_body}"))
        return

    if dialect == "oracle":
        # Oracle: CREATE OR REPLACE VIEW is supported
        conn.execute(text(f"CREATE OR REPLACE VIEW {target_view} AS {view_body}"))
        return

    if dialect == "mssql":
        # No OR REPLACE — drop first if it exists, then create
        schema, name = split_qualified(target_view)
        if schema:
            check = (
                f"IF OBJECT_ID('{schema}.{name}','V') IS NOT NULL "
                f"DROP VIEW {schema}.{name}"
            )
        else:
            check = f"IF OBJECT_ID('{name}','V') IS NOT NULL DROP VIEW {name}"
        conn.execute(text(check))
        conn.execute(text(f"CREATE VIEW {target_view} AS {view_body}"))
        return

    raise ValueError(f"Unsupported target dialect: {dialect}")



class DatabaseViewMigrationComponent(dg.Component, dg.Model, dg.Resolvable):
    """Replicate a single SQL view from source to target, with optional table-ref rewriting."""

    asset_name: str = Field(description="Output Dagster asset name")
    source_connection: Optional[str] = Field(default=None, description="SQLAlchemy URL for source DB. Set this OR source_connection_env_var.")
    source_connection_env_var: Optional[str] = Field(default=None, description="Env var with source SQLAlchemy URL. Set this OR source_connection.")
    target_connection: Optional[str] = Field(default=None, description="SQLAlchemy URL for target DB. Set this OR target_connection_env_var.")
    target_connection_env_var: Optional[str] = Field(default=None, description="Env var with target SQLAlchemy URL. Set this OR target_connection.")
    source_type: str = Field(description="Source DB dialect: postgres / mysql / mssql / oracle / db2 / snowflake / redshift")
    target_type: str = Field(description="Target DB dialect (same options)")
    source_view: str = Field(description="Source view: 'schema.name' or just 'name'")
    target_view: str = Field(description="Target view: 'schema.name' or just 'name'")

    table_replacements: Optional[Dict[str, str]] = Field(
        default=None,
        description="Substitutions applied to the view DDL — old qualified name → new (e.g. {'app.orders': 'raw.orders'})",
    )
    function_replacements: Optional[Dict[str, str]] = Field(
        default=None,
        description="Dialect function replacements applied as case-insensitive word substitution (e.g. {'NVL': 'COALESCE', 'SYSDATE': 'CURRENT_TIMESTAMP'})",
    )

    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
    description: Optional[str] = Field(default=None, description="Asset description")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners")
    asset_tags: Optional[Dict[str, str]] = Field(default=None, description="Additional tags")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds (default: ['view', source_type, target_type])")
    deps: Optional[List[str]] = Field(default=None, description="Upstream asset keys (typically the replicated tables this view depends on)")

    @classmethod
    def get_description(cls) -> str:
        return (
            "Replicate a SQL view from source to target with optional table-ref + function-name substitutions. "
            "Reads source view DDL, rewrites refs, executes CREATE OR REPLACE VIEW on target."
        )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        source_type = self.source_type.lower()
        target_type = self.target_type.lower()
        validate_dialect(source_type, role="source")
        validate_dialect(target_type, role="target")

        kinds_list = self.kinds or ["view", source_type, target_type]
        all_tags = dict(self.asset_tags or {})
        for k in kinds_list:
            all_tags[f"dagster/kind/{k}"] = ""

        source_view = self.source_view
        target_view = self.target_view
        source_connection_literal = self.source_connection
        source_connection_env_var = self.source_connection_env_var
        target_connection_literal = self.target_connection
        target_connection_env_var = self.target_connection_env_var
        table_replacements = dict(self.table_replacements or {})
        function_replacements = dict(self.function_replacements or {})

        @dg.asset(
            key=dg.AssetKey.from_user_string(self.asset_name),
            group_name=self.group_name,
            description=self.description or self.get_description(),
            owners=self.owners or [],
            tags=all_tags,
            kinds=set(kinds_list),
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def _view_asset(context: dg.AssetExecutionContext):
            source_url = resolve_connection(source_connection_literal, source_connection_env_var, "source_connection")
            target_url = resolve_connection(target_connection_literal, target_connection_env_var, "target_connection")
            source_engine = engine_for_url(source_url, source_type)
            target_engine = engine_for_url(target_url, target_type)

            schema, name = split_qualified(source_view)
            with source_engine.connect() as src:
                view_sql = fetch_view_definition(src, source_type, schema, name)
            if view_sql is None:
                raise ValueError(
                    f"View {source_view!r} not found in source ({source_type})"
                )

            rewritten = apply_substitutions(view_sql, table_replacements, function_replacements)
            context.log.info(
                f"View {source_view} → {target_view}: "
                f"{len(table_replacements)} table refs + {len(function_replacements)} function names substituted"
            )

            with target_engine.begin() as tgt:
                replace_view(tgt, target_type, target_view, rewritten)

            return dg.MaterializeResult(
                metadata={
                    "source_view": dg.MetadataValue.text(source_view),
                    "target_view": dg.MetadataValue.text(target_view),
                    "source_type": dg.MetadataValue.text(source_type),
                    "target_type": dg.MetadataValue.text(target_type),
                    "ddl_chars": dg.MetadataValue.int(len(rewritten)),
                    "substitutions_applied": dg.MetadataValue.int(
                        len(table_replacements) + len(function_replacements)
                    ),
                    "source_ddl": dg.MetadataValue.md(f"```sql\n{view_sql}\n```"),
                    "target_ddl": dg.MetadataValue.md(f"```sql\n{rewritten}\n```"),
                }
            )

        return dg.Definitions(assets=[_view_asset])
