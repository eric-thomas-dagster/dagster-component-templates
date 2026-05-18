"""Shared helpers for view-migration components.

Kept inside the component package so the CLI's local-copy install picks it up
alongside `component.py` automatically.
"""
import os
import re
from typing import Dict, List, Optional, Tuple

VALID_DIALECTS = {
    "postgres", "mysql", "mssql", "oracle", "db2",
    "snowflake", "redshift", "duckdb",
}

# Map Sling-style URL schemes to SQLAlchemy-style ones so the same env vars
# work whether the user copies them from the replication demo or writes them fresh.
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


def engine_for(env_var: str, dialect: str):
    """Build a SQLAlchemy engine from a URL stored in env_var."""
    from sqlalchemy import create_engine

    url = os.environ.get(env_var)
    if not url:
        raise EnvironmentError(f"Connection env var {env_var!r} is not set")
    for prefix, sa_prefix in _SCHEME_ALIASES.items():
        if url.startswith(prefix) and not url.startswith(sa_prefix):
            url = sa_prefix + url[len(prefix):]
            break
    return create_engine(url)


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


def exec_with_dry_run(engine, statements: List[str], dry_run: bool) -> None:
    """Execute SQL statements; commit if dry_run=False, rollback if True.

    Best-effort on Oracle / MSSQL where DDL may auto-commit.
    """
    from sqlalchemy import text
    conn = engine.connect()
    trans = conn.begin()
    try:
        for stmt in statements:
            if stmt:
                conn.execute(text(stmt))
        if dry_run:
            trans.rollback()
        else:
            trans.commit()
    except Exception:
        try:
            trans.rollback()
        except Exception:
            pass
        raise
    finally:
        conn.close()


_DIALECT_QUIRK_MARKERS = {
    "oracle":   [r"\bCONNECT\s+BY\b", r"\bSTART\s+WITH\b", r"\bROWNUM\b", r"\(\s*\+\s*\)",
                 r"\bDUAL\b", r"\bSYSDATE\b", r"\bNVL\b", r"\bDECODE\b", r"\bREGEXP_LIKE\b"],
    "mssql":    [r"\bTOP\s+\d+\b", r"\bCROSS\s+APPLY\b", r"\bOUTER\s+APPLY\b",
                 r"\bUNIQUEIDENTIFIER\b", r"\bGETDATE\b", r"\bISNULL\b"],
    "db2":      [r"\bGRAPHIC\b", r"\bVARGRAPHIC\b"],
    "postgres": [r"\bARRAY\s*\["],
}


def detect_dialect_quirks(source_ddl: str, source_dialect: str) -> List[str]:
    """Return source-dialect-specific markers found in a view body. Empty list = portable."""
    if not source_ddl:
        return []
    markers = _DIALECT_QUIRK_MARKERS.get(source_dialect.lower(), [])
    found = []
    for pattern in markers:
        if re.search(pattern, source_ddl, re.IGNORECASE):
            label = re.sub(r"[\\\\bs+()*]+", " ", pattern).strip()
            found.append(label.upper())
    return found


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


def replace_view_statements(dialect: str, target_view: str, view_body: str) -> List[str]:
    """Return the SQL statements needed to (re)create a view on the target.

    Dialect-aware because MSSQL doesn't support `CREATE OR REPLACE VIEW`.
    """
    if dialect in ("postgres", "mysql", "snowflake", "redshift", "duckdb", "db2", "oracle"):
        return [f"CREATE OR REPLACE VIEW {target_view} AS {view_body}"]

    if dialect == "mssql":
        schema, name = split_qualified(target_view)
        if schema:
            check = (
                f"IF OBJECT_ID('{schema}.{name}','V') IS NOT NULL "
                f"DROP VIEW {schema}.{name}"
            )
        else:
            check = f"IF OBJECT_ID('{name}','V') IS NOT NULL DROP VIEW {name}"
        return [check, f"CREATE VIEW {target_view} AS {view_body}"]

    raise ValueError(f"Unsupported target dialect: {dialect}")


def replace_view(conn, dialect: str, target_view: str, view_body: str) -> None:
    """Backwards-compatible wrapper: build statements and execute against a live conn."""
    from sqlalchemy import text
    for stmt in replace_view_statements(dialect, target_view, view_body):
        conn.execute(text(stmt))
