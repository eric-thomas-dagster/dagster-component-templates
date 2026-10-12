"""Database Migration Assessment.

Pre-flight dry-run: read source DDL for every table + view, attempt the
target migration in a transaction, ROLLBACK every change. Returns a single
consolidated DataFrame with per-object status + complexity heuristic +
specific dialect-quirk markers + proposed target DDL.

The "what would happen if I ran this tomorrow?" view, BEFORE you commit.
Inspired by AWS SCT's Assessment Report and Microsoft SSMA's test mode.
"""
from typing import Any, Dict, List, Optional, Tuple
import dagster as dg
from pydantic import Field
import os
import re

# Complexity scoring:
#   simple  — auto-converts with no substitutions, no dialect markers
#   medium  — auto-converts but with substitutions applied OR target gives informational-only support
#   complex — would fail OR uses dialect-specific markers that need manual review
def _classify_complexity(
    would_succeed: bool, markers_count: int, has_substitutions: bool,
    target_informational_count: int,
) -> str:
    if not would_succeed:
        return "complex"
    if markers_count > 0:
        return "complex"
    if has_substitutions or target_informational_count > 0:
        return "medium"
    return "simple"


_ddl_VALID_DIALECTS = {
    "postgres", "mysql", "mssql", "oracle", "db2",
    "snowflake", "redshift", "duckdb",
}


_ddl__SCHEME_ALIASES = {
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


def _ddl_validate_dialect(dialect: str, role: str = "source") -> None:
    if dialect not in _ddl_VALID_DIALECTS:
        raise ValueError(
            f"{role}_type={dialect!r} not supported. Use one of {sorted(_ddl_VALID_DIALECTS)}."
        )


def _ddl_engine_for(env_var: str, dialect: str):
    from sqlalchemy import create_engine
    url = os.environ.get(env_var)
    if not url:
        raise EnvironmentError(f"Connection env var {env_var!r} is not set")
    for prefix, sa_prefix in _ddl__SCHEME_ALIASES.items():
        if url.startswith(prefix) and not url.startswith(sa_prefix):
            url = sa_prefix + url[len(prefix):]
            break
    return create_engine(url)


def _ddl_split_qualified(qualified: str) -> Tuple[Optional[str], str]:
    if "." in qualified:
        s, n = qualified.split(".", 1)
        return s, n
    return None, qualified


def fetch_columns(conn, dialect: str, schema: Optional[str], table: str) -> List[Dict[str, Any]]:
    """Return [{name, data_type, char_max_length, num_precision, num_scale, nullable, default}, ...]."""
    from sqlalchemy import text

    if dialect in ("postgres", "mysql", "snowflake", "redshift", "duckdb"):
        sql = (
            "SELECT column_name, data_type, character_maximum_length, "
            "numeric_precision, numeric_scale, is_nullable, column_default "
            "FROM information_schema.columns "
            f"WHERE table_name = :t {'AND table_schema = :s' if schema else ''} "
            "ORDER BY ordinal_position"
        )
        params = {"t": table, **({"s": schema} if schema else {})}
        rows = conn.execute(text(sql), params).fetchall()
    elif dialect == "oracle":
        if schema:
            sql = (
                "SELECT column_name, data_type, data_length, data_precision, data_scale, "
                "nullable, data_default FROM all_tab_columns "
                "WHERE table_name = :t AND owner = :s ORDER BY column_id"
            )
            params = {"t": table.upper(), "s": schema.upper()}
        else:
            sql = (
                "SELECT column_name, data_type, data_length, data_precision, data_scale, "
                "nullable, data_default FROM all_tab_columns "
                "WHERE table_name = :t ORDER BY column_id"
            )
            params = {"t": table.upper()}
        rows = conn.execute(text(sql), params).fetchall()
    elif dialect == "mssql":
        sql = (
            "SELECT column_name, data_type, character_maximum_length, "
            "numeric_precision, numeric_scale, is_nullable, column_default "
            "FROM information_schema.columns "
            f"WHERE table_name = :t {'AND table_schema = :s' if schema else ''} "
            "ORDER BY ordinal_position"
        )
        params = {"t": table, **({"s": schema} if schema else {})}
        rows = conn.execute(text(sql), params).fetchall()
    elif dialect == "db2":
        sql = (
            "SELECT colname, typename, length, scale, NULL AS num_scale, nulls, default "
            "FROM syscat.columns "
            f"WHERE tabname = :t {'AND tabschema = :s' if schema else ''} "
            "ORDER BY colno"
        )
        params = {"t": table.upper(), **({"s": schema.upper()} if schema else {})}
        rows = conn.execute(text(sql), params).fetchall()
    else:
        raise ValueError(f"Unsupported dialect for column fetch: {dialect}")

    out = []
    for r in rows:
        nullable_raw = r[5]
        if isinstance(nullable_raw, str):
            nullable = nullable_raw.upper() in ("YES", "Y")
        else:
            nullable = bool(nullable_raw)
        out.append({
            "name": str(r[0]),
            "data_type": str(r[1]),
            "char_max_length": r[2],
            "num_precision": r[3],
            "num_scale": r[4],
            "nullable": nullable,
            "default": r[6],
        })
    return out


def fetch_primary_key(conn, dialect: str, schema: Optional[str], table: str) -> Optional[Dict[str, Any]]:
    """Return {name, columns: [...]} or None."""
    from sqlalchemy import text
    if dialect in ("postgres", "mysql", "snowflake", "redshift", "duckdb", "mssql"):
        sql = (
            "SELECT tc.constraint_name, kcu.column_name "
            "FROM information_schema.table_constraints tc "
            "JOIN information_schema.key_column_usage kcu "
            "  ON tc.constraint_name = kcu.constraint_name "
            "  AND tc.table_schema = kcu.table_schema "
            "WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_name = :t "
            f"{'AND tc.table_schema = :s' if schema else ''} "
            "ORDER BY kcu.ordinal_position"
        )
        params = {"t": table, **({"s": schema} if schema else {})}
        rows = conn.execute(text(sql), params).fetchall()
    elif dialect == "oracle":
        sql = (
            "SELECT c.constraint_name, cc.column_name "
            "FROM all_constraints c "
            "JOIN all_cons_columns cc ON c.constraint_name = cc.constraint_name AND c.owner = cc.owner "
            "WHERE c.constraint_type = 'P' AND c.table_name = :t "
            f"{'AND c.owner = :s' if schema else ''} "
            "ORDER BY cc.position"
        )
        params = {"t": table.upper(), **({"s": schema.upper()} if schema else {})}
        rows = conn.execute(text(sql), params).fetchall()
    elif dialect == "db2":
        sql = (
            "SELECT k.constname, k.colname FROM syscat.keycoluse k "
            "JOIN syscat.tabconst t ON t.constname = k.constname AND t.tabschema = k.tabschema "
            "WHERE t.type = 'P' AND t.tabname = :t "
            f"{'AND t.tabschema = :s' if schema else ''} "
            "ORDER BY k.colseq"
        )
        params = {"t": table.upper(), **({"s": schema.upper()} if schema else {})}
        rows = conn.execute(text(sql), params).fetchall()
    else:
        return None

    if not rows:
        return None
    return {"name": str(rows[0][0]), "columns": [str(r[1]) for r in rows]}


def fetch_foreign_keys(conn, dialect: str, schema: Optional[str], table: str) -> List[Dict[str, Any]]:
    """Return [{name, columns, ref_schema, ref_table, ref_columns}, ...]."""
    from sqlalchemy import text
    if dialect in ("postgres", "mysql", "snowflake", "redshift", "duckdb", "mssql"):
        sql = (
            "SELECT tc.constraint_name, kcu.column_name, "
            "       ccu.table_schema, ccu.table_name, ccu.column_name "
            "FROM information_schema.table_constraints tc "
            "JOIN information_schema.key_column_usage kcu "
            "  ON tc.constraint_name = kcu.constraint_name "
            "  AND tc.table_schema = kcu.table_schema "
            "JOIN information_schema.constraint_column_usage ccu "
            "  ON ccu.constraint_name = tc.constraint_name "
            "  AND ccu.table_schema = tc.table_schema "
            "WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_name = :t "
            f"{'AND tc.table_schema = :s' if schema else ''} "
            "ORDER BY tc.constraint_name, kcu.ordinal_position"
        )
        params = {"t": table, **({"s": schema} if schema else {})}
        rows = conn.execute(text(sql), params).fetchall()
    elif dialect == "oracle":
        sql = (
            "SELECT c.constraint_name, cc.column_name, "
            "       rc.owner AS ref_schema, rc.table_name AS ref_table, rcc.column_name AS ref_column "
            "FROM all_constraints c "
            "JOIN all_cons_columns cc ON c.constraint_name = cc.constraint_name AND c.owner = cc.owner "
            "JOIN all_constraints rc ON c.r_constraint_name = rc.constraint_name AND c.r_owner = rc.owner "
            "JOIN all_cons_columns rcc ON rc.constraint_name = rcc.constraint_name AND rc.owner = rcc.owner "
            "                          AND rcc.position = cc.position "
            "WHERE c.constraint_type = 'R' AND c.table_name = :t "
            f"{'AND c.owner = :s' if schema else ''} "
            "ORDER BY c.constraint_name, cc.position"
        )
        params = {"t": table.upper(), **({"s": schema.upper()} if schema else {})}
        rows = conn.execute(text(sql), params).fetchall()
    else:
        return []

    fks: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        cname = str(r[0])
        if cname not in fks:
            fks[cname] = {
                "name": cname,
                "columns": [],
                "ref_schema": str(r[2]) if r[2] is not None else None,
                "ref_table": str(r[3]),
                "ref_columns": [],
            }
        fks[cname]["columns"].append(str(r[1]))
        fks[cname]["ref_columns"].append(str(r[4]))
    return list(fks.values())


def _ddl_apply_patterns(
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


def fetch_check_constraints(conn, dialect: str, schema: Optional[str], table: str) -> List[Dict[str, Any]]:
    """Return [{name, expression}, ...] of CHECK constraints (excluding NOT NULL pseudo-CHECKs)."""
    from sqlalchemy import text

    if dialect in ("postgres", "mysql", "snowflake", "redshift", "duckdb", "mssql"):
        sql = (
            "SELECT cc.constraint_name, cc.check_clause "
            "FROM information_schema.check_constraints cc "
            "JOIN information_schema.table_constraints tc "
            "  ON cc.constraint_name = tc.constraint_name "
            "  AND cc.constraint_schema = tc.constraint_schema "
            "WHERE tc.constraint_type = 'CHECK' AND tc.table_name = :t "
            f"{'AND tc.table_schema = :s' if schema else ''}"
        )
        params = {"t": table, **({"s": schema} if schema else {})}
        try:
            rows = conn.execute(text(sql), params).fetchall()
        except Exception:
            return []
        out = []
        for r in rows:
            name = str(r[0])
            expr = str(r[1])
            # Filter Postgres's synthesized NOT NULL CHECKs (names like '12345_67890_1_not_null')
            if name.endswith("_not_null"):
                continue
            if expr.strip().upper().endswith("IS NOT NULL"):
                continue
            out.append({"name": name, "expression": expr})
        return out

    if dialect == "oracle":
        sql = (
            "SELECT constraint_name, search_condition FROM all_constraints "
            "WHERE constraint_type = 'C' AND table_name = :t "
            f"{'AND owner = :s' if schema else ''}"
        )
        params = {"t": table.upper(), **({"s": schema.upper()} if schema else {})}
        try:
            rows = conn.execute(text(sql), params).fetchall()
        except Exception:
            return []
        out = []
        for r in rows:
            expr = str(r[1]) if r[1] is not None else ""
            if "IS NOT NULL" in expr.upper() and len(expr.split()) < 5:
                continue
            out.append({"name": str(r[0]), "expression": expr})
        return out

    if dialect == "db2":
        sql = (
            "SELECT constname, text FROM syscat.checks "
            "WHERE tabname = :t "
            f"{'AND tabschema = :s' if schema else ''}"
        )
        params = {"t": table.upper(), **({"s": schema.upper()} if schema else {})}
        try:
            rows = conn.execute(text(sql), params).fetchall()
        except Exception:
            return []
        return [{"name": str(r[0]), "expression": str(r[1])} for r in rows]

    return []


def fetch_unique_constraints(conn, dialect: str, schema: Optional[str], table: str) -> List[Dict[str, Any]]:
    """Return [{name, columns}, ...] of UNIQUE constraints (not including PRIMARY KEY)."""
    from sqlalchemy import text

    if dialect in ("postgres", "mysql", "snowflake", "redshift", "duckdb", "mssql"):
        sql = (
            "SELECT tc.constraint_name, kcu.column_name "
            "FROM information_schema.table_constraints tc "
            "JOIN information_schema.key_column_usage kcu "
            "  ON tc.constraint_name = kcu.constraint_name "
            "  AND tc.table_schema = kcu.table_schema "
            "WHERE tc.constraint_type = 'UNIQUE' AND tc.table_name = :t "
            f"{'AND tc.table_schema = :s' if schema else ''} "
            "ORDER BY tc.constraint_name, kcu.ordinal_position"
        )
        params = {"t": table, **({"s": schema} if schema else {})}
        try:
            rows = conn.execute(text(sql), params).fetchall()
        except Exception:
            return []
    elif dialect == "oracle":
        sql = (
            "SELECT c.constraint_name, cc.column_name "
            "FROM all_constraints c "
            "JOIN all_cons_columns cc ON c.constraint_name = cc.constraint_name AND c.owner = cc.owner "
            "WHERE c.constraint_type = 'U' AND c.table_name = :t "
            f"{'AND c.owner = :s' if schema else ''} "
            "ORDER BY c.constraint_name, cc.position"
        )
        params = {"t": table.upper(), **({"s": schema.upper()} if schema else {})}
        try:
            rows = conn.execute(text(sql), params).fetchall()
        except Exception:
            return []
    elif dialect == "db2":
        sql = (
            "SELECT k.constname, k.colname FROM syscat.keycoluse k "
            "JOIN syscat.tabconst t ON t.constname = k.constname AND t.tabschema = k.tabschema "
            "WHERE t.type = 'U' AND t.tabname = :t "
            f"{'AND t.tabschema = :s' if schema else ''} "
            "ORDER BY k.colseq"
        )
        params = {"t": table.upper(), **({"s": schema.upper()} if schema else {})}
        try:
            rows = conn.execute(text(sql), params).fetchall()
        except Exception:
            return []
    else:
        return []

    uqs: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        cname = str(r[0])
        if cname not in uqs:
            uqs[cname] = {"name": cname, "columns": []}
        uqs[cname]["columns"].append(str(r[1]))
    return list(uqs.values())


_POSTGRES_ANY_ARRAY_RE = re.compile(
    r"=\s*ANY\s*\(\s*ARRAY\s*\[(.*?)\]\s*\)", re.IGNORECASE | re.DOTALL
)


_POSTGRES_CAST_RE = re.compile(r"::[a-zA-Z_]\w*", re.IGNORECASE)


_CONSTRAINT_SUPPORT: Dict[str, Dict[str, str]] = {
    # State meanings:
    #   'enforced'      — supported via both CREATE TABLE inline and ALTER ADD, enforced at runtime
    #   'inline_only'   — supported at CREATE TABLE only; ALTER ADD will fail (use DDL-first workflow)
    #   'informational' — accepted but not enforced (constraint visible to BI tools / optimizer; bad rows still land)
    #   'unsupported'   — target rejects the DDL entirely; would fail in any workflow
    "postgres":   {"primary_key": "enforced",      "foreign_key": "enforced",      "not_null": "enforced", "default": "enforced", "check": "enforced",      "unique": "enforced"},
    "mysql":      {"primary_key": "enforced",      "foreign_key": "enforced",      "not_null": "enforced", "default": "enforced", "check": "enforced",      "unique": "enforced"},
    "mssql":      {"primary_key": "enforced",      "foreign_key": "enforced",      "not_null": "enforced", "default": "enforced", "check": "enforced",      "unique": "enforced"},
    "oracle":     {"primary_key": "enforced",      "foreign_key": "enforced",      "not_null": "enforced", "default": "enforced", "check": "enforced",      "unique": "enforced"},
    "db2":        {"primary_key": "enforced",      "foreign_key": "enforced",      "not_null": "enforced", "default": "enforced", "check": "enforced",      "unique": "enforced"},
    "duckdb":     {"primary_key": "enforced",      "foreign_key": "enforced",      "not_null": "enforced", "default": "enforced", "check": "inline_only",   "unique": "inline_only"},
    "snowflake":  {"primary_key": "informational", "foreign_key": "informational", "not_null": "enforced", "default": "enforced", "check": "informational", "unique": "informational"},
    "redshift":   {"primary_key": "informational", "foreign_key": "informational", "not_null": "enforced", "default": "enforced", "check": "informational", "unique": "informational"},
    "bigquery":   {"primary_key": "informational", "foreign_key": "informational", "not_null": "enforced", "default": "enforced", "check": "unsupported",   "unique": "informational"},
}


def constraint_enforcement(target_dialect: str, constraint_type: str) -> str:
    """Return 'enforced' | 'inline_only' | 'informational' | 'unsupported' | 'unknown'."""
    matrix = _CONSTRAINT_SUPPORT.get(target_dialect.lower())
    if not matrix:
        return "unknown"
    return matrix.get(constraint_type, "unknown")


def _ddl_exec_with_dry_run(engine, statements: List[str], dry_run: bool) -> None:
    """Execute SQL statements against engine; commit if dry_run=False, rollback if True.

    NOTE on Oracle / MSSQL auto-commit DDL: many SQL dialects auto-commit DDL
    inside a transaction. For those, dry_run=True is best-effort — the rollback
    may not undo a CREATE TABLE. Postgres / DuckDB / Snowflake / BigQuery handle
    transactional DDL correctly. Oracle CREATE TABLE is auto-committed
    immediately. For Oracle dry-run, the rollback won't help — the table exists
    on target until you drop it. This is documented in the assessment workflow.
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


_ddl__DIALECT_QUIRK_MARKERS = {
    "oracle":   [r"\bCONNECT\s+BY\b", r"\bSTART\s+WITH\b", r"\bROWNUM\b", r"\(\s*\+\s*\)",
                 r"\bDUAL\b", r"\bSYSDATE\b", r"\bNVL\b", r"\bDECODE\b", r"\bREGEXP_LIKE\b",
                 r"\bXMLTYPE\b", r"\bCLOB\b", r"\bNCLOB\b", r"\bBLOB\b"],
    "mssql":    [r"\bTOP\s+\d+\b", r"\bCROSS\s+APPLY\b", r"\bOUTER\s+APPLY\b",
                 r"\bUNIQUEIDENTIFIER\b", r"\bGETDATE\b", r"\bISNULL\b"],
    "db2":      [r"\bGRAPHIC\b", r"\bVARGRAPHIC\b", r"\bDBCLOB\b"],
    "postgres": [r"::[a-zA-Z]", r"\bARRAY\s*\[", r"\bSERIAL\b", r"\bJSONB\b", r"\bTSVECTOR\b"],
}


def _ddl_detect_dialect_quirks(source_ddl: str, source_dialect: str) -> List[str]:
    """Return a list of source-dialect-specific markers found in the DDL.

    Empty list = portable / simple. Non-empty = caller should treat as
    'needs_review' or higher complexity.
    """
    if not source_ddl:
        return []
    markers = _ddl__DIALECT_QUIRK_MARKERS.get(source_dialect.lower(), [])
    found = []
    for pattern in markers:
        if re.search(pattern, source_ddl, re.IGNORECASE):
            # Use the pattern's bracketed core as the human-readable marker
            label = re.sub(r"[\\\\bs+()*]+", " ", pattern).strip()
            found.append(label.upper())
    return found


def constraint_support_warnings(target_dialect: str, workflow: str = "any") -> List[str]:
    """Return human-readable warning strings about constraint enforcement on the target.

    `workflow` is 'ddl_first' / 'data_first' / 'any':
      - 'ddl_first': constraints emitted inline at CREATE TABLE; inline_only is fine
      - 'data_first': constraints emitted as ALTER TABLE ADD; inline_only will fail → emit clear guidance
      - 'any' (default): emit all warnings that could ever apply
    """
    matrix = _CONSTRAINT_SUPPORT.get(target_dialect.lower())
    if not matrix:
        return []
    out = []
    for ctype, status in matrix.items():
        if status == "informational":
            out.append(
                f"{ctype} constraints are INFORMATIONAL on {target_dialect} "
                f"— accepted but not enforced (the query optimizer / BI tools see them, "
                f"but bad rows can still land)."
            )
        elif status == "unsupported":
            out.append(
                f"{ctype} constraints are NOT SUPPORTED on {target_dialect} "
                f"— migration will fail loudly. Skip via include/exclude or "
                f"include_{ctype}_constraints=false."
            )
        elif status == "inline_only":
            if workflow == "data_first":
                out.append(
                    f"{ctype} constraints on {target_dialect} can only be set at CREATE TABLE — "
                    f"ALTER TABLE ADD {ctype.upper()} is not supported. These will be SKIPPED. "
                    f"Use database_tables_migration (DDL-first workflow) to apply them inline instead."
                )
            elif workflow == "any":
                out.append(
                    f"{ctype} constraints on {target_dialect} are inline-only "
                    f"(CREATE TABLE only — ALTER ADD won't work). Use database_tables_migration "
                    f"(DDL-first workflow) to apply them."
                )
    return out


def apply_check_substitutions(
    expression: str, function_replacements: Dict[str, str],
) -> str:
    """Apply dialect-portability rewrites to a CHECK expression.

    Built-in rewrites (always applied):
      - Postgres `col = ANY (ARRAY[…])` → `col IN (…)`
      - Strip Postgres `::type` casts (e.g. `'US'::text` → `'US'`)

    Then apply user function_replacements (case-insensitive whole-word).
    """
    out = expression

    # Strip Postgres-style casts BEFORE the ANY-ARRAY rewrite so the inner list is clean
    out = _POSTGRES_CAST_RE.sub("", out)

    # Rewrite `= ANY (ARRAY[a,b,c])` → `IN (a,b,c)`
    out = _POSTGRES_ANY_ARRAY_RE.sub(lambda m: f"IN ({m.group(1)})", out)

    if function_replacements:
        for old, new in function_replacements.items():
            out = re.sub(rf"\b{re.escape(old)}\b", new, out, flags=re.IGNORECASE)
    return out


def list_tables(conn, dialect: str, schemas: Optional[List[str]]) -> List[Tuple[str, str]]:
    from sqlalchemy import text
    if dialect in ("postgres", "mysql", "snowflake", "redshift", "duckdb", "mssql"):
        sql = (
            "SELECT table_schema, table_name FROM information_schema.tables "
            "WHERE table_type = 'BASE TABLE'"
        )
        if dialect in ("postgres", "redshift"):
            sql += " AND table_schema NOT IN ('pg_catalog','information_schema')"
        elif dialect == "snowflake":
            sql += " AND table_schema <> 'INFORMATION_SCHEMA'"
        elif dialect == "mysql":
            sql += " AND table_schema NOT IN ('mysql','sys','information_schema','performance_schema')"
        rows = conn.execute(text(sql)).fetchall()
    elif dialect == "oracle":
        rows = conn.execute(
            text(
                "SELECT owner, table_name FROM all_tables WHERE owner NOT IN "
                "('SYS','SYSTEM','XDB','MDSYS','CTXSYS','DBSNMP','OUTLN','APPQOSSYS',"
                "'GSMADMIN_INTERNAL','AUDSYS','OJVMSYS','ORDDATA','ORDSYS','LBACSYS',"
                "'DVSYS','DVF','REMOTE_SCHEDULER_AGENT','GGSYS')"
            )
        ).fetchall()
    elif dialect == "db2":
        rows = conn.execute(
            text("SELECT tabschema, tabname FROM syscat.tables WHERE type = 'T' AND tabschema NOT LIKE 'SYS%'")
        ).fetchall()
    else:
        raise ValueError(f"Unsupported dialect: {dialect}")

    out = [(str(r[0]), str(r[1])) for r in rows]
    if schemas:
        wanted = {s.lower() for s in schemas}
        out = [(s, n) for s, n in out if s.lower() in wanted]
    return out


_TYPE_MAP = {
    # Oracle → portable
    "VARCHAR2": "VARCHAR",
    "NVARCHAR2": "VARCHAR",
    "CHAR": "CHAR",
    "CLOB": "TEXT",
    "NCLOB": "TEXT",
    "BLOB": "BLOB",
    "RAW": "VARBINARY",
    "NUMBER": "DECIMAL",
    "BINARY_FLOAT": "REAL",
    "BINARY_DOUBLE": "DOUBLE PRECISION",
    "DATE": "TIMESTAMP",   # Oracle DATE is actually a timestamp
    # SQL Server → portable
    "NVARCHAR": "VARCHAR",
    "NCHAR": "CHAR",
    "NTEXT": "TEXT",
    "DATETIME2": "TIMESTAMP",
    "DATETIMEOFFSET": "TIMESTAMP WITH TIME ZONE",
    "MONEY": "DECIMAL(19,4)",
    "SMALLMONEY": "DECIMAL(10,4)",
    "BIT": "BOOLEAN",
    "UNIQUEIDENTIFIER": "VARCHAR(36)",
    "IMAGE": "BLOB",
    # Db2 → portable
    "GRAPHIC": "VARCHAR",
    "VARGRAPHIC": "VARCHAR",
    "DBCLOB": "TEXT",
    # Postgres → portable
    "CHARACTER VARYING": "VARCHAR",
    "CHARACTER": "CHAR",
    "TIMESTAMP WITHOUT TIME ZONE": "TIMESTAMP",
    "TIMESTAMP WITH TIME ZONE": "TIMESTAMPTZ",
    "DOUBLE PRECISION": "DOUBLE",
    "BYTEA": "BLOB",
    "JSONB": "JSON",
    "USER-DEFINED": "VARCHAR",
}


def translate_type(col: Dict[str, Any], target_dialect: str) -> str:
    """Build a target-dialect column type from a source column row."""
    raw = col["data_type"].upper().strip()
    base = _TYPE_MAP.get(raw, raw)

    char_len = col.get("char_max_length")
    num_p = col.get("num_precision")
    num_s = col.get("num_scale")

    # Snowflake/BQ: VARCHAR has no length parameter; just VARCHAR.
    if target_dialect in ("snowflake", "bigquery") and base in ("VARCHAR", "CHAR"):
        return base

    if base in ("VARCHAR", "CHAR") and char_len:
        return f"{base}({char_len})"

    if base in ("NUMERIC", "DECIMAL") and num_p:
        if num_s is not None and num_s > 0:
            return f"DECIMAL({num_p},{num_s})"
        return f"DECIMAL({num_p})"

    return base


def quote_default(d: Any, target_dialect: str) -> Optional[str]:
    """Pass through column defaults if safe; skip dialect-specific ones."""
    if d is None:
        return None
    s = str(d).strip()
    if not s:
        return None
    # Drop Postgres ::cast suffixes which won't translate cleanly.
    s = re.sub(r"::[a-zA-Z _]+(\([^)]*\))?", "", s)
    # Skip nextval('...') style — those refer to sequences that may not exist on target
    if "nextval" in s.lower():
        return None
    # Map common dialect-specific defaults
    s_lower = s.lower()
    if s_lower in ("now()", "current_timestamp", "sysdate", "getdate()"):
        return "CURRENT_TIMESTAMP"
    if s_lower in ("current_date",):
        return "CURRENT_DATE"
    return s


def build_create_table(
    qualified_target: str,
    columns: List[Dict[str, Any]],
    primary_key: Optional[Dict[str, Any]],
    foreign_keys: List[Dict[str, Any]],
    target_dialect: str,
    table_replacements: Dict[str, str],
    check_constraints: Optional[List[Dict[str, Any]]] = None,
    unique_constraints: Optional[List[Dict[str, Any]]] = None,
    function_replacements: Optional[Dict[str, str]] = None,
    inline_constraints: bool = True,
) -> str:
    """Build a portable CREATE TABLE statement."""
    col_lines = []
    for c in columns:
        type_str = translate_type(c, target_dialect)
        nullability = "" if c["nullable"] else " NOT NULL"
        default = quote_default(c["default"], target_dialect)
        default_str = f" DEFAULT {default}" if default else ""
        col_lines.append(f"  {c['name']} {type_str}{nullability}{default_str}")

    constraint_lines = []
    if inline_constraints:
        if primary_key:
            cols = ", ".join(primary_key["columns"])
            constraint_lines.append(f"  PRIMARY KEY ({cols})")
        for fk in foreign_keys:
            cols = ", ".join(fk["columns"])
            ref_qualified = (
                f"{fk['ref_schema']}.{fk['ref_table']}" if fk["ref_schema"]
                else fk["ref_table"]
            )
            for old, new in table_replacements.items():
                if old.lower() == ref_qualified.lower():
                    ref_qualified = new
                    break
            ref_cols = ", ".join(fk["ref_columns"])
            constraint_lines.append(
                f"  FOREIGN KEY ({cols}) REFERENCES {ref_qualified} ({ref_cols})"
            )
        for uq in (unique_constraints or []):
            cols = ", ".join(uq["columns"])
            constraint_lines.append(f"  UNIQUE ({cols})")
        for ck in (check_constraints or []):
            expr = apply_check_substitutions(ck["expression"], function_replacements or {})
            constraint_lines.append(f"  CHECK ({expr})")

    body = ",\n".join(col_lines + constraint_lines)
    return f"CREATE TABLE {qualified_target} (\n{body}\n)"


def alter_add_primary_key(table: str, pk: Dict[str, Any]) -> str:
    cols = ", ".join(pk["columns"])
    return f"ALTER TABLE {table} ADD PRIMARY KEY ({cols})"


def alter_add_foreign_key(
    table: str, fk: Dict[str, Any], table_replacements: Dict[str, str],
) -> str:
    cols = ", ".join(fk["columns"])
    ref_qualified = (
        f"{fk['ref_schema']}.{fk['ref_table']}" if fk["ref_schema"]
        else fk["ref_table"]
    )
    for old, new in table_replacements.items():
        if old.lower() == ref_qualified.lower():
            ref_qualified = new
            break
    ref_cols = ", ".join(fk["ref_columns"])
    return (
        f"ALTER TABLE {table} ADD FOREIGN KEY ({cols}) "
        f"REFERENCES {ref_qualified} ({ref_cols})"
    )


def alter_set_not_null(table: str, col: str, target_dialect: str) -> str:
    if target_dialect == "mssql":
        return f"-- not supported standalone on {target_dialect} via plain ALTER (needs column re-declare with type)"
    return f"ALTER TABLE {table} ALTER COLUMN {col} SET NOT NULL"


def alter_set_default(table: str, col: str, default: str, target_dialect: str) -> str:
    return f"ALTER TABLE {table} ALTER COLUMN {col} SET DEFAULT {default}"


def alter_add_check(
    table: str, ck: Dict[str, Any], function_replacements: Optional[Dict[str, str]] = None,
) -> str:
    expr = apply_check_substitutions(ck["expression"], function_replacements or {})
    return f"ALTER TABLE {table} ADD CHECK ({expr})"


def alter_add_unique(table: str, uq: Dict[str, Any]) -> str:
    cols = ", ".join(uq["columns"])
    return f"ALTER TABLE {table} ADD UNIQUE ({cols})"


def drop_table_if_exists(target_dialect: str, qualified: str) -> str:
    if target_dialect == "mssql":
        return f"IF OBJECT_ID('{qualified}','U') IS NOT NULL DROP TABLE {qualified}"
    return f"DROP TABLE IF EXISTS {qualified}"


_view_VALID_DIALECTS = {
    "postgres", "mysql", "mssql", "oracle", "db2",
    "snowflake", "redshift", "duckdb",
}


_view__SCHEME_ALIASES = {
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


def _view_validate_dialect(dialect: str, role: str = "source") -> None:
    if dialect not in _view_VALID_DIALECTS:
        raise ValueError(
            f"{role}_type={dialect!r} not supported. Use one of {sorted(_view_VALID_DIALECTS)}."
        )


def _view_engine_for(env_var: str, dialect: str):
    """Build a SQLAlchemy engine from a URL stored in env_var."""
    from sqlalchemy import create_engine

    url = os.environ.get(env_var)
    if not url:
        raise EnvironmentError(f"Connection env var {env_var!r} is not set")
    for prefix, sa_prefix in _view__SCHEME_ALIASES.items():
        if url.startswith(prefix) and not url.startswith(sa_prefix):
            url = sa_prefix + url[len(prefix):]
            break
    return create_engine(url)


def _view_split_qualified(qualified: str) -> Tuple[Optional[str], str]:
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


def _view_exec_with_dry_run(engine, statements: List[str], dry_run: bool) -> None:
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


_view__DIALECT_QUIRK_MARKERS = {
    "oracle":   [r"\bCONNECT\s+BY\b", r"\bSTART\s+WITH\b", r"\bROWNUM\b", r"\(\s*\+\s*\)",
                 r"\bDUAL\b", r"\bSYSDATE\b", r"\bNVL\b", r"\bDECODE\b", r"\bREGEXP_LIKE\b"],
    "mssql":    [r"\bTOP\s+\d+\b", r"\bCROSS\s+APPLY\b", r"\bOUTER\s+APPLY\b",
                 r"\bUNIQUEIDENTIFIER\b", r"\bGETDATE\b", r"\bISNULL\b"],
    "db2":      [r"\bGRAPHIC\b", r"\bVARGRAPHIC\b"],
    "postgres": [r"\bARRAY\s*\["],
}


def _view_detect_dialect_quirks(source_ddl: str, source_dialect: str) -> List[str]:
    """Return source-dialect-specific markers found in a view body. Empty list = portable."""
    if not source_ddl:
        return []
    markers = _view__DIALECT_QUIRK_MARKERS.get(source_dialect.lower(), [])
    found = []
    for pattern in markers:
        if re.search(pattern, source_ddl, re.IGNORECASE):
            label = re.sub(r"[\\\\bs+()*]+", " ", pattern).strip()
            found.append(label.upper())
    return found


def _view_apply_patterns(
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
        schema, name = _view_split_qualified(target_view)
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




class DatabaseMigrationAssessmentComponent(dg.Component, dg.Model, dg.Resolvable):
    """Pre-flight migration assessment.

    Reads source DDL for tables + views, dry-runs the target migration
    (inside a transaction that ROLLS BACK), and returns a consolidated
    DataFrame: { object_type, schema_name, name, target_name, status,
    complexity, dialect_markers, reason, proposed_target_ddl }.

    Run this BEFORE the real migration to scope the project and find
    issues without touching target state.
    """

    asset_name: str = Field(description="Output Dagster asset name")
    source_connection_env_var: str = Field(description="Env var with source SQLAlchemy URL")
    target_connection_env_var: str = Field(description="Env var with target SQLAlchemy URL")
    source_type: str = Field(description="Source DB dialect: postgres / mysql / mssql / oracle / db2 / snowflake / redshift")
    target_type: str = Field(description="Target DB dialect (same options)")

    schemas: Optional[List[str]] = Field(
        default=None,
        description="Source schemas to assess (default: all non-system)",
    )
    include_patterns: Optional[List[str]] = Field(
        default=None,
        description="fnmatch-style glob patterns against qualified object names",
    )
    exclude_patterns: Optional[List[str]] = Field(
        default=None,
        description="fnmatch-style globs to exclude",
    )
    target_schema: Optional[str] = Field(
        default=None,
        description="Override target schema for the proposed target names",
    )
    table_replacements: Optional[Dict[str, str]] = Field(
        default=None,
        description="FK reference / view body rewrites (old qualified name → new)",
    )
    function_replacements: Optional[Dict[str, str]] = Field(
        default=None,
        description="Case-insensitive whole-word substitutions applied to CHECK expressions + view bodies (e.g. {'NVL': 'COALESCE'})",
    )

    assess_tables: bool = Field(default=True, description="Include CREATE TABLE assessment")
    assess_views: bool = Field(default=True, description="Include CREATE VIEW assessment")
    assess_constraints: bool = Field(
        default=False,
        description="Also assess CHECK/UNIQUE/PK/FK as ALTER TABLE (data-first style). Default false — DDL-first is preferred.",
    )

    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
    description: Optional[str] = Field(default=None, description="Asset description")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners")
    asset_tags: Optional[Dict[str, str]] = Field(default=None, description="Additional tags")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds (default: [source_type, 'assessment'])")
    deps: Optional[List[str]] = Field(default=None, description="Upstream asset keys")

    @classmethod
    def get_description(cls) -> str:
        return (
            "Pre-flight migration assessment: dry-run every table + view migration against "
            "target inside a transaction, ROLLBACK every change. Returns a consolidated "
            "DataFrame with status, complexity heuristic, dialect markers, and proposed DDL."
        )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        source_type = self.source_type.lower()
        target_type = self.target_type.lower()
        _ddl_validate_dialect(source_type, role="source")
        _ddl_validate_dialect(target_type, role="target")

        kinds_list = self.kinds or [source_type, "assessment", "migration"]
        all_tags = dict(self.asset_tags or {})
        for k in kinds_list:
            all_tags[f"dagster/kind/{k}"] = ""

        source_connection_env_var = self.source_connection_env_var
        target_connection_env_var = self.target_connection_env_var
        schemas = list(self.schemas) if self.schemas else None
        include_patterns = list(self.include_patterns) if self.include_patterns else None
        exclude_patterns = list(self.exclude_patterns) if self.exclude_patterns else None
        target_schema = self.target_schema
        table_replacements = dict(self.table_replacements or {})
        function_replacements = dict(self.function_replacements or {})
        assess_tables = self.assess_tables
        assess_views = self.assess_views
        assess_constraints = self.assess_constraints

        @dg.asset(
            key=dg.AssetKey.from_user_string(self.asset_name),
            group_name=self.group_name,
            description=self.description or self.get_description(),
            owners=self.owners or [],
            tags=all_tags,
            kinds=set(kinds_list),
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def _assessment(context: dg.AssetExecutionContext):
            import pandas as pd

            from sqlalchemy import text as _sql_text

            source_engine = _ddl_engine_for(source_connection_env_var, source_type)
            target_engine = _ddl_engine_for(target_connection_env_var, target_type)

            # Pre-create target schema if specified — needed so dry-runs can
            # reference the target schema. Empty-schema creation is the only
            # state change the assessment makes; all other changes are rolled back.
            if target_schema:
                try:
                    with target_engine.begin() as tgt:
                        tgt.execute(_sql_text(f"CREATE SCHEMA IF NOT EXISTS {target_schema}"))
                    context.log.info(
                        f"Ensured target schema {target_schema!r} exists for dry-runs "
                        "(only state change this asset makes)"
                    )
                except Exception as e:
                    context.log.warning(
                        f"Could not auto-create target schema {target_schema!r}: "
                        f"{e.__class__.__name__}: {str(e)[:200]}"
                    )

            # Pre-flight target-enforcement warnings
            for warning in constraint_support_warnings(target_type, workflow="any"):
                context.log.warning(f"⚠ {warning}")

            # Build the per-target-type "informational" set so we can flag medium-complexity rows
            target_informational = {
                ctype for ctype in
                ("primary_key", "foreign_key", "not_null", "default", "check", "unique")
                if constraint_enforcement(target_type, ctype) == "informational"
            }

            rows: List[Dict[str, Any]] = []

            # ──────────────────────────────────────────────────────────────
            # Tables
            # ──────────────────────────────────────────────────────────────
            if assess_tables:
                with source_engine.connect() as src:
                    discovered_tables = list_tables(src, source_type, schemas)
                    discovered_tables = _ddl_apply_patterns(
                        discovered_tables, include_patterns, exclude_patterns
                    )
                    context.log.info(f"Assessing {len(discovered_tables)} table(s)")

                    for src_schema, table_name in discovered_tables:
                        src_qual = f"{src_schema}.{table_name}"
                        tgt_qual = (
                            f"{target_schema}.{table_name}" if target_schema
                            else f"{src_schema}.{table_name}"
                        )

                        try:
                            cols = fetch_columns(src, source_type, src_schema, table_name)
                            pk = fetch_primary_key(src, source_type, src_schema, table_name)
                            fks = fetch_foreign_keys(src, source_type, src_schema, table_name)
                            cks = fetch_check_constraints(src, source_type, src_schema, table_name)
                            uqs = fetch_unique_constraints(src, source_type, src_schema, table_name)
                            proposed_ddl = build_create_table(
                                tgt_qual, cols, pk, fks, target_type, table_replacements,
                                check_constraints=cks, unique_constraints=uqs,
                                function_replacements=function_replacements,
                            )
                        except Exception as e:
                            rows.append({
                                "object_type": "table", "schema_name": src_schema,
                                "name": table_name, "target_name": tgt_qual,
                                "status": "will_fail", "complexity": "complex",
                                "dialect_markers": "",
                                "reason": f"DDL extraction failed: {e.__class__.__name__}: {str(e)[:200]}",
                                "proposed_target_ddl": "",
                            })
                            continue

                        # Detect markers in any source CHECK clauses + column defaults
                        markers_set = set()
                        for ck in cks:
                            for m in _ddl_detect_dialect_quirks(
                                ck.get("expression", ""), source_type
                            ):
                                markers_set.add(m)
                        for c in cols:
                            if c.get("default"):
                                for m in _ddl_detect_dialect_quirks(
                                    str(c["default"]), source_type
                                ):
                                    markers_set.add(m)
                        markers = sorted(markers_set)

                        # Substitutions used?
                        has_substitutions = bool(
                            table_replacements and fks
                        ) or bool(function_replacements and cks)

                        # Target informational count for this table's constraints
                        target_info_count = 0
                        if pk and "primary_key" in target_informational: target_info_count += 1
                        if fks and "foreign_key" in target_informational: target_info_count += len(fks)
                        if cks and "check" in target_informational: target_info_count += len(cks)
                        if uqs and "unique" in target_informational: target_info_count += len(uqs)

                        # Dry-run try
                        try:
                            _ddl_exec_with_dry_run(target_engine, [proposed_ddl], dry_run=True)
                            would_succeed = True
                            reason = ""
                        except Exception as e:
                            would_succeed = False
                            reason = f"{e.__class__.__name__}: {str(e)[:300]}"

                        status = (
                            "auto_convertible" if would_succeed and not markers
                            else "needs_review" if would_succeed
                            else "will_fail"
                        )
                        complexity = _classify_complexity(
                            would_succeed, len(markers), has_substitutions, target_info_count,
                        )
                        rows.append({
                            "object_type": "table", "schema_name": src_schema,
                            "name": table_name, "target_name": tgt_qual,
                            "status": status, "complexity": complexity,
                            "dialect_markers": ",".join(markers),
                            "reason": reason,
                            "proposed_target_ddl": proposed_ddl,
                        })

            # ──────────────────────────────────────────────────────────────
            # Views
            # ──────────────────────────────────────────────────────────────
            if assess_views:
                with source_engine.connect() as src:
                    discovered_views = list_views(src, source_type, schemas)
                    discovered_views = _view_apply_patterns(
                        discovered_views, include_patterns, exclude_patterns
                    )
                    context.log.info(f"Assessing {len(discovered_views)} view(s)")

                    for src_schema, view_name in discovered_views:
                        src_qual = f"{src_schema}.{view_name}"
                        tgt_qual = (
                            f"{target_schema}.{view_name}" if target_schema
                            else f"{src_schema}.{view_name}"
                        )

                        try:
                            view_ddl = fetch_view_definition(
                                src, source_type, src_schema, view_name
                            )
                            if view_ddl is None:
                                rows.append({
                                    "object_type": "view", "schema_name": src_schema,
                                    "name": view_name, "target_name": tgt_qual,
                                    "status": "will_fail", "complexity": "complex",
                                    "dialect_markers": "",
                                    "reason": "view definition not found in source",
                                    "proposed_target_ddl": "",
                                })
                                continue
                            rewritten = apply_substitutions(
                                view_ddl, table_replacements, function_replacements
                            )
                            stmts = replace_view_statements(
                                target_type, tgt_qual, rewritten
                            )
                            proposed_view_ddl = "; ".join(stmts)
                        except Exception as e:
                            rows.append({
                                "object_type": "view", "schema_name": src_schema,
                                "name": view_name, "target_name": tgt_qual,
                                "status": "will_fail", "complexity": "complex",
                                "dialect_markers": "",
                                "reason": f"DDL extraction failed: {e.__class__.__name__}: {str(e)[:200]}",
                                "proposed_target_ddl": "",
                            })
                            continue

                        markers = _view_detect_dialect_quirks(view_ddl, source_type)
                        has_substitutions = bool(table_replacements) or bool(function_replacements)

                        try:
                            _view_exec_with_dry_run(target_engine, stmts, dry_run=True)
                            would_succeed = True
                            reason = ""
                        except Exception as e:
                            would_succeed = False
                            reason = f"{e.__class__.__name__}: {str(e)[:300]}"

                        status = (
                            "auto_convertible" if would_succeed and not markers
                            else "needs_review" if would_succeed
                            else "will_fail"
                        )
                        complexity = _classify_complexity(
                            would_succeed, len(markers), has_substitutions, 0,
                        )
                        rows.append({
                            "object_type": "view", "schema_name": src_schema,
                            "name": view_name, "target_name": tgt_qual,
                            "status": status, "complexity": complexity,
                            "dialect_markers": ",".join(markers),
                            "reason": reason,
                            "proposed_target_ddl": proposed_view_ddl,
                        })

            report = pd.DataFrame(rows)
            n_total = len(report)

            if n_total == 0:
                context.log.warning("Assessment found 0 objects — check schemas / include_patterns")
                return dg.MaterializeResult(
                    value=report,
                    metadata={
                        "row_count": dg.MetadataValue.int(0),
                        "dagster/row_count": dg.MetadataValue.int(0),
                    },
                )

            n_auto = int((report["status"] == "auto_convertible").sum())
            n_review = int((report["status"] == "needs_review").sum())
            n_fail = int((report["status"] == "will_fail").sum())
            n_simple = int((report["complexity"] == "simple").sum())
            n_medium = int((report["complexity"] == "medium").sum())
            n_complex = int((report["complexity"] == "complex").sum())

            # Rough effort heuristic
            est_manual_hours = n_complex * 2 + n_medium * 0.5
            est_label = (
                f"~{est_manual_hours:.0f}h" if est_manual_hours < 16
                else f"~{est_manual_hours/8:.1f} person-days"
            )

            context.log.info(
                f"Assessment: {n_auto} auto-convertible, {n_review} need review, "
                f"{n_fail} will fail | complexity: {n_simple} simple, "
                f"{n_medium} medium, {n_complex} complex | est effort: {est_label}"
            )

            return dg.MaterializeResult(
                value=report,
                metadata={
                    "row_count": dg.MetadataValue.int(n_total),
                    "dagster/row_count": dg.MetadataValue.int(n_total),
                    "auto_convertible": dg.MetadataValue.int(n_auto),
                    "needs_review": dg.MetadataValue.int(n_review),
                    "will_fail": dg.MetadataValue.int(n_fail),
                    "auto_convertible_pct": dg.MetadataValue.float(
                        round(100.0 * n_auto / n_total, 1)
                    ),
                    "complexity_simple": dg.MetadataValue.int(n_simple),
                    "complexity_medium": dg.MetadataValue.int(n_medium),
                    "complexity_complex": dg.MetadataValue.int(n_complex),
                    "estimated_manual_effort": dg.MetadataValue.text(est_label),
                    "report": dg.MetadataValue.md(
                        report[["object_type", "schema_name", "name", "status",
                                "complexity", "dialect_markers", "reason"]]
                        .to_markdown(index=False)
                    ),
                },
            )

        return dg.Definitions(assets=[_assessment])
