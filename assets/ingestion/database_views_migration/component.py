"""Database Views Bulk Migration.

Migrate every view in a source schema to a target in one asset. Emits a
status DataFrame ({schema_name, view_name, status, error_message}) so the
team has a concrete pass/fail report of the migration.

For one-off / composable single-view migration, use `database_view_migration`.
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
    """Backwards-compatible wrapper."""
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



class DatabaseViewsMigrationComponent(dg.Component, dg.Model, dg.Resolvable):
    """Migrate every view in a source schema to a target — one-shot bulk move
    with a per-view status DataFrame for the migration completion report.
    """

    asset_name: str = Field(description="Output Dagster asset name")
    source_connection: Optional[str] = Field(default=None, description="SQLAlchemy URL for source DB. Set this OR source_connection_env_var.")
    source_connection_env_var: Optional[str] = Field(default=None, description="Env var with source SQLAlchemy URL. Set this OR source_connection.")
    target_connection: Optional[str] = Field(default=None, description="SQLAlchemy URL for target DB. Set this OR target_connection_env_var.")
    target_connection_env_var: Optional[str] = Field(default=None, description="Env var with target SQLAlchemy URL. Set this OR target_connection.")
    source_type: str = Field(description="Source DB dialect")
    target_type: str = Field(description="Target DB dialect")

    schemas: Optional[List[str]] = Field(
        default=None,
        description="Source schemas to scan (default: all non-system schemas)",
    )
    include_patterns: Optional[List[str]] = Field(
        default=None,
        description=(
            "fnmatch-style glob patterns (case-insensitive) against qualified view "
            "names. Examples: ['APP.V_*'], ['HR.V_EMPLOYEES', 'FINANCE.V_*']. "
            "Default: include all discovered views."
        ),
    )
    exclude_patterns: Optional[List[str]] = Field(
        default=None,
        description=(
            "fnmatch-style glob patterns to exclude. Applied AFTER include_patterns. "
            "Examples: ['*_LEGACY', 'APP.V_ORACLE_*'] for views you've rewritten by hand."
        ),
    )

    target_schema: Optional[str] = Field(
        default=None,
        description="Override target schema name. If unset, uses source schema name verbatim. e.g. 'RAW' → all views land in RAW.<view_name>.",
    )
    table_replacements: Optional[Dict[str, str]] = Field(
        default=None,
        description="Substitutions applied to every view's DDL — old qualified name → new",
    )
    function_replacements: Optional[Dict[str, str]] = Field(
        default=None,
        description="Dialect function substitutions applied case-insensitive whole-word",
    )

    view_ddl_overrides: Optional[Dict[str, str]] = Field(
        default=None,
        description=(
            "Per-view body override. Keys are source qualified names (e.g. "
            "'HR.V_LEGACY'), values are the SELECT body (NOT including "
            "CREATE VIEW). Used when source uses dialect-specific SQL the "
            "target can't run (Oracle CONNECT BY, MSSQL CROSS APPLY, etc.) "
            "and the team has rewritten it manually. Status reports as "
            "'override_success' / 'override_failed'."
        ),
    )

    dry_run: bool = Field(
        default=False,
        description=(
            "If true, attempt CREATE VIEW for each in a transaction then ROLLBACK — no state "
            "changes on target. Status rows use 'would_succeed' / 'would_fail'. Use before the "
            "real run to see what would happen. Best-effort on Oracle (DDL is auto-commit)."
        ),
    )

    fail_on_any_error: bool = Field(
        default=False,
        description="If true, the asset materialization fails when ANY view fails to migrate. Default: succeed and report failures in the status DataFrame.",
    )

    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
    description: Optional[str] = Field(default=None, description="Asset description")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners")
    asset_tags: Optional[Dict[str, str]] = Field(default=None, description="Additional tags")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds (default: ['views', source_type, target_type])")
    deps: Optional[List[str]] = Field(default=None, description="Upstream asset keys (typically table replications)")

    @classmethod
    def get_description(cls) -> str:
        return (
            "Bulk-migrate every view in a source schema to a target with optional "
            "substitutions. Emits a per-view status DataFrame for the migration report."
        )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        source_type = self.source_type.lower()
        target_type = self.target_type.lower()
        validate_dialect(source_type, role="source")
        validate_dialect(target_type, role="target")

        kinds_list = self.kinds or ["views", source_type, target_type]
        all_tags = dict(self.asset_tags or {})
        for k in kinds_list:
            all_tags[f"dagster/kind/{k}"] = ""

        # Capture component state in closure
        source_connection_literal = self.source_connection
        source_connection_env_var = self.source_connection_env_var
        target_connection_literal = self.target_connection
        target_connection_env_var = self.target_connection_env_var
        schemas = list(self.schemas) if self.schemas else None
        include_patterns = list(self.include_patterns) if self.include_patterns else None
        exclude_patterns = list(self.exclude_patterns) if self.exclude_patterns else None
        target_schema = self.target_schema
        table_replacements = dict(self.table_replacements or {})
        function_replacements = dict(self.function_replacements or {})
        view_ddl_overrides = {k.lower(): v for k, v in (self.view_ddl_overrides or {}).items()}
        dry_run = self.dry_run
        fail_on_any_error = self.fail_on_any_error

        @dg.asset(
            key=dg.AssetKey.from_user_string(self.asset_name),
            group_name=self.group_name,
            description=self.description or self.get_description(),
            owners=self.owners or [],
            tags=all_tags,
            kinds=set(kinds_list),
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def _bulk_views(context: dg.AssetExecutionContext):
            import pandas as pd

            source_url = resolve_connection(source_connection_literal, source_connection_env_var, "source_connection")
            target_url = resolve_connection(target_connection_literal, target_connection_env_var, "target_connection")
            source_engine = engine_for_url(source_url, source_type)
            target_engine = engine_for_url(target_url, target_type)

            with source_engine.connect() as src:
                discovered = list_views(src, source_type, schemas)
                discovered = apply_patterns(discovered, include_patterns, exclude_patterns)

                context.log.info(f"Found {len(discovered)} view(s) to migrate")

                rows = []
                for src_schema, view_name in discovered:
                    src_qual = f"{src_schema}.{view_name}"
                    tgt_qual = (
                        f"{target_schema}.{view_name}" if target_schema
                        else f"{src_schema}.{view_name}"
                    )
                    row = {
                        "schema_name": src_schema,
                        "view_name": view_name,
                        "target_view": tgt_qual,
                        "status": "pending",
                        "error_message": None,
                        "ddl_chars": 0,
                    }
                    override_body = view_ddl_overrides.get(src_qual.lower())
                    if override_body is not None:
                        row["ddl_chars"] = len(override_body)
                        stmts = replace_view_statements(target_type, tgt_qual, override_body)
                        try:
                            exec_with_dry_run(target_engine, stmts, dry_run)
                            row["status"] = "would_override_succeed" if dry_run else "override_success"
                            context.log.info(
                                f"View override{' (dry-run)' if dry_run else ''}: {src_qual} → {tgt_qual}"
                            )
                        except Exception as e:
                            row["status"] = "would_override_fail" if dry_run else "override_failed"
                            row["error_message"] = f"{e.__class__.__name__}: {str(e)[:300]}"
                            context.log.warning(
                                f"Override {'would-fail' if dry_run else 'failed'}: {src_qual}: {row['error_message']}"
                            )
                        rows.append(row)
                        continue

                    try:
                        ddl = fetch_view_definition(src, source_type, src_schema, view_name)
                        if ddl is None:
                            row["status"] = "failed"
                            row["error_message"] = "view definition not found"
                            rows.append(row)
                            continue
                        rewritten = apply_substitutions(
                            ddl, table_replacements, function_replacements
                        )
                        row["ddl_chars"] = len(rewritten)
                        stmts = replace_view_statements(target_type, tgt_qual, rewritten)
                        exec_with_dry_run(target_engine, stmts, dry_run)
                        row["status"] = "would_succeed" if dry_run else "success"
                        context.log.info(
                            f"View{' (dry-run)' if dry_run else ''} migrated: {src_qual} → {tgt_qual}"
                        )
                    except Exception as e:
                        row["status"] = "would_fail" if dry_run else "failed"
                        row["error_message"] = (
                            f"{e.__class__.__name__}: {str(e)[:300]}"
                        )
                        context.log.warning(
                            f"{'Would-fail' if dry_run else 'Failed'}: {src_qual} → {tgt_qual}: {row['error_message']}"
                        )
                    rows.append(row)

            report = pd.DataFrame(rows)
            n_total = len(report)
            success_states = ["success", "override_success", "would_succeed", "would_override_succeed"]
            failure_states = ["failed", "override_failed", "would_fail", "would_override_fail"]
            n_ok = int(report["status"].isin(success_states).sum()) if n_total else 0
            n_fail = int(report["status"].isin(failure_states).sum()) if n_total else 0
            verb = "would succeed" if dry_run else "succeeded"
            verb_fail = "would fail" if dry_run else "failed"

            context.log.info(
                f"Bulk views migration{' (DRY RUN — nothing committed)' if dry_run else ''}: "
                f"{n_ok}/{n_total} {verb}, {n_fail} {verb_fail}"
            )

            if fail_on_any_error and n_fail > 0:
                raise RuntimeError(
                    f"{n_fail} view(s) failed to migrate. See asset metadata for details."
                )

            md_summary = (
                report[["schema_name", "view_name", "status", "error_message"]]
                .to_markdown(index=False)
                if n_total else "_no views found_"
            )

            return dg.MaterializeResult(
                value=report,
                metadata={
                    "row_count": dg.MetadataValue.int(n_total),
                    "dagster/row_count": dg.MetadataValue.int(n_total),
                    "views_succeeded": dg.MetadataValue.int(n_ok),
                    "views_failed": dg.MetadataValue.int(n_fail),
                    "auto_convertible_pct": dg.MetadataValue.float(
                        round(100.0 * n_ok / n_total, 1) if n_total else 0.0
                    ),
                    "dry_run": dg.MetadataValue.bool(dry_run),
                    "report": dg.MetadataValue.md(str(md_summary)),
                },
            )

        return dg.Definitions(assets=[_bulk_views])
