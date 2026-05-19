"""Database Table DDL Migration.

DDL-first warehouse migration: read source-side `CREATE TABLE` shape
(columns + types + primary keys + foreign keys + NOT NULL + defaults) from
INFORMATION_SCHEMA, build a portable target-dialect CREATE TABLE statement,
execute it on the target. Run BEFORE `database_replication` (with
`mode: append`) so data streams into pre-shaped tables with all constraints
present.

Emits a per-table status DataFrame (`status`, `error_message`, `ddl_preview`,
column + constraint counts) — the migration completion report.
"""
from typing import Dict, List, Optional

import dagster as dg
from pydantic import Field

from . import _ddl_helpers as h


class DatabaseTablesMigrationComponent(dg.Component, dg.Model, dg.Resolvable):
    """Recreate tables on target with full DDL (types + PKs + FKs + NOT NULL + defaults).

    Stands alone (use it to preview the target schema, or wire `database_replication`
    behind it with `mode: append`). Emits a per-table status DataFrame for the
    migration completion report.
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
        description="Source schemas to scan (default: all non-system)",
    )
    include_patterns: Optional[List[str]] = Field(
        default=None,
        description=(
            "fnmatch-style glob patterns (case-insensitive) against qualified "
            "table names. Examples: ['HR.*'], ['HR.EMPLOYEES', 'FINANCE.STG_*']. "
            "Default: include all discovered tables."
        ),
    )
    exclude_patterns: Optional[List[str]] = Field(
        default=None,
        description=(
            "fnmatch-style glob patterns to exclude. Applied AFTER include_patterns. "
            "Examples: ['*_TEMP', '*.BKP_*', 'HR.AUDIT_*']."
        ),
    )

    target_schema: Optional[str] = Field(
        default=None,
        description="Override target schema (e.g. 'RAW' → all tables land in RAW.<name>). Default: keep source schema name.",
    )
    table_replacements: Optional[Dict[str, str]] = Field(
        default=None,
        description="FK reference rewrites (old qualified name → new). Used when foreign keys point at tables that landed in different schemas on target.",
    )
    drop_if_exists: bool = Field(
        default=False,
        description="If true, DROP TABLE IF EXISTS on target before CREATE. Default: false (CREATE will fail if it already exists, which is safer for incremental re-runs).",
    )
    include_foreign_keys: bool = Field(
        default=True,
        description="Include FK constraints in CREATE TABLE. Set false to migrate types + PKs only.",
    )
    include_primary_keys: bool = Field(
        default=True,
        description="Include PK constraint in CREATE TABLE.",
    )
    include_check_constraints: bool = Field(
        default=True,
        description="Include CHECK constraints. Dialect-specific functions in CHECK expressions can fail on target — use function_replacements or table_ddl_overrides.",
    )
    include_unique_constraints: bool = Field(
        default=True,
        description="Include UNIQUE constraints (non-PK).",
    )

    function_replacements: Optional[Dict[str, str]] = Field(
        default=None,
        description="Case-insensitive whole-word substitutions applied to CHECK expressions. E.g. {'NVL': 'COALESCE', 'REGEXP_LIKE': 'REGEXP_MATCHES'}.",
    )

    table_ddl_overrides: Optional[Dict[str, str]] = Field(
        default=None,
        description=(
            "Per-table CREATE TABLE override. Keys are source qualified names "
            "(e.g. 'HR.WEIRD_TABLE'), values are full CREATE TABLE SQL for the "
            "TARGET (already pointing at the target schema/name). Used when "
            "the auto-generated DDL fails because of dialect-specific types "
            "(XMLTYPE, OBJECT, etc.) or quirky constraints. Status is reported "
            "as 'override_success' / 'override_failed' in the DataFrame."
        ),
    )

    dry_run: bool = Field(
        default=False,
        description=(
            "If true, generate target DDL and TRY each CREATE TABLE against the target inside "
            "a transaction, then ROLLBACK — no state changes on target. Status rows use "
            "'would_succeed' / 'would_fail' instead of 'success' / 'failed'. Use this before "
            "the real run to see what would happen. Note: Oracle DDL is auto-committed, so "
            "dry_run is best-effort for Oracle targets."
        ),
    )

    fail_on_any_error: bool = Field(
        default=False,
        description="If true, asset materialization fails when any table fails. Default: report in status DataFrame and succeed.",
    )

    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
    description: Optional[str] = Field(default=None, description="Asset description")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners")
    asset_tags: Optional[Dict[str, str]] = Field(default=None, description="Additional tags")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds (default: ['ddl', source_type, target_type])")
    deps: Optional[List[str]] = Field(default=None, description="Upstream asset keys")

    @classmethod
    def get_description(cls) -> str:
        return (
            "DDL-first table migration: build portable CREATE TABLE statements from source "
            "INFORMATION_SCHEMA (types + PKs + FKs + NOT NULL + defaults) and execute on target. "
            "Emits per-table status DataFrame."
        )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        source_type = self.source_type.lower()
        target_type = self.target_type.lower()
        h.validate_dialect(source_type, role="source")
        h.validate_dialect(target_type, role="target")

        kinds_list = self.kinds or ["ddl", source_type, target_type]
        all_tags = dict(self.asset_tags or {})
        for k in kinds_list:
            all_tags[f"dagster/kind/{k}"] = ""

        source_connection_literal = self.source_connection
        source_connection_env_var = self.source_connection_env_var
        target_connection_literal = self.target_connection
        target_connection_env_var = self.target_connection_env_var
        schemas = list(self.schemas) if self.schemas else None
        include_patterns = list(self.include_patterns) if self.include_patterns else None
        exclude_patterns = list(self.exclude_patterns) if self.exclude_patterns else None
        target_schema = self.target_schema
        table_replacements = dict(self.table_replacements or {})
        drop_if_exists = self.drop_if_exists
        include_foreign_keys = self.include_foreign_keys
        include_primary_keys = self.include_primary_keys
        include_check_constraints = self.include_check_constraints
        include_unique_constraints = self.include_unique_constraints
        function_replacements = dict(self.function_replacements or {})
        table_ddl_overrides = {k.lower(): v for k, v in (self.table_ddl_overrides or {}).items()}
        dry_run = self.dry_run
        fail_on_any_error = self.fail_on_any_error

        @dg.asset(
            name=self.asset_name,
            group_name=self.group_name,
            description=self.description or self.get_description(),
            owners=self.owners or [],
            tags=all_tags,
            kinds=set(kinds_list),
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def _ddl_migration(context: dg.AssetExecutionContext):
            import pandas as pd
            from sqlalchemy import text

            source_url = h.resolve_connection(source_connection_literal, source_connection_env_var, "source_connection")
            target_url = h.resolve_connection(target_connection_literal, target_connection_env_var, "target_connection")
            source_engine = h.engine_for_url(source_url, source_type)
            target_engine = h.engine_for_url(target_url, target_type)

            if target_schema:
                try:
                    with target_engine.begin() as tgt:
                        tgt.execute(text(f"CREATE SCHEMA IF NOT EXISTS {target_schema}"))
                    context.log.info(f"Target schema {target_schema!r} ensured")
                except Exception as e:
                    context.log.warning(
                        f"Could not auto-create target schema {target_schema!r}: {e.__class__.__name__}: {str(e)[:200]}"
                    )

            # Pre-flight: warn about target dialect's constraint enforcement gaps
            for warning in h.constraint_support_warnings(target_type):
                context.log.warning(f"⚠ {warning}")

            with source_engine.connect() as src:
                discovered = h.list_tables(src, source_type, schemas)
                discovered = h.apply_patterns(discovered, include_patterns, exclude_patterns)

                context.log.info(f"Found {len(discovered)} table(s) to migrate DDL for")

                rows = []
                for src_schema, table_name in discovered:
                    src_qual = f"{src_schema}.{table_name}"
                    tgt_qual = (
                        f"{target_schema}.{table_name}" if target_schema
                        else f"{src_schema}.{table_name}"
                    )
                    row = {
                        "schema_name": src_schema,
                        "table_name": table_name,
                        "target_table": tgt_qual,
                        "status": "pending",
                        "error_message": None,
                        "n_columns": 0,
                        "n_foreign_keys": 0,
                        "n_check_constraints": 0,
                        "n_unique_constraints": 0,
                        "has_primary_key": False,
                        "ddl_preview": "",
                    }
                    override_ddl = table_ddl_overrides.get(src_qual.lower())
                    if override_ddl:
                        row["status"] = "pending_override"
                        row["ddl_preview"] = override_ddl[:500]
                        stmts = []
                        if drop_if_exists and not dry_run:
                            stmts.append(h.drop_table_if_exists(target_type, tgt_qual))
                        stmts.append(override_ddl)
                        try:
                            h.exec_with_dry_run(target_engine, stmts, dry_run)
                            row["status"] = "would_override_succeed" if dry_run else "override_success"
                            context.log.info(
                                f"DDL (override{'/dry-run' if dry_run else ''}): {src_qual} → {tgt_qual}"
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
                        cols = h.fetch_columns(src, source_type, src_schema, table_name)
                        if not cols:
                            row["status"] = "failed"
                            row["error_message"] = "no columns found in source table"
                            rows.append(row)
                            continue
                        row["n_columns"] = len(cols)

                        pk = h.fetch_primary_key(src, source_type, src_schema, table_name) if include_primary_keys else None
                        row["has_primary_key"] = pk is not None

                        fks = h.fetch_foreign_keys(src, source_type, src_schema, table_name) if include_foreign_keys else []
                        row["n_foreign_keys"] = len(fks)

                        cks = h.fetch_check_constraints(src, source_type, src_schema, table_name) if include_check_constraints else []
                        row["n_check_constraints"] = len(cks)

                        uqs = h.fetch_unique_constraints(src, source_type, src_schema, table_name) if include_unique_constraints else []
                        row["n_unique_constraints"] = len(uqs)

                        ddl = h.build_create_table(
                            tgt_qual, cols, pk, fks, target_type, table_replacements,
                            check_constraints=cks,
                            unique_constraints=uqs,
                            function_replacements=function_replacements,
                        )
                        row["ddl_preview"] = ddl[:500]

                        stmts = []
                        if drop_if_exists and not dry_run:
                            stmts.append(h.drop_table_if_exists(target_type, tgt_qual))
                        stmts.append(ddl)
                        h.exec_with_dry_run(target_engine, stmts, dry_run)
                        row["status"] = "would_succeed" if dry_run else "success"
                        context.log.info(
                            f"DDL{' (dry-run)' if dry_run else ''}: {src_qual} → {tgt_qual} "
                            f"({len(cols)} cols, {'PK' if pk else 'no PK'}, {len(fks)} FKs, "
                            f"{len(cks)} CHECKs, {len(uqs)} UNIQUEs)"
                        )
                    except Exception as e:
                        row["status"] = "would_fail" if dry_run else "failed"
                        row["error_message"] = f"{e.__class__.__name__}: {str(e)[:300]}"
                        context.log.warning(
                            f"{'Would-fail' if dry_run else 'Failed'}: {src_qual}: {row['error_message']}"
                        )
                    rows.append(row)

            report = pd.DataFrame(rows)
            n_total = len(report)
            success_states = ["success", "override_success", "would_succeed", "would_override_succeed"]
            failure_states = ["failed", "override_failed", "would_fail", "would_override_fail"]
            override_states = ["override_success", "override_failed", "would_override_succeed", "would_override_fail"]
            n_ok = int(report["status"].isin(success_states).sum()) if n_total else 0
            n_fail = int(report["status"].isin(failure_states).sum()) if n_total else 0
            n_overrides = int(report["status"].isin(override_states).sum()) if n_total else 0
            verb = "would succeed" if dry_run else "succeeded"
            verb_fail = "would fail" if dry_run else "failed"

            context.log.info(
                f"DDL migration{' (DRY RUN — nothing committed)' if dry_run else ''}: "
                f"{n_ok}/{n_total} {verb}, {n_fail} {verb_fail}"
                + (f" ({n_overrides} via override)" if n_overrides else "")
            )

            if fail_on_any_error and n_fail > 0:
                raise RuntimeError(f"{n_fail} table(s) failed DDL migration. See asset metadata.")

            md_summary = (
                report[["schema_name", "table_name", "status", "n_columns", "has_primary_key", "n_foreign_keys", "error_message"]]
                .to_markdown(index=False)
                if n_total else "_no tables found_"
            )

            return dg.MaterializeResult(
                value=report,
                metadata={
                    "row_count": dg.MetadataValue.int(n_total),
                    "dagster/row_count": dg.MetadataValue.int(n_total),
                    "tables_succeeded": dg.MetadataValue.int(n_ok),
                    "tables_failed": dg.MetadataValue.int(n_fail),
                    "tables_via_override": dg.MetadataValue.int(n_overrides),
                    "auto_convertible_pct": dg.MetadataValue.float(
                        round(100.0 * n_ok / n_total, 1) if n_total else 0.0
                    ),
                    "dry_run": dg.MetadataValue.bool(dry_run),
                    "report": dg.MetadataValue.md(str(md_summary)),
                },
            )

        return dg.Definitions(assets=[_ddl_migration])
