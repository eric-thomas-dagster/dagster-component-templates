"""Database Constraints Migration.

Apply primary keys, foreign keys, NOT NULL, and DEFAULTs to target tables
that already exist (typically created by Sling via `database_replication`
with `mode: full_refresh`). Reads constraint metadata from source's
INFORMATION_SCHEMA, generates `ALTER TABLE ... ADD CONSTRAINT` /
`ALTER COLUMN SET NOT NULL` statements, executes on target.

Companion to `database_tables_migration`: that one creates tables with DDL
upfront (DDL-first workflow); this one applies constraints to already-loaded
tables (data-first workflow).

Emits a per-constraint status DataFrame for the migration completion report.
"""
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field

from . import _ddl_helpers as h


_DEFAULT_TYPES = ["primary_key", "foreign_key", "not_null", "default", "check", "unique"]


class DatabaseConstraintsMigrationComponent(dg.Component, dg.Model, dg.Resolvable):
    """Apply constraints (PKs, FKs, NOT NULL, DEFAULTs) to target tables that
    already exist. Use this *after* `database_replication` if you went the
    data-first route — Sling creates tables from type inference and loses
    constraints.

    For the DDL-first workflow (no separate constraints step needed), use
    `database_tables_migration` to create target tables with full DDL upfront.
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
        description="Source schemas to scan for constraints",
    )
    include_patterns: Optional[List[str]] = Field(
        default=None,
        description=(
            "fnmatch-style glob patterns (case-insensitive) against qualified table "
            "names. Examples: ['HR.*'], ['HR.EMPLOYEES', 'FINANCE.STG_*']. "
            "Default: include all discovered tables."
        ),
    )
    exclude_patterns: Optional[List[str]] = Field(
        default=None,
        description=(
            "fnmatch-style glob patterns to exclude. Applied AFTER include_patterns."
        ),
    )

    target_schema: Optional[str] = Field(
        default=None,
        description="Override target schema (defaults to source schema name)",
    )
    table_replacements: Optional[Dict[str, str]] = Field(
        default=None,
        description="FK reference rewrites (old qualified name → new)",
    )

    constraint_types: Optional[List[str]] = Field(
        default=None,
        description="Subset of {primary_key, foreign_key, not_null, default, check, unique}. Default: all six.",
    )
    function_replacements: Optional[Dict[str, str]] = Field(
        default=None,
        description="Case-insensitive whole-word substitutions applied to CHECK expressions (e.g. {'NVL': 'COALESCE', 'REGEXP_LIKE': 'REGEXP_MATCHES'}).",
    )

    dry_run: bool = Field(
        default=False,
        description=(
            "If true, attempt each ALTER TABLE ADD CONSTRAINT in a transaction and ROLLBACK — "
            "no state changes on target. Status rows use 'would_succeed' / 'would_fail'. "
            "Best-effort on Oracle (DDL is auto-commit)."
        ),
    )

    fail_on_any_error: bool = Field(
        default=False,
        description="If true, asset fails on any constraint failure. Default: report and succeed.",
    )

    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
    description: Optional[str] = Field(default=None, description="Asset description")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners")
    asset_tags: Optional[Dict[str, str]] = Field(default=None, description="Additional tags")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds (default: ['constraints', source_type, target_type])")
    deps: Optional[List[str]] = Field(default=None, description="Upstream asset keys (typically the data replications)")

    @classmethod
    def get_description(cls) -> str:
        return (
            "Apply primary keys, foreign keys, NOT NULL, and DEFAULTs to target tables "
            "(typically post-data-load). Reads constraints from source INFORMATION_SCHEMA, "
            "executes ALTER TABLEs on target. Emits per-constraint status DataFrame."
        )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        source_type = self.source_type.lower()
        target_type = self.target_type.lower()
        h.validate_dialect(source_type, role="source")
        h.validate_dialect(target_type, role="target")

        ctypes = [t.lower() for t in (self.constraint_types or _DEFAULT_TYPES)]
        invalid = set(ctypes) - set(_DEFAULT_TYPES)
        if invalid:
            raise ValueError(
                f"constraint_types {sorted(invalid)} unknown; valid: {_DEFAULT_TYPES}"
            )

        kinds_list = self.kinds or ["constraints", source_type, target_type]
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
        function_replacements = dict(self.function_replacements or {})
        dry_run = self.dry_run
        fail_on_any_error = self.fail_on_any_error
        ctypes_set = set(ctypes)

        @dg.asset(
            key=dg.AssetKey.from_user_string(self.asset_name),
            group_name=self.group_name,
            description=self.description or self.get_description(),
            owners=self.owners or [],
            tags=all_tags,
            kinds=set(kinds_list),
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def _constraints_migration(context: dg.AssetExecutionContext):
            import pandas as pd
            from sqlalchemy import text

            source_url = h.resolve_connection(source_connection_literal, source_connection_env_var, "source_connection")
            target_url = h.resolve_connection(target_connection_literal, target_connection_env_var, "target_connection")
            source_engine = h.engine_for_url(source_url, source_type)
            target_engine = h.engine_for_url(target_url, target_type)

            # Pre-flight: warn about target dialect's constraint enforcement gaps
            # (data_first context — inline_only constraints will be auto-skipped below)
            for warning in h.constraint_support_warnings(target_type, workflow="data_first"):
                context.log.warning(f"⚠ {warning}")

            # Determine which constraint types we'll auto-skip on this target.
            # 'inline_only' (DuckDB CHECK/UNIQUE) and 'unsupported' (BigQuery CHECK) get skipped
            # cleanly with a clear status row, rather than failing noisily.
            auto_skip = set()
            skip_reason: Dict[str, str] = {}
            for ctype in ("primary_key", "foreign_key", "not_null", "default", "check", "unique"):
                state = h.constraint_enforcement(target_type, ctype)
                if state == "inline_only":
                    auto_skip.add(ctype)
                    skip_reason[ctype] = (
                        f"{target_type} requires {ctype} inline at CREATE TABLE — "
                        f"use database_tables_migration (DDL-first) instead"
                    )
                elif state == "unsupported":
                    auto_skip.add(ctype)
                    skip_reason[ctype] = f"{target_type} does not support {ctype} constraints"

            with source_engine.connect() as src:
                discovered = h.list_tables(src, source_type, schemas)
                discovered = h.apply_patterns(discovered, include_patterns, exclude_patterns)

                context.log.info(
                    f"Applying constraints across {len(discovered)} table(s); "
                    f"types: {sorted(ctypes_set)}"
                )

                rows = []

                def _skip_row(ctype: str, name: str, columns: str = "") -> Dict[str, Any]:
                    return {
                        "schema_name": src_schema, "table_name": table_name,
                        "target_table": tgt_qual,
                        "constraint_type": ctype,
                        "constraint_name": name,
                        "columns": columns,
                        "status": "skipped",
                        "error_message": skip_reason.get(ctype, "skipped"),
                        "ddl": "",
                    }

                for src_schema, table_name in discovered:
                    tgt_qual = (
                        f"{target_schema}.{table_name}" if target_schema
                        else f"{src_schema}.{table_name}"
                    )

                    # Emit skip rows for any constraint types the user asked for but
                    # the target rejects via ALTER (e.g. DuckDB CHECK/UNIQUE).
                    for ctype in ctypes_set:
                        if ctype in auto_skip:
                            rows.append(_skip_row(ctype, name=f"<all {ctype} for {table_name}>"))

                    # Primary key
                    if "primary_key" in ctypes_set and "primary_key" not in auto_skip:
                        pk = h.fetch_primary_key(src, source_type, src_schema, table_name)
                        if pk:
                            row = {
                                "schema_name": src_schema, "table_name": table_name,
                                "target_table": tgt_qual,
                                "constraint_type": "primary_key",
                                "constraint_name": pk["name"],
                                "columns": ",".join(pk["columns"]),
                                "status": "pending", "error_message": None,
                                "ddl": h.alter_add_primary_key(tgt_qual, pk),
                            }
                            try:
                                h.exec_with_dry_run(target_engine, [row["ddl"]], dry_run)
                                row["status"] = "would_succeed" if dry_run else "success"
                                context.log.info(
                                    f"Applied: {row['constraint_type']} {row['constraint_name']} on {tgt_qual}"
                                )
                            except Exception as e:
                                row["status"] = "would_fail" if dry_run else "failed"
                                row["error_message"] = f"{e.__class__.__name__}: {str(e)[:300]}"
                                context.log.warning(
                                    f"Failed: {row['constraint_type']} {row['constraint_name']} "
                                    f"on {tgt_qual}: {row['error_message']}"
                                )
                            rows.append(row)

                    # Foreign keys
                    if "foreign_key" in ctypes_set and "foreign_key" not in auto_skip:
                        for fk in h.fetch_foreign_keys(src, source_type, src_schema, table_name):
                            row = {
                                "schema_name": src_schema, "table_name": table_name,
                                "target_table": tgt_qual,
                                "constraint_type": "foreign_key",
                                "constraint_name": fk["name"],
                                "columns": ",".join(fk["columns"]),
                                "status": "pending", "error_message": None,
                                "ddl": h.alter_add_foreign_key(tgt_qual, fk, table_replacements),
                            }
                            try:
                                h.exec_with_dry_run(target_engine, [row["ddl"]], dry_run)
                                row["status"] = "would_succeed" if dry_run else "success"
                                context.log.info(
                                    f"Applied: {row['constraint_type']} {row['constraint_name']} on {tgt_qual}"
                                )
                            except Exception as e:
                                row["status"] = "would_fail" if dry_run else "failed"
                                row["error_message"] = f"{e.__class__.__name__}: {str(e)[:300]}"
                                context.log.warning(
                                    f"Failed: {row['constraint_type']} {row['constraint_name']} "
                                    f"on {tgt_qual}: {row['error_message']}"
                                )
                            rows.append(row)

                    # NOT NULL + DEFAULTs (per column)
                    if ("not_null" in ctypes_set and "not_null" not in auto_skip) or \
                       ("default" in ctypes_set and "default" not in auto_skip):
                        cols = h.fetch_columns(src, source_type, src_schema, table_name)
                        for col in cols:
                            if "not_null" in ctypes_set and "not_null" not in auto_skip and not col["nullable"]:
                                row = {
                                    "schema_name": src_schema, "table_name": table_name,
                                    "target_table": tgt_qual,
                                    "constraint_type": "not_null",
                                    "constraint_name": col["name"],
                                    "columns": col["name"],
                                    "status": "pending", "error_message": None,
                                    "ddl": h.alter_set_not_null(tgt_qual, col["name"], target_type),
                                }
                                try:
                                    h.exec_with_dry_run(target_engine, [row["ddl"]], dry_run)
                                    row["status"] = "would_succeed" if dry_run else "success"
                                    context.log.info(
                                        f"Applied: not_null on {tgt_qual}.{col['name']}"
                                    )
                                except Exception as e:
                                    row["status"] = "would_fail" if dry_run else "failed"
                                    row["error_message"] = f"{e.__class__.__name__}: {str(e)[:300]}"
                                    context.log.warning(
                                        f"Failed: not_null on {tgt_qual}.{col['name']}: "
                                        f"{row['error_message']}"
                                    )
                                rows.append(row)

                            if "default" in ctypes_set and "default" not in auto_skip:
                                d = h.quote_default(col["default"], target_type)
                                if d is None:
                                    continue
                                row = {
                                    "schema_name": src_schema, "table_name": table_name,
                                    "target_table": tgt_qual,
                                    "constraint_type": "default",
                                    "constraint_name": col["name"],
                                    "columns": col["name"],
                                    "status": "pending", "error_message": None,
                                    "ddl": h.alter_set_default(tgt_qual, col["name"], d, target_type),
                                }
                                try:
                                    h.exec_with_dry_run(target_engine, [row["ddl"]], dry_run)
                                    row["status"] = "would_succeed" if dry_run else "success"
                                    context.log.info(
                                        f"Applied: default {d!r} on {tgt_qual}.{col['name']}"
                                    )
                                except Exception as e:
                                    row["status"] = "would_fail" if dry_run else "failed"
                                    row["error_message"] = f"{e.__class__.__name__}: {str(e)[:300]}"
                                    context.log.warning(
                                        f"Failed: default on {tgt_qual}.{col['name']}: "
                                        f"{row['error_message']}"
                                    )
                                rows.append(row)

                    # UNIQUE constraints (non-PK)
                    if "unique" in ctypes_set and "unique" not in auto_skip:
                        for uq in h.fetch_unique_constraints(src, source_type, src_schema, table_name):
                            row = {
                                "schema_name": src_schema, "table_name": table_name,
                                "target_table": tgt_qual,
                                "constraint_type": "unique",
                                "constraint_name": uq["name"],
                                "columns": ",".join(uq["columns"]),
                                "status": "pending", "error_message": None,
                                "ddl": h.alter_add_unique(tgt_qual, uq),
                            }
                            try:
                                h.exec_with_dry_run(target_engine, [row["ddl"]], dry_run)
                                row["status"] = "would_succeed" if dry_run else "success"
                                context.log.info(
                                    f"Applied: {row['constraint_type']} {row['constraint_name']} on {tgt_qual}"
                                )
                            except Exception as e:
                                row["status"] = "would_fail" if dry_run else "failed"
                                row["error_message"] = f"{e.__class__.__name__}: {str(e)[:300]}"
                                context.log.warning(
                                    f"Failed: {row['constraint_type']} {row['constraint_name']} "
                                    f"on {tgt_qual}: {row['error_message']}"
                                )
                            rows.append(row)

                    # CHECK constraints (with optional function-name substitutions)
                    if "check" in ctypes_set and "check" not in auto_skip:
                        for ck in h.fetch_check_constraints(src, source_type, src_schema, table_name):
                            row = {
                                "schema_name": src_schema, "table_name": table_name,
                                "target_table": tgt_qual,
                                "constraint_type": "check",
                                "constraint_name": ck["name"],
                                "columns": "",
                                "status": "pending", "error_message": None,
                                "ddl": h.alter_add_check(tgt_qual, ck, function_replacements),
                            }
                            try:
                                h.exec_with_dry_run(target_engine, [row["ddl"]], dry_run)
                                row["status"] = "would_succeed" if dry_run else "success"
                                context.log.info(
                                    f"Applied: {row['constraint_type']} {row['constraint_name']} on {tgt_qual}"
                                )
                            except Exception as e:
                                row["status"] = "would_fail" if dry_run else "failed"
                                row["error_message"] = f"{e.__class__.__name__}: {str(e)[:300]}"
                                context.log.warning(
                                    f"Failed: {row['constraint_type']} {row['constraint_name']} "
                                    f"on {tgt_qual}: {row['error_message']}"
                                )
                            rows.append(row)

            report = pd.DataFrame(rows)
            n_total = len(report)
            success_states = ["success", "would_succeed"]
            failure_states = ["failed", "would_fail"]
            n_ok = int(report["status"].isin(success_states).sum()) if n_total else 0
            n_fail = int(report["status"].isin(failure_states).sum()) if n_total else 0
            n_skipped = int((report["status"] == "skipped").sum()) if n_total else 0
            verb = "would succeed" if dry_run else "succeeded"
            verb_fail = "would fail" if dry_run else "failed"

            context.log.info(
                f"Constraints migration{' (DRY RUN — nothing committed)' if dry_run else ''}: "
                f"{n_ok}/{n_total} {verb}, {n_fail} {verb_fail}"
                + (f", {n_skipped} skipped (target limitations)" if n_skipped else "")
            )

            if fail_on_any_error and n_fail > 0:
                raise RuntimeError(
                    f"{n_fail} constraint(s) failed. See asset metadata."
                )

            md_summary = (
                report[["table_name", "constraint_type", "constraint_name", "columns", "status", "error_message"]]
                .to_markdown(index=False)
                if n_total else "_no constraints found_"
            )

            return dg.MaterializeResult(
                value=report,
                metadata={
                    "row_count": dg.MetadataValue.int(n_total),
                    "dagster/row_count": dg.MetadataValue.int(n_total),
                    "constraints_succeeded": dg.MetadataValue.int(n_ok),
                    "constraints_failed": dg.MetadataValue.int(n_fail),
                    "constraints_skipped": dg.MetadataValue.int(n_skipped),
                    "auto_convertible_pct": dg.MetadataValue.float(
                        round(100.0 * n_ok / n_total, 1) if n_total else 0.0
                    ),
                    "dry_run": dg.MetadataValue.bool(dry_run),
                    "report": dg.MetadataValue.md(str(md_summary)),
                },
            )

        return dg.Definitions(assets=[_constraints_migration])
