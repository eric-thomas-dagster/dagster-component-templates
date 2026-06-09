"""Database Views Bulk Migration.

Migrate every view in a source schema to a target in one asset. Emits a
status DataFrame ({schema_name, view_name, status, error_message}) so the
team has a concrete pass/fail report of the migration.

For one-off / composable single-view migration, use `database_view_migration`.
"""
from typing import Dict, List, Optional

import dagster as dg
from pydantic import Field

from . import _view_helpers as h


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
        h.validate_dialect(source_type, role="source")
        h.validate_dialect(target_type, role="target")

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

            source_url = h.resolve_connection(source_connection_literal, source_connection_env_var, "source_connection")
            target_url = h.resolve_connection(target_connection_literal, target_connection_env_var, "target_connection")
            source_engine = h.engine_for_url(source_url, source_type)
            target_engine = h.engine_for_url(target_url, target_type)

            with source_engine.connect() as src:
                discovered = h.list_views(src, source_type, schemas)
                discovered = h.apply_patterns(discovered, include_patterns, exclude_patterns)

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
                        stmts = h.replace_view_statements(target_type, tgt_qual, override_body)
                        try:
                            h.exec_with_dry_run(target_engine, stmts, dry_run)
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
                        ddl = h.fetch_view_definition(src, source_type, src_schema, view_name)
                        if ddl is None:
                            row["status"] = "failed"
                            row["error_message"] = "view definition not found"
                            rows.append(row)
                            continue
                        rewritten = h.apply_substitutions(
                            ddl, table_replacements, function_replacements
                        )
                        row["ddl_chars"] = len(rewritten)
                        stmts = h.replace_view_statements(target_type, tgt_qual, rewritten)
                        h.exec_with_dry_run(target_engine, stmts, dry_run)
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
