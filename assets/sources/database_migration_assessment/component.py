"""Database Migration Assessment.

Pre-flight dry-run: read source DDL for every table + view, attempt the
target migration in a transaction, ROLLBACK every change. Returns a single
consolidated DataFrame with per-object status + complexity heuristic +
specific dialect-quirk markers + proposed target DDL.

The "what would happen if I ran this tomorrow?" view, BEFORE you commit.
Inspired by AWS SCT's Assessment Report and Microsoft SSMA's test mode.
"""
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field

from . import _ddl_helpers as ddl_h
from . import _view_helpers as view_h


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
        ddl_h.validate_dialect(source_type, role="source")
        ddl_h.validate_dialect(target_type, role="target")

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
            name=self.asset_name,
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

            source_engine = ddl_h.engine_for(source_connection_env_var, source_type)
            target_engine = ddl_h.engine_for(target_connection_env_var, target_type)

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
            for warning in ddl_h.constraint_support_warnings(target_type, workflow="any"):
                context.log.warning(f"⚠ {warning}")

            # Build the per-target-type "informational" set so we can flag medium-complexity rows
            target_informational = {
                ctype for ctype in
                ("primary_key", "foreign_key", "not_null", "default", "check", "unique")
                if ddl_h.constraint_enforcement(target_type, ctype) == "informational"
            }

            rows: List[Dict[str, Any]] = []

            # ──────────────────────────────────────────────────────────────
            # Tables
            # ──────────────────────────────────────────────────────────────
            if assess_tables:
                with source_engine.connect() as src:
                    discovered_tables = ddl_h.list_tables(src, source_type, schemas)
                    discovered_tables = ddl_h.apply_patterns(
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
                            cols = ddl_h.fetch_columns(src, source_type, src_schema, table_name)
                            pk = ddl_h.fetch_primary_key(src, source_type, src_schema, table_name)
                            fks = ddl_h.fetch_foreign_keys(src, source_type, src_schema, table_name)
                            cks = ddl_h.fetch_check_constraints(src, source_type, src_schema, table_name)
                            uqs = ddl_h.fetch_unique_constraints(src, source_type, src_schema, table_name)
                            proposed_ddl = ddl_h.build_create_table(
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
                            for m in ddl_h.detect_dialect_quirks(
                                ck.get("expression", ""), source_type
                            ):
                                markers_set.add(m)
                        for c in cols:
                            if c.get("default"):
                                for m in ddl_h.detect_dialect_quirks(
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
                            ddl_h.exec_with_dry_run(target_engine, [proposed_ddl], dry_run=True)
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
                    discovered_views = view_h.list_views(src, source_type, schemas)
                    discovered_views = view_h.apply_patterns(
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
                            view_ddl = view_h.fetch_view_definition(
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
                            rewritten = view_h.apply_substitutions(
                                view_ddl, table_replacements, function_replacements
                            )
                            stmts = view_h.replace_view_statements(
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

                        markers = view_h.detect_dialect_quirks(view_ddl, source_type)
                        has_substitutions = bool(table_replacements) or bool(function_replacements)

                        try:
                            view_h.exec_with_dry_run(target_engine, stmts, dry_run=True)
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
