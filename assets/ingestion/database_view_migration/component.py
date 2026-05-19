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
from typing import Dict, List, Optional

import dagster as dg
from pydantic import Field

from . import _view_helpers as h


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
        h.validate_dialect(source_type, role="source")
        h.validate_dialect(target_type, role="target")

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
            name=self.asset_name,
            group_name=self.group_name,
            description=self.description or self.get_description(),
            owners=self.owners or [],
            tags=all_tags,
            kinds=set(kinds_list),
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def _view_asset(context: dg.AssetExecutionContext):
            source_url = h.resolve_connection(source_connection_literal, source_connection_env_var, "source_connection")
            target_url = h.resolve_connection(target_connection_literal, target_connection_env_var, "target_connection")
            source_engine = h.engine_for_url(source_url, source_type)
            target_engine = h.engine_for_url(target_url, target_type)

            schema, name = h.split_qualified(source_view)
            with source_engine.connect() as src:
                view_sql = h.fetch_view_definition(src, source_type, schema, name)
            if view_sql is None:
                raise ValueError(
                    f"View {source_view!r} not found in source ({source_type})"
                )

            rewritten = h.apply_substitutions(view_sql, table_replacements, function_replacements)
            context.log.info(
                f"View {source_view} → {target_view}: "
                f"{len(table_replacements)} table refs + {len(function_replacements)} function names substituted"
            )

            with target_engine.begin() as tgt:
                h.replace_view(tgt, target_type, target_view, rewritten)

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
