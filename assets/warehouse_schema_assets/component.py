"""Warehouse Schema Assets Component.

Connects to a database at prepare time, introspects the schema via
information_schema, and creates one Dagster external AssetSpec per table.

Uses StateBackedComponent so the schema is only queried once — code-server
reloads are instant even for warehouses with thousands of tables. The cached
schema also includes column names and types so the Dagster UI shows the full
column schema for each table.

Pairs naturally with other components via deps:
    sql_to_database_asset can declare deps: ['raw/orders'] to reference
    tables discovered by this component.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import dagster as dg

try:
    from dagster.components.component.state_backed_component import StateBackedComponent
    from dagster.components.utils.defs_state import (
        DefsStateConfig,
        DefsStateConfigArgs,
        ResolvedDefsStateConfig,
    )
    _HAS_STATE_BACKED = True
except ImportError:
    StateBackedComponent = None
    _HAS_STATE_BACKED = False


def _introspect_schemas(
    database_url: str,
    schema_names: list[str],
    exclude_tables: Optional[list[str]],
    table_pattern: Optional[str],
    include_views: bool,
    include_columns: bool,
) -> list[dict]:
    """Query information_schema and return table metadata list."""
    import fnmatch
    from sqlalchemy import create_engine, inspect

    engine = create_engine(database_url)
    inspector = inspect(engine)
    results = []

    for schema in schema_names:
        table_names = inspector.get_table_names(schema=schema)
        if include_views:
            table_names += inspector.get_view_names(schema=schema)

        for table_name in table_names:
            if exclude_tables and table_name in exclude_tables:
                continue
            if table_pattern and not fnmatch.fnmatch(table_name, table_pattern):
                continue

            entry: dict = {"schema": schema, "table": table_name, "columns": []}

            if include_columns:
                try:
                    cols = inspector.get_columns(table_name, schema=schema)
                    entry["columns"] = [
                        {"name": c["name"], "type": str(c["type"]), "nullable": c.get("nullable", True)}
                        for c in cols
                    ]
                except Exception:
                    pass  # Some views/tables may not support column introspection

            results.append(entry)

    engine.dispose()
    return results


def _build_schema_defs(
    tables: list[dict],
    key_prefix: Optional[str],
    group_by_schema: bool,
    group_name: Optional[str],
    tag_with_schema: bool,
) -> dg.Definitions:
    specs = []

    for t in tables:
        schema = t["schema"]
        table = t["table"]

        # Asset key: [prefix, schema, table] or [schema, table] or [table]
        key_parts = []
        if key_prefix:
            key_parts.append(key_prefix)
        key_parts.extend([schema, table])
        asset_key = dg.AssetKey(key_parts)

        # Column schema metadata
        metadata: dict = {}
        cols = t.get("columns", [])
        if cols:
            column_schema = dg.TableSchema(columns=[
                dg.TableColumn(
                    name=c["name"],
                    type=c["type"],
                    description="" ,
                )
                for c in cols
            ])
            metadata.update(dg.TableMetadataSet(column_schema=column_schema))

        metadata["warehouse/schema"] = dg.MetadataValue.text(schema)
        metadata["warehouse/table"] = dg.MetadataValue.text(table)
        metadata["warehouse/column_count"] = dg.MetadataValue.int(len(cols))

        # Tags
        tags: dict[str, str] = {}
        if tag_with_schema:
            tags[schema] = ""

        spec = dg.AssetSpec(
            key=asset_key,
            description=f"{schema}.{table}" + (f" ({len(cols)} columns)" if cols else ""),
            group_name=group_name or (schema if group_by_schema else "warehouse"),
            metadata=metadata,
            tags=tags,
            kinds={"sql"},
        )
        specs.append(spec)

    return dg.Definitions(assets=specs)


if _HAS_STATE_BACKED:
    @dataclass
    class WarehouseSchemaAssetsComponent(StateBackedComponent, dg.Resolvable):
        """Create one external Dagster asset per table/view in a database schema.

        Introspects information_schema once at prepare time (write_state_to_path),
        caches the result to disk, then builds AssetSpecs on every reload with zero
        database connections (build_defs_from_state).

        Ideal for making warehouse tables visible as lineage targets in the Dagster
        asset graph without manually declaring each one.

        Example:
            ```yaml
            type: dagster_component_templates.WarehouseSchemaAssetsComponent
            attributes:
              database_url_env_var: WAREHOUSE_URL
              schema_names:
                - raw
                - staging
                - marts
              group_by_schema: true
            ```
        """

        database_url_env_var: str
        schema_names: list = field(default_factory=lambda: ["public"])
        exclude_tables: Optional[list] = None
        table_pattern: Optional[str] = None
        include_views: bool = False
        include_columns: bool = True
        key_prefix: Optional[str] = None
        group_by_schema: bool = True
        group_name: Optional[str] = None
        tag_with_schema: bool = False
        defs_state: ResolvedDefsStateConfig = field(
            default_factory=DefsStateConfigArgs.local_filesystem
        )

        @property
        def defs_state_config(self) -> DefsStateConfig:
            schemas_key = "_".join(sorted(self.schema_names))
            return DefsStateConfig.from_args(
                self.defs_state,
                default_key=f"WarehouseSchemaAssetsComponent[{schemas_key}]",
            )

        def write_state_to_path(self, state_path: Path) -> None:
            """Introspect database schema and cache table list to disk."""
            import os
            db_url = os.environ[self.database_url_env_var]
            tables = _introspect_schemas(
                database_url=db_url,
                schema_names=self.schema_names,
                exclude_tables=self.exclude_tables,
                table_pattern=self.table_pattern,
                include_views=self.include_views,
                include_columns=self.include_columns,
            )
            state_path.write_text(json.dumps(tables))

        def build_defs_from_state(
            self, context: dg.ComponentLoadContext, state_path: Optional[Path]
        ) -> dg.Definitions:
            """Build external AssetSpecs from cached schema — no DB connection."""
            if state_path is None or not state_path.exists():
                return dg.Definitions()

            tables = json.loads(state_path.read_text())
            return _build_schema_defs(
                tables=tables,
                key_prefix=self.key_prefix,
                group_by_schema=self.group_by_schema,
                group_name=self.group_name,
                tag_with_schema=self.tag_with_schema,
            )

else:
    class WarehouseSchemaAssetsComponent(dg.Component, dg.Model, dg.Resolvable):  # type: ignore[no-redef]
        """Fallback: StateBackedComponent not available. Queries DB on every reload."""
        database_url_env_var: str = dg.Field(description="Env var with SQLAlchemy database URL")
        schema_names: list = dg.Field(default=["public"])
        exclude_tables: Optional[list] = dg.Field(default=None)
        table_pattern: Optional[str] = dg.Field(default=None)
        include_views: bool = dg.Field(default=False)
        include_columns: bool = dg.Field(default=True)
        key_prefix: Optional[str] = dg.Field(default=None)
        group_by_schema: bool = dg.Field(default=True)
        group_name: Optional[str] = dg.Field(default=None)
        tag_with_schema: bool = dg.Field(default=False)

        def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
            import os
            db_url = os.environ[self.database_url_env_var]
            tables = _introspect_schemas(
                database_url=db_url,
                schema_names=self.schema_names,
                exclude_tables=self.exclude_tables,
                table_pattern=self.table_pattern,
                include_views=self.include_views,
                include_columns=self.include_columns,
            )
            return _build_schema_defs(
                tables=tables,
                key_prefix=self.key_prefix,
                group_by_schema=self.group_by_schema,
                group_name=self.group_name,
                tag_with_schema=self.tag_with_schema,
            )
