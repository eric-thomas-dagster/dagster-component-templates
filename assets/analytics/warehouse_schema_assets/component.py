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


def _table_asset_key(t: dict, key_prefix: Optional[str]) -> dg.AssetKey:
    key_parts = []
    if key_prefix:
        key_parts.append(key_prefix)
    key_parts.extend([t["schema"], t["table"]])
    return dg.AssetKey(key_parts)


def _build_schema_defs(
    tables: list[dict],
    key_prefix: Optional[str],
    group_by_schema: bool,
    group_name: Optional[str],
    tag_with_schema: bool,
    kinds: Optional[list[str]] = None,
) -> dg.Definitions:
    # TableMetadataSet isn't exported from the top-level `dagster` package
    # (confirmed: `hasattr(dagster, 'TableMetadataSet')` is False on current
    # releases) -- only from its real, private location. `dg.TableMetadataSet`
    # raised AttributeError for every table with introspected columns.
    from dagster._core.definitions.metadata.metadata_set import TableMetadataSet

    specs = []
    spec_kinds = set(kinds) if kinds else {"sql"}

    for t in tables:
        schema = t["schema"]
        table = t["table"]
        asset_key = _table_asset_key(t, key_prefix)

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
            metadata.update(TableMetadataSet(column_schema=column_schema))

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
            kinds=spec_kinds,
        )
        specs.append(spec)

    return dg.Definitions(assets=specs)


def _build_row_count_sensor(
    tables: list[dict],
    database_url_env_var: str,
    key_prefix: Optional[str],
    observation_interval_seconds: int,
):
    """One shared sensor polling each table's row count, emitting
    AssetObservation only for tables whose count actually changed since the
    last tick -- same change-only-on-delta discipline as this registry's
    other polling sensors (external_sftp_path, mirth_connect_workspace,
    qlik_cloud_workspace). Opt-in: row counts are a real query per table per
    tick, not free for warehouses with many/large tables."""
    asset_keys = [_table_asset_key(t, key_prefix) for t in tables]
    tables_by_key_str = {k.to_user_string(): t for k, t in zip(asset_keys, tables)}

    @dg.sensor(
        name="warehouse_schema_assets_row_count_observer",
        minimum_interval_seconds=observation_interval_seconds,
        asset_selection=dg.AssetSelection.assets(*asset_keys),
    )
    def _row_count_sensor(context: dg.SensorEvaluationContext):
        import json as _json
        from sqlalchemy import create_engine, text

        last_counts: dict = _json.loads(context.cursor) if context.cursor else {}
        new_counts: dict = dict(last_counts)
        events = []

        db_url = dg.EnvVar(database_url_env_var).get_value()
        engine = create_engine(db_url)
        try:
            with engine.connect() as conn:
                for key_str, t in tables_by_key_str.items():
                    try:
                        row = conn.execute(
                            text(f'SELECT COUNT(*) FROM "{t["schema"]}"."{t["table"]}"')
                        ).fetchone()
                        count = int(row[0]) if row else 0
                    except Exception as e:
                        context.log.error(f"row-count query failed for {t['schema']}.{t['table']}: {e}")
                        continue
                    previous = last_counts.get(key_str)
                    new_counts[key_str] = count
                    if previous is None:
                        continue  # first sighting -- record baseline, nothing to diff against yet
                    if count == previous:
                        continue
                    events.append(dg.AssetObservation(
                        asset_key=dg.AssetKey.from_user_string(key_str),
                        metadata={"warehouse/row_count": count},
                    ))
        finally:
            engine.dispose()

        new_cursor = _json.dumps(new_counts)
        if events:
            return dg.SensorResult(asset_events=events, cursor=new_cursor)
        return dg.SensorResult(cursor=new_cursor, skip_reason="No table row-count changes since last tick")

    return _row_count_sensor


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
            type: dagster_community_components.WarehouseSchemaAssetsComponent
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
        kinds: Optional[list] = None
        polling_sensor: bool = False
        observation_interval_seconds: int = 300
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

        async def refresh_state(self, project_root: Path) -> str:
            """Refresh LOCAL_FILESYSTEM state atomically.

            The stock ``StateBackedComponent.refresh_state`` does ``shutil.rmtree(state_path)`` and only then
            runs ``write_state_to_path`` (network calls, seconds long), so for the whole fetch -- and again
            while the file is half-written -- a concurrent reader (run worker, step subprocess, ``dg list defs``;
            every ``dg`` command refreshes in dev mode) sees missing/partial state: empty Definitions or a
            JSONDecodeError. Here the new state is built beside the old and swapped in with a rename, so the
            previous state stays readable until the new one is complete (and survives a failed refresh)."""
            import os
            import shutil
            import uuid

            from dagster._core.storage.defs_state.base import DefsStateStorage
            from dagster.components.utils.project_paths import get_local_state_path
            from dagster_shared.serdes.objects.models.defs_state_info import (
                LOCAL_STATE_VERSION,
                DefsStateManagementType,
            )

            config = self.defs_state_config
            if config.management_type != DefsStateManagementType.LOCAL_FILESYSTEM:
                return await super().refresh_state(project_root)
            state_path = get_local_state_path(config.key, project_root)
            state_path.parent.mkdir(parents=True, exist_ok=True)
            token = f"{os.getpid()}-{uuid.uuid4().hex[:8]}"
            work = state_path.parent / f".{state_path.name}.refresh-{token}"
            old = state_path.parent / f".{state_path.name}.old-{token}"
            try:
                await self._write_state_to_path_async(work)
                if work.exists():
                    if work.is_dir() and state_path.is_dir():
                        try:
                            os.rename(state_path, old)
                            os.rename(work, state_path)
                        except OSError:  # e.g. Windows: directory held open by another process
                            shutil.rmtree(state_path, ignore_errors=True)
                            os.rename(work, state_path)
                    else:
                        if state_path.is_dir():
                            shutil.rmtree(state_path, ignore_errors=True)
                        elif work.is_dir() and state_path.exists():
                            state_path.unlink()
                        os.replace(work, state_path)
            finally:
                for _p in (work, old):
                    if _p.is_dir():
                        shutil.rmtree(_p, ignore_errors=True)
                    else:
                        _p.unlink(missing_ok=True)
            state_storage = DefsStateStorage.get()
            if state_storage:
                state_storage.set_latest_version(config.key, LOCAL_STATE_VERSION)
            return LOCAL_STATE_VERSION

        def write_state_to_path(self, state_path: Path) -> None:
            """Introspect database schema and cache table list to disk."""
            db_url = dg.EnvVar(self.database_url_env_var).get_value()
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
            defs = _build_schema_defs(
                tables=tables,
                key_prefix=self.key_prefix,
                group_by_schema=self.group_by_schema,
                group_name=self.group_name,
                tag_with_schema=self.tag_with_schema,
                kinds=self.kinds,
            )
            if self.polling_sensor and tables:
                sensor = _build_row_count_sensor(
                    tables, self.database_url_env_var, self.key_prefix, self.observation_interval_seconds,
                )
                defs = dg.Definitions.merge(defs, dg.Definitions(sensors=[sensor]))
            return defs

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
        kinds: Optional[list] = dg.Field(default=None, description="Asset kinds for the Dagster catalog. Default: ['sql'].")
        polling_sensor: bool = dg.Field(
            default=False,
            description="If true, adds a sensor polling each table's row count and emitting AssetObservation only for tables whose count changed since the last tick.",
        )
        observation_interval_seconds: int = dg.Field(default=300, description="Seconds between sensor polls. Only used when polling_sensor=True.")

        def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
            db_url = dg.EnvVar(self.database_url_env_var).get_value()
            tables = _introspect_schemas(
                database_url=db_url,
                schema_names=self.schema_names,
                exclude_tables=self.exclude_tables,
                table_pattern=self.table_pattern,
                include_views=self.include_views,
                include_columns=self.include_columns,
            )
            defs = _build_schema_defs(
                tables=tables,
                key_prefix=self.key_prefix,
                group_by_schema=self.group_by_schema,
                group_name=self.group_name,
                tag_with_schema=self.tag_with_schema,
                kinds=self.kinds,
            )
            if self.polling_sensor and tables:
                sensor = _build_row_count_sensor(
                    tables, self.database_url_env_var, self.key_prefix, self.observation_interval_seconds,
                )
                defs = dg.Definitions.merge(defs, dg.Definitions(sensors=[sensor]))
            return defs
