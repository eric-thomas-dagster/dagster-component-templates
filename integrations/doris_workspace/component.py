"""DorisWorkspaceComponent.

Auto-discover everything in an [Apache Doris](https://doris.apache.org/)
database — tables, materialized views, async materialized views, views —
and emit one observable Dagster asset per discovered entity. Analogous
to the Snowflake workspace component, but scoped to Doris's narrower
surface (no streams / stages / alerts / openflow).

Each discovered entity becomes an `@observable_source_asset` that emits
an `ObserveResult` with:

  - `doris/row_count` (int)            — approximate via `information_schema.tables.TABLE_ROWS`
                                          (fast — no full scan)
  - `doris/last_update` (str)          — `information_schema.tables.UPDATE_TIME`
  - `doris/storage_size_bytes` (int)   — `information_schema.tables.DATA_LENGTH`
  - `doris/entity_type` (str)          — TABLE / VIEW / MATERIALIZED_VIEW / ASYNC_MV
  - `data_version: f"{row_count}:{last_update}"` — change-sensitive signature
                                                    so downstream
                                                    AutomationCondition.eager()
                                                    doesn't re-fire on every
                                                    observation tick.

Sensible defaults: skips `information_schema`, `__internal_schema`,
`mysql` system DBs. Honors include / exclude glob patterns on table /
schema names.

Doris-specific notes:
  - Tables come in three flavors (`DUPLICATE`, `UNIQUE`, `AGGREGATE`)
    — exposed via the `doris/table_model` metadata key.
  - Async Materialized Views (Doris 2.0+) are listed via
    `SHOW MATERIALIZED VIEWS FROM <db>`. We also try
    `information_schema.materialized_views`; whichever returns data
    wins (Doris version differences).
  - Doris speaks the MySQL wire protocol — uses PyMySQL via
    SQLAlchemy. Pairs naturally with the `doris_resource` component.

Docs:
  https://doris.apache.org/docs/data-table/data-model
  https://doris.apache.org/docs/query-acceleration/materialized-view-overview
"""
import fnmatch
import logging
import re
import urllib.parse
from typing import Any, Dict, List, Optional

from dagster import (
    AssetExecutionContext,
    Component,
    ComponentLoadContext,
    DataVersion,
    Definitions,
    MetadataValue,
    Model,
    ObserveResult,
    Resolvable,
    observable_source_asset,
)
from pydantic import Field

_logger = logging.getLogger(__name__)

# System / metadata schemas to skip during discovery.
_SYSTEM_SCHEMAS = {"information_schema", "__internal_schema", "mysql", "performance_schema"}


def _sanitize_asset_key(name: str) -> str:
    """Convert a Doris object name into a valid Dagster asset key segment."""
    return re.sub(r"[^a-zA-Z0-9_]", "_", name.lower())


def _matches_any(name: str, patterns: List[str]) -> bool:
    return any(fnmatch.fnmatchcase(name, p) for p in patterns)


class DorisWorkspaceComponent(Component, Model, Resolvable):
    """Auto-discover tables / views / MVs in an Apache Doris database; emit observable assets.

    Example (minimal — discover everything in `analytics` database):

        ```yaml
        type: dagster_community_components.DorisWorkspaceComponent
        attributes:
          host: doris.mycompany.com
          query_port: 9030
          database: analytics
          username: dagster_reader
          password_env_var: DORIS_PASSWORD
        ```

    Example (filter + grouping + ownership):

        ```yaml
        type: dagster_community_components.DorisWorkspaceComponent
        attributes:
          host: doris.mycompany.com
          database: analytics
          username: dagster_reader
          password_env_var: DORIS_PASSWORD
          schemas: [sales, marketing]
          include_patterns: ["fact_*", "dim_*"]
          exclude_patterns: ["*_staging", "*_tmp"]
          import_tables: true
          import_views: true
          import_materialized_views: true
          group_name: doris_analytics
          owners: ["data-platform@mycompany.com"]
        ```
    """

    # Connection — same shape as doris_resource.
    host: str = Field(description="Doris FE host.")
    query_port: int = Field(default=9030, description="Doris MySQL-protocol query port.")
    database: str = Field(description="Database to discover (single DB per workspace).")
    username: str = Field(description="Doris user with read access to information_schema + target tables.")
    password: Optional[str] = Field(default=None, description="Password (literal). Set this OR password_env_var.")
    password_env_var: Optional[str] = Field(default="DORIS_PASSWORD", description="Env var with password. Defaults to DORIS_PASSWORD.")
    ssl: bool = Field(default=False, description="Use SSL for the MySQL connection.")

    # Discovery scope.
    schemas: Optional[List[str]] = Field(
        default=None,
        description="Restrict discovery to these schemas (databases). Default = the configured `database`.",
    )
    include_patterns: Optional[List[str]] = Field(
        default=None,
        description="fnmatch-style globs (case-sensitive) — keep matching object names. Patterns match `name` only.",
    )
    exclude_patterns: Optional[List[str]] = Field(
        default=None,
        description="fnmatch-style globs (case-sensitive) — drop matching object names.",
    )

    # Entity-type toggles.
    import_tables: bool = Field(default=True, description="Emit assets for base tables (DUP / UNIQUE / AGG).")
    import_views: bool = Field(default=True, description="Emit assets for views.")
    import_materialized_views: bool = Field(default=True, description="Emit assets for materialized views + async MVs.")

    # Asset-level overrides.
    group_name: Optional[str] = Field(default="doris", description="Dagster group name for all discovered assets.")
    owners: Optional[List[str]] = Field(default=None, description="Owners applied to every discovered asset.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Tags applied to every discovered asset.")
    kinds: Optional[List[str]] = Field(default=None, description="Additional kinds (always includes 'doris').")

    request_timeout_seconds: int = Field(default=30, ge=1)

    # --------------------------------------------------------------------- conn

    def _password(self) -> str:
        import os
        if self.password is not None:
            return self.password
        if self.password_env_var and os.environ.get(self.password_env_var):
            return os.environ[self.password_env_var]
        raise RuntimeError(
            "Doris workspace requires a password — set `password` or "
            f"`password_env_var` (currently {self.password_env_var!r})."
        )

    def _connection_string(self, database: Optional[str] = None) -> str:
        pw = urllib.parse.quote_plus(self._password())
        db = database or self.database
        url = f"mysql+pymysql://{self.username}:{pw}@{self.host}:{self.query_port}/{db}"
        if self.ssl:
            url += "?ssl=true"
        return url

    def _create_engine(self, database: Optional[str] = None):
        try:
            from sqlalchemy import create_engine
        except ImportError:
            raise ImportError(
                "doris_workspace requires sqlalchemy + pymysql; "
                "install with `uv add sqlalchemy pymysql`."
            )
        return create_engine(self._connection_string(database), pool_pre_ping=True)

    # --------------------------------------------------------------------- discover

    def _list_schemas(self, engine) -> List[str]:
        if self.schemas:
            return [s for s in self.schemas if s not in _SYSTEM_SCHEMAS]
        from sqlalchemy import text
        with engine.connect() as c:
            rows = c.execute(text("SHOW DATABASES")).fetchall()
        return [r[0] for r in rows if r[0] not in _SYSTEM_SCHEMAS]

    def _list_tables(self, engine, schema: str) -> List[Dict[str, Any]]:
        """Return [{name, type, row_count, data_length_bytes, update_time, table_model}, ...]."""
        from sqlalchemy import text
        q = text(
            "SELECT TABLE_NAME, TABLE_TYPE, TABLE_ROWS, DATA_LENGTH, UPDATE_TIME "
            "FROM information_schema.tables "
            "WHERE TABLE_SCHEMA = :schema"
        )
        with engine.connect() as c:
            rows = c.execute(q, {"schema": schema}).fetchall()
        out = []
        for r in rows:
            name = r[0]
            ttype = (r[1] or "").upper()
            row_count = int(r[2]) if r[2] is not None else None
            data_len = int(r[3]) if r[3] is not None else None
            update_time = str(r[4]) if r[4] is not None else None
            # Probe table model for base tables only (cheap — single DESC).
            table_model = None
            if "BASE" in ttype:
                try:
                    desc = c.execute(text(f"SHOW CREATE TABLE `{schema}`.`{name}`")).fetchone()
                    if desc:
                        ddl = " ".join(str(x) for x in desc)
                        for marker in ("DUPLICATE KEY", "UNIQUE KEY", "AGGREGATE KEY"):
                            if marker in ddl.upper():
                                table_model = marker.split(" ")[0]
                                break
                except Exception:
                    pass
            out.append({
                "name": name,
                "type": ttype,
                "row_count": row_count,
                "data_length_bytes": data_len,
                "update_time": update_time,
                "table_model": table_model,
            })
        return out

    def _list_materialized_views(self, engine, schema: str) -> List[Dict[str, Any]]:
        """Async materialized views (Doris 2.0+)."""
        from sqlalchemy import text
        out: List[Dict[str, Any]] = []
        # Try information_schema first (newer Doris).
        try:
            with engine.connect() as c:
                rows = c.execute(
                    text(
                        "SELECT TABLE_NAME, REFRESH_TYPE, LAST_REFRESH_TIME, ROW_COUNT "
                        "FROM information_schema.materialized_views "
                        "WHERE TABLE_SCHEMA = :schema"
                    ),
                    {"schema": schema},
                ).fetchall()
            for r in rows:
                out.append({
                    "name": r[0],
                    "refresh_type": r[1],
                    "last_refresh_time": str(r[2]) if r[2] is not None else None,
                    "row_count": int(r[3]) if r[3] is not None else None,
                })
            return out
        except Exception:
            # Fall back to SHOW MATERIALIZED VIEWS (older Doris).
            try:
                with engine.connect() as c:
                    rows = c.execute(text(f"SHOW MATERIALIZED VIEWS FROM `{schema}`")).fetchall()
                for r in rows:
                    out.append({
                        "name": r[0] if r else None,
                        "refresh_type": None,
                        "last_refresh_time": None,
                        "row_count": None,
                    })
            except Exception as exc:
                _logger.warning(f"Could not list materialized views for {schema}: {exc}")
        return out

    def _should_include(self, name: str) -> bool:
        if self.include_patterns and not _matches_any(name, self.include_patterns):
            return False
        if self.exclude_patterns and _matches_any(name, self.exclude_patterns):
            return False
        return True

    # --------------------------------------------------------------------- assets

    def _build_table_asset(self, schema: str, info: Dict[str, Any]):
        """Build one observable_source_asset for a discovered table/view."""
        _self = self
        name = info["name"]
        entity_type = info.get("type") or "TABLE"
        is_view = entity_type == "VIEW"

        asset_key = _sanitize_asset_key(f"{schema}__{name}")

        kinds = set(self.kinds or [])
        kinds.add("doris")
        if is_view:
            kinds.add("view")

        base_metadata = {
            "doris/schema": schema,
            "doris/name": name,
            "doris/entity_type": entity_type,
        }
        if info.get("table_model"):
            base_metadata["doris/table_model"] = info["table_model"]

        @observable_source_asset(
            name=asset_key,
            group_name=self.group_name,
            kinds=kinds,
            owners=self.owners,
            tags=self.tags,
            description=f"Doris {entity_type.lower()}: {schema}.{name}",
            metadata=base_metadata,
        )
        def _the_asset(context: AssetExecutionContext) -> ObserveResult:
            engine = _self._create_engine(schema)
            from sqlalchemy import text
            row_count = None
            data_len = None
            update_time = None
            try:
                with engine.connect() as c:
                    row = c.execute(
                        text(
                            "SELECT TABLE_ROWS, DATA_LENGTH, UPDATE_TIME "
                            "FROM information_schema.tables "
                            "WHERE TABLE_SCHEMA = :schema AND TABLE_NAME = :name"
                        ),
                        {"schema": schema, "name": name},
                    ).fetchone()
                if row:
                    row_count = int(row[0]) if row[0] is not None else None
                    data_len = int(row[1]) if row[1] is not None else None
                    update_time = str(row[2]) if row[2] is not None else None
            except Exception as exc:
                context.log.warning(f"Could not observe {schema}.{name}: {exc}")

            metadata: Dict[str, Any] = dict(base_metadata)
            if row_count is not None:
                metadata["doris/row_count"] = MetadataValue.int(row_count)
            if data_len is not None:
                metadata["doris/storage_size_bytes"] = MetadataValue.int(data_len)
            if update_time is not None:
                metadata["doris/last_update"] = update_time

            signature = f"{row_count}:{update_time}"
            return ObserveResult(
                data_version=DataVersion(signature),
                metadata=metadata,
            )

        return _the_asset

    def _build_mv_asset(self, schema: str, info: Dict[str, Any]):
        """Build one observable_source_asset for an async MV."""
        _self = self
        name = info["name"]
        asset_key = _sanitize_asset_key(f"{schema}__{name}")

        kinds = set(self.kinds or [])
        kinds.add("doris")
        kinds.add("materialized_view")

        base_metadata = {
            "doris/schema": schema,
            "doris/name": name,
            "doris/entity_type": "MATERIALIZED_VIEW",
            "doris/refresh_type": info.get("refresh_type") or "unknown",
        }

        @observable_source_asset(
            name=asset_key,
            group_name=self.group_name,
            kinds=kinds,
            owners=self.owners,
            tags=self.tags,
            description=f"Doris async MV: {schema}.{name}",
            metadata=base_metadata,
        )
        def _the_asset(context: AssetExecutionContext) -> ObserveResult:
            engine = _self._create_engine(schema)
            from sqlalchemy import text
            row_count = None
            last_refresh = None
            try:
                with engine.connect() as c:
                    row = c.execute(
                        text(
                            "SELECT LAST_REFRESH_TIME, ROW_COUNT "
                            "FROM information_schema.materialized_views "
                            "WHERE TABLE_SCHEMA = :schema AND TABLE_NAME = :name"
                        ),
                        {"schema": schema, "name": name},
                    ).fetchone()
                if row:
                    last_refresh = str(row[0]) if row[0] is not None else None
                    row_count = int(row[1]) if row[1] is not None else None
            except Exception as exc:
                context.log.warning(f"Could not observe MV {schema}.{name}: {exc}")

            metadata: Dict[str, Any] = dict(base_metadata)
            if row_count is not None:
                metadata["doris/row_count"] = MetadataValue.int(row_count)
            if last_refresh is not None:
                metadata["doris/last_refresh_time"] = last_refresh

            signature = f"{row_count}:{last_refresh}"
            return ObserveResult(
                data_version=DataVersion(signature),
                metadata=metadata,
            )

        return _the_asset

    # --------------------------------------------------------------------- build_defs

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        engine = self._create_engine()
        assets: list = []

        for schema in self._list_schemas(engine):
            if self.import_tables or self.import_views:
                try:
                    for info in self._list_tables(engine, schema):
                        if not self._should_include(info["name"]):
                            continue
                        is_view = (info["type"] or "").upper() == "VIEW"
                        if is_view and not self.import_views:
                            continue
                        if (not is_view) and not self.import_tables:
                            continue
                        assets.append(self._build_table_asset(schema, info))
                except Exception as exc:
                    _logger.warning(f"Could not list tables in {schema}: {exc}")

            if self.import_materialized_views:
                try:
                    for info in self._list_materialized_views(engine, schema):
                        if not info.get("name"):
                            continue
                        if not self._should_include(info["name"]):
                            continue
                        assets.append(self._build_mv_asset(schema, info))
                except Exception as exc:
                    _logger.warning(f"Could not list MVs in {schema}: {exc}")

        return Definitions(assets=assets)
