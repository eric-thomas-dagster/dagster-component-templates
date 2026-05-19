"""Database Replication.

Move tables between databases without landing them in Python memory.
Wraps Sling (slingdata.io) under the hood via the official `dagster-sling`
integration — the user supplies an opinionated 6-field surface, the component
builds the Sling replication spec and feeds it to `@sling_assets`.

Use for tables too big to fit in a pandas DataFrame, or when you want native
streaming SQL→SQL replication (Postgres / MySQL / MSSQL / Oracle / Db2 /
Snowflake / BigQuery / Redshift / Databricks).
"""
from typing import Dict, List, Optional

import dagster as dg
from pydantic import Field


_VALID_TYPES = {
    "postgres", "mysql", "mssql", "oracle", "db2",
    "snowflake", "bigquery", "redshift", "databricks",
    "duckdb", "clickhouse", "mariadb", "sqlite",
}

_MODE_MAP = {
    "full_refresh": "full-refresh",
    "incremental": "incremental",
    "snapshot": "snapshot",
    "truncate": "truncate",
}


class DatabaseReplicationComponent(dg.Component, dg.Model, dg.Resolvable):
    """Replicate a table between two SQL databases using Sling under the hood.

    No data lands in Dagster/Python memory — Sling streams rows directly from
    source to target. Suitable for tables of any size, including ones too big
    for pandas.
    """

    asset_name: str = Field(description="Output Dagster asset name")
    source_connection: Optional[str] = Field(
        default=None,
        description="Full Sling-compatible source connection URL (e.g. "
                    "'postgres://user:pass@host:5432/db'). Set this OR source_connection_env_var.",
    )
    source_connection_env_var: Optional[str] = Field(
        default=None,
        description="Env var with the source connection URL. Set this OR source_connection.",
    )
    target_connection: Optional[str] = Field(
        default=None,
        description="Full Sling-compatible target connection URL (e.g. "
                    "'snowflake://user:pass@account.snowflakecomputing.com/db/schema?warehouse=W'). "
                    "Set this OR target_connection_env_var.",
    )
    target_connection_env_var: Optional[str] = Field(
        default=None,
        description="Env var with the target connection URL. Set this OR target_connection.",
    )
    source_type: str = Field(
        description="Source database type: postgres, mysql, mssql, oracle, db2, snowflake, bigquery, redshift, databricks, duckdb, clickhouse, mariadb, sqlite"
    )
    target_type: str = Field(
        description="Target database type (same options as source_type)"
    )
    source_table: str = Field(
        description="Source table — either 'table' or 'schema.table'"
    )
    target_table: str = Field(
        description="Target table — either 'table' or 'schema.table'"
    )

    mode: str = Field(
        default="full_refresh",
        description="Replication mode: full_refresh (drop+reload), incremental (append/upsert new rows), snapshot (timestamped reload), truncate (truncate+insert)",
    )
    incremental_column: Optional[str] = Field(
        default=None,
        description="Column to track for incremental loads (e.g. 'updated_at', 'id'). Required when mode='incremental'.",
    )
    primary_key: Optional[List[str]] = Field(
        default=None,
        description="Primary key column(s) for upsert behavior in incremental mode",
    )

    select_columns: Optional[List[str]] = Field(
        default=None,
        description="Columns to replicate. If unset, all columns are replicated.",
    )
    where_clause: Optional[str] = Field(
        default=None,
        description="Optional WHERE clause filter applied to the source (e.g. 'created_at >= 2024-01-01')",
    )

    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
    description: Optional[str] = Field(default=None, description="Asset description")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners")
    asset_tags: Optional[Dict[str, str]] = Field(default=None, description="Additional asset tags")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds (default: ['sling', source_type, target_type])")
    deps: Optional[List[str]] = Field(default=None, description="Lineage-only upstream asset keys")

    @classmethod
    def get_description(cls) -> str:
        return (
            "Replicate a table between two SQL databases using Sling under the hood. "
            "No data lands in Python memory; suitable for any size table."
        )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        import os

        source_type = self.source_type.lower()
        target_type = self.target_type.lower()
        if source_type not in _VALID_TYPES:
            raise ValueError(
                f"source_type={source_type!r} not in {sorted(_VALID_TYPES)}"
            )
        if target_type not in _VALID_TYPES:
            raise ValueError(
                f"target_type={target_type!r} not in {sorted(_VALID_TYPES)}"
            )

        mode_key = self.mode.lower()
        if mode_key not in _MODE_MAP:
            raise ValueError(
                f"mode={mode_key!r} not in {sorted(_MODE_MAP.keys())}"
            )
        sling_mode = _MODE_MAP[mode_key]

        if mode_key == "incremental" and not self.incremental_column:
            raise ValueError("mode='incremental' requires incremental_column")

        try:
            from dagster_sling import (
                DagsterSlingTranslator,
                SlingConnectionResource,
                SlingResource,
                sling_assets,
            )
        except ImportError as e:
            raise ImportError(
                "dagster-sling required: pip install dagster-sling"
            ) from e

        def _resolve(literal, env_var, name):
            if literal:
                return literal
            if env_var:
                v = os.environ.get(env_var)
                if not v:
                    raise EnvironmentError(f"Env var '{env_var}' (for {name}) is not set")
                return v
            raise ValueError(f"Set either '{name}' or '{name}_env_var'")
        source_url = _resolve(self.source_connection, self.source_connection_env_var, "source_connection")
        target_url = _resolve(self.target_connection, self.target_connection_env_var, "target_connection")

        source_conn_name = f"SRC_{self.asset_name.upper()}"
        target_conn_name = f"TGT_{self.asset_name.upper()}"

        source_conn = SlingConnectionResource(
            name=source_conn_name,
            type=source_type,
            connection_string=source_url,
        )
        target_conn = SlingConnectionResource(
            name=target_conn_name,
            type=target_type,
            connection_string=target_url,
        )

        stream_config: Dict[str, object] = {
            "object": self.target_table,
            "mode": sling_mode,
        }
        if self.incremental_column:
            stream_config["update_key"] = self.incremental_column
        if self.primary_key:
            stream_config["primary_key"] = self.primary_key
        if self.select_columns:
            stream_config["select"] = self.select_columns
        if self.where_clause:
            stream_config["sql"] = (
                f"SELECT * FROM {self.source_table} WHERE {self.where_clause}"
            )

        replication_config = {
            "source": source_conn_name,
            "target": target_conn_name,
            "streams": {
                self.source_table: stream_config,
            },
        }

        kinds_list = self.kinds or ["sling", source_type, target_type]
        all_tags = dict(self.asset_tags or {})
        for k in kinds_list:
            all_tags[f"dagster/kind/{k}"] = ""

        asset_name = self.asset_name
        group_name = self.group_name
        description = self.description or self.get_description()
        owners = self.owners or []
        deps_keys = [dg.AssetKey.from_user_string(k) for k in (self.deps or [])]

        class _Translator(DagsterSlingTranslator):
            def get_asset_spec(self, stream_definition):  # type: ignore[override]
                spec = super().get_asset_spec(stream_definition)
                return spec.replace_attributes(
                    key=dg.AssetKey(asset_name),
                    group_name=group_name,
                    description=description,
                    owners=owners,
                    tags=all_tags,
                    deps=deps_keys,
                    kinds=set(kinds_list),
                )

        # Unique resource key per instance so multiple `database_replication`
        # components can coexist in one project without colliding on the
        # default "sling" resource key.
        sling_key = f"sling_{asset_name}"
        _src = (
            f"def _replication(context, {sling_key}):\n"
            f"    yield from {sling_key}.replicate(context=context)\n"
        )
        _ns: dict = {}
        exec(_src, {}, _ns)
        _replication_fn = _ns["_replication"]
        _replication_fn.__annotations__[sling_key] = SlingResource

        replication = sling_assets(
            replication_config=replication_config,
            dagster_sling_translator=_Translator(),
            name=asset_name,
        )(_replication_fn)

        return dg.Definitions(
            assets=[replication],
            resources={
                sling_key: SlingResource(
                    connections=[source_conn, target_conn],
                ),
            },
        )
