"""Database Replication.

Move one or more tables between databases without landing them in Python
memory. Wraps Sling (slingdata.io) under the hood via the official
`dagster-sling` integration — the user supplies an opinionated surface (a
single source/target table, or a `streams` list for several), the component
builds the Sling replication spec and feeds it to `@sling_assets`.

Use for tables too big to fit in a pandas DataFrame, or when you want native
streaming SQL→SQL replication (Postgres / MySQL / MSSQL / Oracle / Db2 /
Snowflake / BigQuery / Redshift / Databricks). Supports time-partitioned
replication (`partition_type`), where each partition replaces exactly its
own window in the target.
"""
from typing import Dict, List, Optional, Union

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

# Partition types that map onto a time window of a date/timestamp column.
_TIME_PARTITIONS = ("daily", "hourly", "weekly", "monthly")



# BEGIN field-shape normalization (inlined per component; keep copies identical — see tests/test_field_shapes.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def coerce_field_shapes(data, *, lists=(), csv=(), tags=()):
    """Accept the shapes people (and LLMs) naturally write for common boilerplate fields."""
    if not isinstance(data, dict):
        return data
    out = dict(data)
    for k in lists:  # List[str]: "a" / "a, b" -> ["a", "b"]; strip + drop empties
        v = out.get(k)
        if isinstance(v, str):
            v = v.split(",")
        if isinstance(v, (list, tuple)):
            out[k] = [x.strip() if isinstance(x, str) else x for x in v if not (isinstance(x, str) and not x.strip())]
    for k in csv:  # comma-separated str given as a list -> "a,b"
        v = out.get(k)
        if isinstance(v, (list, tuple, set)):
            out[k] = ",".join(str(x).strip() for x in v)
    for k in tags:  # Dict[str, str]: scalars -> str, ["k=v"] / ["k:v"] -> {k: v}
        v = out.get(k)
        if isinstance(v, (list, tuple)) and v and all(isinstance(x, str) and ("=" in x or ":" in x) for x in v):
            v = dict((p.strip() for p in (x.split("=", 1) if "=" in x else x.split(":", 1))) for x in v)
        if isinstance(v, dict):
            out[k] = {str(a).strip(): (str(b).lower() if isinstance(b, bool) else str(b)) for a, b in v.items() if b is not None}
    return out
# END field-shape normalization


class StreamSpec(dg.Model, dg.Resolvable):
    """One table in a multi-table replication. Unset fields fall back to the component's defaults."""

    source_table: str = Field(description="Source table — either 'table' or 'schema.table'")
    target_table: Optional[str] = Field(
        default=None, description="Target table. Defaults to the source table name."
    )
    mode: Optional[str] = Field(
        default=None,
        description="Replication mode for this table (full_refresh / incremental / snapshot / truncate). Defaults to the component's mode.",
    )
    incremental_column: Optional[Union[str, int]] = Field(
        default=None, description="Watermark column for this table. Required when its mode is incremental."
    )
    primary_key: Optional[List[str]] = Field(
        default=None, description="Primary key column(s) for this table. Required when the component is partitioned."
    )
    select_columns: Optional[List[Union[str, int]]] = Field(
        default=None, description="Columns to replicate. If unset, all columns."
    )
    where_clause: Optional[str] = Field(default=None, description="Optional WHERE filter on this table's source rows")
    partition_column: Optional[str] = Field(
        default=None,
        description="Date/timestamp column that slices this table by partition. Defaults to the component's partition_column.",
    )


class DatabaseReplicationComponent(dg.Component, dg.Model, dg.Resolvable):
    """Replicate one or more tables between two SQL databases using Sling under the hood.

    No data lands in Dagster/Python memory — Sling streams rows directly from
    source to target. Suitable for tables of any size, including ones too big
    for pandas.

    Give either ``source_table``/``target_table`` (one table, one asset named
    ``asset_name``) or ``streams`` (several tables over one connection pair, one
    asset each). With ``partition_type`` set, each partition replaces exactly its
    own time window in the target, so any slice can be re-replicated.
    """

    asset_name: str = Field(
        description="Output Dagster asset name. With several streams it is the prefix of each asset key."
    )
    source_connection: Optional[str] = Field(
        default=None,
        description="Full Sling-compatible source connection URL (e.g. "
                    "'postgres://user:pass@host:5432/db'). Set this OR source_connection_env_var.",
    )
    source_connection_env_var: Optional[str] = Field(
        default=None,
        description="Env var with the source connection URL, read when the asset runs. Set this OR source_connection.",
    )
    target_connection: Optional[str] = Field(
        default=None,
        description="Full Sling-compatible target connection URL (e.g. "
                    "'snowflake://user:pass@account.snowflakecomputing.com/db/schema?warehouse=W'). "
                    "Set this OR target_connection_env_var.",
    )
    target_connection_env_var: Optional[str] = Field(
        default=None,
        description="Env var with the target connection URL, read when the asset runs. Set this OR target_connection.",
    )
    source_type: str = Field(
        description="Source database type: postgres, mysql, mssql, oracle, db2, snowflake, bigquery, redshift, databricks, duckdb, clickhouse, mariadb, sqlite"
    )
    target_type: str = Field(
        description="Target database type (same options as source_type)"
    )
    source_table: Optional[str] = Field(
        default=None, description="Source table — either 'table' or 'schema.table'. Not used with `streams`."
    )
    target_table: Optional[str] = Field(
        default=None, description="Target table — either 'table' or 'schema.table'. Defaults to the source table."
    )
    streams: Optional[List[StreamSpec]] = Field(
        default=None,
        description="Several tables over one connection pair, one asset each. Replaces source_table/target_table.",
    )

    mode: str = Field(
        default="full_refresh",
        description="Replication mode: full_refresh (drop+reload), incremental (append/upsert new rows), snapshot (timestamped reload), truncate (truncate+insert). Ignored for partitioned components, which always replace the partition's window.",
    )
    incremental_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column to track for incremental loads (e.g. 'updated_at', 'id'). Required when mode='incremental'.",
    )
    primary_key: Optional[List[str]] = Field(
        default=None,
        description="Primary key column(s) for upsert behavior in incremental mode, and for partition reloads",
    )

    select_columns: Optional[List[Union[str, int]]] = Field(
        default=None,
        description="Columns to replicate. If unset, all columns are replicated.",
    )
    where_clause: Optional[str] = Field(
        default=None,
        description="Optional WHERE clause filter applied to the source (e.g. 'created_at >= 2024-01-01')",
    )

    partition_type: Optional[str] = Field(
        default=None,
        description="daily, hourly, weekly or monthly. Each partition copies only the rows whose partition_column falls in its window, replacing that window in the target.",
    )
    partition_start: Optional[str] = Field(
        default=None, description="First partition, as an ISO date (e.g. '2024-01-01'). Required with partition_type."
    )
    partition_column: Optional[str] = Field(
        default=None,
        description="Date or timestamp column in the source that decides which partition a row belongs to. Required with partition_type unless every stream sets its own.",
    )
    partition_replace: bool = Field(
        default=True,
        description="Delete the partition's window in the target before loading it, so rows deleted at the source disappear too. If false, a re-run only upserts (requires a primary key).",
    )

    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
    description: Optional[str] = Field(default=None, description="Asset description")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners")
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Additional asset tags")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds (default: ['sling', source_type, target_type])")
    deps: Optional[List[str]] = Field(default=None, description="Lineage-only upstream asset keys")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('owners', 'kinds', 'deps'), csv=(), tags=('asset_tags',))

    @classmethod
    def get_description(cls) -> str:
        return (
            "Replicate one or more tables between two SQL databases using Sling under the hood. "
            "No data lands in Python memory; suitable for any size table."
        )

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='Database Replication', editable=True)

    # ------------------------------------------------------------------ helpers

    def _stream_specs(self) -> List[StreamSpec]:
        """The tables to copy, whether given as `streams` or as a single source/target table."""
        if self.streams:
            if self.source_table or self.target_table:
                raise ValueError("Set either `streams` or source_table/target_table, not both")
            return list(self.streams)
        if not self.source_table:
            raise ValueError("Set `source_table` (and `target_table`), or `streams`")
        return [
            StreamSpec(
                source_table=self.source_table,
                target_table=self.target_table or self.source_table,
            )
        ]

    def _partitions_def(self):
        if not self.partition_type:
            return None
        ptype = self.partition_type.lower()
        if ptype not in _TIME_PARTITIONS:
            raise ValueError(f"partition_type={ptype!r} not in {list(_TIME_PARTITIONS)}")
        if not self.partition_start:
            raise ValueError("partition_type requires partition_start")
        factory = {
            "daily": dg.DailyPartitionsDefinition,
            "hourly": dg.HourlyPartitionsDefinition,
            "weekly": dg.WeeklyPartitionsDefinition,
            "monthly": dg.MonthlyPartitionsDefinition,
        }[ptype]
        return factory(start_date=self.partition_start)

    def _stream_config(self, spec: StreamSpec, window=None, replace: bool = True) -> Dict[str, object]:
        """The Sling stream for one table; with `window` it replaces exactly that time window."""
        mode_key = (spec.mode or self.mode).lower()
        if mode_key not in _MODE_MAP:
            raise ValueError(f"mode={mode_key!r} not in {sorted(_MODE_MAP.keys())}")
        incremental_column = spec.incremental_column or self.incremental_column
        primary_key = spec.primary_key or self.primary_key
        select_columns = spec.select_columns or self.select_columns
        where_clause = spec.where_clause or self.where_clause
        target_table = spec.target_table or spec.source_table

        config: Dict[str, object] = {"object": target_table}
        if window is None:
            if mode_key == "incremental" and not incremental_column:
                raise ValueError(f"{spec.source_table}: mode='incremental' requires incremental_column")
            config["mode"] = _MODE_MAP[mode_key]
            if incremental_column:
                config["update_key"] = incremental_column
            if primary_key:
                config["primary_key"] = primary_key
        else:
            column = spec.partition_column or self.partition_column
            if not column:
                raise ValueError(f"{spec.source_table}: partitioned replication requires partition_column")
            if not primary_key:
                raise ValueError(f"{spec.source_table}: partitioned replication requires primary_key")
            start, end = window
            # Sling's range is inclusive at both ends, so stop just before the next window.
            from datetime import timedelta

            last = end - timedelta(seconds=1)
            fmt = "%Y-%m-%d %H:%M:%S"
            config["mode"] = "backfill"
            config["update_key"] = column
            config["primary_key"] = primary_key
            config["source_options"] = {"range": f"{start.strftime(fmt)},{last.strftime(fmt)}"}
            if self.partition_replace and replace:
                config["target_options"] = {
                    "pre_sql": (
                        f"DELETE FROM {target_table} WHERE {column} >= '{start.strftime(fmt)}' "
                        f"AND {column} < '{end.strftime(fmt)}'"
                    )
                }
        if select_columns:
            config["select"] = select_columns
        if where_clause:
            config["sql"] = f"SELECT * FROM {spec.source_table} WHERE {where_clause}"
        return config

    # --------------------------------------------------------------- definitions

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
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

        specs = self._stream_specs()
        partitions_def = self._partitions_def()
        if partitions_def is not None:
            # Validate the per-table requirements now, so a bad config fails at load rather than mid-run.
            import datetime as _dt

            sample = (_dt.datetime(2024, 1, 1), _dt.datetime(2024, 1, 2))
            for spec in specs:
                self._stream_config(spec, sample)
        else:
            for spec in specs:
                self._stream_config(spec)

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

        # Connection strings are resolved when the asset runs (a missing variable fails the run, not
        # the whole code location).
        def _connection(literal, env_var, name):
            if literal:
                return literal
            if env_var:
                return dg.EnvVar(env_var)
            raise ValueError(f"Set either '{name}' or '{name}_env_var'")

        source_conn_name = f"SRC_{self.asset_name.upper()}"
        target_conn_name = f"TGT_{self.asset_name.upper()}"
        source_conn = SlingConnectionResource(
            name=source_conn_name,
            type=source_type,
            connection_string=_connection(self.source_connection, self.source_connection_env_var, "source_connection"),
        )
        target_conn = SlingConnectionResource(
            name=target_conn_name,
            type=target_type,
            connection_string=_connection(self.target_connection, self.target_connection_env_var, "target_connection"),
        )

        # Source tables whose target does not exist yet (the first load), so no delete is attempted.
        no_target_yet: set = set()

        def _replication_config(window=None):
            return {
                "source": source_conn_name,
                "target": target_conn_name,
                "streams": {
                    spec.source_table: self._stream_config(spec, window, replace=spec.source_table not in no_target_yet)
                    for spec in specs
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
        single = len(specs) == 1 and not self.streams
        target_by_source = {s.source_table: (s.target_table or s.source_table) for s in specs}

        class _Translator(DagsterSlingTranslator):
            def get_asset_spec(self, stream_definition):  # type: ignore[override]
                spec = super().get_asset_spec(stream_definition)
                if single:
                    key = dg.AssetKey(asset_name)
                else:
                    target = target_by_source.get(stream_definition.get("name"), "")
                    key = dg.AssetKey([asset_name, *target.split(".")])
                return spec.replace_attributes(
                    key=key,
                    group_name=group_name,
                    description=description,
                    owners=owners,
                    tags=all_tags,
                    deps=deps_keys,
                    kinds=set(kinds_list),
                )

        translator = _Translator()

        # Unique resource key per instance so multiple `database_replication`
        # components can coexist in one project without colliding on the
        # default "sling" resource key.
        sling_key = f"sling_{asset_name}"
        _src = (
            f"def _replication(context, {sling_key}):\n"
            f"    yield from _run(context, {sling_key})\n"
        )

        def _window(context):
            window = context.partition_time_window
            return window.start.replace(tzinfo=None), window.end.replace(tzinfo=None)

        class _PartitionAwareSling(SlingResource):
            # dagster-sling replaces the streams passed to `replicate()` with the ones stored on the
            # assets. A partitioned run needs this partition's window in them, so build those here.
            @staticmethod
            def _get_replication_streams_for_context(context):  # type: ignore[override]
                selected = SlingResource._get_replication_streams_for_context(context)
                if partitions_def is None or not context.has_partition_key:
                    return selected
                windowed = _replication_config(_window(context))["streams"]
                return {name: windowed[name] for name in selected if name in windowed}

        def _replicate(context, sling_resource):
            return sling_resource.replicate(
                context=context,
                replication_config=_replication_config(),
                dagster_sling_translator=translator,
            )

        def _target_exists(url: str, table: str) -> bool:
            """Whether the target table exists, asked through Sling so no other client is needed."""
            import sling

            try:
                list(sling.Sling(src_conn=url, src_stream=f"SELECT 1 FROM {table} WHERE 1=0").stream())
                return True
            except Exception as error:  # noqa: BLE001
                text = str(error).lower()
                gone = ("does not exist", "doesn't exist", "not found", "no such table", "invalid object name")
                return not any(phrase in text for phrase in gone)

        def _run(context, sling_resource):
            if partitions_def is not None and self.partition_replace:
                target_url = next(
                    c.connection_string for c in sling_resource.connections if c.name == target_conn_name
                )
                no_target_yet.clear()
                for spec in specs:
                    table = spec.target_table or spec.source_table
                    if not _target_exists(target_url, table):
                        context.log.info(f"{table} does not exist yet; loading it without deleting first.")
                        no_target_yet.add(spec.source_table)
            yield from sling_resource.replicate(
                context=context,
                replication_config=_replication_config(),
                dagster_sling_translator=translator,
            )

        _ns: dict = {}
        exec(_src, {"_run": _run}, _ns)
        _replication_fn = _ns["_replication"]
        _replication_fn.__annotations__[sling_key] = SlingResource

        replication = sling_assets(
            replication_config=_replication_config(),
            dagster_sling_translator=translator,
            name=asset_name,
            partitions_def=partitions_def,
        )(_replication_fn)

        return dg.Definitions(
            assets=[replication],
            resources={
                sling_key: _PartitionAwareSling(
                    connections=[source_conn, target_conn],
                ),
            },
        )
