"""SQL to Database Asset Component.

Reads rows from a source database table/query and writes them to a destination
database table via SQLAlchemy. Designed to be triggered by sql_monitor or a schedule.

Supports any SQLAlchemy-compatible source and destination (Postgres, MySQL, MSSQL,
SQLite, Snowflake, BigQuery, Redshift, DuckDB, etc.).
"""
from typing import Any, Dict, List, Optional, Union
import dagster as dg
from dagster import AssetExecutionContext, Config
from pydantic import Field



def _build_partitions_def(
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
    partition_dimensions,
):
    """Construct a Dagster partitions_def from the canonical partition fields.

    Strict: raises ValueError on misconfigured combinations rather than
    silently picking a default. Specifically:
      - time-based partition_type without partition_start
      - partition_type=multi without partition_values
      - partition_type=dynamic without dynamic_partition_name
      - both partition_dimensions AND flat fields set (ambiguous intent)
    """
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )

    if partition_dimensions and partition_type:
        raise ValueError(
            "Set either partition_type (flat-fields shape) or "
            "partition_dimensions (multi-axis shape), not both."
        )

    def _build_axis(spec):
        t = spec.get("type")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily":
            return DailyPartitionsDefinition(start_date=spec["start"])
        if t == "weekly":
            return WeeklyPartitionsDefinition(start_date=spec["start"])
        if t == "monthly":
            return MonthlyPartitionsDefinition(start_date=spec["start"])
        if t == "hourly":
            return HourlyPartitionsDefinition(start_date=spec["start"])
        if t == "static":
            vals = spec.get("values") or []
            if isinstance(vals, str):
                vals = [v.strip() for v in vals.split(",") if v.strip()]
            if not vals:
                raise ValueError("partition dimension type='static' requires non-empty 'values'")
            return StaticPartitionsDefinition(list(vals))
        if t == "dynamic":
            name = spec.get("dynamic_partition_name") or spec.get("name")
            if not name:
                raise ValueError("partition dimension type='dynamic' requires a name")
            return DynamicPartitionsDefinition(name=name)
        raise ValueError(f"unknown partition type: {t!r}")

    if partition_dimensions:
        if len(partition_dimensions) == 1:
            return _build_axis(partition_dimensions[0])
        axes = {d["name"]: _build_axis(d) for d in partition_dimensions}
        return MultiPartitionsDefinition(axes)

    if not partition_type:
        return None
    if isinstance(partition_values, (list, tuple)):
        _values = [str(v).strip() for v in partition_values if str(v).strip()]
    else:
        _values = [v.strip() for v in (str(partition_values) if partition_values else "").split(",") if v.strip()]
    if partition_type in ("daily", "weekly", "monthly", "hourly") and not partition_start:
        raise ValueError(
            f"partition_type={partition_type!r} requires partition_start (ISO date, e.g. '2024-01-01')."
        )
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "static":
        if not _values:
            raise ValueError("partition_type='static' requires partition_values (comma-separated).")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError(
                "partition_type='dynamic' requires dynamic_partition_name."
            )
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    if partition_type == "multi":
        if not _values:
            raise ValueError("partition_type='multi' requires partition_values (comma-separated).")
        if not partition_start:
            raise ValueError("partition_type='multi' requires partition_start (the date axis start).")
        return MultiPartitionsDefinition({
            "date": DailyPartitionsDefinition(start_date=partition_start),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")


class SQLToDatabaseAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Read from a source database and write to a destination database table.

    Supports full-table copy, incremental watermark-based loads, and custom SQL.
    Triggered by sql_monitor or run on a schedule.

    Example:
        ```yaml
        type: dagster_component_templates.SQLToDatabaseAssetComponent
        attributes:
          asset_name: crm_contacts_sync
          source_url_env_var: SOURCE_DB_URL
          destination_url_env_var: DESTINATION_DB_URL
          source_table: contacts
          destination_table: raw_contacts
          watermark_column: updated_at
          watermark_env_var: CONTACTS_WATERMARK
        ```
    """

    asset_name: str = Field(description="Dagster asset name")
    source_url_env_var: Optional[str] = Field(default=None, description="Env var with source SQLAlchemy database URL. Set this OR source_resource_key.")
    destination_url_env_var: Optional[str] = Field(default=None, description="Env var with destination SQLAlchemy database URL. Set this OR destination_resource_key.")
    source_resource_key: Optional[str] = Field(
        default=None,
        description="Key of a registered SQL resource to use for the source connection instead of source_url_env_var. The resource must expose a connection_string property (e.g. PostgresResource, MongoDBResource); not all resource types do.",
    )
    destination_resource_key: Optional[str] = Field(
        default=None,
        description="Key of a registered SQL resource to use for the destination connection instead of destination_url_env_var. The resource must expose a connection_string property (e.g. PostgresResource, MongoDBResource); not all resource types do.",
    )
    source_table: Optional[str] = Field(default=None, description="Source table name (use source_table OR source_query)")
    source_schema: Optional[str] = Field(default=None, description="Source schema name")
    source_query: Optional[str] = Field(default=None, description="Custom SQL query (overrides source_table)")
    destination_table: str = Field(description="Destination table name")
    destination_schema: Optional[str] = Field(default=None, description="Destination schema name")
    if_exists: str = Field(default="append", description="fail, replace, or append")
    watermark_column: Optional[Union[str, int]] = Field(default=None, description="Incremental watermark column (e.g. updated_at, id)")
    watermark_env_var: Optional[str] = Field(default=None, description="Env var storing the last watermark value")
    chunksize: int = Field(default=10000, description="Rows to read/write per chunk")
    column_mapping: Optional[dict] = Field(default=None, description="Rename columns: {old: new}")
    group_name: Optional[str] = Field(default="ingestion", description="Asset group name")
    description: Optional[str] = Field(default=None)
    partition_type: str = Field(default="none", description="none, daily, weekly, monthly, hourly, static, multi, or dynamic")
    partition_start_date: Optional[str] = Field(default=None, description="Partition start date YYYY-MM-DD (required if partition_type != none)")
    deps: Optional[list[str]] = Field(default=None, description="Upstream asset keys this asset depends on (e.g. ['raw_orders', 'schema/asset'])")

    retry_policy_max_retries: Optional[int] = Field(

        default=None,

        description="Max retries on asset failure. Defines a RetryPolicy. Useful for transient network failures, rate limits, etc.",

    )

    retry_policy_delay_seconds: Optional[int] = Field(

        default=None,

        description="Seconds between retries (default 1).",

    )

    retry_policy_backoff: str = Field(

        default="exponential",

        description="Backoff strategy: 'linear' or 'exponential'.",

    )



    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners — team names ('team:analytics') or email addresses.",
    )

    asset_tags: Optional[Dict[str, str]] = Field(
        default=None,
        description="Additional key-value tags applied to the asset in the Dagster catalog.",
    )

    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds for the catalog (e.g. ['snowflake', 'python']). Auto-inferred from component name when unset.",
    )

    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale. Builds a FreshnessPolicy when set.",
    )

    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5' (weekdays 9am).",
    )

    column_lineage: Optional[Dict[str, List[str]]] = Field(
        default=None,
        description="Column-level lineage: output column → list of upstream columns it derives from, e.g. {'revenue': ['price', 'quantity']}.",
    )

    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format (e.g. '2024-01-01'). Required for time-based partition types.",
    )

    partition_date_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter the upstream DataFrame to the current date partition key.",
    )

    partition_values: Optional[str] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'acme,globex,initech'.",
    )

    partition_static_dim: Optional[str] = Field(
        default=None,
        description="Dimension name for the static axis in multi-partitioning, e.g. 'customer'.",
    )

    partition_static_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter the upstream DataFrame to the current static partition value.",
    )

    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition (when partition_type='dynamic'), e.g. 'tenants'.",
    )

    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Multi-axis partition spec: list of {name, type, start, values, dynamic_partition_name} dicts. Overrides flat fields when set.",
    )

    include_preview_metadata: bool = Field(
        default=False,
        description="Include a markdown preview of the written rows in the materialization metadata.",
    )

    preview_rows: int = Field(
        default=25,
        description="Max rows to include in the preview when include_preview_metadata is True.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        partitions_def = _build_partitions_def(
            _self.partition_type if _self.partition_type != "none" else None,
            _self.partition_start or _self.partition_start_date,
            _self.partition_values,
            _self.dynamic_partition_name,
            _self.partition_dimensions,
        )

        source_desc = _self.source_query[:50] + "..." if _self.source_query else (
            f"{_self.source_schema + '.' if _self.source_schema else ''}{_self.source_table}"
        )

        class SQLRunConfig(Config):
            watermark_value: Optional[str] = None  # override watermark at runtime

        # Build retry policy (auto-generated; opt-in via retry_policy_max_retries).

        _retry_policy = None

        if self.retry_policy_max_retries is not None:

            from dagster import Backoff, RetryPolicy

            _retry_policy = RetryPolicy(

                max_retries=self.retry_policy_max_retries,

                delay=self.retry_policy_delay_seconds or 1,

                backoff=Backoff[self.retry_policy_backoff.upper()],

            )


        @dg.asset(retry_policy=_retry_policy, 
            key=dg.AssetKey.from_user_string(_self.asset_name),
            description=_self.description or f"SQL:{source_desc} → {_self.destination_table}",
            group_name=_self.group_name,
            kinds={"sql"},
            deps=[dg.AssetKey.from_user_string(k) for k in (_self.deps or [])],
            partitions_def=partitions_def,
            required_resource_keys={
                k for k in (_self.source_resource_key, _self.destination_resource_key) if k
            },
        )
        def sql_to_database_asset(context: AssetExecutionContext, config: SQLRunConfig):
            import os
            import pandas as pd
            from sqlalchemy import create_engine, text

            def _resolve_db_url(resource_key, url_env_var, side):
                """Resolve a SQLAlchemy-compatible database URL: from a registered
                resource (WHERE to connect) if resource_key is set and the resource
                exposes 'connection_string', otherwise from url_env_var. The write/
                read mechanics (HOW) are unchanged either way."""
                if resource_key:
                    resource = getattr(context.resources, resource_key, None)
                    if resource is None:
                        raise ValueError(f"Resource {resource_key!r} is not registered in this project's Definitions.")
                    url = getattr(resource, "connection_string", None)
                    if not url:
                        raise ValueError(
                            f"Resource {resource_key!r} ({type(resource).__name__}) does not expose a "
                            f"'connection_string' property, so it can't be used to determine the {side} "
                            f"database URL automatically. Use {side}_url_env_var instead."
                        )
                    return url
                if url_env_var:
                    if url_env_var not in os.environ:
                        raise KeyError(f"Env var '{url_env_var}' is not set")
                    return os.environ[url_env_var]
                raise ValueError(f"Set '{side}_resource_key' or '{side}_url_env_var'.")

            src_url = _resolve_db_url(_self.source_resource_key, _self.source_url_env_var, "source")
            dst_url = _resolve_db_url(_self.destination_resource_key, _self.destination_url_env_var, "destination")

            src_engine = create_engine(src_url)
            dst_engine = create_engine(dst_url)

            # Build source query
            if _self.source_query:
                query = _self.source_query
            else:
                src_table = f"{_self.source_schema + '.' if _self.source_schema else ''}{_self.source_table}"
                query = f"SELECT * FROM {src_table}"

            # Apply watermark filter for incremental loads
            watermark = config.watermark_value
            if not watermark and _self.watermark_env_var:
                watermark = os.environ.get(_self.watermark_env_var)

            # Time-based partitions (daily/weekly/monthly/hourly) get a real
            # bounded [start, end) window instead of an open-ended watermark --
            # otherwise every partition after the first would re-pull an
            # ever-growing superset instead of just its own slice.
            range_start = None
            range_end = None
            if context.has_partition_key:
                watermark = context.partition_key
                try:
                    _window = context.partition_time_window
                    range_start = _window.start.isoformat()
                    range_end = _window.end.isoformat()
                except Exception:
                    pass  # static/dynamic/multi partition -- no natural time window

            use_bounded_range = range_start is not None and _self.watermark_column and not _self.source_query

            if use_bounded_range:
                query += f" WHERE {_self.watermark_column} >= :range_start AND {_self.watermark_column} < :range_end"
            elif watermark and _self.watermark_column and not _self.source_query:
                query += f" WHERE {_self.watermark_column} > :watermark"

            context.log.info(f"Reading from source: {source_desc}")
            if use_bounded_range:
                context.log.info(f"Partition window: {_self.watermark_column} in [{range_start}, {range_end})")
            elif watermark and _self.watermark_column:
                context.log.info(f"Watermark: {_self.watermark_column} > {watermark}")

            with src_engine.connect() as src_conn:
                if use_bounded_range:
                    df = pd.read_sql(text(query), src_conn, params={"range_start": range_start, "range_end": range_end})
                elif watermark and _self.watermark_column and not _self.source_query:
                    df = pd.read_sql(text(query), src_conn, params={"watermark": watermark})
                else:
                    df = pd.read_sql(text(query), src_conn)

            context.log.info(f"Read {len(df)} rows, {len(df.columns)} columns from source")

            if _self.column_mapping:
                df = df.rename(columns=_self.column_mapping)

            if context.has_partition_key:
                df["_partition_key"] = context.partition_key

            dest_table = _self.destination_table
            if context.has_partition_key:
                dest_table = dest_table.replace("{partition_key}", context.partition_key)

            df.to_sql(dest_table, con=dst_engine, schema=_self.destination_schema,
                      if_exists=_self.if_exists, index=False, method="multi", chunksize=_self.chunksize)

            _preview_metadata = {}
            if _self.include_preview_metadata:
                _prev_df = df.sample(min(_self.preview_rows, len(df))) if len(df) > _self.preview_rows * 10 else df.head(_self.preview_rows)
                _cols = list(_prev_df.columns)
                _preview_metadata["preview"] = dg.MetadataValue.md(
                    "| " + " | ".join(_cols) + " |\n"
                    "| " + " | ".join(["---"] * len(_cols)) + " |\n" +
                    "\n".join("| " + " | ".join(str(v) for v in row) + " |" for row in _prev_df.itertuples(index=False))
                )


            context.log.info(f"Wrote {len(df)} rows to {_self.destination_schema + '.' if _self.destination_schema else ''}{dest_table}")
            return dg.MaterializeResult(metadata={**_preview_metadata, 
                "num_rows": len(df),
                "num_columns": len(df.columns),
                "columns": list(df.columns),
                "source": source_desc,
                "table": f"{_self.destination_schema + '.' if _self.destination_schema else ''}{dest_table}",
                **({"partition_window_start": range_start, "partition_window_end": range_end} if use_bounded_range else {}),
                **({"watermark": watermark} if watermark and not use_bounded_range else {}),
            })

        return dg.Definitions(assets=[sql_to_database_asset])
