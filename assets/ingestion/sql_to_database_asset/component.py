"""SQL to Database Asset Component.

Reads rows from a source database table/query and writes them to a destination
database table via SQLAlchemy. Designed to be triggered by sql_monitor or a schedule.

Supports any SQLAlchemy-compatible source and destination (Postgres, MySQL, MSSQL,
SQLite, Snowflake, BigQuery, Redshift, DuckDB, etc.).
"""
from typing import Dict, List, Optional
import dagster as dg
from dagster import AssetExecutionContext, Config
from pydantic import Field


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
    source_url_env_var: str = Field(description="Env var with source SQLAlchemy database URL")
    destination_url_env_var: str = Field(description="Env var with destination SQLAlchemy database URL")
    source_table: Optional[str] = Field(default=None, description="Source table name (use source_table OR source_query)")
    source_schema: Optional[str] = Field(default=None, description="Source schema name")
    source_query: Optional[str] = Field(default=None, description="Custom SQL query (overrides source_table)")
    destination_table: str = Field(description="Destination table name")
    destination_schema: Optional[str] = Field(default=None, description="Destination schema name")
    if_exists: str = Field(default="append", description="fail, replace, or append")
    watermark_column: Optional[str] = Field(default=None, description="Incremental watermark column (e.g. updated_at, id)")
    watermark_env_var: Optional[str] = Field(default=None, description="Env var storing the last watermark value")
    chunksize: int = Field(default=10000, description="Rows to read/write per chunk")
    column_mapping: Optional[dict] = Field(default=None, description="Rename columns: {old: new}")
    group_name: Optional[str] = Field(default="ingestion", description="Asset group name")
    description: Optional[str] = Field(default=None)
    partition_type: str = Field(default="none", description="none, daily, weekly, or monthly")
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

    partition_date_column: Optional[str] = Field(
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

    partition_static_column: Optional[str] = Field(
        default=None,
        description="Column used to filter the upstream DataFrame to the current static partition value.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        partitions_def = None
        if _self.partition_type == "daily":
            partitions_def = dg.DailyPartitionsDefinition(start_date=_self.partition_start_date or "2020-01-01")
        elif _self.partition_type == "weekly":
            partitions_def = dg.WeeklyPartitionsDefinition(start_date=_self.partition_start_date or "2020-01-01")
        elif _self.partition_type == "monthly":
            partitions_def = dg.MonthlyPartitionsDefinition(start_date=_self.partition_start_date or "2020-01-01")

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
            name=_self.asset_name,
            description=_self.description or f"SQL:{source_desc} → {_self.destination_table}",
            group_name=_self.group_name,
            kinds={"sql"},
            deps=[dg.AssetKey.from_user_string(k) for k in (_self.deps or [])],
            partitions_def=partitions_def,
        )
        def sql_to_database_asset(context: AssetExecutionContext, config: SQLRunConfig):
            import os
            import pandas as pd
            from sqlalchemy import create_engine, text

            src_url = os.environ[_self.source_url_env_var]
            dst_url = os.environ[_self.destination_url_env_var]

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

            if context.has_partition_key:
                watermark = context.partition_key

            if watermark and _self.watermark_column and not _self.source_query:
                query += f" WHERE {_self.watermark_column} > :watermark"

            context.log.info(f"Reading from source: {source_desc}")
            if watermark and _self.watermark_column:
                context.log.info(f"Watermark: {_self.watermark_column} > {watermark}")

            with src_engine.connect() as src_conn:
                if watermark and _self.watermark_column and not _self.source_query:
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

            context.log.info(f"Wrote {len(df)} rows to {_self.destination_schema + '.' if _self.destination_schema else ''}{dest_table}")
            return dg.MaterializeResult(metadata={
                "num_rows": len(df),
                "num_columns": len(df.columns),
                "columns": list(df.columns),
                "source": source_desc,
                "table": f"{_self.destination_schema + '.' if _self.destination_schema else ''}{dest_table}",
                **({"watermark": watermark} if watermark else {}),
            })

        return dg.Definitions(assets=[sql_to_database_asset])
