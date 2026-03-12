"""SQL to Database Asset Component.

Reads rows from a source database table/query and writes them to a destination
database table via SQLAlchemy. Designed to be triggered by sql_monitor or a schedule.

Supports any SQLAlchemy-compatible source and destination (Postgres, MySQL, MSSQL,
SQLite, Snowflake, BigQuery, Redshift, DuckDB, etc.).
"""
from typing import Optional
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

        @dg.asset(
            name=_self.asset_name,
            description=_self.description or f"SQL:{source_desc} → {_self.destination_table}",
            group_name=_self.group_name,
            kinds={"sql"},
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
