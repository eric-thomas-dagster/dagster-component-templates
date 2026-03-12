"""Redis Streams to Database Asset Component.

Reads entries from a Redis Stream and writes them to a database table via SQLAlchemy.
Designed to be triggered by redis_streams_monitor.

Each entry's fields are treated as a flat record. Uses XREAD with count/block.
"""
from typing import Optional
import dagster as dg
from dagster import AssetExecutionContext, Config
from pydantic import Field


class RedisStreamsToDatabaseAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Read entries from a Redis Stream and write them to a database table.

    Triggered by redis_streams_monitor passing last_id via run_config,
    or run on a schedule to read new entries.

    Example:
        ```yaml
        type: dagster_component_templates.RedisStreamsToDatabaseAssetComponent
        attributes:
          asset_name: redis_events_ingest
          redis_url_env_var: REDIS_URL
          stream_name: events
          database_url_env_var: DATABASE_URL
          table_name: raw_events
          max_entries: 10000
        ```
    """

    asset_name: str = Field(description="Dagster asset name")
    redis_url_env_var: str = Field(description="Env var with Redis URL (redis://host:6379/0)")
    stream_name: str = Field(description="Redis stream name (key)")
    consumer_group: Optional[str] = Field(default=None, description="Consumer group name (if using XREADGROUP)")
    consumer_name: Optional[str] = Field(default="dagster-worker", description="Consumer name for XREADGROUP")
    database_url_env_var: str = Field(description="Env var with SQLAlchemy database URL")
    table_name: str = Field(description="Destination table name")
    schema_name: Optional[str] = Field(default=None, description="Destination schema name")
    if_exists: str = Field(default="append", description="fail, replace, or append")
    max_entries: int = Field(default=10000, description="Max stream entries to read per run")
    block_ms: int = Field(default=2000, description="XREAD block timeout in milliseconds (0 = no block)")
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

        class RedisRunConfig(Config):
            last_id: str = "0-0"          # start from this stream ID (from sensor)
            max_entries: Optional[int] = None  # override at runtime

        @dg.asset(
            name=_self.asset_name,
            description=_self.description or f"Redis:{_self.stream_name} → {_self.table_name}",
            group_name=_self.group_name,
            kinds={"redis", "sql"},
            partitions_def=partitions_def,
        )
        def redis_streams_to_database_asset(context: AssetExecutionContext, config: RedisRunConfig):
            import os
            import redis
            import pandas as pd
            from sqlalchemy import create_engine

            redis_url = os.environ[_self.redis_url_env_var]
            db_url = os.environ[_self.database_url_env_var]
            max_ent = config.max_entries or _self.max_entries

            r = redis.from_url(redis_url, decode_responses=True)

            context.log.info(f"Reading up to {max_ent} entries from {_self.stream_name} (from {config.last_id})")

            records = []
            entry_ids = []
            last_id = config.last_id

            while len(records) < max_ent:
                batch_size = min(500, max_ent - len(records))

                if _self.consumer_group:
                    results = r.xreadgroup(
                        groupname=_self.consumer_group,
                        consumername=_self.consumer_name,
                        streams={_self.stream_name: ">"},
                        count=batch_size,
                        block=_self.block_ms,
                    )
                else:
                    results = r.xread(
                        streams={_self.stream_name: last_id},
                        count=batch_size,
                        block=_self.block_ms,
                    )

                if not results:
                    break

                for stream_key, entries in results:
                    for entry_id, fields in entries:
                        records.append({**fields, "_stream_id": entry_id})
                        entry_ids.append(entry_id)
                        last_id = entry_id

            if not records:
                context.log.info("No entries in stream.")
                return dg.MaterializeResult(metadata={"num_rows": 0, "stream": _self.stream_name})

            df = pd.DataFrame(records)
            context.log.info(f"Read {len(records)} entries → {len(df)} rows, {len(df.columns)} columns")

            if _self.column_mapping:
                df = df.rename(columns=_self.column_mapping)

            if context.has_partition_key:
                df["_partition_key"] = context.partition_key

            table_name = _self.table_name
            if context.has_partition_key:
                table_name = table_name.replace("{partition_key}", context.partition_key)

            engine = create_engine(db_url)
            df.to_sql(table_name, con=engine, schema=_self.schema_name,
                      if_exists=_self.if_exists, index=False, method="multi", chunksize=1000)

            # Acknowledge entries if using consumer group
            if _self.consumer_group and entry_ids:
                r.xack(_self.stream_name, _self.consumer_group, *entry_ids)

            context.log.info(f"Wrote {len(df)} rows to {_self.schema_name + '.' if _self.schema_name else ''}{table_name}")
            return dg.MaterializeResult(metadata={
                "num_rows": len(df),
                "num_columns": len(df.columns),
                "columns": list(df.columns),
                "entries_consumed": len(entry_ids),
                "last_id": last_id,
                "stream": _self.stream_name,
                "table": f"{_self.schema_name + '.' if _self.schema_name else ''}{table_name}",
            })

        return dg.Definitions(assets=[redis_streams_to_database_asset])
