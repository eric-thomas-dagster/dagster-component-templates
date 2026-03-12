"""Amazon Kinesis to Database Asset Component.

Reads records from an Amazon Kinesis Data Stream and writes them to a database
table via SQLAlchemy. Designed to be triggered by kinesis_monitor.

Each record data is expected to be JSON. Uses GetRecords API with shard iteration.
"""
from typing import Optional
import dagster as dg
from dagster import AssetExecutionContext, Config
from pydantic import Field


class KinesisToDatabaseAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Read records from a Kinesis Data Stream and write them to a database table.

    Triggered by kinesis_monitor passing shard_id/sequence_number via run_config,
    or run on a schedule to read all shards.

    Example:
        ```yaml
        type: dagster_component_templates.KinesisToDatabaseAssetComponent
        attributes:
          asset_name: kinesis_events_ingest
          stream_name: my-stream
          database_url_env_var: DATABASE_URL
          table_name: raw_events
          max_records: 10000
          region_name: us-east-1
        ```
    """

    asset_name: str = Field(description="Dagster asset name")
    stream_name: str = Field(description="Kinesis stream name")
    database_url_env_var: str = Field(description="Env var with SQLAlchemy database URL")
    table_name: str = Field(description="Destination table name")
    schema_name: Optional[str] = Field(default=None, description="Destination schema name")
    if_exists: str = Field(default="append", description="fail, replace, or append")
    max_records: int = Field(default=10000, description="Max records to consume per run")
    shard_iterator_type: str = Field(default="TRIM_HORIZON", description="TRIM_HORIZON, LATEST, AT_SEQUENCE_NUMBER, AFTER_SEQUENCE_NUMBER")
    region_name: str = Field(default="us-east-1", description="AWS region")
    aws_access_key_env_var: Optional[str] = Field(default=None, description="Env var with AWS access key ID")
    aws_secret_key_env_var: Optional[str] = Field(default=None, description="Env var with AWS secret access key")
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

        class KinesisRunConfig(Config):
            shard_id: Optional[str] = None            # consume specific shard (from sensor)
            sequence_number: Optional[str] = None      # start from specific sequence
            max_records: Optional[int] = None          # override at runtime

        @dg.asset(
            name=_self.asset_name,
            description=_self.description or f"Kinesis:{_self.stream_name} → {_self.table_name}",
            group_name=_self.group_name,
            kinds={"kinesis", "sql"},
            partitions_def=partitions_def,
        )
        def kinesis_to_database_asset(context: AssetExecutionContext, config: KinesisRunConfig):
            import os, json
            import boto3
            import pandas as pd
            from sqlalchemy import create_engine

            db_url = os.environ[_self.database_url_env_var]
            max_recs = config.max_records or _self.max_records

            boto_kwargs: dict = {"region_name": _self.region_name}
            if _self.aws_access_key_env_var:
                boto_kwargs["aws_access_key_id"] = os.environ[_self.aws_access_key_env_var]
            if _self.aws_secret_key_env_var:
                boto_kwargs["aws_secret_access_key"] = os.environ[_self.aws_secret_key_env_var]
            kinesis = boto3.client("kinesis", **boto_kwargs)

            # Determine shards to read
            if config.shard_id:
                shard_ids = [config.shard_id]
            else:
                stream_desc = kinesis.describe_stream(StreamName=_self.stream_name)
                shard_ids = [s["ShardId"] for s in stream_desc["StreamDescription"]["Shards"]]

            context.log.info(f"Reading up to {max_recs} records from {_self.stream_name} ({len(shard_ids)} shards)")

            records = []

            for shard_id in shard_ids:
                if len(records) >= max_recs:
                    break

                iter_kwargs: dict = {
                    "StreamName": _self.stream_name,
                    "ShardId": shard_id,
                }
                if config.sequence_number and config.shard_id == shard_id:
                    iter_kwargs["ShardIteratorType"] = "AT_SEQUENCE_NUMBER"
                    iter_kwargs["StartingSequenceNumber"] = config.sequence_number
                else:
                    iter_kwargs["ShardIteratorType"] = _self.shard_iterator_type

                shard_iter = kinesis.get_shard_iterator(**iter_kwargs)["ShardIterator"]
                empty_count = 0

                while len(records) < max_recs and empty_count < 3:
                    resp = kinesis.get_records(ShardIterator=shard_iter, Limit=min(1000, max_recs - len(records)))
                    kinesis_records = resp.get("Records", [])
                    shard_iter = resp.get("NextShardIterator")

                    if not kinesis_records:
                        empty_count += 1
                        if not shard_iter:
                            break
                        continue

                    empty_count = 0
                    for rec in kinesis_records:
                        try:
                            parsed = json.loads(rec["Data"].decode("utf-8"))
                            if isinstance(parsed, dict):
                                records.append(parsed)
                            elif isinstance(parsed, list):
                                records.extend(parsed)
                        except Exception as e:
                            context.log.warning(f"Skipping unparseable record: {e}")

                    if not shard_iter:
                        break

            if not records:
                context.log.info("No records in stream.")
                return dg.MaterializeResult(metadata={"num_rows": 0, "stream": _self.stream_name})

            df = pd.DataFrame(records)
            context.log.info(f"Read {len(records)} records → {len(df)} rows, {len(df.columns)} columns")

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

            context.log.info(f"Wrote {len(df)} rows to {_self.schema_name + '.' if _self.schema_name else ''}{table_name}")
            return dg.MaterializeResult(metadata={
                "num_rows": len(df),
                "num_columns": len(df.columns),
                "columns": list(df.columns),
                "records_consumed": len(records),
                "stream": _self.stream_name,
                "shards_read": len(shard_ids),
                "table": f"{_self.schema_name + '.' if _self.schema_name else ''}{table_name}",
            })

        return dg.Definitions(assets=[kinesis_to_database_asset])
