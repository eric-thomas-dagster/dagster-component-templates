"""NATS to Database Asset Component.

Subscribes to a NATS subject or JetStream consumer and writes messages to a
database table via SQLAlchemy. Designed to be triggered by nats_monitor.

Each message payload is expected to be JSON.
"""
from typing import Optional
import dagster as dg
from dagster import AssetExecutionContext, Config
from pydantic import Field


class NATSToDatabaseAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Subscribe to a NATS subject/JetStream and write messages to a database table.

    Connects to NATS, fetches a batch of messages, disconnects.
    Triggered by nats_monitor or run on a schedule.

    Example:
        ```yaml
        type: dagster_component_templates.NATSToDatabaseAssetComponent
        attributes:
          asset_name: nats_events_ingest
          nats_url_env_var: NATS_URL
          subject: events.>
          database_url_env_var: DATABASE_URL
          table_name: raw_events
          max_messages: 10000
          use_jetstream: true
          stream_name: EVENTS
          consumer_name: dagster-ingest
        ```
    """

    asset_name: str = Field(description="Dagster asset name")
    nats_url_env_var: str = Field(description="Env var with NATS server URL (nats://host:4222)")
    subject: str = Field(description="NATS subject to subscribe to (supports wildcards)")
    use_jetstream: bool = Field(default=False, description="Use JetStream for durable consumption")
    stream_name: Optional[str] = Field(default=None, description="JetStream stream name (required if use_jetstream=true)")
    consumer_name: Optional[str] = Field(default="dagster-ingest", description="JetStream durable consumer name")
    credentials_env_var: Optional[str] = Field(default=None, description="Env var with path to NATS credentials file")
    database_url_env_var: str = Field(description="Env var with SQLAlchemy database URL")
    table_name: str = Field(description="Destination table name")
    schema_name: Optional[str] = Field(default=None, description="Destination schema name")
    if_exists: str = Field(default="append", description="fail, replace, or append")
    max_messages: int = Field(default=10000, description="Max messages to fetch per run")
    fetch_timeout_seconds: float = Field(default=5.0, description="Seconds to wait for each fetch batch")
    column_mapping: Optional[dict] = Field(default=None, description="Rename columns: {old: new}")
    group_name: Optional[str] = Field(default="ingestion", description="Asset group name")
    description: Optional[str] = Field(default=None)
    partition_type: str = Field(default="none", description="none, daily, weekly, or monthly")
    partition_start_date: Optional[str] = Field(default=None, description="Partition start date YYYY-MM-DD (required if partition_type != none)")
    deps: Optional[list[str]] = Field(default=None, description="Upstream asset keys this asset depends on (e.g. ['raw_orders', 'schema/asset'])")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        partitions_def = None
        if _self.partition_type == "daily":
            partitions_def = dg.DailyPartitionsDefinition(start_date=_self.partition_start_date or "2020-01-01")
        elif _self.partition_type == "weekly":
            partitions_def = dg.WeeklyPartitionsDefinition(start_date=_self.partition_start_date or "2020-01-01")
        elif _self.partition_type == "monthly":
            partitions_def = dg.MonthlyPartitionsDefinition(start_date=_self.partition_start_date or "2020-01-01")

        class NATSRunConfig(Config):
            max_messages: Optional[int] = None  # override at runtime

        @dg.asset(
            name=_self.asset_name,
            description=_self.description or f"NATS:{_self.subject} → {_self.table_name}",
            group_name=_self.group_name,
            kinds={"nats", "sql"},
            deps=[dg.AssetKey.from_user_string(k) for k in (_self.deps or [])],
            partitions_def=partitions_def,
        )
        def nats_to_database_asset(context: AssetExecutionContext, config: NATSRunConfig):
            import os, json, asyncio
            import nats
            import pandas as pd
            from sqlalchemy import create_engine

            nats_url = os.environ[_self.nats_url_env_var]
            db_url = os.environ[_self.database_url_env_var]
            max_msgs = config.max_messages or _self.max_messages

            async def fetch_messages():
                connect_kwargs: dict = {"servers": nats_url}
                if _self.credentials_env_var:
                    connect_kwargs["user_credentials"] = os.environ[_self.credentials_env_var]

                nc = await nats.connect(**connect_kwargs)
                records = []

                try:
                    if _self.use_jetstream:
                        js = nc.jetstream()
                        consumer = await js.subscribe(
                            _self.subject,
                            stream=_self.stream_name,
                            durable=_self.consumer_name,
                        )
                        empty_count = 0
                        while len(records) < max_msgs and empty_count < 3:
                            try:
                                batch = await consumer.fetch(
                                    batch=min(500, max_msgs - len(records)),
                                    timeout=_self.fetch_timeout_seconds,
                                )
                                if not batch:
                                    empty_count += 1
                                    continue
                                empty_count = 0
                                for msg in batch:
                                    try:
                                        parsed = json.loads(msg.data.decode("utf-8"))
                                        if isinstance(parsed, dict):
                                            records.append(parsed)
                                        elif isinstance(parsed, list):
                                            records.extend(parsed)
                                        await msg.ack()
                                    except Exception as e:
                                        context.log.warning(f"Skipping unparseable message: {e}")
                            except nats.errors.TimeoutError:
                                break
                        await consumer.unsubscribe()
                    else:
                        # Core NATS: subscribe and collect for a timeout
                        collected = []
                        sub = await nc.subscribe(_self.subject)
                        try:
                            while len(collected) < max_msgs:
                                try:
                                    msg = await asyncio.wait_for(
                                        sub.next_msg(), timeout=_self.fetch_timeout_seconds
                                    )
                                    collected.append(msg)
                                except asyncio.TimeoutError:
                                    break
                        finally:
                            await sub.unsubscribe()

                        for msg in collected:
                            try:
                                parsed = json.loads(msg.data.decode("utf-8"))
                                if isinstance(parsed, dict):
                                    records.append(parsed)
                                elif isinstance(parsed, list):
                                    records.extend(parsed)
                            except Exception as e:
                                context.log.warning(f"Skipping unparseable message: {e}")
                finally:
                    await nc.close()

                return records

            context.log.info(f"Fetching up to {max_msgs} messages from NATS subject {_self.subject}")
            records = asyncio.run(fetch_messages())

            if not records:
                context.log.info("No messages received.")
                return dg.MaterializeResult(metadata={"num_rows": 0, "subject": _self.subject})

            df = pd.DataFrame(records)
            context.log.info(f"Fetched {len(records)} messages → {len(df)} rows, {len(df.columns)} columns")

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
                "messages_consumed": len(records),
                "subject": _self.subject,
                "table": f"{_self.schema_name + '.' if _self.schema_name else ''}{table_name}",
            })

        return dg.Definitions(assets=[nats_to_database_asset])
