"""Azure Service Bus to Database Asset Component.

Drains messages from an Azure Service Bus queue or subscription and writes them
to a database table via SQLAlchemy. Designed to be triggered by servicebus_monitor.

Each message body is expected to be JSON. Messages are completed (deleted) after
successful write.
"""
from typing import Optional
import dagster as dg
from dagster import AssetExecutionContext, Config
from pydantic import Field


class ServiceBusToDatabaseAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Drain messages from an Azure Service Bus queue/subscription and write to a database.

    Triggered by servicebus_monitor, or run on a schedule to drain a queue batch.
    Messages are completed after successful write.

    Example:
        ```yaml
        type: dagster_component_templates.ServiceBusToDatabaseAssetComponent
        attributes:
          asset_name: servicebus_orders_ingest
          connection_string_env_var: SERVICEBUS_CONNECTION_STRING
          queue_name: orders-queue
          database_url_env_var: DATABASE_URL
          table_name: raw_orders
          max_messages: 5000
        ```
    """

    asset_name: str = Field(description="Dagster asset name")
    connection_string_env_var: str = Field(description="Env var with Service Bus connection string")
    queue_name: Optional[str] = Field(default=None, description="Queue name (set queue_name OR topic_name+subscription_name)")
    topic_name: Optional[str] = Field(default=None, description="Topic name")
    subscription_name: Optional[str] = Field(default=None, description="Subscription name (required with topic_name)")
    database_url_env_var: str = Field(description="Env var with SQLAlchemy database URL")
    table_name: str = Field(description="Destination table name")
    schema_name: Optional[str] = Field(default=None, description="Destination schema name")
    if_exists: str = Field(default="append", description="fail, replace, or append")
    max_messages: int = Field(default=5000, description="Max messages to consume per run")
    max_wait_seconds: float = Field(default=5.0, description="Max seconds to wait per receive call")
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

        source_desc = _self.queue_name or f"{_self.topic_name}/{_self.subscription_name}"

        class ServiceBusRunConfig(Config):
            max_messages: Optional[int] = None  # override at runtime

        @dg.asset(
            name=_self.asset_name,
            description=_self.description or f"ServiceBus:{source_desc} → {_self.table_name}",
            group_name=_self.group_name,
            kinds={"servicebus", "sql"},
            partitions_def=partitions_def,
        )
        def servicebus_to_database_asset(context: AssetExecutionContext, config: ServiceBusRunConfig):
            import os, json
            import pandas as pd
            from azure.servicebus import ServiceBusClient
            from sqlalchemy import create_engine

            conn_str = os.environ[_self.connection_string_env_var]
            db_url = os.environ[_self.database_url_env_var]
            max_msgs = config.max_messages or _self.max_messages

            records = []
            received_msgs = []

            sb_client = ServiceBusClient.from_connection_string(conn_str)
            with sb_client:
                if _self.queue_name:
                    receiver = sb_client.get_queue_receiver(queue_name=_self.queue_name)
                else:
                    receiver = sb_client.get_subscription_receiver(
                        topic_name=_self.topic_name,
                        subscription_name=_self.subscription_name,
                    )

                context.log.info(f"Draining up to {max_msgs} messages from {source_desc}")

                with receiver:
                    while len(records) < max_msgs:
                        batch = receiver.receive_messages(
                            max_message_count=min(100, max_msgs - len(records)),
                            max_wait_time=_self.max_wait_seconds,
                        )
                        if not batch:
                            break
                        for msg in batch:
                            try:
                                body = json.loads(str(msg))
                                if isinstance(body, dict):
                                    records.append(body)
                                elif isinstance(body, list):
                                    records.extend(body)
                                received_msgs.append(msg)
                            except Exception as e:
                                context.log.warning(f"Skipping unparseable message: {e}")

                    if not records:
                        context.log.info("No messages in queue.")
                        return dg.MaterializeResult(metadata={"num_rows": 0, "source": source_desc})

                    df = pd.DataFrame(records)
                    context.log.info(f"Received {len(records)} messages → {len(df)} rows, {len(df.columns)} columns")

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

                    # Complete (delete) messages after successful write
                    for msg in received_msgs:
                        receiver.complete_message(msg)

                context.log.info(f"Wrote {len(df)} rows to {_self.schema_name + '.' if _self.schema_name else ''}{table_name}")
                return dg.MaterializeResult(metadata={
                    "num_rows": len(df),
                    "num_columns": len(df.columns),
                    "columns": list(df.columns),
                    "messages_consumed": len(received_msgs),
                    "table": f"{_self.schema_name + '.' if _self.schema_name else ''}{table_name}",
                })

        return dg.Definitions(assets=[servicebus_to_database_asset])
