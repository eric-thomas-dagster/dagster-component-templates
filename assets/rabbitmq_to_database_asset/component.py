"""RabbitMQ to Database Asset Component.

Drains messages from a RabbitMQ queue and writes them to a database table
via SQLAlchemy. Designed to be triggered by rabbitmq_monitor.

Each message body is expected to be JSON. Messages are acked after successful write.
"""
from typing import Optional
import dagster as dg
from dagster import AssetExecutionContext, Config
from pydantic import Field


class RabbitMQToDatabaseAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Drain messages from a RabbitMQ queue and write them to a database table.

    Triggered by rabbitmq_monitor, or run on a schedule to drain a queue batch.
    Messages are acknowledged after successful write.

    Example:
        ```yaml
        type: dagster_component_templates.RabbitMQToDatabaseAssetComponent
        attributes:
          asset_name: rabbitmq_orders_ingest
          amqp_url_env_var: RABBITMQ_URL
          queue_name: orders
          database_url_env_var: DATABASE_URL
          table_name: raw_orders
          max_messages: 10000
        ```
    """

    asset_name: str = Field(description="Dagster asset name")
    amqp_url_env_var: str = Field(description="Env var with AMQP URL (amqp://user:pass@host/vhost)")
    queue_name: str = Field(description="RabbitMQ queue name")
    database_url_env_var: str = Field(description="Env var with SQLAlchemy database URL")
    table_name: str = Field(description="Destination table name")
    schema_name: Optional[str] = Field(default=None, description="Destination schema name")
    if_exists: str = Field(default="append", description="fail, replace, or append")
    max_messages: int = Field(default=10000, description="Max messages to consume per run")
    prefetch_count: int = Field(default=100, description="AMQP prefetch count")
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

        class RabbitMQRunConfig(Config):
            max_messages: Optional[int] = None  # override at runtime

        @dg.asset(
            name=_self.asset_name,
            description=_self.description or f"RabbitMQ:{_self.queue_name} → {_self.table_name}",
            group_name=_self.group_name,
            kinds={"rabbitmq", "sql"},
            partitions_def=partitions_def,
        )
        def rabbitmq_to_database_asset(context: AssetExecutionContext, config: RabbitMQRunConfig):
            import os, json
            import pika
            import pandas as pd
            from sqlalchemy import create_engine

            amqp_url = os.environ[_self.amqp_url_env_var]
            db_url = os.environ[_self.database_url_env_var]
            max_msgs = config.max_messages or _self.max_messages

            params = pika.URLParameters(amqp_url)
            connection = pika.BlockingConnection(params)
            channel = connection.channel()
            channel.basic_qos(prefetch_count=_self.prefetch_count)

            context.log.info(f"Draining up to {max_msgs} messages from {_self.queue_name}")

            records = []
            delivery_tags = []

            while len(records) < max_msgs:
                method, properties, body = channel.basic_get(queue=_self.queue_name, auto_ack=False)
                if method is None:
                    break
                try:
                    parsed = json.loads(body.decode("utf-8"))
                    if isinstance(parsed, dict):
                        records.append(parsed)
                    elif isinstance(parsed, list):
                        records.extend(parsed)
                    delivery_tags.append(method.delivery_tag)
                except Exception as e:
                    context.log.warning(f"Skipping unparseable message: {e}")
                    channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

            if not records:
                connection.close()
                context.log.info("No messages in queue.")
                return dg.MaterializeResult(metadata={"num_rows": 0, "queue": _self.queue_name})

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

            # Ack all messages after successful write
            for tag in delivery_tags:
                channel.basic_ack(delivery_tag=tag)
            connection.close()

            context.log.info(f"Wrote {len(df)} rows to {_self.schema_name + '.' if _self.schema_name else ''}{table_name}")
            return dg.MaterializeResult(metadata={
                "num_rows": len(df),
                "num_columns": len(df.columns),
                "columns": list(df.columns),
                "messages_consumed": len(delivery_tags),
                "table": f"{_self.schema_name + '.' if _self.schema_name else ''}{table_name}",
            })

        return dg.Definitions(assets=[rabbitmq_to_database_asset])
