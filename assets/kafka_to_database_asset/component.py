"""Kafka to Database Asset Component.

Consumes a batch of messages from a Kafka topic and writes them to a database
table via SQLAlchemy. Designed to be triggered by kafka_monitor.

Each message body is expected to be JSON. The batch size is configurable.
"""
from typing import Optional
import dagster as dg
from dagster import AssetExecutionContext, Config
from pydantic import Field


class KafkaToDatabaseAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Consume messages from a Kafka topic and write them to a database table.

    Triggered by kafka_monitor passing topic/partition/offset via run_config,
    or run on a schedule to drain a topic batch.

    Example:
        ```yaml
        type: dagster_component_templates.KafkaToDatabaseAssetComponent
        attributes:
          asset_name: kafka_events_ingest
          bootstrap_servers_env_var: KAFKA_BOOTSTRAP_SERVERS
          database_url_env_var: DATABASE_URL
          topic: events
          table_name: raw_events
          max_messages: 10000
        ```
    """

    asset_name: str = Field(description="Dagster asset name")
    bootstrap_servers_env_var: str = Field(description="Env var with Kafka bootstrap servers (comma-separated)")
    database_url_env_var: str = Field(description="Env var with SQLAlchemy database URL")
    topic: str = Field(description="Kafka topic to consume from")
    consumer_group: str = Field(default="dagster-ingestion", description="Kafka consumer group ID")
    table_name: str = Field(description="Destination table name")
    schema_name: Optional[str] = Field(default=None, description="Destination schema name")
    if_exists: str = Field(default="append", description="fail, replace, or append")
    max_messages: int = Field(default=10000, description="Max messages to consume per run")
    poll_timeout_seconds: float = Field(default=5.0, description="Seconds to wait for messages before stopping")
    security_protocol: str = Field(default="PLAINTEXT", description="PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL")
    sasl_mechanism: Optional[str] = Field(default=None, description="PLAIN, SCRAM-SHA-256, SCRAM-SHA-512")
    sasl_username_env_var: Optional[str] = Field(default=None, description="Env var with SASL username")
    sasl_password_env_var: Optional[str] = Field(default=None, description="Env var with SASL password")
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

        class KafkaRunConfig(Config):
            topic: Optional[str] = None          # override topic at runtime
            max_messages: Optional[int] = None   # override max_messages at runtime
            partition: Optional[int] = None      # consume specific partition (from sensor)
            offset: Optional[int] = None         # start from specific offset (from sensor)

        @dg.asset(
            name=_self.asset_name,
            description=_self.description or f"Kafka:{_self.topic} → {_self.table_name}",
            group_name=_self.group_name,
            kinds={"kafka", "sql"},
            deps=[dg.AssetKey.from_user_string(k) for k in (_self.deps or [])],
            partitions_def=partitions_def,
        )
        def kafka_to_database_asset(context: AssetExecutionContext, config: KafkaRunConfig):
            import os, json
            import pandas as pd
            from confluent_kafka import Consumer, KafkaError, TopicPartition
            from sqlalchemy import create_engine

            bootstrap = os.environ[_self.bootstrap_servers_env_var]
            db_url = os.environ[_self.database_url_env_var]
            topic = config.topic or _self.topic
            max_msgs = config.max_messages or _self.max_messages

            consumer_config = {
                "bootstrap.servers": bootstrap,
                "group.id": _self.consumer_group,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
                "security.protocol": _self.security_protocol,
            }
            if _self.sasl_mechanism:
                consumer_config["sasl.mechanisms"] = _self.sasl_mechanism
                consumer_config["sasl.username"] = os.environ.get(_self.sasl_username_env_var or "", "")
                consumer_config["sasl.password"] = os.environ.get(_self.sasl_password_env_var or "", "")

            consumer = Consumer(consumer_config)

            if config.partition is not None and config.offset is not None:
                consumer.assign([TopicPartition(topic, config.partition, config.offset)])
            else:
                consumer.subscribe([topic])

            context.log.info(f"Consuming up to {max_msgs} messages from {topic}")

            records = []
            consumed = 0
            empty_polls = 0

            while consumed < max_msgs and empty_polls < 3:
                msg = consumer.poll(timeout=_self.poll_timeout_seconds)
                if msg is None:
                    empty_polls += 1
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        break
                    raise Exception(f"Kafka error: {msg.error()}")
                empty_polls = 0
                try:
                    record = json.loads(msg.value().decode("utf-8"))
                    if isinstance(record, dict):
                        records.append(record)
                    elif isinstance(record, list):
                        records.extend(record)
                except Exception as e:
                    context.log.warning(f"Skipping unparseable message: {e}")
                consumed += 1

            consumer.commit()
            consumer.close()

            if not records:
                context.log.info("No messages consumed.")
                return dg.MaterializeResult(metadata={"num_rows": 0, "topic": topic})

            df = pd.DataFrame(records)
            context.log.info(f"Consumed {len(records)} messages → {len(df)} rows, {len(df.columns)} columns")

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
                "topic": topic,
                "messages_consumed": consumed,
                "table": f"{_self.schema_name + '.' if _self.schema_name else ''}{table_name}",
            })

        return dg.Definitions(assets=[kafka_to_database_asset])
