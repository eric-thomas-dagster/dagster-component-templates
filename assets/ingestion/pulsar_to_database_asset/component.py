"""Apache Pulsar to Database Asset Component.

Consumes a batch of messages from an Apache Pulsar topic and writes them to a
database table via SQLAlchemy. Designed to be triggered by pulsar_monitor.

Each message payload is expected to be JSON. Messages are acknowledged after write.
"""
from typing import Optional
import dagster as dg
from dagster import AssetExecutionContext, Config
from pydantic import Field


class PulsarToDatabaseAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Consume messages from an Apache Pulsar topic and write to a database table.

    Triggered by pulsar_monitor, or run on a schedule to drain a topic batch.
    Uses a shared subscription for durable consumption across runs.

    Example:
        ```yaml
        type: dagster_component_templates.PulsarToDatabaseAssetComponent
        attributes:
          asset_name: pulsar_events_ingest
          service_url_env_var: PULSAR_SERVICE_URL
          topic: persistent://public/default/events
          database_url_env_var: DATABASE_URL
          table_name: raw_events
          max_messages: 10000
        ```
    """

    asset_name: str = Field(description="Dagster asset name")
    service_url_env_var: str = Field(description="Env var with Pulsar service URL (pulsar://host:6650)")
    topic: str = Field(description="Pulsar topic (e.g. persistent://public/default/events)")
    subscription_name: str = Field(default="dagster-ingest", description="Pulsar subscription name")
    subscription_type: str = Field(default="Shared", description="Exclusive, Shared, Failover, or KeyShared")
    token_env_var: Optional[str] = Field(default=None, description="Env var with Pulsar JWT auth token")
    database_url_env_var: str = Field(description="Env var with SQLAlchemy database URL")
    table_name: str = Field(description="Destination table name")
    schema_name: Optional[str] = Field(default=None, description="Destination schema name")
    if_exists: str = Field(default="append", description="fail, replace, or append")
    max_messages: int = Field(default=10000, description="Max messages to consume per run")
    receive_timeout_ms: int = Field(default=5000, description="Timeout in ms for each receive call")
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

        class PulsarRunConfig(Config):
            max_messages: Optional[int] = None  # override at runtime

        @dg.asset(
            name=_self.asset_name,
            description=_self.description or f"Pulsar:{_self.topic} → {_self.table_name}",
            group_name=_self.group_name,
            kinds={"pulsar", "sql"},
            deps=[dg.AssetKey.from_user_string(k) for k in (_self.deps or [])],
            partitions_def=partitions_def,
        )
        def pulsar_to_database_asset(context: AssetExecutionContext, config: PulsarRunConfig):
            import os, json
            import pulsar
            from pulsar import ConsumerType
            import pandas as pd
            from sqlalchemy import create_engine

            service_url = os.environ[_self.service_url_env_var]
            db_url = os.environ[_self.database_url_env_var]
            max_msgs = config.max_messages or _self.max_messages

            client_kwargs: dict = {}
            if _self.token_env_var:
                client_kwargs["authentication"] = pulsar.AuthenticationToken(os.environ[_self.token_env_var])

            subscription_type_map = {
                "Exclusive": ConsumerType.Exclusive,
                "Shared": ConsumerType.Shared,
                "Failover": ConsumerType.Failover,
                "KeyShared": ConsumerType.KeyShared,
            }

            client = pulsar.Client(service_url, **client_kwargs)
            consumer = client.subscribe(
                _self.topic,
                subscription_name=_self.subscription_name,
                consumer_type=subscription_type_map.get(_self.subscription_type, ConsumerType.Shared),
            )

            context.log.info(f"Consuming up to {max_msgs} messages from {_self.topic}")

            records = []
            msg_ids = []

            while len(records) < max_msgs:
                try:
                    msg = consumer.receive(timeout_millis=_self.receive_timeout_ms)
                    try:
                        parsed = json.loads(msg.data().decode("utf-8"))
                        if isinstance(parsed, dict):
                            records.append(parsed)
                        elif isinstance(parsed, list):
                            records.extend(parsed)
                        msg_ids.append(msg.message_id())
                    except Exception as e:
                        context.log.warning(f"Skipping unparseable message: {e}")
                        consumer.negative_acknowledge(msg)
                except Exception:
                    # Timeout — no more messages
                    break

            consumer.close()
            client.close()

            if not records:
                context.log.info("No messages in topic.")
                return dg.MaterializeResult(metadata={"num_rows": 0, "topic": _self.topic})

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

            # Re-open client briefly to acknowledge
            client2 = pulsar.Client(service_url, **client_kwargs)
            consumer2 = client2.subscribe(
                _self.topic,
                subscription_name=_self.subscription_name,
                consumer_type=subscription_type_map.get(_self.subscription_type, ConsumerType.Shared),
            )
            for mid in msg_ids:
                consumer2.acknowledge(mid)
            consumer2.close()
            client2.close()

            context.log.info(f"Wrote {len(df)} rows to {_self.schema_name + '.' if _self.schema_name else ''}{table_name}")
            return dg.MaterializeResult(metadata={
                "num_rows": len(df),
                "num_columns": len(df.columns),
                "columns": list(df.columns),
                "messages_consumed": len(msg_ids),
                "topic": _self.topic,
                "table": f"{_self.schema_name + '.' if _self.schema_name else ''}{table_name}",
            })

        return dg.Definitions(assets=[pulsar_to_database_asset])
