"""MQTT to Database Asset Component.

Subscribes to an MQTT topic, collects messages for a configured duration, and
writes them to a database table via SQLAlchemy. Designed to be triggered by
mqtt_monitor or run on a schedule.

Each message payload is expected to be JSON.
"""
from typing import Optional
import dagster as dg
from dagster import AssetExecutionContext, Config
from pydantic import Field


class MQTTToDatabaseAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Subscribe to an MQTT topic and write messages to a database table.

    Connects, subscribes, collects messages for collect_seconds, then disconnects.
    Triggered by mqtt_monitor or run on a schedule.

    Example:
        ```yaml
        type: dagster_component_templates.MQTTToDatabaseAssetComponent
        attributes:
          asset_name: mqtt_sensors_ingest
          broker_host_env_var: MQTT_BROKER_HOST
          topic: sensors/#
          database_url_env_var: DATABASE_URL
          table_name: raw_sensor_readings
          collect_seconds: 30
        ```
    """

    asset_name: str = Field(description="Dagster asset name")
    broker_host_env_var: str = Field(description="Env var with MQTT broker hostname")
    broker_port: int = Field(default=1883, description="MQTT broker port (1883 or 8883 for TLS)")
    topic: str = Field(description="MQTT topic to subscribe to (supports wildcards # and +)")
    username_env_var: Optional[str] = Field(default=None, description="Env var with MQTT username")
    password_env_var: Optional[str] = Field(default=None, description="Env var with MQTT password")
    use_tls: bool = Field(default=False, description="Enable TLS/SSL connection")
    database_url_env_var: str = Field(description="Env var with SQLAlchemy database URL")
    table_name: str = Field(description="Destination table name")
    schema_name: Optional[str] = Field(default=None, description="Destination schema name")
    if_exists: str = Field(default="append", description="fail, replace, or append")
    collect_seconds: float = Field(default=30.0, description="Seconds to collect messages before writing")
    max_messages: int = Field(default=10000, description="Max messages to collect per run")
    qos: int = Field(default=1, description="MQTT QoS level (0, 1, or 2)")
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

        class MQTTRunConfig(Config):
            collect_seconds: Optional[float] = None  # override at runtime
            max_messages: Optional[int] = None        # override at runtime

        @dg.asset(
            name=_self.asset_name,
            description=_self.description or f"MQTT:{_self.topic} → {_self.table_name}",
            group_name=_self.group_name,
            kinds={"mqtt", "sql"},
            deps=[dg.AssetKey.from_user_string(k) for k in (_self.deps or [])],
            partitions_def=partitions_def,
        )
        def mqtt_to_database_asset(context: AssetExecutionContext, config: MQTTRunConfig):
            import os, json, time
            import paho.mqtt.client as mqtt
            import pandas as pd
            from sqlalchemy import create_engine

            broker_host = os.environ[_self.broker_host_env_var]
            db_url = os.environ[_self.database_url_env_var]
            collect_secs = config.collect_seconds or _self.collect_seconds
            max_msgs = config.max_messages or _self.max_messages

            records = []
            done = False

            def on_connect(client, userdata, flags, rc):
                if rc == 0:
                    client.subscribe(_self.topic, qos=_self.qos)
                    context.log.info(f"Connected to {broker_host}, subscribed to {_self.topic}")
                else:
                    raise Exception(f"MQTT connect failed with code {rc}")

            def on_message(client, userdata, msg):
                if len(records) >= max_msgs:
                    return
                try:
                    payload = json.loads(msg.payload.decode("utf-8"))
                    record = payload if isinstance(payload, dict) else {"payload": payload}
                    record["_topic"] = msg.topic
                    records.append(record)
                except Exception as e:
                    context.log.warning(f"Skipping unparseable message on {msg.topic}: {e}")

            client = mqtt.Client()
            client.on_connect = on_connect
            client.on_message = on_message

            if _self.use_tls:
                client.tls_set()
            if _self.username_env_var:
                username = os.environ[_self.username_env_var]
                password = os.environ.get(_self.password_env_var or "", "") if _self.password_env_var else None
                client.username_pw_set(username, password)

            context.log.info(f"Collecting messages for {collect_secs}s from {broker_host} (max {max_msgs})")
            client.connect(broker_host, _self.broker_port, keepalive=60)
            client.loop_start()
            time.sleep(collect_secs)
            client.loop_stop()
            client.disconnect()

            if not records:
                context.log.info("No messages received.")
                return dg.MaterializeResult(metadata={"num_rows": 0, "topic": _self.topic})

            df = pd.DataFrame(records)
            context.log.info(f"Collected {len(records)} messages → {len(df)} rows, {len(df.columns)} columns")

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
                "topic": _self.topic,
                "collect_seconds": collect_secs,
                "table": f"{_self.schema_name + '.' if _self.schema_name else ''}{table_name}",
            })

        return dg.Definitions(assets=[mqtt_to_database_asset])
