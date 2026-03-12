"""Kafka Observation Sensor Component."""
from typing import Optional
import dagster as dg
from dagster import AssetKey, AssetObservation, SensorEvaluationContext, SensorResult, sensor
from pydantic import Field

class KafkaObservationSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    sensor_name: str = Field(description="Unique sensor name")
    asset_key: str = Field(description="Asset key of the ExternalKafkaAsset to observe")
    bootstrap_servers: str = Field(description="Comma-separated Kafka broker addresses")
    topic: str = Field(description="Kafka topic name")
    check_interval_seconds: int = Field(default=300, description="Seconds between health checks")
    resource_key: Optional[str] = Field(default=None, description="Optional Dagster resource key.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        resource_key = self.resource_key
        required_resource_keys = {resource_key} if resource_key else set()

        @sensor(
            name=_self.sensor_name,
            minimum_interval_seconds=_self.check_interval_seconds,
            required_resource_keys=required_resource_keys,
            monitored_assets=dg.AssetSelection.keys(
                dg.AssetKey(_self.asset_key.split("/"))
            ),
        )
        def _kafka_obs(context: SensorEvaluationContext):
            try:
                from kafka import KafkaAdminClient, KafkaConsumer
                from kafka.admin import NewTopic
            except ImportError:
                return SensorResult(skip_reason="kafka-python not installed")

            try:
                if resource_key:
                    admin = getattr(context.resources, resource_key)
                else:
                    admin = KafkaAdminClient(bootstrap_servers=_self.bootstrap_servers)
            except Exception as e:
                return SensorResult(skip_reason=f"Connect failed: {e}")

            try:
                consumer = KafkaConsumer(bootstrap_servers=_self.bootstrap_servers)
                partitions = consumer.partitions_for_topic(_self.topic) or set()
                from kafka import TopicPartition
                tps = [TopicPartition(_self.topic, p) for p in partitions]
                end_offsets = consumer.end_offsets(tps)
                total_messages = sum(end_offsets.values())
                consumer.close()
            except Exception as e:
                return SensorResult(skip_reason=f"Offset query failed: {e}")
            finally:
                try:
                    admin.close()
                except Exception:
                    pass

            metadata = {
                "partition_count": len(partitions),
                "total_end_offset": total_messages,
                "topic": _self.topic,
                "bootstrap_servers": _self.bootstrap_servers,
            }
            return SensorResult(asset_events=[AssetObservation(
                asset_key=AssetKey(_self.asset_key.split("/")), metadata=metadata)])

        return dg.Definitions(sensors=[_kafka_obs])
