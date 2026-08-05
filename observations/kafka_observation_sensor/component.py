"""Kafka Observation Sensor Component.

Polls a Kafka topic's end-offsets on a schedule and emits an AssetObservation
tagged with a DataVersion. Downstream AutomationCondition.newly_updated() /
.eager() fires when the DataVersion changes — i.e. when the topic has new
messages relative to the prior tick.

Two operating modes:

- resource_key set (declarative / demo-mode / shared client): sensor calls
  `context.resources.<resource_key>.observe(topic)`. The resource returns a
  dict of the form `{"data_version": "...", **metadata}`. This is the
  preferred contract — it lets you plug in a demo-mode observer or a shared
  admin-client wrapper without touching the sensor.

- resource_key unset (real Kafka broker): sensor constructs a KafkaConsumer
  against `bootstrap_servers`, reads per-partition end-offsets, and derives a
  DataVersion from the sorted `{partition: end_offset}` JSON (repartition-safe).
"""
import json
from typing import Any, Optional

import dagster as dg
from dagster import AssetKey, AssetObservation, SensorEvaluationContext, SensorResult, sensor
from dagster._core.definitions.data_version import DATA_VERSION_TAG
from pydantic import Field


class KafkaObservationSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    sensor_name: str = Field(description="Unique sensor name")
    asset_key: str = Field(description="Asset key of the ExternalKafkaAsset to observe")
    bootstrap_servers: str = Field(description="Comma-separated Kafka broker addresses (ignored when resource_key is set)")
    topic: str = Field(description="Kafka topic name")
    check_interval_seconds: int = Field(default=300, description="Seconds between health checks")
    resource_key: Optional[str] = Field(
        default=None,
        description=(
            "Dagster resource key exposing `.observe(topic) -> dict` that returns "
            "`{'data_version': str, **metadata}`. When set, the sensor uses this "
            "instead of connecting to bootstrap_servers directly — enables demo-mode "
            "and offline testing."
        ),
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        resource_key = self.resource_key
        required_resource_keys = {resource_key} if resource_key else set()

        @sensor(
            name=_self.sensor_name,
            minimum_interval_seconds=_self.check_interval_seconds,
            required_resource_keys=required_resource_keys,
            asset_selection=dg.AssetSelection.keys(
                dg.AssetKey.from_user_string(_self.asset_key)
            ),
        )
        def _kafka_obs(context: SensorEvaluationContext, **_resources):
            # ── Resource-backed path (preferred) ────────────────────────────
            if resource_key:
                client = getattr(context.resources, resource_key, None)
                if client is None:
                    return SensorResult(skip_reason=f"resource '{resource_key}' not found on context")
                try:
                    observed: dict[str, Any] = dict(client.observe(_self.topic))
                except Exception as e:
                    context.log.error(f"resource '{resource_key}'.observe failed: {e}")
                    return SensorResult(skip_reason=f"resource observe failed: {e}")
                data_version = str(observed.pop("data_version", ""))
                return SensorResult(asset_events=[AssetObservation(
                    asset_key=AssetKey.from_user_string(_self.asset_key),
                    metadata=observed,
                    tags={DATA_VERSION_TAG: data_version} if data_version else None,
                )])

            # ── Native kafka-python path ────────────────────────────────────
            try:
                from kafka import KafkaConsumer, TopicPartition
            except ImportError:
                return SensorResult(skip_reason="kafka-python not installed")

            try:
                consumer = KafkaConsumer(bootstrap_servers=_self.bootstrap_servers)
                partitions = consumer.partitions_for_topic(_self.topic) or set()
                tps = [TopicPartition(_self.topic, p) for p in partitions]
                end_offsets = consumer.end_offsets(tps) if tps else {}
                total_messages = sum(end_offsets.values())
                consumer.close()
            except Exception as e:
                return SensorResult(skip_reason=f"Offset query failed: {e}")

            # Per-partition offsets → repartition-safe DataVersion.
            offsets_by_partition = {str(tp.partition): int(off) for tp, off in end_offsets.items()}
            data_version = json.dumps(offsets_by_partition, sort_keys=True)

            metadata = {
                "partition_count": len(partitions),
                "total_end_offset": total_messages,
                "topic": _self.topic,
                "bootstrap_servers": _self.bootstrap_servers,
            }
            return SensorResult(asset_events=[AssetObservation(
                asset_key=AssetKey.from_user_string(_self.asset_key),
                metadata=metadata,
                tags={DATA_VERSION_TAG: data_version},
            )])

        return dg.Definitions(sensors=[_kafka_obs])
