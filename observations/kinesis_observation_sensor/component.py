"""Kinesis Observation Sensor Component."""
from typing import Optional
import dagster as dg
import json
from dagster import AssetKey, AssetObservation, SensorEvaluationContext, SensorResult, sensor
from dagster._core.definitions.data_version import DATA_VERSION_TAG
from pydantic import Field

class KinesisObservationSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    sensor_name: str = Field(description="Unique sensor name")
    asset_key: str = Field(description="Asset key of the ExternalKinesisAsset to observe")
    stream_name: str = Field(description="Kinesis stream name")
    region_name: Optional[str] = Field(default=None, description="AWS region")
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
            asset_selection=dg.AssetSelection.keys(
                dg.AssetKey.from_user_string(_self.asset_key)
            ),
        )
        def _kinesis_obs(context: SensorEvaluationContext, **_resources):
            try:
                import boto3
            except ImportError:
                return SensorResult(skip_reason="boto3 not installed")

            try:
                if resource_key:
                    kinesis = getattr(context.resources, resource_key).get_client()
                else:
                    kinesis = boto3.client("kinesis", region_name=_self.region_name)
            except Exception as e:
                return SensorResult(skip_reason=f"Connect failed: {e}")

            try:
                desc = kinesis.describe_stream_summary(StreamName=_self.stream_name)
                summary = desc["StreamDescriptionSummary"]
                shard_count = summary.get("OpenShardCount", 0)
                retention = summary.get("RetentionPeriodHours", 24)
                enhanced = len(summary.get("EnhancedMonitoring", []))
            except Exception as e:
                return SensorResult(skip_reason=f"DescribeStream failed: {e}")

            metadata = {
                "open_shard_count": shard_count,
                "retention_period_hours": retention,
                "stream_name": _self.stream_name,
            }
            return SensorResult(asset_events=[AssetObservation(
                asset_key=AssetKey.from_user_string(_self.asset_key),
                metadata=metadata,
                tags={DATA_VERSION_TAG: json.dumps(metadata, sort_keys=True, default=str)},
            )])

        return dg.Definitions(sensors=[_kinesis_obs])
