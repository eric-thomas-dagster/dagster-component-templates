"""SQS Observation Sensor Component."""
from typing import Optional
import dagster as dg
from dagster import AssetKey, AssetObservation, SensorEvaluationContext, SensorResult, sensor
from pydantic import Field

class SqsObservationSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    sensor_name: str = Field(description="Unique sensor name")
    asset_key: str = Field(description="Asset key of the ExternalSqsAsset to observe")
    queue_url: str = Field(description="Full SQS queue URL")
    region_name: Optional[str] = Field(default=None, description="AWS region")
    check_interval_seconds: int = Field(default=60, description="Seconds between health checks")
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
        def _sqs_obs(context: SensorEvaluationContext):
            try:
                import boto3
            except ImportError:
                return SensorResult(skip_reason="boto3 not installed")

            try:
                if resource_key:
                    sqs = getattr(context.resources, resource_key).get_client()
                else:
                    sqs = boto3.client("sqs", region_name=_self.region_name)
            except Exception as e:
                return SensorResult(skip_reason=f"Connect failed: {e}")

            try:
                attrs = sqs.get_queue_attributes(
                    QueueUrl=_self.queue_url,
                    AttributeNames=["ApproximateNumberOfMessages",
                                    "ApproximateNumberOfMessagesNotVisible",
                                    "ApproximateNumberOfMessagesDelayed",
                                    "CreatedTimestamp"]
                )["Attributes"]
            except Exception as e:
                return SensorResult(skip_reason=f"GetQueueAttributes failed: {e}")

            metadata = {
                "approximate_messages": int(attrs.get("ApproximateNumberOfMessages", 0)),
                "messages_in_flight": int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0)),
                "messages_delayed": int(attrs.get("ApproximateNumberOfMessagesDelayed", 0)),
                "queue_url": _self.queue_url,
            }
            return SensorResult(asset_events=[AssetObservation(
                asset_key=AssetKey(_self.asset_key.split("/")), metadata=metadata)])

        return dg.Definitions(sensors=[_sqs_obs])
