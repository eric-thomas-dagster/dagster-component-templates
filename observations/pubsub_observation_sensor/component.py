"""Pub/Sub Observation Sensor Component."""
from typing import Optional
import dagster as dg
from dagster import AssetKey, AssetObservation, SensorEvaluationContext, SensorResult, sensor
from pydantic import Field

class PubsubObservationSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    sensor_name: str = Field(description="Unique sensor name")
    asset_key: str = Field(description="Asset key of the ExternalPubsubAsset to observe")
    project_id: str = Field(description="GCP project ID")
    topic_id: str = Field(description="Pub/Sub topic ID")
    subscription_id: Optional[str] = Field(default=None, description="Subscription ID for lag metrics")
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
        def _pubsub_obs(context: SensorEvaluationContext):
            try:
                from google.cloud import pubsub_v1
            except ImportError:
                return SensorResult(skip_reason="google-cloud-pubsub not installed")

            try:
                if resource_key:
                    publisher = getattr(context.resources, resource_key)
                else:
                    publisher = pubsub_v1.PublisherClient()
            except Exception as e:
                return SensorResult(skip_reason=f"Connect failed: {e}")

            topic_path = f"projects/{_self.project_id}/topics/{_self.topic_id}"
            metadata = {"topic_path": topic_path, "project_id": _self.project_id, "topic_id": _self.topic_id}

            try:
                topic = publisher.get_topic(request={"topic": topic_path})
                metadata["message_retention_duration_seconds"] = (
                    topic.message_retention_duration.seconds if topic.message_retention_duration else 0
                )
            except Exception as e:
                context.log.warning(f"Could not get topic details: {e}")

            if _self.subscription_id:
                try:
                    subscriber = pubsub_v1.SubscriberClient()
                    sub_path = f"projects/{_self.project_id}/subscriptions/{_self.subscription_id}"
                    sub = subscriber.get_subscription(request={"subscription": sub_path})
                    metadata["subscription_id"] = _self.subscription_id
                except Exception as e:
                    context.log.warning(f"Could not get subscription details: {e}")

            return SensorResult(asset_events=[AssetObservation(
                asset_key=AssetKey(_self.asset_key.split("/")), metadata=metadata)])

        return dg.Definitions(sensors=[_pubsub_obs])
