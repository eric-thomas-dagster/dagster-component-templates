"""Pub/Sub Observation Sensor Component."""
from typing import Optional
import dagster as dg
import json
from dagster import AssetKey, AssetObservation, SensorEvaluationContext, SensorResult, sensor
from dagster._core.definitions.data_version import DATA_VERSION_TAG
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
            asset_selection=dg.AssetSelection.keys(
                dg.AssetKey.from_user_string(_self.asset_key)
            ),
        )
        def _pubsub_obs(context: SensorEvaluationContext, **_resources):
            # ── Resource-backed path (v0.10.46) ─────────────────────────────
            if resource_key:
                _rk_client = getattr(context.resources, resource_key, None)
                if _rk_client is None:
                    return SensorResult(skip_reason=f"resource '{resource_key}' not found on context")
                try:
                    _rk_observed = dict(_rk_client.observe(f'projects/{_self.project_id}/topics/{_self.topic_id}'))
                except Exception as _rk_e:
                    context.log.error(f"resource '{resource_key}'.observe failed: {_rk_e}")
                    return SensorResult(skip_reason=f"resource observe failed: {_rk_e}")
                _rk_dv = str(_rk_observed.pop("data_version", ""))
                return SensorResult(asset_events=[AssetObservation(
                    asset_key=AssetKey.from_user_string(_self.asset_key),
                    metadata=_rk_observed,
                    tags={DATA_VERSION_TAG: _rk_dv} if _rk_dv else None,
                )])
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
                asset_key=AssetKey.from_user_string(_self.asset_key),
                metadata=metadata,
                tags={DATA_VERSION_TAG: json.dumps(metadata, sort_keys=True, default=str)},
            )])

        return dg.Definitions(sensors=[_pubsub_obs])
