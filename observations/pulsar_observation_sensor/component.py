"""Pulsar Observation Sensor Component."""
from typing import Optional
import dagster as dg
from dagster import AssetKey, AssetObservation, SensorEvaluationContext, SensorResult, sensor
from pydantic import Field

class PulsarObservationSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    sensor_name: str = Field(description="Unique sensor name")
    asset_key: str = Field(description="Asset key of the ExternalPulsarAsset to observe")
    service_url: str = Field(description="Pulsar service URL")
    topic: str = Field(description="Pulsar topic name")
    admin_url: Optional[str] = Field(default=None, description="Pulsar admin URL (default: HTTP port of service_url)")
    jwt_token_env_var: Optional[str] = Field(default=None, description="Env var with JWT auth token")
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
        def _pulsar_obs(context: SensorEvaluationContext):
            import os, urllib.request, json as _json
            # Use Pulsar admin REST API to get topic stats
            admin_url = _self.admin_url
            if not admin_url:
                # Derive admin URL from service URL
                admin_url = _self.service_url.replace("pulsar://", "http://").replace("pulsar+ssl://", "https://")
                # Replace broker port 6650 with admin port 8080
                admin_url = admin_url.replace(":6650", ":8080")

            # Build topic REST path: persistent/public/default/my-topic
            topic = _self.topic
            if topic.startswith("persistent://") or topic.startswith("non-persistent://"):
                # persistent://public/default/my-topic -> admin/v2/persistent/public/default/my-topic/stats
                scheme_end = topic.index("://") + 3
                rest_path = topic[topic.index("://") - len(topic.split("://")[0]):].lstrip("://")
                # simpler: strip scheme
                parts = topic.replace("persistent://", "").replace("non-persistent://", "")
                kind = "persistent" if "persistent://" in topic else "non-persistent"
                stats_url = f"{admin_url}/admin/v2/{kind}/{parts}/stats"
            else:
                stats_url = f"{admin_url}/admin/v2/persistent/public/default/{topic}/stats"

            headers = {}
            if _self.jwt_token_env_var:
                token = os.environ.get(_self.jwt_token_env_var, "")
                if token:
                    headers["Authorization"] = f"Bearer {token}"

            metadata = {"topic": _self.topic, "service_url": _self.service_url}
            try:
                req = urllib.request.Request(stats_url, headers=headers)
                with urllib.request.urlopen(req, timeout=10) as resp:
                    stats = _json.loads(resp.read())
                metadata["producer_count"] = stats.get("producersCount", 0)
                metadata["subscription_count"] = stats.get("subscriptionsCount", 0)
                metadata["msg_rate_in"] = stats.get("msgRateIn", 0.0)
                metadata["storage_size_bytes"] = stats.get("storageSize", 0)
            except Exception as e:
                context.log.warning(f"Could not fetch Pulsar stats: {e}")

            return SensorResult(asset_events=[AssetObservation(
                asset_key=AssetKey(_self.asset_key.split("/")), metadata=metadata)])

        return dg.Definitions(sensors=[_pulsar_obs])
