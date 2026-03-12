"""NATS Observation Sensor Component."""
import asyncio
from typing import Optional
import dagster as dg
from dagster import AssetKey, AssetObservation, SensorEvaluationContext, SensorResult, sensor
from pydantic import Field

class NatsObservationSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    sensor_name: str = Field(description="Unique sensor name")
    asset_key: str = Field(description="Asset key of the ExternalNatsAsset to observe")
    servers: str = Field(description="Comma-separated NATS server URLs")
    stream_name: str = Field(description="JetStream stream name")
    credentials_env_var: Optional[str] = Field(default=None, description="Env var with path to NATS .creds file")
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
        def _nats_obs(context: SensorEvaluationContext):
            try:
                import nats
            except ImportError:
                return SensorResult(skip_reason="nats-py not installed")

            import os

            async def _get_stream_info():
                import nats as nats_lib
                servers = [s.strip() for s in _self.servers.split(",")]
                kwargs = {"servers": servers}
                if _self.credentials_env_var:
                    creds_path = os.environ.get(_self.credentials_env_var)
                    if creds_path:
                        kwargs["credentials"] = creds_path
                nc = await nats_lib.connect(**kwargs)
                js = nc.jetstream()
                info = await js.find_stream(_self.stream_name)
                state = info.state
                result = {
                    "messages": state.messages,
                    "bytes": state.bytes,
                    "first_seq": state.first_seq,
                    "last_seq": state.last_seq,
                    "consumer_count": state.consumer_count,
                    "stream_name": _self.stream_name,
                }
                await nc.close()
                return result

            try:
                metadata = asyncio.run(_get_stream_info())
            except Exception as e:
                return SensorResult(skip_reason=f"NATS stream info failed: {e}")

            return SensorResult(asset_events=[AssetObservation(
                asset_key=AssetKey(_self.asset_key.split("/")), metadata=metadata)])

        return dg.Definitions(sensors=[_nats_obs])
