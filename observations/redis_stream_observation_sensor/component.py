"""Redis Streams Observation Sensor Component."""
from typing import Optional
import dagster as dg
from dagster import AssetKey, AssetObservation, SensorEvaluationContext, SensorResult, sensor
from pydantic import Field

class RedisStreamObservationSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    sensor_name: str = Field(description="Unique sensor name")
    asset_key: str = Field(description="Asset key of the ExternalRedisStreamAsset to observe")
    stream_name: str = Field(description="Redis stream name")
    host: str = Field(default="localhost", description="Redis host")
    port: int = Field(default=6379, description="Redis port")
    db: int = Field(default=0, description="Redis database index")
    password_env_var: Optional[str] = Field(default=None, description="Env var with Redis password")
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
        def _redis_obs(context: SensorEvaluationContext):
            try:
                import redis as redis_lib
            except ImportError:
                return SensorResult(skip_reason="redis not installed")

            import os
            password = os.environ.get(_self.password_env_var) if _self.password_env_var else None

            try:
                if resource_key:
                    r = getattr(context.resources, resource_key)
                else:
                    r = redis_lib.Redis(host=_self.host, port=_self.port, db=_self.db,
                                        password=password, decode_responses=True)
            except Exception as e:
                return SensorResult(skip_reason=f"Connect failed: {e}")

            try:
                stream_len = r.xlen(_self.stream_name)
                groups_info = r.xinfo_groups(_self.stream_name)
                groups_count = len(groups_info)
                total_pending = sum(g.get("pending", 0) for g in groups_info)
            except Exception as e:
                return SensorResult(skip_reason=f"XLEN/XINFO failed: {e}")

            metadata = {
                "stream_length": stream_len,
                "consumer_groups_count": groups_count,
                "total_pending_messages": total_pending,
                "stream_name": _self.stream_name,
                "host": _self.host,
            }
            return SensorResult(asset_events=[AssetObservation(
                asset_key=AssetKey(_self.asset_key.split("/")), metadata=metadata)])

        return dg.Definitions(sensors=[_redis_obs])
