"""Observation sensor — S3ObservationSensorComponent."""
from typing import Optional
import dagster as dg
from dagster import AssetKey, AssetObservation, SensorEvaluationContext, SensorResult, sensor
from dagster._core.definitions.sensor_definition import DefaultSensorStatus
from pydantic import Field

class S3ObservationSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    sensor_name: str = Field(description="Unique sensor name")
    asset_key: str = Field(description="Asset key of the external asset to observe")
    bucket_name: str = Field(description="S3 bucket name")
    prefix: str = Field(default="", description="Key prefix")
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
            monitored_assets=dg.AssetSelection.keys(
                dg.AssetKey(_self.asset_key.split("/"))
            ),
        )
        def _s3_obs(context: SensorEvaluationContext):
            try:
                import boto3
            except ImportError:
                return SensorResult(skip_reason="boto3 not installed")
            try:
                if resource_key:
                    s3 = getattr(context.resources, resource_key).get_client()
                else:
                    s3 = boto3.client("s3", region_name=_self.region_name)
            except Exception as e:
                return SensorResult(skip_reason=f"Connect failed: {e}")
            count=0; total=0; latest=None
            try:
                for page in s3.get_paginator("list_objects_v2").paginate(Bucket=_self.bucket_name, Prefix=_self.prefix):
                    for o in page.get("Contents", []):
                        count+=1; total+=o.get("Size",0)
                        lm=o.get("LastModified")
                        if lm and (latest is None or lm>latest): latest=lm
            except Exception as e:
                return SensorResult(skip_reason=f"List failed: {e}")
            metadata={"object_count":count,"total_size_bytes":total,"latest_modified_iso":latest.isoformat() if latest else "","bucket":_self.bucket_name,"prefix":_self.prefix}
            return SensorResult(asset_events=[AssetObservation(
                asset_key=AssetKey(_self.asset_key.split("/")), metadata=metadata)])

        return dg.Definitions(sensors=[_s3_obs])
