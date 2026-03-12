"""GCS Observation Sensor Component."""
from typing import Optional
import dagster as dg
from dagster import AssetKey, AssetObservation, SensorEvaluationContext, SensorResult, sensor
from pydantic import Field

class GcsObservationSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    sensor_name: str = Field(description="Unique sensor name")
    asset_key: str = Field(description="Asset key of the ExternalGcsAsset to observe")
    bucket_name: str = Field(description="GCS bucket name")
    prefix: str = Field(default="", description="Object prefix")
    project: Optional[str] = Field(default=None, description="GCP project ID")
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
        def _gcs_obs(context: SensorEvaluationContext):
            try:
                from google.cloud import storage
            except ImportError:
                return SensorResult(skip_reason="google-cloud-storage not installed")

            try:
                if resource_key:
                    gcs = getattr(context.resources, resource_key)
                else:
                    gcs = storage.Client(project=_self.project)
                bucket = gcs.bucket(_self.bucket_name)
            except Exception as e:
                return SensorResult(skip_reason=f"Connect failed: {e}")

            count = 0; total = 0; latest = None
            try:
                for blob in gcs.list_blobs(_self.bucket_name, prefix=_self.prefix or None):
                    if blob.name.endswith("/"):
                        continue
                    count += 1
                    total += blob.size or 0
                    lm = blob.updated
                    if lm and (latest is None or lm > latest):
                        latest = lm
            except Exception as e:
                return SensorResult(skip_reason=f"List failed: {e}")

            metadata = {
                "object_count": count,
                "total_size_bytes": total,
                "latest_modified_iso": latest.isoformat() if latest else "",
                "bucket_name": _self.bucket_name,
                "prefix": _self.prefix,
            }
            return SensorResult(asset_events=[AssetObservation(
                asset_key=AssetKey(_self.asset_key.split("/")), metadata=metadata)])

        return dg.Definitions(sensors=[_gcs_obs])
