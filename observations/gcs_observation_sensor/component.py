"""GCS Observation Sensor Component.

Polls a GCS bucket/prefix on a schedule and emits an AssetObservation tagged
with a DataVersion. Downstream AutomationCondition.newly_updated() / .eager()
fires when the DataVersion changes.

Two operating modes:

- resource_key set: `.observe(source)` on the resource where source is
  `"bucket/prefix"`. Returns `{"data_version": str, **metadata}`.
- resource_key unset: uses google-cloud-storage, counts blobs, tracks latest
  update time, derives DataVersion from `f"{object_count}-{latest_iso}"`.
"""
from typing import Any, Optional

import dagster as dg
from dagster import AssetKey, AssetObservation, SensorEvaluationContext, SensorResult, sensor
from dagster._core.definitions.data_version import DATA_VERSION_TAG
from pydantic import Field


class GcsObservationSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    sensor_name: str = Field(description="Unique sensor name")
    asset_key: str = Field(description="Asset key of the ExternalGcsAsset to observe")
    bucket_name: str = Field(description="GCS bucket name")
    prefix: str = Field(default="", description="Object prefix")
    project: Optional[str] = Field(default=None, description="GCP project ID")
    check_interval_seconds: int = Field(default=300, description="Seconds between health checks")
    resource_key: Optional[str] = Field(
        default=None,
        description=(
            "Dagster resource key exposing `.observe(source) -> dict` (source is "
            "'bucket/prefix') that returns `{'data_version': str, **metadata}`."
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
        def _gcs_obs(context: SensorEvaluationContext, **_resources):
            # ── Resource-backed path ────────────────────────────────────────
            if resource_key:
                client = getattr(context.resources, resource_key, None)
                if client is None:
                    return SensorResult(skip_reason=f"resource '{resource_key}' not found on context")
                try:
                    source = f"{_self.bucket_name}/{_self.prefix}" if _self.prefix else _self.bucket_name
                    observed: dict[str, Any] = dict(client.observe(source))
                except Exception as e:
                    context.log.error(f"resource '{resource_key}'.observe failed: {e}")
                    return SensorResult(skip_reason=f"resource observe failed: {e}")
                data_version = str(observed.pop("data_version", ""))
                return SensorResult(asset_events=[AssetObservation(
                    asset_key=AssetKey.from_user_string(_self.asset_key),
                    metadata=observed,
                    tags={DATA_VERSION_TAG: data_version} if data_version else None,
                )])

            # ── Native google-cloud-storage path ────────────────────────────
            try:
                from google.cloud import storage
            except ImportError:
                return SensorResult(skip_reason="google-cloud-storage not installed")

            try:
                gcs = storage.Client(project=_self.project)
            except Exception as e:
                return SensorResult(skip_reason=f"Connect failed: {e}")

            count = 0
            total = 0
            latest = None
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

            latest_iso = latest.isoformat() if latest else ""
            data_version = f"{count}-{latest_iso}"
            metadata = {
                "object_count": count,
                "total_size_bytes": total,
                "latest_modified_iso": latest_iso,
                "bucket_name": _self.bucket_name,
                "prefix": _self.prefix,
            }
            return SensorResult(asset_events=[AssetObservation(
                asset_key=AssetKey.from_user_string(_self.asset_key),
                metadata=metadata,
                tags={DATA_VERSION_TAG: data_version},
            )])

        return dg.Definitions(sensors=[_gcs_obs])
