"""S3 Observation Sensor Component.

Polls an S3 bucket/prefix on a schedule and emits an AssetObservation tagged
with a DataVersion. Downstream AutomationCondition.newly_updated() / .eager()
fires when the DataVersion changes — i.e. when new objects have arrived or
existing ones have been replaced.

Two operating modes:

- resource_key set (declarative / demo-mode / shared client): sensor calls
  `context.resources.<resource_key>.observe(source)` where source is
  `"bucket/prefix"`. The resource returns a dict of the form
  `{"data_version": "...", **metadata}`.

- resource_key unset (real S3): sensor uses boto3 to paginate list_objects_v2,
  counts objects + tracks latest LastModified, and derives a DataVersion from
  `f"{object_count}-{latest_modified_iso}"`.
"""
from typing import Any, Optional

import dagster as dg
from dagster import AssetKey, AssetObservation, SensorEvaluationContext, SensorResult, sensor
from dagster._core.definitions.data_version import DATA_VERSION_TAG
from pydantic import Field


class S3ObservationSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    sensor_name: str = Field(description="Unique sensor name")
    asset_key: str = Field(description="Asset key of the external asset to observe")
    bucket_name: str = Field(description="S3 bucket name")
    prefix: str = Field(default="", description="Key prefix")
    region_name: Optional[str] = Field(default=None, description="AWS region")
    check_interval_seconds: int = Field(default=300, description="Seconds between health checks")
    resource_key: Optional[str] = Field(
        default=None,
        description=(
            "Dagster resource key exposing `.observe(source) -> dict` (source is "
            "'bucket/prefix') that returns `{'data_version': str, **metadata}`. "
            "When set, the sensor uses this instead of instantiating boto3 directly."
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
        def _s3_obs(context: SensorEvaluationContext, **_resources):
            # ── Resource-backed path (preferred) ────────────────────────────
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

            # ── Native boto3 path ───────────────────────────────────────────
            try:
                import boto3
            except ImportError:
                return SensorResult(skip_reason="boto3 not installed")
            try:
                s3 = boto3.client("s3", region_name=_self.region_name)
            except Exception as e:
                return SensorResult(skip_reason=f"Connect failed: {e}")

            count = 0
            total = 0
            latest = None
            try:
                for page in s3.get_paginator("list_objects_v2").paginate(
                    Bucket=_self.bucket_name, Prefix=_self.prefix
                ):
                    for o in page.get("Contents", []):
                        count += 1
                        total += o.get("Size", 0)
                        lm = o.get("LastModified")
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
                "bucket": _self.bucket_name,
                "prefix": _self.prefix,
            }
            return SensorResult(asset_events=[AssetObservation(
                asset_key=AssetKey.from_user_string(_self.asset_key),
                metadata=metadata,
                tags={DATA_VERSION_TAG: data_version},
            )])

        return dg.Definitions(sensors=[_s3_obs])
